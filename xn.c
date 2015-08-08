
/* TODO: this whole module ignores malloc failures. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <assert.h>

#include <ccan/list/list.h>
#include <ccan/htable/htable.h>
#include <ccan/hash/hash.h>
#include <ccan/darray/darray.h>

#include "xn.h"


#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))

#define CHUNK_SIZE (16 * 1024)


struct xn_rec;


struct xn_chunk {
	uint16_t next_pos;
	void *data;
};


struct xn_client
{
	int txnid;		/* 24 low bits */
	darray(struct xn_chunk) rec_chunks;	/* last is active */
};


struct xn_rec
{
	struct xn_item *item;
	uint16_t is_write:1, length:15;
	uint8_t data[];
};


/* an alteration. created during item modification, and added to a xn_item at
 * pre-commit.
 *
 * there are three kinds: lazy, eager, and smart. the first represents data
 * that'll be copied in at commit, the second is data that'll be copied back
 * on rollback, and the third is a piece of code that runs at commit/rollback
 * and always does the right thing. kind is indicated by content of ->len; if
 * it's positive, the change is lazy; negative for eager; and zero for smart.
 */
struct xn_alt
{
	int len, version;	/* version is new for lazy, old otherwise */
	union {
		struct {
			void (*fn)(void *priv);
			void *priv;
		} smart;
		uint8_t data[0];
	} u0;
};


struct xn_item {
	void *address;
	struct xn_alt *alt;
	int version;	/* txnid */
};


static pthread_key_t local_key;		/* <struct xn_client *> */
static struct htable item_hash;
static pthread_mutex_t item_hash_lock = PTHREAD_MUTEX_INITIALIZER;


static void destroy_xn_client(void *ptr);
static size_t rehash_xn_item(const void *item, void *priv);


static void mod_init_fn(void) {
	pthread_key_create(&local_key, &destroy_xn_client);
	htable_init(&item_hash, &rehash_xn_item, NULL);
}


static void destroy_xn_client(void *ptr)
{
	struct xn_client *c = ptr;
	for(int i=0; i < c->rec_chunks.size; i++) {
		free(c->rec_chunks.item[i].data);
	}
	darray_free(c->rec_chunks);
	free(c);
}


static struct xn_client *client_ctor(void)
{
	struct xn_client *c = malloc(sizeof(*c));
	*c = (struct xn_client){ /* zeroes */ };

	darray_init(c->rec_chunks);
	struct xn_chunk ck = { .data = malloc(CHUNK_SIZE) };
	darray_push(c->rec_chunks, ck);

	return c;
}


static struct xn_client *get_client(void) {
	struct xn_client *c = pthread_getspecific(local_key);
	if(c == NULL) {
		c = client_ctor();
		pthread_setspecific(local_key, c);
	}
	return c;
}


static size_t rehash_xn_item(const void *ptr, void *priv) {
	const struct xn_item *it = ptr;
	return hash_pointer(it->address, 0);
}


static struct xn_item *get_item(void *ptr)
{
	size_t hash = hash_pointer(ptr, 0);
	pthread_mutex_lock(&item_hash_lock);
	struct htable_iter it;
	struct xn_item *item;
	for(item = htable_firstval(&item_hash, &it, hash);
		item != NULL;
		item = htable_nextval(&item_hash, &it, hash))
	{
		if(item->address == ptr) break;
	}
	if(item == NULL) {
		item = malloc(sizeof(*item));
		if(item == NULL) {
			perror("malloc of xn_item");
			abort();
		}
		item->address = ptr;
		item->version = 0;
		bool ok = htable_add(&item_hash, hash, item);
		if(!ok) {
			fprintf(stderr, "htable_add of xn_item failed!\n");
			abort();
		}
	}
	pthread_mutex_unlock(&item_hash_lock);
	return item;
}


/* proof-of-concept QUALITY */
static int gen_txnid(void)
{
	static int next_txnid = 1;
	static pthread_mutex_t mx = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_lock(&mx);
	int ret = next_txnid++;
	pthread_mutex_unlock(&mx);
	return ret;
}


int xn_begin(void)
{
	static pthread_once_t mod_init = PTHREAD_ONCE_INIT;
	pthread_once(&mod_init, &mod_init_fn);

	struct xn_client *c = get_client();
	assert(c->txnid == 0);
	c->txnid = gen_txnid();

	return 0;
}


int xn_commit(void)
{
	struct xn_client *c = get_client();

	for(int i=1; i < c->rec_chunks.size; i++) {
		free(c->rec_chunks.item[i].data);
	}
	c->rec_chunks.size = 1;
	c->rec_chunks.item[0].next_pos = 0;

	return 0;
}


void xn_abort(int status)
{
	if(status == 0) return;

	/* otherwise, uhh, ... */
}


static struct xn_rec *new_xn_rec(struct xn_client *c, size_t n_bytes)
{
	assert(n_bytes < (1 << 15));
	assert(c->rec_chunks.size > 0);

	struct xn_chunk *ck = &c->rec_chunks.item[c->rec_chunks.size - 1];
	int max_seg = CHUNK_SIZE - ck->next_pos - sizeof(struct xn_rec);
	struct xn_rec *rec;
	if(n_bytes <= max_seg) {
		/* use the last chunk. */
		rec = ck->data + ck->next_pos;
		ck->next_pos += n_bytes + sizeof(struct xn_rec);
	} else {
		/* allocate a new chunk. */
		struct xn_chunk newck = {
			.data = malloc(CHUNK_SIZE),
			.next_pos = n_bytes + sizeof(struct xn_rec),
		};
		darray_push(c->rec_chunks, newck);
		rec = newck.data;
	}

	return rec;
}


int xn_read_int(int *iptr)
{
	struct xn_client *c = get_client();

	struct xn_item *it = get_item(iptr);
	int value, old_ver, new_ver = ACCESS_ONCE(it->version);
	do {
		old_ver = new_ver;
		value = atomic_load_explicit(iptr, memory_order_acquire);
	} while((new_ver = ACCESS_ONCE(it->version)) != old_ver);

	/* add (iptr, old_ver) to c's read set */
	struct xn_rec *rec = new_xn_rec(c, sizeof(int));
	rec->item = (struct xn_item *)it;
	rec->length = sizeof(int);
	rec->is_write = false;
	memcpy(rec->data, &value, sizeof(int));

	return value;
}


void xn_put(int *iptr, int value)
{
	*iptr = value;
}
