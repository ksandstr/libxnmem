
/* TODO: this whole module ignores malloc failures. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include <ccan/list/list.h>
#include <ccan/htable/htable.h>
#include <ccan/hash/hash.h>
#include <ccan/darray/darray.h>

#include "xn.h"


#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))

#define CHUNK_SIZE (12 * 1024)
#define BF_SIZE (sizeof(uintptr_t) * 8 * 4)

#define ITEM_WRITE_BIT (1 << 24)


struct xn_rec;


struct xn_chunk {
	uint16_t next_pos;
	void *data;
};


struct xn_client
{
	int txnid;		/* 24 low bits */
	darray(struct xn_chunk) rec_chunks;	/* last is active */
	bool snapshot_valid;	/* early abort criteria */

	size_t salt;	/* for bloom-filter hashing */

	/* bloom filters to track the client's read and write sets. the read set
	 * has two count bits per slot with an invertible uint16_t index for each;
	 * the write set has a single bit per slot. this means 4 native words for
	 * the read set, 2 for the write set, and 64-or-128 slots.
	 *
	 * each member of rs_words[] has the following format: high 8 bits
	 * indicate chunk index, and low 8 are bits 11..4 of the in-chunk index
	 * (aligned to 16).
	 */
	uintptr_t read_set[4], write_set[2];
	uint16_t rs_words[BF_SIZE];
};


struct xn_rec
{
	struct xn_item *item;
	int version;
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
	uint32_t version;	/* 24: write bit, 23..0: version (txnid) */
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
	/* FIXME: generate a randomized salt instead */
	c->salt = 0x1234abcd ^ (uintptr_t)&c ^ (uintptr_t)&c->salt;

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


/* find item for @ptr in the global table, or create and add it. to be used
 * only after find_item_rec() returns NULL.
 */
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


/* TODO: this should handle roll-over. currently it does not. */
static int gen_txnid(void)
{
	static int next_txnid = 1;
	return atomic_fetch_add_explicit(&next_txnid, 1, memory_order_relaxed);
}


int xn_begin(void)
{
	static pthread_once_t mod_init = PTHREAD_ONCE_INIT;
	pthread_once(&mod_init, &mod_init_fn);

	struct xn_client *c = get_client();
	assert(c->txnid == 0);
	c->txnid = gen_txnid();
	c->snapshot_valid = true;

	for(int i=0; i < 4; i++) c->read_set[i] = 0;
	for(int i=0; i < 2; i++) c->write_set[i] = 0;
	/* no need to clear the actual slots. */

	return 0;
}


static void clean_client(struct xn_client *c)
{
	for(int i=1; i < c->rec_chunks.size; i++) {
		free(c->rec_chunks.item[i].data);
	}
	c->rec_chunks.size = 1;
	c->rec_chunks.item[0].next_pos = 0;

	c->txnid = 0;
}


int xn_commit(void)
{
	int rc, comm_id = gen_txnid();
	darray(struct xn_rec *) w_list = darray_new();
	struct xn_client *client = get_client();
	if(!client->snapshot_valid) goto serfail;

	/* the "records haven't changed, or fail" model. collects locked items'
	 * recs in w_list.
	 */
	for(int c = 0; c < client->rec_chunks.size; c++) {
		struct xn_chunk *ck = &client->rec_chunks.item[c];
		size_t pos = 0;
		while(pos < ck->next_pos) {
			struct xn_rec *rec = ck->data + pos;
			pos += (sizeof(struct xn_rec) + rec->length + 15) & ~15;

			int v_seen = ACCESS_ONCE(rec->item->version);
			if((v_seen & 0xffffff) != rec->version) goto serfail;
			if((v_seen & ITEM_WRITE_BIT) != 0) goto serfail;
			if(rec->is_write) {
				if(!atomic_compare_exchange_strong_explicit(
					&rec->item->version, &v_seen, comm_id | ITEM_WRITE_BIT,
					memory_order_relaxed, memory_order_relaxed))
				{
					/* changed between read and write. */
					goto serfail;
				}
				darray_push(w_list, rec);
			}
		}
	}

	if(w_list.size > 0) {
		atomic_thread_fence(memory_order_acquire);
		/* hooray, let's committing! */
		for(int i=0; i < w_list.size; i++) {
			struct xn_rec *rec = w_list.item[i];
			memcpy(rec->item->address, rec->data, rec->length);
		}
		atomic_thread_fence(memory_order_release);
		/* unlock in relaxed order. */
		for(int i=0; i < w_list.size; i++) {
			struct xn_rec *rec = w_list.item[i];
			atomic_store_explicit(&rec->item->version, comm_id,
				memory_order_relaxed);
		}
	}

	rc = 0;

end:
	clean_client(client);
	darray_free(w_list);
	return rc;

serfail:
	/* drop write locks. */
	for(int i=0; i < w_list.size; i++) {
		struct xn_rec *rec = w_list.item[i];
		atomic_fetch_and_explicit(&rec->item->version,
			~ITEM_WRITE_BIT, memory_order_relaxed);
	}
	rc = -EDEADLK;
	goto end;
}


void xn_abort(int status)
{
	if(status == 0) return;

	/* otherwise, uhh, ... */
	struct xn_client *c = get_client();

	clean_client(c);
}


/* NOTE: caller must fill ret->length in */
static struct xn_rec *new_xn_rec(
	struct xn_client *c,
	uint16_t *idx_p,
	size_t n_bytes)
{
	assert(n_bytes < (1 << 15));
	assert(c->rec_chunks.size > 0);

	n_bytes = ((n_bytes + sizeof(struct xn_rec) + 15) & ~15)
		- sizeof(struct xn_rec);
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
		ck = &c->rec_chunks.item[c->rec_chunks.size - 1];
	}
	assert((ck->next_pos & 0xf) == 0);

	*idx_p = (c->rec_chunks.size - 1) << 8 | ((void *)rec - ck->data) >> 4;
	return rec;
}


static inline size_t bf_hash(struct xn_client *c, void *ptr, int i) {
	return hash_pointer(ptr, c->salt + i * 7);
}


static inline bool is_valid_index(struct xn_client *c, int ix)
{
	int chunk = ix >> 8, pos = (ix & 0xff) << 4;
	return chunk < c->rec_chunks.size
		&& pos < c->rec_chunks.item[chunk].next_pos;
}


static inline struct xn_rec *index_to_rec(struct xn_client *c, int ix)
{
	assert(is_valid_index(c, ix));
	int chunk = ix >> 8, pos = (ix & 0xff) << 4;
	return c->rec_chunks.item[chunk].data + pos;
}


static inline void probe_pos(int *slot, int *limb, int *ix, size_t hash)
{
	/* limb is the word index, and ix is the low bit's offset. */
	const int shift = sizeof(uintptr_t) > 4 ? 6 : 5,
		mask = (1 << shift) - 1;

	*slot = hash & (BF_SIZE - 1);
	hash *= 2;
	*limb = (hash >> shift) & 0x3;
	*ix = hash & mask;
}


/* probes the read set to try and find an existing xn_rec directly, without
 * going through the global item hash.
 */
static struct xn_rec *bf_probe(struct xn_client *c, void *addr, bool *ambig_p)
{
	*ambig_p = false;
	int count[3], val[3];
	struct xn_rec *rec[3];
	for(int i=0; i < 3; i++) {
		size_t hash = bf_hash(c, addr, i);
		int limb, ix, slot;
		probe_pos(&slot, &limb, &ix, hash);
		count[i] = (c->read_set[limb] >> ix) & 0x3;
		val[i] = c->rs_words[slot];

		switch(count[i]) {
			case 0: return NULL;		/* strongly absent. */
			case 1:
				/* absent if invalid, or wrong item. */
				if(!is_valid_index(c, val[i])) return NULL;
				rec[i] = index_to_rec(c, val[i]);
				if(rec[i]->item->address != addr) return NULL;
				break;
			case 2:
				/* TODO: store val[i] ^= val[0], if possible */
				/* (in the mean time, FALL THRU) */
			case 3:
				/* uncertain. */
				rec[i] = NULL;
				break;
		}
	}

	/* pick the median. */
	struct xn_rec *ret = NULL;
	if(rec[0] == rec[1] || rec[0] == rec[2]) ret = rec[0];
	if(ret == NULL || rec[1] == rec[2]) ret = rec[1];
	if(ret == NULL) {
		/* no quorum. */
		*ambig_p = true;
		return NULL;
	}

	assert(ret->item->address == addr);
	return ret;
}


static void bf_insert(struct xn_client *c, void *addr, uint16_t rec_index)
{
	for(int i=0; i < 3; i++) {
		size_t hash = bf_hash(c, addr, i);
		int limb, ix, slot;
		probe_pos(&slot, &limb, &ix, hash);
		/* brute-force saturating increment. */
		uintptr_t oldval = c->read_set[limb] & (0x3ul << ix);
		if(oldval == 0) c->rs_words[slot] = 0;	/* lazy cleaning */
		c->rs_words[slot] ^= rec_index;
		if((~c->read_set[limb] & (0x3ul << ix)) != 0) {
			c->read_set[limb] += 1ul << ix;
		}
#if 0
		/* fancy two-bit saturating increment.
		 * 00 -> 01, 01 -> 10, 10 -> 11, 11 -> 11;
		 * so for H, L -> H|L, H|~L
		 */
		int a = c->read_set[limb] & (0x2 << (ix * 2)),
			b = c->read_set[limb] & (0x1 << (ix * 2));
		int h = a | (b << 1), l = (a >> 1) | (b ^ (1 << ix * 2));
		/* FAJSLKFJALKSFJLKSAJFKSAJFLA: this isn't actually any faster than a
		 * mask-test, jump, and add. if there were multiple items to be
		 * incremented, maybe.
		 */
#endif
		assert((c->read_set[limb] & (0x3ul << ix)) != 0);
		assert(oldval < (c->read_set[limb] & (0x3ul << ix))
			|| (oldval >> ix) == 0x3);
	}
}


static struct xn_rec *find_xn_rec(struct xn_client *client, void *ptr)
{
	for(int c = 0; c < client->rec_chunks.size; c++) {
		struct xn_chunk *ck = &client->rec_chunks.item[c];
		size_t pos = 0;
		while(pos < ck->next_pos) {
			struct xn_rec *rec = ck->data + pos;
			pos += (sizeof(struct xn_rec) + rec->length + 15) & ~15;
			if(rec->item->address == ptr) return rec;
		}
	}

	return NULL;
}


static struct xn_rec *find_item_rec(struct xn_client *c, void *ptr)
{
	bool ambiguous = false;
	struct xn_rec *rec = bf_probe(c, ptr, &ambiguous);
	if(ambiguous) {
		assert(rec == NULL);
		rec = find_xn_rec(c, ptr);
	}
	return rec;
}


int xn_read_int(int *iptr)
{
	struct xn_client *c = get_client();
	struct xn_rec *rec = find_item_rec(c, iptr);
	if(rec != NULL) {
		int val;
		memcpy(&val, rec->data, sizeof(int));
		return val;
	}

	struct xn_item *it = get_item(iptr);
	int value, old_ver, new_ver;
	do {
		while(((new_ver = ACCESS_ONCE(it->version)) & ITEM_WRITE_BIT) != 0) {
			/* sit & spin */
		}
		do {
			if((new_ver & ITEM_WRITE_BIT) != 0) break;
			old_ver = new_ver;
			value = atomic_load_explicit(iptr, memory_order_acquire);
		} while((new_ver = ACCESS_ONCE(it->version)) != old_ver);
	} while((new_ver & ITEM_WRITE_BIT) != 0);

	/* make new record. */
	uint16_t idx;
	rec = new_xn_rec(c, &idx, sizeof(int));
	rec->item = (struct xn_item *)it;
	rec->length = sizeof(int);
	rec->is_write = false;
	rec->version = old_ver;
	if(rec->version > c->txnid) c->snapshot_valid = false;
	memcpy(rec->data, &value, sizeof(int));
	bf_insert(c, iptr, idx);
	assert(find_item_rec(c, iptr) == rec);

	return value;
}


static void *xn_modify(void *ptr, size_t length)
{
	struct xn_client *c = get_client();
	struct xn_rec *rec = find_item_rec(c, ptr);
	if(rec != NULL) {
		if(rec->length < length) {
			fprintf(stderr, "length conflict on %p\n", ptr);
			abort();
		}
	} else {
		uint16_t rec_idx;
		rec = new_xn_rec(c, &rec_idx, length);
		rec->item = get_item(ptr);
		rec->length = length;
		rec->is_write = false;
		/* (not caring about concurrent writes! if one was in progress, this
		 * transaction will be aborted anyway.)
		 */
		rec->version = ACCESS_ONCE(rec->item->version) & 0xffffff;
		if(rec->version > c->txnid) c->snapshot_valid = false;
		bf_insert(c, ptr, rec_idx);
	}

	if(!rec->is_write) {
		rec->is_write = true;
		/* add to write set.
		 * note: [v1] this could recycle hashes computed for bf_insert().
		 */
		for(int i=0; i < 3; i++) {
			uint32_t h = bf_hash(c, ptr, i);
			int limb = (h >> (sizeof(uintptr_t) > 4 ? 6 : 5)) & 0x1,
				bit = h & (sizeof(uintptr_t) > 4 ? 0x3f : 0x1f);
			c->write_set[limb] |= 1ul << bit;
		}
	}

	return rec->data;
}


void xn_put(int *iptr, int value)
{
	int *p = xn_modify(iptr, sizeof(*iptr));
	*p = value;
}
