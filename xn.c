
/* TODO: this whole module ignores malloc failures. */

#define __USE_XOPEN

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include <ccan/list/list.h>
#include <ccan/htable/htable.h>
#include <ccan/hash/hash.h>
#include <ccan/darray/darray.h>

#include "xn.h"


/* TODO: remove this in favour of C11 atomic loads */
#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))

#define MSB(x) (sizeof((x)) * 8 - __builtin_clzl((x)) - 1)

#define CHUNK_SIZE (12 * 1024)
#define BF_SIZE (sizeof(uintptr_t) * 8 * 4)

#define ITEM_WRITE_BIT (1 << 24)

#define ALIGN_TO_CACHELINE __attribute__((aligned(64)))


struct xn_chunk {
	uint16_t next_pos;
	void *data;
};


struct xn_item {
	void *address;
	uint32_t version;	/* 24: write bit, 23..0: version (txnid) */
};


struct xn_rec
{
	struct xn_item *item;
	int version;
	uint16_t is_write:1, length:15;
	uint8_t data[];
};


/* destructor closure inna list. */
struct xn_dtor {
	struct list_node link;		/* in xn_txn.dtors */
	void (*dtor_fn)(void *param);
	void *param;
};


/* transaction record. immutable after publication except for ->next which
 * can be zeroed when the list is trimmed. removed per the epoch mechanism via
 * rotation of txn_list[] thru txn_epoch.
 */
struct xn_txn
{
	struct xn_txn *_Atomic next;		/* NULL for last */
	int txnid, commit_id;
	/* read_set has been compressed to write_set[]'s format, and contains all
	 * of write_set[].
	 */
	uintptr_t read_set[2], write_set[2];
	struct list_head dtors;			/* <struct xn_dtor>, malloc'd */
};


struct xn_client
{
	/* the in-progress transaction. its txnid and dtors fields are filled in
	 * during the transaction's execution, and it is destroyed or published by
	 * xn_abort() or xn_commit() respectively.
	 */
	struct xn_txn *txn;

	darray(struct xn_chunk) rec_chunks;	/* last is active */
	bool snapshot_valid;	/* early abort criteria */

	/* bloom filters track the client's read and write sets. the read set has
	 * two count bits per slot with an invertible uint16_t index for each; the
	 * write set has a single bit per slot. this means 4 native words for the
	 * read set, 2 for the write set, and 64-or-128 slots.
	 *
	 * each member of rs_words[] has the following format: high 8 bits
	 * indicate chunk index, and low 8 are bits 11..4 of the in-chunk index
	 * (aligned to 16).
	 */
	uintptr_t read_set[4], write_set[2];
	uint16_t rs_words[BF_SIZE];

	/* data subject to concurrent access by other threads. */
	_Atomic uint32_t epoch ALIGN_TO_CACHELINE;
	struct list_node link;	/* in client_list, under client_list_lock */
};


static pthread_key_t local_key;		/* <struct xn_client *> */
static uint32_t bloom_salt;

/* the previous transactions' list. cleared with the epoch method, where ticks
 * happen when a (selected) beginning transaction notes that all active
 * clients have seen txn_epoch.
 *
 * FIXME: txn_epoch doesn't handle roll-over. this is unfortunate, but
 * gen_txnid() and the txnid comparisons also don't handle roll-over, and
 * that'll always happen sooner -- so it doesn't matter for now.
 *
 * TODO: unfortunately this requires all client threads to be active at least
 * once before a tick can happen, and twice to clear a backlog caused by e.g.
 * a long absence. to solve this, add a "txnids under X are dead" rule, so
 * that a single client's extended inactivity will only cause its transaction
 * to fail instead of keeping useless memory around indefinitely.
 *
 * note that the proposed algorithm doesn't involve ticking the epoch forward
 * despite a client not having observed it. that could be done if each client
 * set a flag saying that the epoch must not tick, in addition to its observed
 * epoch; this'd defer epoch-tick processing.
 */
static _Atomic uint32_t txn_epoch = 2;
static volatile atomic_flag epoch_lock;
/* [txn_epoch + 1 mod 4] = NULL
 * [txn_epoch     mod 4] = committed txns (r/w by xn_commit());
 * [txn_epoch - 1 mod 4] = pre-dead txns (r by xn_commit());
 * [txn_epoch - 2 mod 4] = dead txns
 *
 * bumping the epoch requires that the next slot has been cleared.
 */
static struct xn_txn *_Atomic txn_list[4];

static struct list_head client_list = LIST_HEAD_INIT(client_list);
static pthread_rwlock_t client_list_lock = PTHREAD_RWLOCK_INITIALIZER;
static _Atomic int client_count = 0;

static struct htable item_hash;
static pthread_mutex_t item_hash_lock = PTHREAD_MUTEX_INITIALIZER;


static void destroy_xn_client(void *ptr);
static size_t rehash_xn_item(const void *item, void *priv);
static void finish_txn(struct xn_txn *txn);


static inline int size_to_shift(size_t size) {
	int msb = MSB(size);
	return (1 << msb) < size ? msb + 1 : msb;
}


static void mod_init_fn(void)
{
	pthread_key_create(&local_key, &destroy_xn_client);
	htable_init(&item_hash, &rehash_xn_item, NULL);
	bloom_salt = 0x1234abcd;	/* TODO: generate a random number */
	for(int i=0; i < 4; i++) txn_list[i] = NULL;
	atomic_flag_clear(&epoch_lock);
}


static void destroy_xn_client(void *ptr)
{
	struct xn_client *c = ptr;
	atomic_fetch_sub_explicit(&client_count, 1, memory_order_relaxed);
	pthread_rwlock_wrlock(&client_list_lock);
	list_del_from(&client_list, &c->link);
	pthread_rwlock_unlock(&client_list_lock);
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

	atomic_fetch_add_explicit(&client_count, 1, memory_order_relaxed);
	pthread_rwlock_wrlock(&client_list_lock);
	list_add(&client_list, &c->link);
	pthread_rwlock_unlock(&client_list_lock);

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


/* move the epoch lists over and destroy the old dead-list's contents. */
static void tick_txn_epoch(void)
{
	/* get the lock or go away. */
	if(atomic_flag_test_and_set(&epoch_lock)) {
		/* true = was already locked */
		return;
	}

	/* take the dead-list, prepare the next tick's new dead-list, and bump the
	 * epoch number.
	 */
	uint32_t old_epoch = atomic_load(&txn_epoch);
	struct xn_txn *dead = atomic_exchange(
		&txn_list[(old_epoch - 2) & 3], NULL);
	assert(txn_list[(old_epoch + 1) & 3] == NULL);
	atomic_fetch_add_explicit(&txn_epoch, 1, memory_order_release);
	atomic_flag_clear_explicit(&epoch_lock, memory_order_release);

	for(struct xn_txn *cur = dead, *next; cur != NULL; cur = next) {
		next = cur->next;
		finish_txn(cur);
	}
}


static void check_txn_epoch(uint32_t cur_epoch)
{
	bool all_seen = true;
	pthread_rwlock_rdlock(&client_list_lock);
	struct xn_client *c;
	list_for_each(&client_list, c, link) {
		if(atomic_load_explicit(&c->epoch,
			memory_order_relaxed) != cur_epoch)
		{
			all_seen = false;
			break;
		}
	}
	pthread_rwlock_unlock(&client_list_lock);
	if(all_seen) tick_txn_epoch();
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
	assert(c->txn == NULL);
	uint32_t cur_epoch;
	atomic_store_explicit(&c->epoch,
		cur_epoch = atomic_load_explicit(&txn_epoch, memory_order_relaxed),
		memory_order_relaxed);
	c->snapshot_valid = true;
	c->txn = malloc(sizeof(struct xn_txn));
	c->txn->txnid = gen_txnid();
	list_head_init(&c->txn->dtors);

	for(int i=0; i < 4; i++) c->read_set[i] = 0;
	for(int i=0; i < 2; i++) c->write_set[i] = 0;
	/* no need to clear the actual slots. */

	/* see if the epoch scheme needs a spin, which it does if txnid + 1 has N
	 * lowest bits cleared, where 2^N >= 2 * client_count .
	 */
	int mask = (1 << size_to_shift(2 * atomic_load_explicit(&client_count,
		memory_order_relaxed))) - 1;
	if(((c->txn->txnid + 1) & mask) == 0) check_txn_epoch(cur_epoch);

	return 0;
}


static void clean_client(struct xn_client *c)
{
	if(c->txn != NULL) {
		struct xn_dtor *cur, *next;
		list_for_each_safe(&c->txn->dtors, cur, next, link) {
			list_del_from(&c->txn->dtors, &cur->link);
			free(cur);
		}
		free(c->txn); c->txn = NULL;
	}

	for(int i=1; i < c->rec_chunks.size; i++) {
		free(c->rec_chunks.item[i].data);
	}
	c->rec_chunks.size = 1;
	c->rec_chunks.item[0].next_pos = 0;
}


/* squash src[4] to dst[2] by compressing 2-bit counts to 1-bit, with
 * saturation.
 *
 * it's almost certainly better to just store the read set in split fields,
 * than doing 6 copy-shift-ors per input word, or 24 total. a running
 * accumulator can usually fit under memory loads and/or stores.
 */
static void compress_read_set(uintptr_t *dst, const uintptr_t *src)
{
	/* byte-sized broadcast, to make mask generation easier. */
	const uintptr_t bcast = sizeof(uintptr_t) == 4
		? 0x01010101ul : 0x0101010101010101ul;
	const uintptr_t wmask = sizeof(uintptr_t) == 4
		? 0x00ff00fful : 0x00ff00ff00ff00fful;
	/* bit magic time! */
	for(int i=0; i < 2; i++) {
		uintptr_t x = src[i*2 + 0], y = src[i*2 + 1];
		x |= x >> 1;	/* squash counts. */
		y |= y >> 1;
		x &= bcast * 0x55; y &= bcast * 0x55;
		x |= x >> 1; y |= y >> 1;	/* 0x0x -> 00xx */
		x &= bcast * 0x33; y &= bcast * 0x33;
		x |= x >> 2; y |= y >> 2;	/* 00xx00xx -> 0000xxxx */
		x &= bcast * 0x0f; y &= bcast * 0x0f;
		x |= x >> 4; y |= y >> 4;	/* 8 bits each */
		x &= wmask; y &= wmask;
		x |= x >> 8; y |= y >> 8;	/* 16 bits each. */
#if __WORDSIZE == 64
		const uintptr_t dmask = 0x0000ffff0000fffful;
		x &= dmask; y &= dmask;
		x |= x >> 16; y |= y >> 16;
		dst[i] = (x & 0xfffffffful) | (y << 32);
#else
#error "not yet done for 32-bit"
#endif
	}

#ifndef NDEBUG
	for(int i=0; i < 4; i++) {
		for(int j=0; j < __WORDSIZE / 2; j++) {
			bool d_set = (dst[i / 2] & (1ul << ((i & 1) * 32 + j))) != 0,
				s_set = (src[i] & (0x3ul << j * 2)) != 0;
			if(d_set != s_set) {
				printf("dst[%d]=%#lx, src[%d]=%#lx (i=%d, j=%d)\n",
					i / 2, dst[i / 2], i, src[i], i, j);
				printf("... d_set=%d, s_set=%d\n", (int)d_set, (int)s_set);
			}
			assert(d_set == s_set);
		}
	}
#endif
}


int xn_commit(void)
{
	int rc, comm_id = gen_txnid();
	darray(struct xn_rec *) w_list = darray_new();
	struct xn_txn *txn = NULL;

	struct xn_client *client = get_client();
	if(!client->snapshot_valid) goto serfail;

	txn = client->txn;
	client->txn = NULL;
	txn->commit_id = comm_id;
	memcpy(txn->write_set, client->write_set, sizeof(txn->write_set));
	compress_read_set(txn->read_set, client->read_set);
	uint32_t epoch = atomic_load_explicit(&client->epoch,
		memory_order_consume);
	struct xn_txn *old_txns[2] = {
		atomic_load_explicit(&txn_list[epoch & 3], memory_order_relaxed),
		atomic_load_explicit(&txn_list[(epoch - 1) & 3], memory_order_relaxed),
	};
	/* the bloom-filter intersection test for read-to-write dependency. */
	for(int lst = 0; lst < 2; lst++) {
		for(struct xn_txn *cur = old_txns[lst];
			cur != NULL;
			cur = atomic_load_explicit(&cur->next, memory_order_consume))
		{
			if(cur->commit_id < txn->txnid) continue;
			bool w_match = false, r_match = false;
			for(int i=0; i < 2; i++) {
				w_match |= (cur->write_set[i] & txn->read_set[i]) != 0;
				r_match |= (cur->read_set[i] & txn->write_set[i]) != 0;
			}
			if(r_match && w_match) goto serfail;	/* boom! */
		}
	}

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
			assert((rec->version & ITEM_WRITE_BIT) == 0);
			if(v_seen != rec->version) goto serfail;
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

	/* FIXME: this should insert txn into txn_list in order of descending
	 * commit ID so that the query loop can early-exit.
	 */
	txn->next = atomic_load_explicit(&txn_list[epoch & 3], memory_order_consume);
	while(!atomic_compare_exchange_weak_explicit(&txn_list[epoch & 3],
		&txn->next, txn, memory_order_release, memory_order_consume))
	{
		/* sit & spin */
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
	free(txn);
	rc = -EDEADLK;
	goto end;
}


static void finish_txn(struct xn_txn *txn)
{
#ifndef NDEBUG
	for(int i=0; i < 2; i++) {
		txn->read_set[i] = txn->write_set[i] = ~0ull;
	}
#endif

	struct xn_dtor *cur, *next;
	list_for_each_safe(&txn->dtors, cur, next, link) {
		list_del_from(&txn->dtors, &cur->link);
		(*cur->dtor_fn)(cur->param);
		free(cur);
	}

	free(txn);
}


void xn_abort(int status)
{
	if(status == 0) return;

	/* otherwise, uhh, ... */
	struct xn_client *c = get_client();

	clean_client(c);
}


void xn_dtor(void (*fn)(void *param), void *param)
{
	struct xn_client *c = get_client();
	struct xn_dtor *d = malloc(sizeof(*d));
	d->dtor_fn = fn;
	d->param = param;
	list_add_tail(&c->txn->dtors, &d->link);
}


void xn_free(void *ptr) {
	xn_dtor(&free, ptr);
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
	return hash_pointer(ptr, bloom_salt + i * 7);
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
	if(rec->version > c->txn->txnid) c->snapshot_valid = false;
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
		if(rec->version > c->txn->txnid) c->snapshot_valid = false;
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
