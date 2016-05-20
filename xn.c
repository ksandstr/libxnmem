
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
#include <sched.h>
#include <semaphore.h>
#include <assert.h>

#include <ccan/list/list.h>
#include <ccan/htable/htable.h>
#include <ccan/hash/hash.h>
#include <ccan/darray/darray.h>

#include "xn.h"


#define MSB(x) (sizeof((x)) * 8 - __builtin_clzl((x)) - 1)
#define ALIGN_TO_CACHELINE __attribute__((aligned(64)))

#define ITEM_WRITE_BIT (1 << 24)	/* in xn_item.version */
#define CHUNK_SIZE (12 * 1024)		/* bytes under xn_chunk.data */

/* bloom filter configuration. larger filters are more accurate, but cause
 * more processing overhead. must be at least 6, and 8 is usually the sweet
 * spot.
 */
#define BF_SIZE_LOG2 8
#define BF_NUM_SLOTS (1 << BF_SIZE_LOG2)
#define BF_NUM_WORDS (BF_NUM_SLOTS / (sizeof(uintptr_t) * 8))

#define TXNID_SUSPECT (1 << 24)
#define TXNID_KILLED (1 << 25)
#define TXNID_COMMIT (1 << 26)


struct xn_item {
	void *address;
	_Atomic uint32_t version;	/* 24: writelock bit, 23..0: version (txnid) */
};


struct xn_rec
{
	struct xn_item *item;
	int version;
	uint16_t is_write:1, length:15;
	uint8_t data[];
};


/* destructor closure. */
struct xn_dtor {
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
	/* read_set[] contains the count's lower bits side by side. it has the
	 * exact same format as write_set[] once published.
	 */
	uintptr_t read_set[BF_NUM_WORDS], write_set[BF_NUM_WORDS];
	darray(struct xn_dtor) dtors;
};


/* a wait record for a transaction that ended with xn_retry(). */
struct xn_wait {
	struct xn_wait *_Atomic next;	/* NULL for last */
	int txnid;
	uintptr_t read_set[BF_NUM_WORDS];
	sem_t sem;
};


/* transaction log chunk in xn_client */
struct xn_chunk {
	uint16_t next_pos;
	void *data;
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
	 * two count bits per slot (split so that the low bit is in
	 * txn->read_set[], and the high bit here) with an invertible uint16_t
	 * index for each; the write set has a single bit per slot.
	 *
	 * each item in rs_words[] has the following format: high 8 bits indicate
	 * chunk index, and low 8 are bits 11..4 of the in-chunk index (implied
	 * zeroes in the lowest 4).
	 */
	uintptr_t read_set_hi[BF_NUM_WORDS];
	uint16_t rs_words[BF_NUM_SLOTS];

	/* data subject to concurrent access by other threads. */
	_Atomic uint32_t epoch ALIGN_TO_CACHELINE;
	struct list_node link;	/* in client_list, under client_list_lock */
	/* while the transaction is unpublished, txnid's high 8 bits are assigned
	 * thus:
	 *   24: "suspect" bit. set if the transaction has been open for
	 *       so long as to prevent epoch ticks.
	 *   25: "killed" bit. set if the suspect bit would've been set twice.
	 *       xn_commit() will abort the transaction with -ETIMEDOUT.
	 *   26: "commit" bit. when set, "suspect" and "killed" won't be.
	 *
	 * at publication, txnid is copied into txn->txnid with high 8 bits set to
	 * zero.
	 */
	_Atomic int txnid;
};


static pthread_key_t local_key;		/* <struct xn_client *> */
static uint32_t bloom_salt;

/* the previous transactions' list. cleared with the epoch method, where ticks
 * happen when a (selected) beginning transaction notes that all active
 * clients have seen txn_epoch. this also drives the destructor mechanism.
 *
 * FIXME: txn_epoch doesn't handle roll-over. this is unfortunate, but
 * gen_txnid() and the txnid comparisons also don't handle roll-over, and
 * that'll always happen sooner -- so it doesn't matter for now.
 */
static _Atomic uint32_t txn_epoch = 2;
static volatile atomic_flag epoch_lock;
/* [txn_epoch + 1 mod 4] = NULL
 * [txn_epoch     mod 4] = committed txns (r/w in xn_commit());
 * [txn_epoch - 1 mod 4] = pre-dead txns (r in xn_commit() old epoch);
 * [txn_epoch - 2 mod 4] = dead txns
 *
 * therefore slot +0 is the live txn list, and -1, -2, and +1 are the epoch
 * deadlists.
 */
static struct xn_txn *_Atomic txn_list[4];

/* xn_retry()'d transactions.
 *
 * TODO: these should also be subject to epoch reclamation via spurious
 * wakeups.
 */
static struct xn_wait *_Atomic wait_list = NULL;

static struct list_head client_list = LIST_HEAD_INIT(client_list);
static pthread_rwlock_t client_list_lock = PTHREAD_RWLOCK_INITIALIZER;
static _Atomic int client_count = 0;

static struct htable item_hash;
static pthread_mutex_t item_hash_lock = PTHREAD_MUTEX_INITIALIZER;


static void destroy_xn_client(void *ptr);
static size_t rehash_xn_item(const void *item, void *priv);
static void finish_txn(struct xn_txn *txn);
/* (TODO: rename to clean_txn()) */
static void clean_client(struct xn_client *c);


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
	atomic_flag_clear_explicit(&epoch_lock, memory_order_release);
}


static void destroy_xn_client(void *ptr)
{
	struct xn_client *c = ptr;
	atomic_fetch_sub_explicit(&client_count, 1, memory_order_relaxed);
	pthread_rwlock_wrlock(&client_list_lock);
	list_del_from(&client_list, &c->link);
	pthread_rwlock_unlock(&client_list_lock);

	clean_client(c);
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
static void tick_txn_epoch(uint32_t old_epoch)
{
	assert((old_epoch & 0x80000000) == 0);

	/* get the lock or go away. */
	if(atomic_flag_test_and_set(&epoch_lock)) {
		/* true = was already locked */
		return;
	}

	/* try to bump the epoch number, because another epoch-bumper may have
	 * done its thing right before our test-and-set. on failure, back down.
	 */
	uint32_t new_epoch = old_epoch + 1;
	if((new_epoch & 0x80000000) != 0) new_epoch = 2;
	if(!atomic_compare_exchange_strong_explicit(&txn_epoch,
		&old_epoch, new_epoch, memory_order_relaxed,
		memory_order_relaxed))
	{
		atomic_flag_clear_explicit(&epoch_lock, memory_order_relaxed);
		return;
	}

	/* take the dead-list, prepare the next tick's new dead-list, and unlock
	 * the epoch-tick mechanism.
	 */
	struct xn_txn *dead = atomic_exchange_explicit(
		&txn_list[(old_epoch - 2) & 3], NULL, memory_order_relaxed);
	atomic_flag_clear_explicit(&epoch_lock, memory_order_release);

	for(struct xn_txn *cur = dead, *next; cur != NULL; cur = next) {
		next = cur->next;
		finish_txn(cur);
	}
}


/* try to kill a suspiciously long open transaction. returns true if the
 * transaction is now dead.
 *
 * TODO: the algorithm between this function and check_txn_epoch() effectively
 * kills transactions even when doing so doesn't result in an epoch tick.
 * for now, it starts killing eagerly and then switches to not-killing if it
 * sees one client that wasn't suspect already.
 */
static bool try_kill_txn(struct xn_client *c, bool *active_seen_p)
{
	bool dead, avoid;
	int new_txnid, old_txnid = atomic_load_explicit(
		&c->txnid, memory_order_relaxed);
	do {
		new_txnid = old_txnid;
		avoid = *active_seen_p;
		if((old_txnid & TXNID_COMMIT) != 0) {
			/* can't tick or kill: a client is in the middle of
			 * xn_commit().
			 */
			dead = false;
		} else if((old_txnid & TXNID_KILLED) != 0) {
			dead = true;
		} else if((old_txnid & TXNID_SUSPECT) != 0) {
			if(avoid) {
				/* avoidance mode. */
				dead = false;
				break;
			} else {
				new_txnid |= TXNID_KILLED;
				new_txnid &= ~TXNID_SUSPECT;
				dead = true;
			}
		} else {
			/* enter avoidance mode. */
			avoid = true;
			new_txnid |= TXNID_SUSPECT;
			dead = false;
		}
		if(old_txnid == new_txnid) break;
	} while(!atomic_compare_exchange_weak_explicit(&c->txnid,
		&old_txnid, new_txnid, memory_order_release, memory_order_relaxed));

	*active_seen_p = avoid;
	return dead;
}


static void check_txn_epoch(uint32_t cur_epoch)
{
	bool all_seen = true, active_seen = false;
	pthread_rwlock_rdlock(&client_list_lock);
	struct xn_client *c;
	list_for_each(&client_list, c, link) {
		uint32_t c_epoch = atomic_load_explicit(&c->epoch,
			memory_order_consume);
		if((c_epoch & 0x80000000) != 0) continue;
		else if(c_epoch != cur_epoch) {
			if(try_kill_txn(c, &active_seen)) continue;
			all_seen = false;
		}
	}
	pthread_rwlock_unlock(&client_list_lock);
	if(all_seen) tick_txn_epoch(cur_epoch);
}


/* TODO: this should handle roll-over. currently it does not. */
static int gen_txnid(void)
{
	static _Atomic int next_txnid = 1;
	return atomic_fetch_add_explicit(&next_txnid, 1, memory_order_relaxed);
}


int xn_begin(void)
{
	static pthread_once_t mod_init = PTHREAD_ONCE_INIT;
	pthread_once(&mod_init, &mod_init_fn);

	struct xn_client *c = get_client();
	assert(c->txn == NULL);
	c->snapshot_valid = true;
	c->txn = malloc(sizeof(struct xn_txn));
	c->txnid = gen_txnid();
	darray_init(c->txn->dtors);
	for(int i=0; i < BF_NUM_WORDS; i++) {
		c->read_set_hi[i] = 0;
		c->txn->write_set[i] = 0;
		c->txn->read_set[i] = 0;
	}
	/* no need to clear the actual slots. */
	atomic_store_explicit(&c->txn->txnid, gen_txnid(), memory_order_relaxed);
	/* begin epoch-protected section. */
	uint32_t cur_epoch;
	atomic_store_explicit(&c->epoch,
		cur_epoch = atomic_load_explicit(&txn_epoch, memory_order_relaxed),
		memory_order_release);
	assert((cur_epoch & 0x80000000) == 0);

	/* see if the epoch scheme needs a spin, which it does if txnid + 1 has N
	 * lowest bits cleared, where 2^N >= 2 * client_count ∧ N >= 3 .
	 */
	int shift = size_to_shift(2 * atomic_load_explicit(&client_count,
		memory_order_relaxed));
	if(shift < 3) shift = 3;
	int mask = (1 << shift) - 1;
	if(((c->txnid + 1) & mask) == 0) check_txn_epoch(cur_epoch);

	return 0;
}


static void clean_client(struct xn_client *c)
{
	/* the "end" bracket of the epoch mechanism. this lets the epoch tick
	 * forward freely even if this thread never starts another transaction
	 * again. consumers recognize that the high bit is set and ignore the
	 * client.
	 */
	atomic_fetch_or_explicit(&c->epoch, 0x80000000, memory_order_release);

	struct xn_txn *txn = atomic_exchange_explicit(&c->txn, NULL,
		memory_order_relaxed);
	if(txn != NULL) {
		darray_free(txn->dtors);
		free(txn);
	}

	for(int i=1; i < c->rec_chunks.size; i++) {
		free(c->rec_chunks.item[i].data);
	}
	c->rec_chunks.size = 1;
	c->rec_chunks.item[0].next_pos = 0;
}


/* insert txn into txn_list in order of descending commit ID so that the query
 * loop can early-exit.
 */
static void insert_txn(struct xn_txn *_Atomic *list_p, struct xn_txn *txn)
{
	struct xn_txn *head = atomic_load_explicit(list_p, memory_order_consume);
	if(head != NULL && head->commit_id > txn->commit_id) {
		insert_txn(&head->next, txn);
	} else {
		/* become the new head, or retry. */
		txn->next = head;
		if(!atomic_compare_exchange_strong_explicit(list_p,
			&head, txn, memory_order_release, memory_order_consume))
		{
			insert_txn(list_p, txn);
		}
	}

#ifdef DEBUG_ME_HARDER
	uint32_t last = ~0u;
	for(struct xn_txn *cur = atomic_load(list_p);
		cur != NULL;
		cur = atomic_load(&cur->next))
	{
		assert(cur->commit_id < last);
		last = cur->commit_id;
	}
#endif
}


/* tests overlap between read_set[] format. */
static bool bf_overlap(const uintptr_t *a, const uintptr_t *b)
{
	int n_hits = 0;
	for(int i=0; i < BF_NUM_WORDS; i++) {
		n_hits += __builtin_popcountl(a[i] & b[i]);
	}
	/* TODO: it'd be ideal if the bloomfilter hash function could produce
	 * three _distinct_ indexes for any given key. then this could be a > 3
	 * instead, improving utilization.
	 */
	return n_hits > 0;
}


int xn_commit(void)
{
	int rc, comm_id;
	darray(struct xn_rec *) w_list = darray_new();
	struct xn_txn *txn = NULL;

	struct xn_client *client = get_client();
	assert(client->txn != NULL);
	if(!client->snapshot_valid) goto serfail;

	uint32_t txnid = atomic_fetch_or_explicit(&client->txnid,
		TXNID_COMMIT, memory_order_relaxed);
	if((txnid & TXNID_KILLED) != 0) {
#if 0
		printf("%s: async kill of txnid=%#x (comm=%#x, epoch delta=%d)\n",
			__func__, txnid & 0xffffff, comm_id,
			(int)atomic_load(&txn_epoch) - client->epoch);
#endif
		goto timeout;
	}
	comm_id = gen_txnid();
	txn = atomic_exchange_explicit(&client->txn, NULL,
		memory_order_acq_rel);
	if(txn == NULL) goto serfail;
	txn->txnid = txnid & 0xffffff;
	txn->commit_id = comm_id;
	for(int i=0; i < BF_NUM_WORDS; i++) {
		txn->read_set[i] |= client->read_set_hi[i];
	}
	uint32_t epoch = atomic_load_explicit(
			&client->epoch, memory_order_consume),
		g_epoch = atomic_load_explicit(&txn_epoch, memory_order_relaxed);
	assert(epoch == g_epoch || epoch + 1 == g_epoch);

	/* examine old transactions in our epoch, and possibly the next if the
	 * tick happened concurrently.
	 */
	int n_olds = 0;
	struct xn_txn *old_txns[2];
	if(g_epoch > epoch) {
		old_txns[n_olds++] = atomic_load_explicit(
			&txn_list[g_epoch & 3], memory_order_relaxed);
	}
	old_txns[n_olds++] = atomic_load_explicit(
		&txn_list[epoch & 3], memory_order_relaxed);

	/* the bloom-filter intersection test for read-to-write dependency. */
	for(int lst = 0; lst < n_olds; lst++) {
		for(struct xn_txn *cur = old_txns[lst];
			cur != NULL;
			cur = atomic_load_explicit(&cur->next, memory_order_relaxed))
		{
			if(cur->commit_id < txn->txnid) {
#ifndef NDEBUG
				while(cur != NULL) {
					assert(cur->commit_id < txn->txnid);
					cur = atomic_load(&cur->next);
				}
#endif
				break;
			}

			bool w_match = false, r_match = false;
			for(int i=0; i < BF_NUM_WORDS; i++) {
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

			int v_value = atomic_load_explicit(&rec->item->version,
					memory_order_relaxed),
				v_seen = v_value & 0xffffff;
			assert((rec->version & ITEM_WRITE_BIT) == 0);
			if(v_seen != rec->version) goto serfail;
			if(rec->is_write) {
				if(!atomic_compare_exchange_strong_explicit(
					&rec->item->version, &v_value, comm_id | ITEM_WRITE_BIT,
					memory_order_relaxed, memory_order_relaxed))
				{
					/* changed between read and write. */
					goto serfail;
				}
				darray_push(w_list, rec);
			}
		}
	}

	rc = 0;
	if(w_list.size == 0) {
		/* read-only: cannot participate in deadlocks, won't influence waiting
		 * clients.
		 */
		finish_txn(txn);
		atomic_thread_fence(memory_order_release);
	} else {
		insert_txn(&txn_list[g_epoch & 3], txn);

		atomic_thread_fence(memory_order_acquire);
		/* hooray, let's committing! */
		for(int i=0; i < w_list.size; i++) {
			struct xn_rec *rec = w_list.item[i];
			memcpy(rec->item->address, rec->data, rec->length);
		}
		atomic_thread_fence(memory_order_release);
		/* unlock whenever. */
		for(int i=0; i < w_list.size; i++) {
			struct xn_rec *rec = w_list.item[i];
			atomic_store_explicit(&rec->item->version, comm_id,
				memory_order_relaxed);
		}

		/* wake waiters up. */
		struct xn_wait *_Atomic *head = &wait_list,
			*w = atomic_load_explicit(head, memory_order_consume);
		while(w != NULL) {
			if(!bf_overlap(w->read_set, txn->write_set)) {
				/* on to the next one. */
				head = &w->next;
				w = atomic_load_explicit(head, memory_order_consume);
			} else {
				/* try to dequeue & signal this one. */
				struct xn_wait *next = atomic_load_explicit(&w->next,
					memory_order_consume);
				if(atomic_compare_exchange_strong_explicit(head, &w, next,
					memory_order_release, memory_order_consume))
				{
					w->next = NULL;
					sem_post(&w->sem);
					w = next;
				}
			}
		}
	}

end:
	darray_free(w_list);
	clean_client(client);
	return rc;

serfail:
	/* drop write locks. */
	for(int i=0; i < w_list.size; i++) {
		struct xn_rec *rec = w_list.item[i];
		atomic_fetch_and_explicit(&rec->item->version,
			~ITEM_WRITE_BIT, memory_order_relaxed);
	}
	if(txn != NULL) {
		darray_free(txn->dtors);
		free(txn);
	}
	rc = -EDEADLK;
	goto end;

timeout:
	rc = -ETIMEDOUT;
	goto end;
}


static void finish_txn(struct xn_txn *txn)
{
#ifndef NDEBUG
	for(int i=0; i < BF_NUM_WORDS; i++) {
		txn->read_set[i] = txn->write_set[i] = ~0ul;
	}
#endif

	for(int i=0; i < txn->dtors.size; i++) {
		(*txn->dtors.item[i].dtor_fn)(txn->dtors.item[i].param);
	}
	darray_free(txn->dtors);

	free(txn);
}


void xn_abort(int status)
{
	if(status == 0) return;

	/* otherwise, uhh, ... */
	struct xn_client *c = get_client();

	clean_client(c);
}


/* find @w in wait_list and remove it. if this removes @w, it also sets
 * @w->next to NULL.
 */
static void remove_wait(struct xn_wait *w)
{
	struct xn_wait *_Atomic *head = &wait_list,
		*cur = atomic_load_explicit(head, memory_order_consume);
	while(cur != NULL) {
		struct xn_wait *next = atomic_load_explicit(&cur->next,
			memory_order_consume);
		if(cur != w) {
			head = &cur->next;
			cur = next;
		} else {
			if(atomic_compare_exchange_strong_explicit(head, &cur, next,
				memory_order_release, memory_order_consume))
			{
				w->next = NULL;
				return;
			}
		}
	}
}


/* wait on a change to any of @c's existing readset, or some other wakeup
 * event.
 */
void xn_retry(void)
{
	struct xn_client *c = get_client();

	struct xn_wait *w = malloc(sizeof(*w));
	if(w == NULL) {
		perror("malloc in xn_retry");
		abort();
	}
	int n = sem_init(&w->sem, 0, 0);
	if(n != 0) {
		perror("sem_init in xn_retry");
		abort();
	}
	memcpy(w->read_set, c->txn->read_set, sizeof(c->txn->read_set));
	w->txnid = atomic_load(&c->txnid) & ~0xff000000;

	/* publish. */
	w->next = atomic_load_explicit(&wait_list, memory_order_consume);
	int iter = 0;
	while(!atomic_compare_exchange_strong_explicit(&wait_list,
		&w->next, w, memory_order_release, memory_order_consume))
	{
		if(iter++ >= 40) {
			sched_yield();
			iter--;
		}
	}

	/* examine contemporary transactions from pre-publication. */
	uint32_t my_epoch = atomic_load_explicit(&c->epoch, memory_order_consume),
		g_epoch = atomic_load_explicit(&txn_epoch, memory_order_relaxed);
	assert(my_epoch == g_epoch || my_epoch + 1 == g_epoch);
	bool instawake = false;
	for(int j = 0, lim = g_epoch - my_epoch + 1;
		j < lim && !instawake;
		j++)
	{
		assert(j == 0 || g_epoch == my_epoch + 1);
		struct xn_txn *txn = atomic_load_explicit(
			&txn_list[(g_epoch + 4 - j) & 3], memory_order_consume);
		while(txn != NULL && txn->commit_id > w->txnid) {
			if(bf_overlap(w->read_set, txn->write_set)) {
				instawake = true;
				break;
			}
			txn = atomic_load_explicit(&txn->next, memory_order_consume);
		}
	}

	if(instawake) {
		/* remove @w, but test whether it signaled while we spun as an
		 * optimization.
		 */
		if(sem_trywait(&w->sem) < 0 && errno == EAGAIN) remove_wait(w);
	} else {
		/* sleep. */
		do {
			n = sem_wait(&w->sem);
		} while(n < 0 && errno == EINTR);
		if(n < 0 && errno != EINTR) {
			perror("sem_wait in xn_retry");
			abort();
		}
	}
	/* poster or remove_wait() dequeues @w. */
	assert(w->next == NULL);
	sem_destroy(&w->sem);
	/* FIXME: must use epoch reclamation here: otherwise @w will be examined
	 * by any number of concurrents during free().
	 */
	// free(w);

	/* make the following xn_commit() break immediately. */
	c->snapshot_valid = false;
}


void xn_dtor(void (*fn)(void *param), void *param)
{
	struct xn_client *c = get_client();
	struct xn_dtor d = { .dtor_fn = fn, .param = param };
	darray_push(c->txn->dtors, d);
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

	*slot = hash & (BF_NUM_SLOTS - 1);
	*limb = (hash >> shift) & (BF_NUM_WORDS - 1);
	*ix = hash & mask;
}


/* probes the read set to try and find an existing xn_rec directly, without
 * going through the global item hash.
 */
static struct xn_rec *bf_probe(struct xn_client *c, void *addr, bool *ambig_p)
{
	*ambig_p = false;
	int count[3], val[3];
	struct xn_rec *rec = NULL;
	for(int i=0; i < 3 && rec == NULL; i++) {
		int slot, limb, ix;
		probe_pos(&slot, &limb, &ix, bf_hash(c, addr, i));
		count[i] = ((c->read_set_hi[limb] >> (ix - 1)) & 0x2)
			| ((c->txn->read_set[limb] >> ix) & 0x1);
		val[i] = c->rs_words[slot];

		switch(count[i]) {
			case 0:
				/* strongly absent. */
				return NULL;
			case 1:
				/* absent if invalid, or wrong item. */
				if(!is_valid_index(c, val[i])) return NULL;
				rec = index_to_rec(c, val[i]);
				if(rec->item->address != addr) return NULL;
				break;
			case 2:
			case 3:
				/* uncertain. */
				break;
		}
	}

	if(rec == NULL) *ambig_p = true;	/* needs extended lookup */
	assert(rec == NULL || rec->item->address == addr);
	return rec;
}


static void bf_insert(struct xn_client *c, void *addr, uint16_t rec_index)
{
	int prior[3] = { -1, -1, -1 };
	for(int i=0; i < 3; i++) {
		size_t hash = bf_hash(c, addr, i);
		int limb, ix, slot;
		probe_pos(&slot, &limb, &ix, hash);
		if(prior[0] == slot || prior[1] == slot) continue;
		prior[i] = slot;

		/* fancy two-bit saturating increment & lazy cleanup.
		 *
		 * transform 00 -> 01, 01 -> 10, 10 -> 11, 11 -> 11;
		 * so for H, L -> H|L, H|~L
		 */
		uintptr_t hi = c->read_set_hi[limb] & (1ul << ix),
			lo = c->txn->read_set[limb] & (1ul << ix);
		if((hi | lo) == 0) c->rs_words[slot] = 0;	/* clean */
		c->rs_words[slot] ^= rec_index;
		c->read_set_hi[limb] |= lo;
		c->txn->read_set[limb] = (c->txn->read_set[limb] & ~(1ul << ix))
			| hi | (lo ^ (1ul << ix));
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
		int n_loops = 0;
		while(((new_ver = atomic_load_explicit(&it->version,
			memory_order_acquire)) & ITEM_WRITE_BIT) != 0)
		{
			if(n_loops == 40) sched_yield(); else n_loops++;
		}
		do {
			if((new_ver & ITEM_WRITE_BIT) != 0) break;
			old_ver = new_ver;
			value = atomic_load_explicit(iptr, memory_order_relaxed);
		} while((new_ver = atomic_load_explicit(&it->version,
			memory_order_seq_cst)) != old_ver);
	} while((new_ver & ITEM_WRITE_BIT) != 0);

	/* make new record. */
	uint16_t idx;
	rec = new_xn_rec(c, &idx, sizeof(int));
	rec->item = (struct xn_item *)it;
	rec->length = sizeof(int);
	rec->is_write = false;
	rec->version = old_ver & 0xffffff;
	if(rec->version > (c->txnid & 0xffffff)) c->snapshot_valid = false;
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
		rec->version = atomic_load_explicit(&rec->item->version,
			memory_order_relaxed) & 0xffffff;
		if(rec->version > (c->txnid & 0xffffff)) c->snapshot_valid = false;
		bf_insert(c, ptr, rec_idx);
	}

	if(!rec->is_write) {
		rec->is_write = true;
		/* add to write set.
		 * note: [v1] this could recycle hashes computed for bf_insert().
		 */
		for(int i=0; i < 3; i++) {
			int slot, limb, ix;
			probe_pos(&slot, &limb, &ix, bf_hash(c, ptr, i));
			c->txn->write_set[limb] |= 1ul << ix;
		}
	}

	return rec->data;
}


void xn_put(int *iptr, int value)
{
	int *p = xn_modify(iptr, sizeof(*iptr));
	*p = value;
}
