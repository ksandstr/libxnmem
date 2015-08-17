
/* test whether a client sleeping outside of a transaction can retard
 * destructor processing. passes when it doesn't.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <semaphore.h>

#include <ccan/tap/tap.h>
#include <ccan/talloc/talloc.h>

#include "xn.h"


/* heavily influenced by 010_basic_dtor.c */
#define NUM_THREADS 64
#define NUM_TXNS 128


struct oth_param {
	int *data;
	sem_t *go_sem, *done_sem, *exit_sem;

	int restart_count;
	_Atomic int dtor_count;
};


static void bump_dtor_count(void *ptr) {
	struct oth_param *p = ptr;
	atomic_fetch_add_explicit(&p->dtor_count, 1, memory_order_relaxed);
}


static void *other_fn(void *param_ptr)
{
	struct oth_param *p = param_ptr;

	p->dtor_count = 0;
	p->restart_count = 0;
	bool waited = false;
	for(int i=0; i < NUM_TXNS; i++) {
		bool first = true;
		int status;
		do {
			status = xn_begin();
			if(!first) p->restart_count++; else first = false;
			int ix = i & (NUM_THREADS - 1);
			int v = xn_read_int(&p->data[ix]);
			xn_put(&p->data[ix], v + 1);
			xn_dtor(&bump_dtor_count, p);
		} while(status = xn_commit(), XN_RESTART(status));
		xn_abort(status);

		if(!waited) {
			sem_wait(p->go_sem);
			waited = true;
		}
	}

	sem_post(p->done_sem);
	sem_wait(p->exit_sem);
	return p;
}


/* TODO: more util.c fodder */
static sem_t *mksem(void *context, unsigned int startval)
{
	sem_t *s = talloc(context, sem_t);
	sem_init(s, 0, startval);
	talloc_set_destructor(s, &sem_destroy);
	return s;
}


int main(void)
{
	plan_tests(2);
	todo_start("not expected to work yet");

	/* allocate data and control structures, and spawn threads. */
	int *data = talloc_zero_array(NULL, int, NUM_THREADS);
	sem_t *go_sems[NUM_THREADS], *exit_sems[NUM_THREADS],
		*done_sems[NUM_THREADS];
	pthread_t threads[NUM_THREADS];
	for(int i=0; i < NUM_THREADS; i++) {
		struct oth_param *p = talloc(data, struct oth_param);
		*p = (struct oth_param){
			.data = data,
			.go_sem = go_sems[i] = mksem(p, 0),
			.done_sem = done_sems[i] = mksem(p, 0),
			.exit_sem = exit_sems[i] = mksem(p, 0),
		};
		int n = pthread_create(&threads[i], NULL, &other_fn, p);
		if(n != 0) {
			perror("pthread_create");
			abort();
		}
	}

	/* have the first one run to completion, but not yet go into pre-join
	 * status.
	 */
	sem_post(go_sems[0]); sem_wait(done_sems[0]);
	/* set the rest off. */
	for(int i=1; i < NUM_THREADS; i++) sem_post(go_sems[i]);
	/* flush & join them. */
	int dtor_total = 0, dtor_max = 0;
	for(int i=1; i < NUM_THREADS; i++) {
		sem_wait(done_sems[i]);
		sem_post(exit_sems[i]);
		void *retval = NULL;
		int n = pthread_join(threads[i], &retval);
		if(n != 0) {
			perror("pthread_join");
			abort();
		}
		struct oth_param *p = retval;
		int tot = atomic_load_explicit(&p->dtor_count, memory_order_relaxed);
#ifdef DEBUG_ME_HARDER
		diag("i=%d, tot=%d, restart_count=%d", i, tot, p->restart_count);
#endif
		dtor_total += tot;
		if(dtor_max < tot) dtor_max = tot;
		if(tot == NUM_TXNS) talloc_free(p);
	}

	/* analysis. */
	if(!ok1(dtor_total >= NUM_THREADS * NUM_TXNS / 2)) {
		diag("dtor_total=%d", dtor_total);
	}
	if(!ok1(dtor_max <= NUM_TXNS)) {
		diag("dtor_max=%d", dtor_max);
	}

	/* close off the first one as well. */
	sem_post(exit_sems[0]);
	void *retval = NULL;
	int n = pthread_join(threads[0], &retval);
	if(n != 0) {
		perror("pthread_join (final)");
		abort();
	}

	/* TODO: synchronize a clean-up with the transaction broker, then
	 * call talloc_free(data).
	 */
	return exit_status();
}
