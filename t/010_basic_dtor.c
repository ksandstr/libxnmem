
/* basic tests on destructor behaviour, viz. that they get called
 * eventually.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>

#include <ccan/tap/tap.h>

#include "xn.h"


#define NUM_THREADS 64
#define NUM_TXNS 128


struct oth_param {
	int *data;
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
	}

	return p;
}


int main(void)
{
	diag("NUM_TXNS=%d, NUM_THREADS=%d", NUM_TXNS, NUM_THREADS);
	plan_tests(3);

	int *data = calloc(NUM_THREADS, sizeof(int));

	pthread_t threads[NUM_THREADS];
	for(int i=0; i < NUM_THREADS; i++) {
		struct oth_param *p = malloc(sizeof(*p));
		*p = (struct oth_param){ .data = data };
		int n = pthread_create(&threads[i], NULL, &other_fn, p);
		if(n != 0) {
			perror("pthread_create");
			abort();
		}
	}

	/* collect results. */
	int dtor_total = 0, dtor_max = 0;
	for(int i=0; i < NUM_THREADS; i++) {
		void *ret = NULL;
		int n = pthread_join(threads[i], &ret);
		if(n != 0) {
			perror("pthread_join");
			continue;
		}
		struct oth_param *p = ret;
		int tot = atomic_load(&p->dtor_count);
#ifdef DEBUG_ME_HARDER
		diag("i=%d, tot=%d, restart_count=%d", i, tot, p->restart_count);
#endif
		dtor_total += tot;
		if(dtor_max < tot) dtor_max = tot;
		if(tot == NUM_TXNS) free(p);	/* otherwise, leak */
	}

	ok1(dtor_total > NUM_THREADS);
	ok1(dtor_max <= NUM_TXNS);

	/* examine entrails. */
	bool all_done = true;
	for(int i=0; i < NUM_THREADS; i++) {
		if(data[i] != NUM_TXNS) {
			diag("data[%d] = %d", i, data[i]);
			all_done = false;
		}
	}
	ok(all_done, "all data[] = %d", NUM_TXNS);

	free(data);
	return exit_status();
}
