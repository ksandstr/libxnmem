
/* tests whether the transaction manager avoids basic ABAB race conditions
 * when there are more than two participants.
 *
 * the scheme is simple: have clients T_0..T_n accessing data items V_0..V_n
 * such that T_x reads V_x and V_(x+1 [mod n]), and writes V_(x+1 [mod n]).
 * since this test makes sure all transactions are open at the same time, the
 * result should be a deadlock resolved by repeating the last transaction.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sched.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>

#include "xn.h"


struct other_param {
	int own_id, max_id, retval;
	int *data;

	pthread_barrier_t *begin;	/* makes transactions contemporary */
	sem_t *goahead;				/* for commit-order control */
	sem_t *committed;			/* signals back to controller */
};


static void *other_fn(void *param_ptr)
{
	struct other_param *p = param_ptr;
	const int high_ix = (p->own_id + 1) % (p->max_id + 1);

	bool waited = false;
	int status, v0;
	do {
		status = xn_begin();
		if(!waited) {
			pthread_barrier_wait(p->begin);
			sem_wait(p->goahead);
			waited = true;
		}
		v0 = xn_read_int(&p->data[p->own_id]);
		int v1 = xn_read_int(&p->data[high_ix]);
		if(v0 != 1 || v1 != 1) {
			/* it's my constitutional right[i]!![/i] */
			diag("other=%d refusing to add v0=%d, v1=%d",
				p->own_id, v0, v1);
		} else {
			xn_put(&p->data[high_ix], v0 + v1);
		}
	} while(status = xn_commit(), XN_RESTART(status));
	xn_abort(status);
	sem_post(p->committed);

	p->retval = v0;
	return p;
}


int main(void)
{
	const int test_size = 11;

	/* initialize V_x to 1.
	 *
	 * this way, the other_fn transaction makes a result where
	 * V_0,V_2..V_(n-2) will be 2, and V_1 will be 1.
	 *
	 * if the transaction broker ends up slaying a different transaction, the
	 * counts will differ; so this isn't actually testing any kind of
	 * consistency but rather the specific form of insta-commit deadlock
	 * detection.
	 */
	int *data = malloc(sizeof(*data) * test_size);
	for(int i=0; i < test_size; i++) data[i] = 1;

	diag("test_size=%d", test_size);
	plan_tests(3);

	sem_t *go_sems = malloc(sizeof(*go_sems) * test_size),
		*committed_sem = malloc(sizeof(*committed_sem));
	sem_init(committed_sem, 0, 0);
	pthread_barrier_t *begin = malloc(sizeof(*begin));
	pthread_barrier_init(begin, NULL, test_size);
	pthread_t others[test_size];
	for(int i=0; i < test_size; i++) {
		struct other_param *p = malloc(sizeof(*p));
		*p = (struct other_param){
			.own_id = i, .max_id = test_size - 1, .data = data,
			.goahead = &go_sems[i], .committed = committed_sem,
			.begin = begin,
		};
		sem_init(p->goahead, 0, 0);
		int n = pthread_create(&others[i], NULL, &other_fn, p);
		if(n != 0) {
			perror("pthread_create");
			abort();
		}
	}

	/* order participants' execution by upping the respective goahead
	 * semaphore, then waiting for the (shared) commit semaphore. proceed in
	 * reverse so that the deadlock is detected in the first thread.
	 */
	for(int i=test_size - 1; i >= 0; i--) {
		sem_post(&go_sems[i]);
		sem_wait(committed_sem);
	}

	/* collect results and clean up */
	for(int i=0; i < test_size; i++) {
		void *ret = NULL;
		int n = pthread_join(others[i], &ret);
		if(n != 0) {
			perror("pthread_join");
			continue;
		}
		struct other_param *p = ret;
		sem_destroy(p->goahead);
		free(p);
	}
	free(go_sems);
	sem_destroy(committed_sem); free(committed_sem);
	pthread_barrier_destroy(begin); free(begin);

	/* analysis */
	ok(data[1] == 1, "output V_1 was refused");
	ok(data[0] == 2, "output V_0 was generated");
	bool all_two = true;
	for(int i=2; i < test_size; i++) {
		if(data[i] != 2) {
			all_two = false;
			diag("data[%d]=%d (expected 2)", i, data[i]);
		}
	}
	ok(all_two, "V_2..V_(n-1) were all 2");

	return exit_status();
}
