
/* the basic "dining philosophers" problem. signaling (i.e. retry) substituted
 * for with a 5ms sleep.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#include <ccan/tap/tap.h>
#include <ccan/darray/darray.h>
#include <ccan/compiler/compiler.h>

#include "xn.h"
#include "util.h"


#define NUM_CHAIRS 3
#define THINK_MS 5
#define EAT_MS 7


/* transaction state */
static int next_id = 1;
static int *fork_owner;

/* a log about who started and stopped eating. start is positive philosopher
 * ID, end is negative. using mutexes here is _fine_.
 */
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static darray(int) eat_log = darray_new();


static void *philosopher_fn(void *priv UNUSED)
{
	int status;

	/* get us an ID, first. */
	intptr_t id;
	do {
		status = xn_begin();
		id = xn_read_int(&next_id);
		xn_put(&next_id, id + 1);
	} while(status = xn_commit(), XN_RESTART(status));
	xn_abort(status);
	assert(id > 0);

	bool cond_ok = true;
	int n_loops = id / 2 + 3;
	int left_ix = id - 1, right_ix = id % NUM_CHAIRS;
	diag("id=%d, n_loops=%d, left_ix=%d, right_ix=%d",
		(int)id, n_loops, left_ix, right_ix);
	for(int i=0; i < n_loops; i++) {
		/* grab both forks. */
		do {
			status = xn_begin();
			int f0 = xn_read_int(&fork_owner[left_ix]),
				f1 = xn_read_int(&fork_owner[right_ix]);
			if(f0 != 0 || f1 != 0) {
				/* can't grab 'em, so abort and think a bit. */
				xn_abort(status);
				diag("id=%d can't pick up %d & %d (owned by f0=%d, f1=%d), thinks instead",
					(int)id, left_ix, right_ix, f0, f1);
				usleep(THINK_MS * 1000);
				continue;
			}
			xn_put(&fork_owner[left_ix], (int)id);
			xn_put(&fork_owner[right_ix], (int)id);
		} while(status = xn_commit(), XN_RESTART(status));
		xn_abort(status);

		/* got forks, so log 'em and chow down. */
		pthread_mutex_lock(&log_mutex);
		darray_push(eat_log, (int)id);
		pthread_mutex_unlock(&log_mutex);

		diag("id=%d starts eating with %d & %d", (int)id, left_ix, right_ix);
		usleep(EAT_MS * 1000);
		diag("id=%d is done eating with %d & %d", (int)id, left_ix, right_ix);

		pthread_mutex_lock(&log_mutex);
		darray_push(eat_log, -(int)id);
		pthread_mutex_unlock(&log_mutex);

		/* down tools. */
		do {
			status = xn_begin();
			int f0 = xn_read_int(&fork_owner[left_ix]),
				f1 = xn_read_int(&fork_owner[right_ix]);
			if(f0 != id || f1 != id) {
				diag("release precondition failed; f0=%d, f1=%d; expected %d",
					f0, f1, (int)id);
				cond_ok = false;
				/* ... but overwrite 'em anyway. */
			}
			xn_put(&fork_owner[left_ix], 0);
			xn_put(&fork_owner[right_ix], 0);
		} while(status = xn_commit(), XN_RESTART(status));
		xn_abort(status);
	}

	intptr_t retval = cond_ok ? id : -id;
	return (void *)retval;
}


static int int_cmp(const void *a, const void *b) {
	return *(const int *)a - *(const int *)b;
}


int main(void)
{
	plan_tests(4);
	todo_start("currently broken");

	fork_owner = calloc(NUM_CHAIRS, sizeof(int));
	pthread_t threads[NUM_CHAIRS];
	for(int i=0; i < NUM_CHAIRS; i++) {
		int n = pthread_create(&threads[i], NULL, &philosopher_fn, NULL);
		if(n != 0) {
			perror("pthread_create");
			abort();
		}
	}

	bool all_cond_ok = true;
	darray(int) ids = darray_new();
	for(int i=0; i < NUM_CHAIRS; i++) {
		void *rv = NULL;
		int n = pthread_join(threads[i], &rv);
		if(n != 0) {
			diag("join of thread %d: n=%d (%s)", i, n, strerror(n));
		}
		int id = (intptr_t)rv;
		if(id < 0) {
			all_cond_ok = false;
			diag("precondition failed in id=%d", abs(id));
		}
		id = abs(id);
		darray_push(ids, id);
	}
	ok1(all_cond_ok);	/* no precond failures. */

	/* all IDs were unique. */
	qsort(ids.item, ids.size, sizeof(*ids.item), &int_cmp);
	bool no_repeat_ids = true;
	for(int i=1, prev = ids.item[0]; i < ids.size; i++) {
		if(ids.item[i] == prev) {
			no_repeat_ids = false;
			diag("i=%d: found repeat of prev=%d", i, prev);
		}
		prev = ids.item[i];
	}
	ok1(no_repeat_ids);

	/* no forks were in use simultaneously, and only forks that were in use
	 * are downed.
	 */
	bool no_simult = true, no_double_drop = true,
		fork_status[NUM_CHAIRS];
	for(int i=0; i < NUM_CHAIRS; i++) fork_status[i] = false;
	pthread_mutex_lock(&log_mutex);
	for(int i=0; i < eat_log.size; i++) {
		int id = eat_log.item[i],
			left = abs(id) - 1, right = abs(id) % NUM_CHAIRS;
		if(id < 0) {
			/* stopped eating. */
			if(!fork_status[left] || !fork_status[right]) {
				diag("log[%d]: id=%d, l[%d]=%s, r[%d]=%s on release",
					i, id, left, btos(fork_status[left]),
					right, btos(fork_status[right]));
				no_double_drop = false;
			}
			fork_status[left] = false;
			fork_status[right] = false;
		} else {
			if(fork_status[left] || fork_status[right]) {
				diag("log[%d]: id=%d, l[%d]=%s, r[%d]=%s on acquire",
					i, id, left, btos(fork_status[left]),
					right, btos(fork_status[right]));
				no_simult = false;
			}
			fork_status[left] = true;
			fork_status[right] = true;
		}
	}
	pthread_mutex_unlock(&log_mutex);
	ok1(no_simult);
	ok1(no_double_drop);

	darray_free(ids);
	darray_free(eat_log);
	free(fork_owner);

	return exit_status();
}
