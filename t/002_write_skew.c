
/* test that the write-skew anomaly is prevented. */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <semaphore.h>

#include <ccan/tap/tap.h>

#include "xn.h"


/* TODO: move all this into a test.h */

#define NUM_ELEMENTS(ary) (sizeof(ary) / sizeof((ary)[0]))
#define CHECK_FLAG(x, f) (((x) & (f)) != 0)
#define btos(b) ((b) ? "true" : "false")

/* >implying implications */
#define imply_ok1(left, right) \
	ok(!(left) || (right), "%s --> %s", #left, #right)

#define imply_ok(left, right, test, ...) \
	ok(!(left) || (right), test, ##__VA_ARGS__)

/* alias for left == right, printed as "iff". */
#define iff_ok1(left, right) \
	ok((left) == (right), "%s iff %s", #left, #right)


/* control the other transaction's execution. */
static sem_t goahead_sem, done_sem;


/* the thread that does data[0] -= 200; abort if data[0] + data[1] < 0.
 * main() does the opposite.
 */
static void *other_fn(void *param_ptr)
{
	int *data = param_ptr, status;
	bool waited = false;
	do {
		if(!waited) {
			sem_wait(&goahead_sem);
			waited = true;
		}
		status = xn_begin();
		diag("other_fn txn started");
		int v0 = xn_read_int(&data[0]), v1 = xn_read_int(&data[1]);
		v0 -= 200;
		if(v0 + v1 < 0) {
			diag("invariant broken in other");
			break;
		}
		xn_put(&data[0], v0);
	} while(status = xn_commit(), XN_RESTART(status));
	xn_abort(status);
	diag("other_fn txn finished");
	sem_post(&done_sem);

	return NULL;
}


/* ... currently nothing supports this protocol. once mung's testing framework
 * makes it out of that project, it should. TAP's basic prove(1) seems rather
 * insufficient.
 *
 * so, for the time being, always running the test with iter=0 is Good Enough.
 * but FIXME, still.
 */
static int get_iter(int min, int max)
{
	const char *query = getenv("TEST_QUERY");
	if(query != NULL && query[0] != '\0') {
		printf("min=%d, max=%d\n", min, max);
		exit(EXIT_SUCCESS);
	}

	const char *str = getenv("TEST_ITER");
	return str != NULL ? atoi(str) : 0;
}


int main(void)
{
	const int iter = get_iter(0, 1);
	const bool oth_first = CHECK_FLAG(iter, 1);
	diag("oth_first=%s", btos(oth_first));

	plan_tests(1);

	sem_t *sems[] = { &goahead_sem, &done_sem };
	for(int i=0; i < NUM_ELEMENTS(sems); i++) {
		int n = sem_init(sems[i], 0, 0);
		if(n != 0) {
			perror("sem_init");
			abort();
		}
	}

	int *data = malloc(sizeof(*data) * 2);
	data[0] = 100;
	data[1] = 100;

	pthread_t other;
	int n = pthread_create(&other, NULL, &other_fn, data);
	if(n != 0) {
		perror("pthread_create");
		abort();
	}

	int status;
	bool posted = false;
	do {
		status = xn_begin();
		diag("main txn started");
		int v0 = xn_read_int(&data[0]), v1 = xn_read_int(&data[1]);
		if(oth_first && !posted) {
			sem_post(&goahead_sem);
			sem_wait(&done_sem);
			posted = true;
		}
		v1 -= 200;
		if(v0 + v1 < 0) {
			diag("invariant broken in main");
			break;
		}
		xn_put(&data[1], v1);
	} while(status = xn_commit(), XN_RESTART(status));
	xn_abort(status);
	diag("main txn done");
	if(!posted) {
		assert(!oth_first);
		sem_post(&goahead_sem);
		sem_wait(&done_sem);
	}

	void *retval = NULL;
	n = pthread_join(other, &retval);
	if(n != 0) {
		perror("pthread_join");
		abort();
	}

	if(!ok1(data[0] + data[1] >= 0)) {
		diag("data[0]=%d, data[1]=%d", data[0], data[1]);
	}

	free(data);
	for(int i=0; i < NUM_ELEMENTS(sems); i++) sem_destroy(sems[i]);
	return exit_status();
}
