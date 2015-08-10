
/* most basic tests of the xn_*() primitives. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sched.h>
#include <unistd.h>
#include <pthread.h>

#include <ccan/tap/tap.h>
#include <ccan/compiler/compiler.h>

#include "xn.h"


static int global_data[2];


static void *other_fn(void *priv UNUSED)
{
	int status;
	intptr_t v0;
	do {
		status = xn_begin();
		v0 = xn_read_int(&global_data[0]);
		xn_put(&global_data[0], 0);
	} while(status = xn_commit(), XN_RESTART(status));
	xn_abort(status);

	printf("extracted value was %d\n", (int)v0);

	return (void *)v0;
}


int main(void)
{
	plan_tests(3);
	todo_start("not done yet");

	global_data[0] = 1000; global_data[1] = 0;

	pthread_t other;
	int n = pthread_create(&other, NULL, &other_fn, NULL);
	if(n != 0) {
		perror("pthread_create");
		return EXIT_FAILURE;
	}

	int status, v0;
	do {
		status = xn_begin();
		v0 = xn_read_int(&global_data[0]);
		usleep(500);		/* provoke read-to-write race */
		xn_put(&global_data[0], v0 + 1);
		xn_put(&global_data[1], xn_read_int(&global_data[1]) + 2);
	} while(status = xn_commit(), XN_RESTART(status));
	xn_abort(status);
	printf("main's old value was %d\n", v0);

	void *ret = NULL;
	n = pthread_join(other, &ret);
	if(n != 0) {
		perror("pthread_join");
	}
	int oth_v0 = (intptr_t)ret;

	if(!ok1((v0 == 1000 && oth_v0 == 1001) || (v0 == 0 && oth_v0 == 1000))) {
		diag("v0=%d, oth_v0=%d", v0, oth_v0);
	}

	if(!ok1(global_data[0] == 0 || global_data[0] == 1)) {
		diag("global_data[0]=%d", global_data[0]);
	}
	if(!ok1(global_data[1] == 2)) {
		diag("global_data[1]=%d", global_data[1]);
	}

	return exit_status();
}
