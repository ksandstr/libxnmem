
#ifndef __XN_H__
#define __XN_H__

#include <stdbool.h>
#include <errno.h>


#define XN_RESTART(status) ((status) == -EDEADLK || (status) == -ETIMEDOUT)


extern int xn_begin(void);
extern int xn_commit(void);
extern void xn_abort(int status);

/* destructors are run sometime after successful commit. they execute in an
 * indeterminate thread's context and must not call any xn_*() function. at
 * transaction abort, all dtors are discarded and won't be run.
 */
extern void xn_dtor(void (*fn)(void *param), void *param);
extern void xn_free(void *ptr);		/* wrapper for free(3) */

extern int xn_read_int(int *iptr);
extern void xn_put(int *iptr, int value);


#endif
