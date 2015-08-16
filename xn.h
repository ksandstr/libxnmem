
#ifndef __XN_H__
#define __XN_H__

#include <stdbool.h>
#include <errno.h>


#define XN_RESTART(status) ((status) == -EDEADLK)


extern int xn_begin(void);
extern int xn_commit(void);
extern void xn_abort(int status);

/* destructors are run sometime after successful commit. they are discarded at
 * transaction abort.
 */
extern void xn_dtor(void (*fn)(void *param), void *param);
extern void xn_free(void *ptr);		/* wrapper for free(3) */

extern int xn_read_int(int *iptr);
extern void xn_put(int *iptr, int value);


#endif
