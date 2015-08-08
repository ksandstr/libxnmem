
#ifndef __XN_H__
#define __XN_H__

#include <stdbool.h>


#define XN_RESTART(status) false


extern int xn_begin(void);
extern int xn_commit(void);
extern void xn_abort(int status);

extern int xn_read_int(int *iptr);
extern void xn_put(int *iptr, int value);


#endif
