
#include "xn.h"


int xn_begin(void);
int xn_commit(void);
void xn_abort(int status);

int xn_read_int(int *iptr);
void xn_put(int *iptr, int value);
