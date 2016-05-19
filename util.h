
#ifndef SEEN_UTIL_H
#define SEEN_UTIL_H


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


#endif
