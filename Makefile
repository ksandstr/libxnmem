
CFLAGS=-std=gnu11 -Wall -g -march=native -D_GNU_SOURCE -pthread


all: xntest


clean:
	rm -f *.o xntest


xntest: xntest.o xn.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS) $(LIBS)


%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)
