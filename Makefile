
CCAN_DIR=~/src/ccan

CFLAGS=-std=gnu11 -Wall -g -march=native -D_GNU_SOURCE -pthread \
	-I $(CCAN_DIR)


all: xntest


clean:
	rm -f *.o xntest


xntest: xntest.o xn.o \
		ccan-list.o ccan-htable.o ccan-hash.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS) $(LIBS)


ccan-%.o ::
	@echo "  CC $@ <ccan>"
	@$(CC) -c -o $@ $(CCAN_DIR)/ccan/$*/$*.c $(CFLAGS)


%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)
