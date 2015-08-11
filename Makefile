
CCAN_DIR=~/src/ccan

CFLAGS=-O2 -std=gnu11 -Wall -g -march=native \
	-D_GNU_SOURCE -pthread -I $(CCAN_DIR) \
	-DCCAN_LIST_DEBUG=1 #-DNDEBUG


all: tags xntest


clean:
	rm -f *.o xntest


distclean: clean
	rm -f tags


tags: $(shell find . -iname "*.[ch]" -or -iname "*.p[lm]")
	@ctags -R *


xntest: xntest.o xn.o \
		ccan-list.o ccan-htable.o ccan-hash.o ccan-tap.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS) $(LIBS)


ccan-%.o ::
	@echo "  CC $@ <ccan>"
	@$(CC) -c -o $@ $(CCAN_DIR)/ccan/$*/$*.c $(CFLAGS)


%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)
