
CCAN_DIR=~/src/ccan

CFLAGS:=-O2 -std=gnu11 -Wall -g -march=native \
	-D_GNU_SOURCE -pthread -I $(CCAN_DIR) -I $(abspath .) \
	-DCCAN_LIST_DEBUG=1 #-DNDEBUG

TEST_BIN:=$(patsubst t/%.c,t/%,$(wildcard t/*.c))


all: tags $(TEST_BIN)


clean:
	rm -f *.o xntest $(TEST_BIN)


distclean: clean
	rm -f tags
	rm -rf .deps


check: all
	prove $(TEST_BIN)


tags: $(shell find . -iname "*.[ch]" -or -iname "*.p[lm]")
	@ctags -R *


t/%: t/%.c xn.o \
		ccan-list.o ccan-htable.o ccan-hash.o ccan-tap.o
	@echo "  LD $@"
	@$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS) $(LIBS)


ccan-%.o ::
	@echo "  CC $@ <ccan>"
	@$(CC) -c -o $@ $(CCAN_DIR)/ccan/$*/$*.c $(CFLAGS)


%.o: %.c xn.h
	@echo "  CC $@"
	@$(CC) -c -o $@ $< $(CFLAGS) -MMD
	@test -d .deps || mkdir -p .deps
	@mv $(<:.c=.d) .deps/


include $(wildcard .deps/*.d)
