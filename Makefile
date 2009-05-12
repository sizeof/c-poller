CC=gcc
CFLAGS=-I/usr/local/lib/erlang/lib/erl_interface-3.6.1/include
LIBS=-L/usr/local/lib/erlang/lib/erl_interface-3.6.1/lib -lerl_interface -lei -levent -lmemcached

cpolld: cpolld.c
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)
