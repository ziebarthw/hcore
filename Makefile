# Point EVHTP_PREFIX at a libevhtp install (headers in include/evhtp,
# libevhtp.a in lib). libevent 2.1 and OpenSSL headers/libs must be on the
# default paths. EXTRA_LIBS adds libraries your libevhtp build needs, e.g.
# EXTRA_LIBS=-lnghttp2 for a libevhtp with HTTP/2 (ziebarthw/libevhtp).
EVHTP_PREFIX ?= /usr/local
EXTRA_LIBS   ?=

CC      ?= cc
CFLAGS  ?= -O2 -g
HC_CFLAGS = -std=c99 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude
# Only include/, as libevhtp's pkg-config file says: its headers are included
# as <evhtp/...>, and include/evhtp itself holds names (string.h) that would
# shadow the system headers.
EVHTP_CFLAGS = -I$(EVHTP_PREFIX)/include
EVHTP_LIBS   = $(EVHTP_PREFIX)/lib/libevhtp.a $(EXTRA_LIBS) -levent -levent_openssl -levent_pthreads -lssl -lcrypto -lpthread

all: reverse_proxy echo_server full_duplex_client

src/hc_evhtp.o: src/hc_evhtp.c include/hc.h include/hc_pool.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE $(EVHTP_CFLAGS) -c -o $@ $<

# Engine-independent parts: hc.h (and hc_pool.h) only.
src/hc_pool.o: src/hc_pool.c include/hc_pool.h include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

src/hc_host.o: src/hc_host.c include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

src/hc_support.o: src/hc_support.c include/hc_support.h include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

HC_OBJS = src/hc_evhtp.o src/hc_pool.o src/hc_host.o src/hc_support.o

# The examples compile against hc*.h only - no libevhtp/libevent/OpenSSL headers.
examples/%.o: examples/%.c include/hc.h include/hc_pool.h include/hc_support.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

reverse_proxy: examples/reverse_proxy.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

echo_server: examples/echo_server.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

full_duplex_client: examples/full_duplex_client.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

# Pool unit tests: fake connections, no sockets.
tests/pool_test: tests/pool_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -o $@ $^ $(EVHTP_LIBS)

# Support library + engine integration tests: in-process HTTP/HTTPS servers.
tests/support_test: tests/support_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -o $@ $^ $(EVHTP_LIBS)

check: all tests/pool_test tests/support_test
	tests/pool_test
	tests/support_test tests/tls
	tests/run.sh

clean:
	rm -f src/*.o examples/*.o reverse_proxy echo_server full_duplex_client tests/pool_test tests/support_test

.PHONY: all clean check
