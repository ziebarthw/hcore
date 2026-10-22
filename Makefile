# Point EVHTP_PREFIX at a libevhtp install (headers in include/evhtp,
# libevhtp.a in lib). libevent 2.1 and OpenSSL headers/libs must be on the
# default paths. EXTRA_LIBS adds libraries your libevhtp build needs, e.g.
# EXTRA_LIBS=-lnghttp2 for a libevhtp with HTTP/2 (ziebarthw/libevhtp).
#
# Local settings: put them in config.mk (see config.mk.example). It is read
# if present and ignored by git, so machine-specific paths never end up in
# the repository. Values given on the make command line still win.
-include config.mk

EVHTP_PREFIX ?= /usr/local
EXTRA_LIBS   ?=
# Where `make install` puts the headers (PREFIX/include) and libhcore.a
# (PREFIX/lib). DESTDIR is prepended for staged installs (packaging, Docker).
PREFIX       ?= /usr/local
DESTDIR      ?=

CC      ?= cc
CFLAGS  ?= -O2 -g
HC_CFLAGS = -std=c99 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude
# Only include/, as libevhtp's pkg-config file says: its headers are included
# as <evhtp/...>, and include/evhtp itself holds names (string.h) that would
# shadow the system headers.
EVHTP_CFLAGS = -I$(EVHTP_PREFIX)/include
EVHTP_LIBS   = $(EVHTP_PREFIX)/lib/libevhtp.a $(EXTRA_LIBS) -levent -levent_openssl -levent_pthreads -lssl -lcrypto -lpthread

all: libhcore.a reverse_proxy echo_server full_duplex_client full_duplex_server

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
HC_HDRS = include/hc.h include/hc_pool.h include/hc_support.h

# The library other projects link with: -lhcore, then libevhtp and what it needs
# (the EVHTP_LIBS list below).
libhcore.a: $(HC_OBJS)
	rm -f $@
	$(AR) rcs $@ $^

# The examples compile against hc*.h only - no libevhtp/libevent/OpenSSL headers.
examples/%.o: examples/%.c include/hc.h include/hc_pool.h include/hc_support.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

reverse_proxy: examples/reverse_proxy.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

echo_server: examples/echo_server.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

full_duplex_client: examples/full_duplex_client.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

full_duplex_server: examples/full_duplex_server.o $(HC_OBJS)
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

# Editor support (clangd, e.g. the VS Code clangd extension): a compilation
# database listing the exact command for every source file, so the editor
# finds include/ and libevhtp's headers. It holds this machine's paths
# (config.mk), so it is git-ignored; rerun after changing config.mk.
ENGINE_SRCS = src/hc_evhtp.c
PLAIN_SRCS  = $(filter-out $(ENGINE_SRCS),$(sort $(wildcard src/*.c examples/*.c tests/*.c)))

compile_commands.json: Makefile $(wildcard config.mk)
	@{ printf '['; sep=''; \
	   for f in $(ENGINE_SRCS); do \
	     printf '%s\n  {"directory": "%s", "file": "%s", "command": "%s"}' "$$sep" "$(CURDIR)" "$$f" \
	       "$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE $(EVHTP_CFLAGS) -c $$f"; sep=','; \
	   done; \
	   for f in $(PLAIN_SRCS); do \
	     printf '%s\n  {"directory": "%s", "file": "%s", "command": "%s"}' "$$sep" "$(CURDIR)" "$$f" \
	       "$(CC) $(HC_CFLAGS) $(CFLAGS) -c $$f"; sep=','; \
	   done; \
	   printf '\n]\n'; } > $@
	@echo "wrote $@ ($(words $(ENGINE_SRCS) $(PLAIN_SRCS)) files)"

# Copies the public headers and libhcore.a under PREFIX (nothing else). Run it
# again after changing hcore, so projects using the installed copy see it.
install: libhcore.a
	install -d $(DESTDIR)$(PREFIX)/include $(DESTDIR)$(PREFIX)/lib
	install -m 644 $(HC_HDRS) $(DESTDIR)$(PREFIX)/include
	install -m 644 libhcore.a $(DESTDIR)$(PREFIX)/lib

clean:
	rm -f libhcore.a src/*.o examples/*.o reverse_proxy echo_server full_duplex_client full_duplex_server tests/pool_test tests/support_test

.PHONY: all clean check install
