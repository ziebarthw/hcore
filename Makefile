# Point EVHTP_PREFIX at a libevhtp install (headers in include/evhtp,
# libevhtp.a in lib). libevent 2.1 and OpenSSL headers/libs must be on the
# default paths. EXTRA_LIBS adds libraries your libevhtp build needs, e.g.
# EXTRA_LIBS="-lhtstring -lnghttp2" for the ziebarthw/libevhtp fork (htstring,
# HTTP/2).
#
# Local settings: put them in config.mk (see config.mk.example). It is read
# if present and ignored by git, so machine-specific paths never end up in
# the repository. Values given on the make command line still win.
-include config.mk

EVHTP_PREFIX ?= /usr/local
EXTRA_LIBS   ?=
# Where `make install` puts the headers (PREFIX/include), libhcore.a
# (PREFIX/lib) and libhcore.pc (PKGCONFIGDIR, for pkg-config and meson's
# dependency('libhcore')). DESTDIR is prepended for staged installs
# (packaging, Docker).
PREFIX       ?= /usr/local
PKGCONFIGDIR ?= $(PREFIX)/lib/pkgconfig
HC_VERSION    = 0.1.0
DESTDIR      ?=

CC      ?= cc
CFLAGS  ?= -O2 -g
# third_party/htstring: htstring_t, vendored with git subtree (see README).
# Only its header-only htstring.h is used, so nothing of it is linked or
# installed, and it comes before libevhtp's include path.
HC_CFLAGS = -std=c99 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Iinclude -Ithird_party/htstring/include
# Only include/, as libevhtp's pkg-config file says: its headers are included
# as <evhtp/...>, and older versions of the ziebarthw/libevhtp fork put a
# string.h in include/evhtp that would shadow the system header.
EVHTP_CFLAGS = -I$(EVHTP_PREFIX)/include
EVHTP_LIBS   = $(EVHTP_PREFIX)/lib/libevhtp.a $(EXTRA_LIBS) -levent -levent_openssl -levent_pthreads -lssl -lcrypto -lpthread

all: libhcore.a reverse_proxy echo_server full_duplex_client full_duplex_server file_server

src/hc_evhtp.o: src/hc_evhtp.c include/hc.h include/hc_pool.h include/hc_processor.h third_party/htstring/include/htstring/htstring.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE $(EVHTP_CFLAGS) -c -o $@ $<

# Engine-independent parts: hc.h (and hc_pool.h) only.
src/hc_pool.o: src/hc_pool.c include/hc_pool.h include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

src/hc_host.o: src/hc_host.c include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

src/hc_support.o: src/hc_support.c include/hc_support.h include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

src/hc_processor.o: src/hc_processor.c include/hc_processor.h include/hc.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

HC_OBJS = src/hc_evhtp.o src/hc_pool.o src/hc_host.o src/hc_support.o src/hc_processor.o
HC_HDRS = include/hc.h include/hc_pool.h include/hc_support.h include/hc_processor.h

# The library other projects link with: -lhcore, then libevhtp and what it needs
# (the EVHTP_LIBS list below).
libhcore.a: $(HC_OBJS)
	rm -f $@
	$(AR) rcs $@ $^

# The examples compile against hc*.h only - no libevhtp/libevent/OpenSSL headers.
examples/%.o: examples/%.c include/hc.h include/hc_pool.h include/hc_support.h include/hc_processor.h
	$(CC) $(HC_CFLAGS) $(CFLAGS) -c -o $@ $<

reverse_proxy: examples/reverse_proxy.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

echo_server: examples/echo_server.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

full_duplex_client: examples/full_duplex_client.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

full_duplex_server: examples/full_duplex_server.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

file_server: examples/file_server.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

hcurl: examples/hcurl.o $(HC_OBJS)
	$(CC) -o $@ $^ $(EVHTP_LIBS)

# Pool unit tests: fake connections, no sockets.
tests/pool_test: tests/pool_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -o $@ $^ $(EVHTP_LIBS)

# Support library + engine integration tests: in-process HTTP/HTTPS servers.
tests/support_test: tests/support_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -o $@ $^ $(EVHTP_LIBS)

# hcore inside the application's own event loop: reuse_port, pause/resume,
# graceful drain.
tests/embed_test: tests/embed_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE -o $@ $^ $(EVHTP_LIBS)

# Freeing a requester while the application's loop keeps running.
tests/shutdown_test: tests/shutdown_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -o $@ $^ $(EVHTP_LIBS)

# https to an address, presenting and verifying a host name (hc_host.tls_name).
tests/tls_name_test: tests/tls_name_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -o $@ $^ $(EVHTP_LIBS)

# Interceptors and processors on both sides of real exchanges.
tests/processor_test: tests/processor_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE -o $@ $^ $(EVHTP_LIBS)

# Client and server timeouts, request body limit.
tests/timeout_test: tests/timeout_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE -o $@ $^ $(EVHTP_LIBS) -lpthread

# hc_basic_server_factory(): whole requests, checked with curl.
tests/basic_server_test: tests/basic_server_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE -o $@ $^ $(EVHTP_LIBS) -lpthread

# Server TLS options, checked with openssl s_client.
tests/tls_opts_test: tests/tls_opts_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE -o $@ $^ $(EVHTP_LIBS) -lpthread

# Server statistics, endpoint attachments.
tests/stats_test: tests/stats_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) -D_DEFAULT_SOURCE -o $@ $^ $(EVHTP_LIBS) -lpthread

# HTTP/2 server (only with a libevhtp that has HTTP/2: the ziebarthw fork
# built with nghttp2; needs curl with HTTP/2 to run).
HAVE_H2 := $(shell grep -qs EVHTP_CONN_FLAG_IS_HTTP2 $(EVHTP_PREFIX)/include/evhtp/evhtp.h && echo yes)
H2_TEST := $(if $(HAVE_H2),tests/h2_test)

# hc_server_abort() resetting one stream needs evhtp_request_reset_stream().
HAVE_RESET := $(shell grep -qs EVHTP_HAVE_REQUEST_RESET_STREAM $(EVHTP_PREFIX)/include/evhtp/evhtp.h && echo yes)

tests/h2_test: tests/h2_test.c $(HC_OBJS)
	$(CC) $(HC_CFLAGS) $(CFLAGS) $(if $(HAVE_RESET),-DHC_TEST_STREAM_RESET) -o $@ $^ $(EVHTP_LIBS)

check: all tests/pool_test tests/support_test tests/embed_test tests/shutdown_test tests/tls_name_test tests/processor_test tests/timeout_test tests/stats_test tests/tls_opts_test tests/basic_server_test $(H2_TEST)
	tests/pool_test
	tests/support_test tests/tls
	tests/embed_test
	tests/shutdown_test
	tests/tls_name_test tests/tls
	tests/processor_test
	tests/timeout_test tests/tls
	tests/stats_test tests/tls
	tests/tls_opts_test tests/tls
	tests/basic_server_test tests/tls
	$(if $(HAVE_H2),tests/h2_test tests/tls,@echo "h2_test: skipped (libevhtp without HTTP/2)")
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

# Copies the public headers, libhcore.a and libhcore.pc under PREFIX (nothing
# else). Run it again after changing hcore, so projects using the installed
# copy see it.
install: libhcore.a libhcore.pc
	install -d $(DESTDIR)$(PREFIX)/include $(DESTDIR)$(PREFIX)/lib $(DESTDIR)$(PKGCONFIGDIR)
	install -m 644 $(HC_HDRS) $(DESTDIR)$(PREFIX)/include
	install -m 644 libhcore.a $(DESTDIR)$(PREFIX)/lib
	install -m 644 libhcore.pc $(DESTDIR)$(PKGCONFIGDIR)

# pkg-config file, regenerated on every install because it records PREFIX and
# EVHTP_PREFIX, which may come from the command line. libhcore.a is a static
# library, so what it needs from libevhtp, libevent and OpenSSL is in the
# .private fields: pkg-config --static (and meson with static: true) adds
# them; a plain lookup gives just -lhcore, for applications that link those
# libraries themselves.
libhcore.pc:
	@printf '%s\n' \
	  'prefix=$(PREFIX)' \
	  'includedir=$${prefix}/include' \
	  'libdir=$${prefix}/lib' \
	  '' \
	  'Name: libhcore' \
	  'Description: HttpCore-style async HTTP/1.1 client and server on libevhtp' \
	  'Version: $(HC_VERSION)' \
	  'URL: https://github.com/ziebarthw/hcore' \
	  'Requires.private: libevent libevent_openssl libevent_pthreads openssl' \
	  'Cflags: -I$${includedir}' \
	  'Libs: -L$${libdir} -lhcore' \
	  'Libs.private: $(EVHTP_PREFIX)/lib/libevhtp.a $(EXTRA_LIBS) -lpthread' > $@
	@echo "wrote $@"

clean:
	rm -f libhcore.a libhcore.pc src/*.o examples/*.o reverse_proxy echo_server full_duplex_client full_duplex_server file_server tests/pool_test tests/support_test tests/embed_test tests/shutdown_test tests/tls_name_test tests/h2_test tests/processor_test tests/timeout_test tests/stats_test tests/tls_opts_test tests/basic_server_test

.PHONY: all clean check install libhcore.pc
