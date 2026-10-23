# htstring: non-owning string views (htstring_t) plus the URI parser and
# header tokenizer built on them. No dependencies beyond the C library.
#
# Local settings: put them in config.mk (see config.mk.example). It is read
# if present and ignored by git, so machine-specific paths never end up in
# the repository. Values given on the make command line still win.
-include config.mk

# Where `make install` puts the headers (PREFIX/include/htstring),
# libhtstring.a (PREFIX/lib) and htstring.pc (PKGCONFIGDIR, for pkg-config,
# CMake's pkg_check_modules and meson's dependency('htstring')). DESTDIR is
# prepended for staged installs (packaging, Docker).
PREFIX       ?= /usr/local
PKGCONFIGDIR ?= $(PREFIX)/lib/pkgconfig
DESTDIR      ?=
HT_VERSION    = 0.1.0

CC      ?= cc
AR      ?= ar
CFLAGS  ?= -O2 -g
# -fPIC: libhtstring.a may end up inside a shared library (libevhtp.so).
HT_CFLAGS = -std=gnu11 -Wall -Wextra -fPIC -Iinclude

HT_HDRS = include/htstring/htstring.h include/htstring/parsed_uri.h include/htstring/tokenizer.h
HT_OBJS = src/parsed_uri.o src/tokenizer.o
TESTS   = tests/htstring_test tests/parsed_uri_test tests/tokenizer_test

all: libhtstring.a

src/%.o: src/%.c $(HT_HDRS) src/ht_log.h
	$(CC) $(HT_CFLAGS) $(CFLAGS) -c -o $@ $<

libhtstring.a: $(HT_OBJS)
	rm -f $@
	$(AR) rcs $@ $^

tests/%: tests/%.c tests/check.h $(HT_HDRS) libhtstring.a
	$(CC) $(HT_CFLAGS) $(CFLAGS) -o $@ $< libhtstring.a

check: $(TESTS)
	@for t in $(TESTS); do $$t || exit 1; done

# Copies the headers, libhtstring.a and htstring.pc under PREFIX. Run it again
# after changing htstring, so projects using the installed copy see it.
install: libhtstring.a htstring.pc
	install -d $(DESTDIR)$(PREFIX)/include/htstring $(DESTDIR)$(PREFIX)/lib $(DESTDIR)$(PKGCONFIGDIR)
	install -m 644 $(HT_HDRS) $(DESTDIR)$(PREFIX)/include/htstring
	install -m 644 libhtstring.a $(DESTDIR)$(PREFIX)/lib
	install -m 644 htstring.pc $(DESTDIR)$(PKGCONFIGDIR)

# pkg-config file, regenerated on every install because it records PREFIX,
# which may come from the command line.
htstring.pc:
	@printf '%s\n' \
	  'prefix=$(PREFIX)' \
	  'includedir=$${prefix}/include' \
	  'libdir=$${prefix}/lib' \
	  '' \
	  'Name: htstring' \
	  'Description: Non-owning string views, URI parser and HTTP header tokenizer' \
	  'Version: $(HT_VERSION)' \
	  'URL: https://github.com/ziebarthw/htstring' \
	  'Cflags: -I$${includedir}' \
	  'Libs: -L$${libdir} -lhtstring' > $@
	@echo "wrote $@"

# Editor support (clangd): the exact compile command for every source file.
compile_commands.json: Makefile $(wildcard config.mk)
	@{ printf '['; sep=''; \
	   for f in $(sort $(wildcard src/*.c tests/*.c)); do \
	     printf '%s\n  {"directory": "%s", "file": "%s", "command": "%s"}' "$$sep" "$(CURDIR)" "$$f" \
	       "$(CC) $(HT_CFLAGS) $(CFLAGS) -c $$f"; sep=','; \
	   done; \
	   printf '\n]\n'; } > $@
	@echo "wrote $@"

clean:
	rm -f libhtstring.a htstring.pc src/*.o $(TESTS)

.PHONY: all check install clean htstring.pc
