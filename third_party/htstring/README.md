# htstring

Non-owning string views for C, plus an HTTP URI parser and an HTTP header
tokenizer built on them. Nothing is copied or allocated while parsing, and the
input is never modified: every result is an `htstring_t`, a pointer and a
length into the caller's buffer.

The code was extracted, with its history, from
[ziebarthw/libevhtp](https://github.com/ziebarthw/libevhtp), where its message
parser (modelled on httpcore5's `AbstractMessageParser`, with `htstring_t`
playing the role of its cursor) depends on it. It stands alone now so that
other projects, such as [hcore](https://github.com/ziebarthw/hcore), can use it
without depending on libevhtp.

It needs only the C library. The headers compile as C99 and later, and as C++.

## What's here

| File | What it is |
|---|---|
| `include/htstring/htstring.h` | `htstring_t` and its operations. Header-only. |
| `include/htstring/parsed_uri.h`, `src/parsed_uri.c` | URI parser: absolute, origin and authority (CONNECT) forms. |
| `include/htstring/tokenizer.h`, `src/tokenizer.c` | Header value tokenizer: parameterized lists, cookies, `Forwarded`, query strings. |

### `htstring_t`

```c
typedef struct htstring { const char *startp; size_t len; } htstring_t;
```

- **Making one:** `htstring_ctor(p, len)`, or `HTSTRING_LIT("literal")`.
- **Using it as a cursor:**
  - `htstring_consume(&cur, n)` when the stream advanced `n` bytes;
  - `htstring_advance(&cur, token)` moves just past a token and one optional separator;
  - `htstring_end(s)` gives the end pointer.
- **Shrinking it in place:** `htstring_trim` / `ltrim` / `rtrim` remove whitespace. `htstring_unquote` (`"…"` or `'…'`), `htstring_unltgt` (`<…>`) and `htstring_unbracket` (`[…]`) remove the enclosing characters.
- **Comparing:** `htstring_equals` / `equalsi` and `htstring_has_prefix` / `has_suffix`, plus their `i` variants. The `i` versions ignore ASCII case.
- **Getting a NUL-terminated copy when an API needs one:** `htstring_to_cstr(&scratch, s)`. It uses a 256-byte stack buffer (`HT_SCRATCH_SIZE`, overridable before the include) when the string fits and `malloc` otherwise. `ht_scratch_auto(name)` declares a scratch buffer that is freed automatically when it goes out of scope (GCC/Clang).
- **Concatenating into a NUL-terminated string:** `htstring_join_cstr(&scratch, parts, n)`, or `htstring_join_cstr_v(&scratch, a, HTSTRING_LIT("?"), b)` for an inline list. Same stack-or-heap rule, applied to the total length. A scratch can be reused: any earlier heap buffer is freed first.

```c
const char *line = "Content-Type:  text/html; charset=UTF-8\r\n";
htstring_t  v    = htstring_ctor(line + 13, strlen(line) - 13);

htstring_trim(&v);                                   /* "text/html; charset=UTF-8" */
if (htstring_has_prefixi(v, HTSTRING_LIT("TEXT/"))) { ... }
```

### URIs

```c
const char       *p = "https://user@example.org:8443/a/b?x=1#top";
struct parsed_uri u = parse_uri_view(htstring_ctor(p, strlen(p)), false);

if (!parsed_uri_has_error(&u)) {
    htstring_t host = parsed_uri_get_host(p, &u);    /* "example.org", points into p */
    int        port = parsed_uri_get_port(p, &u);    /* 8443, or -1 when absent */
    htstring_t path = parsed_uri_get_path(p, &u);    /* "/a/b" */
}
```

- **Request targets:** origin form (`/path?query`), absolute form, and `*` are all accepted.
- **CONNECT:** pass `true` for the authority form (`host:port`, `[v6]:port`). The port is then required and must be 1–65535.
- **IPv6:** the host is returned without its brackets, and `URI_IS_IPV6` is set. `parsed_uri_get_host_port` and `parsed_uri_get_authority` add the brackets back.
- **Building a URI:** `parsed_uri_join` and `parsed_uri_to_cstr` return a newly `malloc`ed string.
- **Limits:** components are stored as 16-bit offsets, so input longer than 65535 bytes is rejected.
- **Percent-decoding:** `htstring_pct_decode(path, buf, sizeof(buf))` turns `%XX` back into bytes and returns the length, or -1 for a bad escape or a full buffer. `+` stays `+`, and `%00` is decoded, so check for a NUL before using the result as a file name.

### Header values

- **`parse_parameterized_header`** walks `a; k=v, b; k=v` lists and calls your handler once per element. **`ht_element_iter`** does the same walk as a loop with no callback.
- **`parse_set_cookie`** and **`parse_cookie`** parse cookie headers. **`ht_cookie_iter`** is the loop form of `parse_cookie`.
- **`ht_forwarded_iter`** walks the hops of a `Forwarded` header.
- **`ht_param_iter`** walks `a=b&c=d` query strings.
- **`httokenizer_next`** splits on any delimiter set you give it.

Each element holds up to `HT_MAX_PARAMS` (16) parameters. The tests show each of these in use.

## Build, test, install

```sh
make              # libhtstring.a
make check        # unit tests
make install      # headers, libhtstring.a and htstring.pc under PREFIX
```

**Local settings.** Copy `config.mk.example` to `config.mk` and set them there. `config.mk` is git-ignored.
- `PREFIX` defaults to `/usr/local`.
- `PKGCONFIGDIR` defaults to `$(PREFIX)/lib/pkgconfig`.
- `DESTDIR` is prepended for staged installs.

**Installed layout:**
- Headers go to `$(PREFIX)/include/htstring/`, so code includes `"htstring/htstring.h"`.
- `pkg-config --cflags --libs htstring` gives `-I$(PREFIX)/include -L$(PREFIX)/lib -lhtstring`.

**Using it from a build system:**
- **meson:** `dependency('htstring')`.
- **CMake:** `pkg_check_modules(HTSTRING REQUIRED IMPORTED_TARGET htstring)`, then link `PkgConfig::HTSTRING`.

**Vendoring.** A project that only needs `htstring.h` can carry a copy instead, for example with `git subtree`. The headers use named include guards (`HTSTRING_H` and so on), so an installed copy and a vendored copy reached in the same build do not collide.

**Logging.** The `.c` files keep the logging they had in libevhtp:
- `log_info` lines go to stderr, for example when an element has too many parameters. Build with `-DHTSTRING_NO_LOG` to remove them.
- `log_debug` lines are compiled in only with `-DHTSTRING_DEBUG`.

`make compile_commands.json` writes a compilation database for clangd (VS Code).
