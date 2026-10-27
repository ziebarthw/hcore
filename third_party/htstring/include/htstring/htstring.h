
#ifndef HTSTRING_H
#define HTSTRING_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#if defined(__GNUC__) || defined(__clang__)
#  define ht_likely(x)    __builtin_expect(!!(x), 1)
#  define ht_unlikely(x)  __builtin_expect(!!(x), 0)
#else
#  define ht_likely(x)    (x)
#  define ht_unlikely(x)  (x)
#endif

#define HTSTRING_LIT(s) ((htstring_t){ .startp = (s), .len = sizeof(s) - 1 })

typedef struct htstring htstring_t;
struct htstring {
    const char * startp;
    size_t len;
};

static inline struct htstring
htstring_ctor(const char * startp, size_t len)
{
    return (struct htstring){
        .startp = startp,
        .len = len
    };
}

/*
 * Cursor primitives.
 *
 *   htstring_end()      - end pointer of a region.
 *   htstring_consume()  - count-driven: "the stream advanced n bytes".
 *                         For nread-driven loops only.
 *   htstring_advance()  - position-driven: "the cursor goes just past
 *                         this token, plus one optional separator byte".
 *                         For field parsers.
 *
 * Mandatory, typed separators (the SP after a status code, the ':'
 * after a header name) must be validated BEFORE being consumed —
 * advance() is for optional/anonymous separators only.
 */
static inline const char *
htstring_end(const htstring_t s)
{
    return s.startp + s.len;
}

static inline void
htstring_consume(htstring_t * cursor, size_t n)
{
    cursor->startp += n;
    cursor->len    -= n;
}

static inline void
htstring_advance(htstring_t * cursor, const htstring_t token)
{
    const char * endp  = cursor->startp + cursor->len;
    const char * nextp = token.startp + token.len;

    if (nextp < endp)
        ++nextp;                        /* one separator byte, if present */

    cursor->startp = nextp;
    cursor->len    = (size_t)(endp - nextp);
}

static inline bool
htstring_is_whitespace(unsigned char c)
{
    return (c == ' ' || c == '\t' || c == '\r' || c == '\n');
}

static inline htstring_t *
htstring_ltrim(htstring_t * self)
{
    while (self->len > 0 &&
        htstring_is_whitespace((unsigned char)*self->startp)) {
        self->startp++;
        self->len--;
    }
    return self;
}

static inline htstring_t *
htstring_rtrim(htstring_t * self)
{
    while (self->len > 0 &&
        htstring_is_whitespace((unsigned char)self->startp[self->len - 1])) {
        --self->len;
    }
    return self;
}

static inline htstring_t *
htstring_trim(htstring_t * self)
{
    return htstring_ltrim(htstring_rtrim(self));
}

static inline htstring_t *
htstring_unenclose(htstring_t * self, char cfirst, char clast)
{
    if (self->len >= 2) {
        char first = self->startp[0];
        char last  = self->startp[self->len - 1];

        if (first == cfirst && last == clast) {
            self->startp++;
            self->len -= 2;
        }
    }
    return self;
}

static inline htstring_t *
htstring_unquote(htstring_t * self)
{
    size_t slen = self->len;
    if (htstring_unenclose(self, '"', '"')->len != slen)
        return self;
    return htstring_unenclose(self, '\'', '\'');
}

static inline htstring_t *
htstring_unltgt(htstring_t * self)
{
    return htstring_unenclose(self, '<', '>');
}

static inline htstring_t *
htstring_unbracket(htstring_t * self)
{
    return htstring_unenclose(self, '[', ']');
}

static inline unsigned char
ht_ascii_lower(unsigned char c)
{
    return c | (unsigned char)(((unsigned)(c - 'A') < 26u) << 5);
}

static inline bool
htstring_equalsi(const htstring_t s1, const htstring_t s2)
{
    if (s1.len != s2.len) return false;
    for (size_t i = 0; i < s1.len; ++i) {
        if (ht_ascii_lower((unsigned char)s1.startp[i]) !=
            ht_ascii_lower((unsigned char)s2.startp[i])) {
            return false;
        }
    }
    return true;
}

static inline bool
htstring_equals(const htstring_t s1, const htstring_t s2)
{
    if (s1.len != s2.len) return false;
    return memcmp(s1.startp, s2.startp, s1.len) == 0;
}

static inline bool
htstring_has_prefixi(const htstring_t str, const htstring_t prefix)
{
    if (str.len < prefix.len) return false;
    htstring_t target = str;
    target.len = prefix.len;
    return htstring_equalsi(target, prefix);
}

static inline bool
htstring_has_prefix(const htstring_t str, const htstring_t prefix)
{
    if (str.len < prefix.len) return false;
    htstring_t target = str;
    target.len = prefix.len;
    return htstring_equals(target, prefix);
}

static inline bool
htstring_has_suffixi(const htstring_t str, const htstring_t suffix)
{
    if (str.len < suffix.len) return false;
    htstring_t target = str;
    target.startp += str.len - suffix.len;
    target.len = suffix.len;
    return htstring_equalsi(target, suffix);
}

static inline bool
htstring_has_suffix(const htstring_t str, const htstring_t suffix)
{
    if (str.len < suffix.len) return false;
    htstring_t target = str;
    target.startp += str.len - suffix.len;
    target.len = suffix.len;
    return htstring_equals(target, suffix);
}

/* Override before including this header to change the stack size. */
#ifndef HT_SCRATCH_SIZE
#define HT_SCRATCH_SIZE 256
#endif

typedef struct {
    char *ptr;
    char stack[HT_SCRATCH_SIZE];
} ht_scratch_t;

static inline void
ht_scratch_clear(ht_scratch_t * buf)
{
    if (buf->ptr && buf->ptr != buf->stack)
        free(buf->ptr);
    buf->ptr = NULL;
}

/*
 * Cleanup macro compatible with GLib's g_auto style.
 *
 * Only ptr is initialized: "= { .ptr = NULL }" would also zero-fill the
 * whole stack array on every call. Because this expands to a declaration
 * plus a statement, do not jump over it with goto.
 */
#define ht_scratch_auto(name) \
    ht_scratch_t name __attribute__((cleanup(ht_scratch_clear))); \
    name.ptr = NULL

/*
 * Reserves room for n bytes plus a NUL: the stack buffer when it fits,
 * malloc() otherwise. Any earlier heap buffer in buf is freed first, so a
 * scratch can be reused; buf->ptr must start out NULL (ht_scratch_auto or
 * "= { .ptr = NULL }"). Returns NULL on allocation failure or overflow.
 */
static inline char *
ht_scratch_reserve(ht_scratch_t * buf, size_t n)
{
    ht_scratch_clear(buf);
    if (ht_unlikely(n == SIZE_MAX))
        return NULL;
    if (ht_likely(n < sizeof(buf->stack)))
        buf->ptr = buf->stack;
    else
        buf->ptr = (char*)malloc(n + 1);
    return buf->ptr;
}

/*
 * Concatenates nparts strings into one NUL-terminated C string in buf
 * (stack when the total fits, heap otherwise). NULL on failure.
 */
static inline const char *
htstring_join_cstr(ht_scratch_t * buf, const htstring_t * parts, size_t nparts)
{
    size_t len = 0;
    char * p;

    for (size_t i = 0; i < nparts; ++i) {
        if (ht_unlikely(parts[i].len > SIZE_MAX - 1 - len))
            return NULL;
        len += parts[i].len;
    }

    if ((p = ht_scratch_reserve(buf, len)) == NULL)
        return NULL;

    for (size_t i = 0; i < nparts; ++i) {
        if (parts[i].len > 0) {     /* memcpy with a NULL pointer is UB */
            memcpy(p, parts[i].startp, parts[i].len);
            p += parts[i].len;
        }
    }
    *p = '\0';

    return buf->ptr;
}

/* htstring_join_cstr() over an inline list:
 *   htstring_join_cstr_v(&buf, path, HTSTRING_LIT("?"), query) */
#define htstring_join_cstr_v(buf, ...)                                   \
    htstring_join_cstr((buf), (const htstring_t[]){ __VA_ARGS__ },       \
        sizeof((const htstring_t[]){ __VA_ARGS__ }) / sizeof(htstring_t))

/* Converts an htstring_t into a guaranteed NUL-terminated C string */
static inline const char *
htstring_to_cstr(ht_scratch_t * buf, const htstring_t s)
{
    return htstring_join_cstr(buf, &s, 1);
}

#endif
