/* htstring.h: views, cursors, trimming, comparisons, C-string conversion. */
#include <stdlib.h>
#include "check.h"

static void
test_ctor_and_cursors(void)
{
    const char *text = "GET /index.html HTTP/1.1";
    htstring_t  s    = htstring_ctor(text, strlen(text));
    htstring_t  lit  = HTSTRING_LIT("abc");

    CHECK(s.startp == text && s.len == 24);
    CHECK(lit.len == 3);
    CHECK(htstring_end(s) == text + 24);

    /* consume: the stream advanced n bytes */
    htstring_t cur = s;
    htstring_consume(&cur, 4);
    CHECK_STR(cur, "/index.html HTTP/1.1");

    /* advance: past a token plus one separator */
    htstring_t token = htstring_ctor(cur.startp, 11);   /* "/index.html" */
    htstring_advance(&cur, token);
    CHECK_STR(cur, "HTTP/1.1");

    /* advance over the last token: no separator to skip, nothing left */
    htstring_advance(&cur, cur);
    CHECK(cur.len == 0 && cur.startp == htstring_end(s));
}

static void
test_trim_and_unenclose(void)
{
    char       buf[] = " \t \"quoted value\" \r\n";
    htstring_t s     = htstring_ctor(buf, sizeof(buf) - 1);

    CHECK_STR(*htstring_ltrim(&s), "\"quoted value\" \r\n");
    CHECK_STR(*htstring_rtrim(&s), "\"quoted value\"");
    CHECK_STR(*htstring_unquote(&s), "quoted value");
    CHECK_STR(*htstring_unquote(&s), "quoted value");    /* nothing left to strip */

    htstring_t t = HTSTRING_LIT("  both  ");
    CHECK_STR(*htstring_trim(&t), "both");

    htstring_t all_ws = HTSTRING_LIT(" \t\r\n");
    CHECK(htstring_trim(&all_ws)->len == 0);

    htstring_t sq = HTSTRING_LIT("'single'");
    CHECK_STR(*htstring_unquote(&sq), "single");

    htstring_t mixed = HTSTRING_LIT("\"mixed'");          /* mismatched: unchanged */
    CHECK_STR(*htstring_unquote(&mixed), "\"mixed'");

    htstring_t angle = HTSTRING_LIT("<https://example.org/>");
    CHECK_STR(*htstring_unltgt(&angle), "https://example.org/");

    htstring_t v6 = HTSTRING_LIT("[2001:db8::1]");
    CHECK_STR(*htstring_unbracket(&v6), "2001:db8::1");

    htstring_t one = HTSTRING_LIT("\"");                  /* too short to unquote */
    CHECK(htstring_unquote(&one)->len == 1);

    htstring_t empty_quotes = HTSTRING_LIT("\"\"");
    CHECK(htstring_unquote(&empty_quotes)->len == 0);
}

static void
test_compare(void)
{
    CHECK(htstring_equals(HTSTRING_LIT("Host"), HTSTRING_LIT("Host")));
    CHECK(!htstring_equals(HTSTRING_LIT("Host"), HTSTRING_LIT("host")));
    CHECK(htstring_equalsi(HTSTRING_LIT("Content-Type"), HTSTRING_LIT("content-TYPE")));
    CHECK(!htstring_equalsi(HTSTRING_LIT("abc"), HTSTRING_LIT("abcd")));
    CHECK(htstring_equals(HTSTRING_LIT(""), HTSTRING_LIT("")));

    /* only A-Z fold: '@' (0x40) and '`' (0x60), '[' (0x5b) and '{' (0x7b)
     * differ by the same bit as a case change */
    CHECK(!htstring_equalsi(HTSTRING_LIT("@"), HTSTRING_LIT("`")));
    CHECK(!htstring_equalsi(HTSTRING_LIT("["), HTSTRING_LIT("{")));
    CHECK(ht_ascii_lower('Z') == 'z' && ht_ascii_lower('z') == 'z' && ht_ascii_lower('5') == '5');
    CHECK(ht_ascii_lower(0xC0) == 0xC0);

    htstring_t path = HTSTRING_LIT("/api/v2/users.json");
    CHECK(htstring_has_prefix(path, HTSTRING_LIT("/api/")));
    CHECK(!htstring_has_prefix(path, HTSTRING_LIT("/API/")));
    CHECK(htstring_has_prefixi(path, HTSTRING_LIT("/API/")));
    CHECK(htstring_has_suffix(path, HTSTRING_LIT(".json")));
    CHECK(htstring_has_suffixi(path, HTSTRING_LIT(".JSON")));
    CHECK(!htstring_has_suffix(path, HTSTRING_LIT(".JSON")));
    CHECK(!htstring_has_prefix(HTSTRING_LIT("/a"), HTSTRING_LIT("/abc")));   /* longer prefix */
    CHECK(!htstring_has_suffix(HTSTRING_LIT("c"), HTSTRING_LIT("abc")));
    CHECK(htstring_has_prefix(path, HTSTRING_LIT("")));
}

static void
test_to_cstr(void)
{
    /* a view into the middle of a buffer: no NUL where it ends */
    const char *text  = "Host: example.org\r\n";
    htstring_t  value = htstring_ctor(text + 6, 11);
    ht_scratch_t buf  = { .ptr = NULL };

    const char *c = htstring_to_cstr(&buf, value);
    CHECK(c != NULL && strcmp(c, "example.org") == 0);
    CHECK(buf.ptr == buf.stack);                /* short: no allocation */
    ht_scratch_clear(&buf);

    /* longer than the stack buffer: heap, freed by ht_scratch_clear */
    char big[1000];
    memset(big, 'x', sizeof(big));
    c = htstring_to_cstr(&buf, htstring_ctor(big, sizeof(big)));
    CHECK(c != NULL && strlen(c) == sizeof(big) && buf.ptr != buf.stack);
    ht_scratch_clear(&buf);
    CHECK(buf.ptr == NULL);

    /* exactly at the boundary: 255 fits the 256-byte stack with its NUL */
    c = htstring_to_cstr(&buf, htstring_ctor(big, HT_SCRATCH_SIZE - 1));
    CHECK(c != NULL && buf.ptr == buf.stack && strlen(c) == HT_SCRATCH_SIZE - 1);
    c = htstring_to_cstr(&buf, htstring_ctor(big, HT_SCRATCH_SIZE));
    CHECK(c != NULL && buf.ptr != buf.stack && strlen(c) == HT_SCRATCH_SIZE);
    ht_scratch_clear(&buf);

    CHECK(strcmp(htstring_to_cstr(&buf, HTSTRING_LIT("")), "") == 0);

    {
        ht_scratch_auto(autobuf);               /* freed when the block ends */
        c = htstring_to_cstr(&autobuf, htstring_ctor(big, sizeof(big)));
        CHECK(c != NULL && strlen(c) == sizeof(big));
    }
}

static void
test_join_cstr(void)
{
    ht_scratch_t buf = { .ptr = NULL };
    htstring_t   path  = HTSTRING_LIT("/a/b");
    htstring_t   query = HTSTRING_LIT("x=1&y=2");

    const char *c = htstring_join_cstr_v(&buf, path, HTSTRING_LIT("?"), query);
    CHECK(c != NULL && strcmp(c, "/a/b?x=1&y=2") == 0);
    CHECK(buf.ptr == buf.stack);

    /* empty and NULL-pointer parts contribute nothing */
    c = htstring_join_cstr_v(&buf, htstring_ctor(NULL, 0), path, HTSTRING_LIT(""));
    CHECK(c != NULL && strcmp(c, "/a/b") == 0);

    c = htstring_join_cstr(&buf, NULL, 0);
    CHECK(c != NULL && strcmp(c, "") == 0 && buf.ptr == buf.stack);

    /* total over the stack size goes to the heap... */
    char big[HT_SCRATCH_SIZE];
    memset(big, 'q', sizeof(big));
    c = htstring_join_cstr_v(&buf, path, HTSTRING_LIT("?"), htstring_ctor(big, sizeof(big)));
    CHECK(c != NULL && buf.ptr != buf.stack && strlen(c) == 5 + sizeof(big));

    /* ...and reusing the scratch frees it and goes back to the stack */
    c = htstring_join_cstr_v(&buf, path, HTSTRING_LIT("?"), query);
    CHECK(c != NULL && buf.ptr == buf.stack && strcmp(c, "/a/b?x=1&y=2") == 0);

    /* boundary: total of HT_SCRATCH_SIZE - 1 still fits with its NUL */
    c = htstring_join_cstr_v(&buf, htstring_ctor(big, 10),
                             htstring_ctor(big, HT_SCRATCH_SIZE - 11));
    CHECK(c != NULL && buf.ptr == buf.stack && strlen(c) == HT_SCRATCH_SIZE - 1);
    c = htstring_join_cstr_v(&buf, htstring_ctor(big, 10),
                             htstring_ctor(big, HT_SCRATCH_SIZE - 10));
    CHECK(c != NULL && buf.ptr != buf.stack && strlen(c) == HT_SCRATCH_SIZE);
    ht_scratch_clear(&buf);

    /* overflowing total length: refused, nothing allocated */
    htstring_t huge[2] = { htstring_ctor(big, SIZE_MAX / 2 + 1),
                           htstring_ctor(big, SIZE_MAX / 2 + 1) };
    CHECK(htstring_join_cstr(&buf, huge, 2) == NULL && buf.ptr == NULL);

    {
        ht_scratch_auto(autobuf);
        CHECK(autobuf.ptr == NULL);
        c = htstring_join_cstr_v(&autobuf, path, htstring_ctor(big, sizeof(big)));
        CHECK(c != NULL && strlen(c) == 4 + sizeof(big));
    }
}

static void
test_split(void)
{
    htstring_t seg[8];
    ptrdiff_t  n;

    n = htstring_split(HTSTRING_LIT("/a/b.txt"), '/', seg, 8);
    CHECK(n == 3);
    CHECK_STR(seg[0], "");
    CHECK_STR(seg[1], "a");
    CHECK_STR(seg[2], "b.txt");

    /* empty parts are kept, at either end and in the middle */
    n = htstring_split(HTSTRING_LIT("/a//b/"), '/', seg, 8);
    CHECK(n == 5);
    CHECK_STR(seg[2], "");
    CHECK_STR(seg[3], "b");
    CHECK_STR(seg[4], "");

    /* no delimiter: one part, the whole string; "" is one empty part */
    n = htstring_split(HTSTRING_LIT("abc"), '/', seg, 8);
    CHECK(n == 1);
    CHECK_STR(seg[0], "abc");
    n = htstring_split(HTSTRING_LIT(""), '/', seg, 8);
    CHECK(n == 1 && seg[0].len == 0);
    n = htstring_split(HTSTRING_LIT("/"), '/', seg, 8);
    CHECK(n == 2 && seg[0].len == 0 && seg[1].len == 0);

    /* the parts point into the input */
    {
        const char *p = "x,yy,z";

        n = htstring_split(htstring_ctor(p, strlen(p)), ',', seg, 8);
        CHECK(n == 3 && seg[1].startp == p + 2 && seg[1].len == 2);
    }

    /* too many: -1, the first max filled in; exactly max: fine */
    n = htstring_split(HTSTRING_LIT("a/b/c"), '/', seg, 2);
    CHECK(n == -1);
    CHECK_STR(seg[0], "a");
    CHECK_STR(seg[1], "b");
    CHECK(htstring_split(HTSTRING_LIT("a/b/c"), '/', seg, 3) == 3);
    CHECK(htstring_split(HTSTRING_LIT("a"), '/', seg, 0) == -1);

    /* counting only */
    CHECK(htstring_split(HTSTRING_LIT("/a//b/"), '/', NULL, 0) == 5);
    CHECK(htstring_split(HTSTRING_LIT(""), '/', NULL, 0) == 1);

    /* a path's segments: ".." found as a whole segment only */
    {
        int dotdot = 0;

        n = htstring_split(HTSTRING_LIT("/a/..x/../b"), '/', seg, 8);
        for (ptrdiff_t i = 0; i < n; i++)
            dotdot += htstring_equals(seg[i], HTSTRING_LIT(".."));
        CHECK(n == 5 && dotdot == 1);
    }
}

static void
test_pct_decode(void)
{
    char      out[64];
    ptrdiff_t n;

#define DECODE(lit) htstring_pct_decode(HTSTRING_LIT(lit), out, sizeof(out))

    /* plain text is copied; escapes in either case are decoded */
    n = DECODE("/a/b.txt");
    CHECK(n == 8 && memcmp(out, "/a/b.txt", 8) == 0);
    n = DECODE("/a%20b%2Fc%2fd");
    CHECK(n == 8 && memcmp(out, "/a b/c/d", 8) == 0);
    n = DECODE("%41%7a%7A%e2%82%ac");       /* "Azz" and the euro sign's UTF-8 */
    CHECK(n == 6 && memcmp(out, "Azz\xe2\x82\xac", 6) == 0);
    n = DECODE("%25%2525");                 /* "%%25": decoded once only */
    CHECK(n == 4 && memcmp(out, "%%25", 4) == 0);
    n = DECODE("a+b");                      /* '+' is not a space in a path */
    CHECK(n == 3 && memcmp(out, "a+b", 3) == 0);
    n = DECODE("");
    CHECK(n == 0);

    /* "%00" decodes; refusing it is the caller's call */
    n = DECODE("a%00b");
    CHECK(n == 3 && memcmp(out, "a\0b", 3) == 0 && memchr(out, '\0', (size_t)n) != NULL);

    /* bad escapes: refused */
    CHECK(DECODE("%") == -1);
    CHECK(DECODE("abc%") == -1);
    CHECK(DECODE("abc%4") == -1);
    CHECK(DECODE("%zz") == -1);
    CHECK(DECODE("%4g") == -1);
    CHECK(DECODE("%g4") == -1);
    CHECK(DECODE("% 41") == -1);

    /* a dst too small: refused; exactly big enough: fine */
    CHECK(htstring_pct_decode(HTSTRING_LIT("abc"), out, 2) == -1);
    CHECK(htstring_pct_decode(HTSTRING_LIT("%41%42"), out, 1) == -1);
    CHECK(htstring_pct_decode(HTSTRING_LIT("%41%42"), out, 2) == 2 && memcmp(out, "AB", 2) == 0);
    CHECK(htstring_pct_decode(HTSTRING_LIT(""), NULL, 0) == 0);

    /* in place */
    {
        char buf[] = "/x%2Fy%20z";

        n = htstring_pct_decode(htstring_ctor(buf, strlen(buf)), buf, sizeof(buf));
        CHECK(n == 6 && memcmp(buf, "/x/y z", 6) == 0);
    }

    /* only the view is decoded, not what follows it */
    {
        const char *p = "/a%20b?q=%zz";

        n = htstring_pct_decode(htstring_ctor(p, 7), out, sizeof(out));
        CHECK(n == 5 && memcmp(out, "/a b?", 5) == 0);
    }
#undef DECODE
}

int
main(void)
{
    test_ctor_and_cursors();
    test_trim_and_unenclose();
    test_compare();
    test_to_cstr();
    test_join_cstr();
    test_split();
    test_pct_decode();
    return check_done("htstring_test");
}
