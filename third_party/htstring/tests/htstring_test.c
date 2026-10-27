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

int
main(void)
{
    test_ctor_and_cursors();
    test_trim_and_unenclose();
    test_compare();
    test_to_cstr();
    test_join_cstr();
    return check_done("htstring_test");
}
