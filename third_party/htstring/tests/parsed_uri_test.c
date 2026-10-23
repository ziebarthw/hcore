/* parsed_uri: components are views into the caller's string. */
#include <stdlib.h>
#include "check.h"
#include "htstring/parsed_uri.h"

/* Parse a NUL-terminated string. */
static struct parsed_uri
parse(const char *s, bool is_connect)
{
    return parse_uri_view(htstring_ctor(s, strlen(s)), is_connect);
}

static void
test_absolute_form(void)
{
    const char        *p = "http://user:pw@example.com:8080/a/b?x=1&y=2#frag";
    struct parsed_uri  u = parse(p, false);

    CHECK(!parsed_uri_has_error(&u));
    CHECK_STR(parsed_uri_get_scheme(p, &u), "http");
    CHECK_STR(parsed_uri_get_user_info(p, &u), "user:pw");
    CHECK_STR(parsed_uri_get_host(p, &u), "example.com");
    CHECK(parsed_uri_get_port(p, &u) == 8080);
    CHECK_STR(parsed_uri_get_path(p, &u), "/a/b");
    CHECK_STR(parsed_uri_get_query(p, &u), "x=1&y=2");
    CHECK_STR(parsed_uri_get_fragment(p, &u), "frag");
    CHECK_STR(parsed_uri_get_authority(p, &u), "user:pw@example.com:8080");
    CHECK_STR(parsed_uri_get_host_port(p, &u), "example.com:8080");
    CHECK(!(u.flags & URI_IS_IPV6) && !parsed_uri_is_authority_form(&u));

    /* views point into the original string: no copies */
    CHECK(parsed_uri_get_host(p, &u).startp == p + 15);

    char *again = parsed_uri_to_cstr(p, &u);
    CHECK(again && strcmp(again, p) == 0);
    free(again);

    const char        *q = "https://example.org";
    struct parsed_uri  v = parse(q, false);
    CHECK(!parsed_uri_has_error(&v));
    CHECK(!parsed_uri_has_port(&v) && parsed_uri_get_port(q, &v) == -1);
    CHECK(!parsed_uri_has_user_info(&v) && !parsed_uri_has_query(&v) && !parsed_uri_has_fragment(&v));
    CHECK(parsed_uri_has_path(&v) && parsed_uri_get_path(q, &v).len == 0);
    CHECK_STR(parsed_uri_get_authority(q, &v), "example.org");
}

static void
test_ipv6(void)
{
    const char        *p = "http://[2001:db8::1]:8443/x";
    struct parsed_uri  u = parse(p, false);

    CHECK(!parsed_uri_has_error(&u));
    CHECK(u.flags & URI_IS_IPV6);
    CHECK_STR(parsed_uri_get_host(p, &u), "2001:db8::1");
    CHECK(parsed_uri_get_port(p, &u) == 8443);
    CHECK_STR(parsed_uri_get_host_port(p, &u), "[2001:db8::1]:8443");
    CHECK_STR(parsed_uri_get_authority(p, &u), "[2001:db8::1]:8443");
    CHECK_STR(parsed_uri_get_path(p, &u), "/x");

    const char        *q = "http://me@[::1]/";
    struct parsed_uri  v = parse(q, false);
    CHECK(!parsed_uri_has_error(&v));
    CHECK_STR(parsed_uri_get_authority(q, &v), "me@[::1]");

    CHECK(parsed_uri_has_error((const struct parsed_uri[]){ parse("http://[::1/x", false) }));
}

static void
test_origin_form(void)
{
    const char        *p = "/search?q=c+strings#top";
    struct parsed_uri  u = parse(p, false);

    CHECK(!parsed_uri_has_error(&u));
    CHECK(!parsed_uri_has_scheme(&u) && !parsed_uri_has_host(&u) && !parsed_uri_has_port(&u));
    CHECK_STR(parsed_uri_get_path(p, &u), "/search");
    CHECK_STR(parsed_uri_get_query(p, &u), "q=c+strings");
    CHECK_STR(parsed_uri_get_fragment(p, &u), "top");

    const char        *q = "/only/a/path";
    struct parsed_uri  v = parse(q, false);
    CHECK_STR(parsed_uri_get_path(q, &v), "/only/a/path");
    CHECK(!parsed_uri_has_query(&v) && !parsed_uri_has_fragment(&v));

    const char        *e = "/p?";            /* '?' with nothing after it */
    struct parsed_uri  w = parse(e, false);
    CHECK(parsed_uri_has_query(&w) && parsed_uri_get_query(e, &w).len == 0);

    const char        *star = "*";           /* OPTIONS * */
    struct parsed_uri  x    = parse(star, false);
    CHECK(!parsed_uri_has_error(&x));
    CHECK_STR(parsed_uri_get_path(star, &x), "*");
}

static void
test_connect(void)
{
    const char        *p = "example.com:443";
    struct parsed_uri  u = parse(p, true);

    CHECK(!parsed_uri_has_error(&u));
    CHECK(parsed_uri_is_authority_form(&u));
    CHECK_STR(parsed_uri_get_host(p, &u), "example.com");
    CHECK(parsed_uri_get_port(p, &u) == 443);
    CHECK(!parsed_uri_has_scheme(&u) && !parsed_uri_has_path(&u));

    const char        *q = "[::1]:8443";
    struct parsed_uri  v = parse(q, true);
    CHECK(!parsed_uri_has_error(&v) && (v.flags & URI_IS_IPV6));
    CHECK_STR(parsed_uri_get_host(q, &v), "::1");
    CHECK(parsed_uri_get_port(q, &v) == 8443);
    CHECK_STR(parsed_uri_get_host_port(q, &v), "[::1]:8443");

    /* CONNECT needs host and a port in 1..65535 */
    static const char *bad[] = {
        "example.com", "example.com:", ":443", "example.com:0", "example.com:65536",
        "example.com:44x", "[::1]", "[::1:443", "",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        struct parsed_uri b = parse(bad[i], true);
        if (!parsed_uri_has_error(&b)) {
            fprintf(stderr, "  CONNECT \"%s\" accepted\n", bad[i]);
        }
        CHECK(parsed_uri_has_error(&b));
    }
    CHECK(!parsed_uri_has_error((const struct parsed_uri[]){ parse("h:65535", true) }));
}

static void
test_errors(void)
{
    CHECK(parsed_uri_has_error((const struct parsed_uri[]){ parse("http://h:8x/", false) }));
    CHECK(parsed_uri_has_error((const struct parsed_uri[]){ parse("http://h:65536/", false) }));
    CHECK(!parsed_uri_has_error((const struct parsed_uri[]){ parse("http://h:65535/", false) }));

    /* 4295032831 is 2^32 + 65535: it must not wrap around to a valid port */
    CHECK(parsed_uri_has_error((const struct parsed_uri[]){ parse("http://h:4295032831/", false) }));
    CHECK(parsed_uri_has_error((const struct parsed_uri[]){ parse("h:4295032831", true) }));
    CHECK(parsed_uri_has_error((const struct parsed_uri[]){ parse("h:4294967376", true) }));    /* 2^32 + 80 */
    CHECK(!parsed_uri_has_error((const struct parsed_uri[]){ parse("http://h:00080/", false) }));

    struct parsed_uri empty = parse_uri_view(HTSTRING_LIT(""), false);
    CHECK(parsed_uri_has_error(&empty));

    /* offsets are 16-bit: longer input is refused rather than mis-parsed */
    size_t n    = 70000;
    char  *long_uri = malloc(n);
    memset(long_uri, 'a', n);
    long_uri[0] = '/';
    struct parsed_uri l = parse_uri_view(htstring_ctor(long_uri, n), false);
    CHECK(parsed_uri_has_error(&l));
    l = parse_uri_view(htstring_ctor(long_uri, 65535), false);
    CHECK(!parsed_uri_has_error(&l) && parsed_uri_get_path(long_uri, &l).len == 65535);
    free(long_uri);

    /* NULL-safe accessors */
    CHECK(!parsed_uri_has_host(NULL) && !parsed_uri_has_error(NULL));
    CHECK(parsed_uri_to_cstr(NULL, NULL) == NULL);
}

/* The bracket check must not look outside the string: a CONNECT target's host
 * starts at offset 0. Here the byte just before the string is '[', so reading
 * it shows up as a wrong answer, not just a stray read. */
static void
test_no_read_before_start(void)
{
    const char         buf[] = "[example.com:443";
    const char        *p     = buf + 1;
    struct parsed_uri  u     = parse(p, true);

    CHECK(!parsed_uri_has_error(&u));
    CHECK_STR(parsed_uri_get_host_port(p, &u), "example.com:443");
    CHECK_STR(parsed_uri_get_authority(p, &u), "example.com:443");

    const char         buf2[] = "[/no/authority";
    const char        *q      = buf2 + 1;
    struct parsed_uri  v      = parse(q, false);

    CHECK(parsed_uri_get_host_port(q, &v).len == 0);
    CHECK(parsed_uri_get_authority(q, &v).len == 0);
}

static void
test_join(void)
{
    char *s = parsed_uri_join(HTSTRING_LIT("https"), HTSTRING_LIT(""), HTSTRING_LIT("example.org:8443"),
                              HTSTRING_LIT("/a"), HTSTRING_LIT("b=1"), HTSTRING_LIT(""));
    CHECK(s && strcmp(s, "https://example.org:8443/a?b=1") == 0);
    free(s);

    s = parsed_uri_join(HTSTRING_LIT("http"), HTSTRING_LIT("u"), HTSTRING_LIT("h"),
                        HTSTRING_LIT(""), HTSTRING_LIT(""), HTSTRING_LIT("f"));
    CHECK(s && strcmp(s, "http://u@h#f") == 0);
    free(s);
}

int
main(void)
{
    test_absolute_form();
    test_ipv6();
    test_origin_form();
    test_connect();
    test_errors();
    test_no_read_before_start();
    test_join();
    return check_done("parsed_uri_test");
}
