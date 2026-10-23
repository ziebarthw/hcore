/* tokenizer: header values split without copying. Several inputs are the
 * real-world samples from the WITH_TOKENIZER_TEST main() this replaces. */
#include "check.h"
#include "htstring/tokenizer.h"

static void
test_tokenizer(void)
{
    httokenizer_t t = httokenizer_ctor(HTSTRING_LIT("a, b ,c"));
    htstring_t    tok;

    CHECK(httokenizer_next(&t, &p_comma, &tok));
    CHECK_STR(tok, "a");
    CHECK(httokenizer_get_last_delim(&t) == ',');
    CHECK(httokenizer_next(&t, &p_comma, &tok));
    CHECK_STR(tok, "b ");                       /* leading space trimmed, trailing kept */
    CHECK(httokenizer_next(&t, &p_comma, &tok));
    CHECK_STR(tok, "c");
    CHECK(httokenizer_get_last_delim(&t) == 0);  /* ended by the end of input */
    CHECK(!httokenizer_next(&t, &p_comma, &tok));

    /* several delimiters: the one that ended each token is recorded */
    ht_delim_policy_t multi = ht_delim_policy_ctor(";,");
    t = httokenizer_ctor(HTSTRING_LIT("x;y,z"));
    CHECK(httokenizer_next(&t, &multi, &tok) && httokenizer_get_last_delim(&t) == ';');
    CHECK_STR(tok, "x");
    CHECK(httokenizer_next(&t, &multi, &tok) && httokenizer_get_last_delim(&t) == ',');
    CHECK_STR(tok, "y");
    CHECK(httokenizer_next(&t, &multi, &tok));
    CHECK_STR(tok, "z");

    ht_delim_policy_t single = ht_delim_policy_ctor("&");
    CHECK(single.is_single && single.single_delim == '&');

    t = httokenizer_ctor(HTSTRING_LIT("   "));
    CHECK(!httokenizer_next(&t, &p_comma, &tok));
    CHECK(httokenizer_get_last_delim(NULL) == 0);
}

/* parse_parameterized_header: collect what the handler sees */
typedef struct {
    int        n;
    htstring_t names[8];
    size_t     params[8];
    htstring_t first_param_name[8];
    htstring_t first_param_value[8];
    int        stop_after;
} elements_seen;

static int
on_element(ht_element_t *el, void *ctx)
{
    elements_seen *seen = ctx;

    if (seen->n < 8) {
        seen->names[seen->n]  = el->name;
        seen->params[seen->n] = el->param_count;
        if (el->param_count) {
            seen->first_param_name[seen->n]  = el->params[0].name;
            seen->first_param_value[seen->n] = *htstring_unquote(&el->params[0].value);
        }
    }

    seen->n++;
    return seen->stop_after && seen->n >= seen->stop_after;
}

static void
test_parameterized_header(void)
{
    elements_seen seen = { 0 };

    CHECK(parse_parameterized_header(
        HTSTRING_LIT("\"Google Chrome\";v=\"153\", \"Not_A Brand\";v=\"8\", \"Chromium\";v=\"153\""),
        on_element, &seen));
    CHECK(seen.n == 3);
    CHECK_STR(seen.names[0], "\"Google Chrome\"");
    CHECK(seen.params[0] == 1);
    CHECK_STR(seen.first_param_name[0], "v");
    CHECK_STR(seen.first_param_value[0], "153");
    CHECK_STR(seen.first_param_value[1], "8");

    memset(&seen, 0, sizeof(seen));
    CHECK(parse_parameterized_header(HTSTRING_LIT("text/html; charset=UTF-8"), on_element, &seen));
    CHECK(seen.n == 1);
    CHECK_STR(seen.names[0], "text/html");
    CHECK_STR(seen.first_param_name[0], "charset");
    CHECK_STR(seen.first_param_value[0], "UTF-8");

    memset(&seen, 0, sizeof(seen));
    CHECK(parse_parameterized_header(
        HTSTRING_LIT("<https://example.org/wp-json/>; rel=\"https://api.w.org/\""), on_element, &seen));
    CHECK_STR(*htstring_unltgt(&seen.names[0]), "https://example.org/wp-json/");
    CHECK_STR(seen.first_param_value[0], "https://api.w.org/");

    /* a non-zero return from the handler stops the walk */
    memset(&seen, 0, sizeof(seen));
    seen.stop_after = 1;
    CHECK(parse_parameterized_header(HTSTRING_LIT("Accept-Encoding,Cookie"), on_element, &seen));
    CHECK(seen.n == 1);
    CHECK_STR(seen.names[0], "Accept-Encoding");

    /* more than HT_MAX_PARAMS parameters: refused */
    char many[256] = "x";
    for (int i = 0; i <= HT_MAX_PARAMS; i++) {
        strcat(many, ";p=1");
    }
    memset(&seen, 0, sizeof(seen));
    CHECK(!parse_parameterized_header(htstring_ctor(many, strlen(many)), on_element, &seen));
}

/* parse_set_cookie */
static ht_cookie_t last_cookie;
static int         cookies_seen;

static int
on_set_cookie(const ht_cookie_t *c, void *ctx)
{
    (void)ctx;
    last_cookie = *c;
    cookies_seen++;
    return 0;
}

static void
test_set_cookie(void)
{
    CHECK(parse_set_cookie(HTSTRING_LIT(
        "_fbp=fb.1.1.78947505464E%2B12.1170290678.AQECAQIB; expires=Mon, 14 Dec 2026 12:24:14 GMT; "
        "Max-Age=7776000; path=/; domain=example.org; secure; SameSite=Lax"), on_set_cookie, NULL));
    CHECK(cookies_seen == 1);
    CHECK_STR(last_cookie.name, "_fbp");
    CHECK_STR(last_cookie.value, "fb.1.1.78947505464E%2B12.1170290678.AQECAQIB");
    CHECK(last_cookie.attr_count == 6);
    CHECK_STR(last_cookie.attrs[0].name, "expires");
    CHECK_STR(last_cookie.attrs[0].value, "Mon, 14 Dec 2026 12:24:14 GMT");
    CHECK(last_cookie.attrs[0].has_value);
    CHECK_STR(last_cookie.attrs[4].name, "secure");
    CHECK(!last_cookie.attrs[4].has_value);
    CHECK_STR(last_cookie.attrs[5].value, "Lax");

    /* values may contain '=': only the first one splits the pair */
    CHECK(parse_set_cookie(HTSTRING_LIT("tok=abc==; path=/"), on_set_cookie, NULL));
    CHECK_STR(last_cookie.value, "abc==");

    CHECK(!parse_set_cookie(HTSTRING_LIT("no-equals-sign; path=/"), on_set_cookie, NULL));
}

/* parse_cookie and ht_cookie_iter */
static int pairs_seen;

static int
on_cookie_pair(const ht_cookie_pair_t *pair, void *ctx)
{
    htstring_t *last = ctx;

    *last = pair->name;
    pairs_seen++;
    return htstring_equals(pair->name, HTSTRING_LIT("_ga")) ? -1 : 0;   /* stop at _ga */
}

static void
test_cookie(void)
{
    htstring_t header = HTSTRING_LIT("_gcl_au=1.1.733836047; ac_enable_tracking=1; _ga=GA1.1.1046736607; "
                                     "PHPSESSID=ba0993; flag");
    htstring_t last   = { 0 };

    CHECK(parse_cookie(header, on_cookie_pair, &last));
    CHECK(pairs_seen == 3);
    CHECK_STR(last, "_ga");

    ht_cookie_iter_t it = ht_cookie_iter_ctor(header);
    ht_cookie_pair_t pair;
    int              n  = 0;
    while (ht_cookie_iter_next(&it, &pair)) {
        n++;
        if (n == 4) {
            CHECK_STR(pair.name, "PHPSESSID");
            CHECK_STR(pair.value, "ba0993");
        }
        if (n == 5) {
            CHECK_STR(pair.name, "flag");
            CHECK(pair.value.startp == NULL && pair.value.len == 0);
        }
    }
    CHECK(n == 5);
}

static void
test_param_iter(void)
{
    ht_param_iter_t it = ht_param_iter_ctor(HTSTRING_LIT("a=b&c=d&e=f"));
    htstring_t      key, val;
    int             n  = 0;

    while (ht_param_iter_next(&it, &key, &val)) {
        n++;
        if (n == 2) {
            CHECK_STR(key, "c");
            CHECK_STR(val, "d");
        }
    }
    CHECK(n == 3);

    /* a bare parameter has no value: NULL and empty, whatever val held */
    it      = ht_param_iter_ctor(HTSTRING_LIT("flag&x=1"));
    val.len = 99;
    CHECK(ht_param_iter_next(&it, &key, &val));
    CHECK_STR(key, "flag");
    CHECK(val.startp == NULL && val.len == 0);
    CHECK(ht_param_iter_next(&it, &key, &val));
    CHECK_STR(val, "1");
}

static void
test_forwarded(void)
{
    ht_forwarded_iter_t    it = ht_forwarded_iter_ctor(HTSTRING_LIT(
        "for=192.0.2.43, for=\"[2001:db8:cafe::17]\", for=192.0.2.60;proto=http;by=203.0.113.43, by=x"));
    ht_forwarded_element_t el;
    int                    n  = 0;

    while (ht_forwarded_iter_next(&it, &el)) {
        n++;
        if (n == 1) {
            CHECK_STR(el.for_val, "192.0.2.43");
        }
        if (n == 2) {
            CHECK_STR(*htstring_unbracket(&el.for_val), "2001:db8:cafe::17");
        }
        if (n == 3) {
            CHECK_STR(el.for_val, "192.0.2.60");
            CHECK_STR(el.proto_val, "http");
            CHECK_STR(el.by_val, "203.0.113.43");
            CHECK(el.host_val.len == 0);
        }
    }
    CHECK(n == 3);                              /* the hop without "for" is skipped */
}

/* ht_element_iter, used the way the scratch main() validated
 * Transfer-Encoding: chunked may appear once and must be the last coding
 * (RFC 9112 6.1). */
static int
validate_transfer_encoding(htstring_t value)
{
    ht_element_iter_t it = ht_element_iter_ctor(value);
    ht_element_t      el, last = { 0 };
    unsigned          count = 0, chunked = 0;

    while (ht_element_iter_next(&it, &el)) {
        count++;
        if (htstring_equalsi(el.name, HTSTRING_LIT("chunked"))) {
            chunked++;
        }
        last = el;
    }

    if (count == 0)   return -1;
    if (chunked > 1)  return -2;
    if (chunked == 1 && !htstring_equalsi(last.name, HTSTRING_LIT("chunked"))) return -3;
    return 0;
}

static void
test_element_iter(void)
{
    CHECK(validate_transfer_encoding(HTSTRING_LIT("gzip, chunked")) == 0);
    CHECK(validate_transfer_encoding(HTSTRING_LIT("gzip, unknown, chunked, compress, chunked")) == -2);
    CHECK(validate_transfer_encoding(HTSTRING_LIT("chunked, gzip")) == -3);
    CHECK(validate_transfer_encoding(HTSTRING_LIT("")) == -1);

    ht_element_iter_t it = ht_element_iter_ctor(HTSTRING_LIT("text/plain; q=0.5; flag, , text/html"));
    ht_element_t      el;

    CHECK(ht_element_iter_next(&it, &el));
    CHECK_STR(el.name, "text/plain");
    CHECK(el.param_count == 2);
    CHECK_STR(ht_element_get_parameter_by_name(&el, HTSTRING_LIT("Q")), "0.5");
    CHECK_STR(el.params[1].name, "flag");
    CHECK(el.params[1].value.startp == NULL);
    CHECK(ht_element_get_parameter_by_name(&el, HTSTRING_LIT("missing")).len == 0);

    CHECK(ht_element_iter_next(&it, &el));      /* the empty element */
    CHECK(el.name.startp == NULL && el.name.len == 0);
    CHECK(ht_element_iter_next(&it, &el));
    CHECK_STR(el.name, "text/html");
    CHECK(!ht_element_iter_next(&it, &el));
}

int
main(void)
{
    test_tokenizer();
    test_parameterized_header();
    test_set_cookie();
    test_cookie();
    test_param_iter();
    test_forwarded();
    test_element_iter();
    return check_done("tokenizer_test");
}
