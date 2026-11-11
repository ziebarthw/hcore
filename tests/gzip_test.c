/*
 * gzip_test.c - gzip and deflate (hc_gzip.h; built with ZLIB=1):
 *
 * Codecs, no network:
 *   - round trips (gzip, deflate) of 1 MB, smaller on the wire
 *   - decoding what zlib itself makes: zlib-wrapped and raw deflate,
 *     concatenated gzip members; input fed a byte at a time
 *   - an empty body accepted; truncated and corrupt ones refused
 *   - limits: max_size, and max_ratio past the first MiB (but not below)
 *   - flush(): what was written so far decodes before the end
 *   - hc_gzip_coding()
 *
 * Server (hc_gzip_server_decorator), with curl over HTTP/1.1, and over
 * HTTP/2 when curl and libevhtp have it:
 *   - curl --compressed: gzip, Vary, no Content-Length, body intact
 *   - Accept-Encoding negotiation: deflate only, q=0, "*", q-values,
 *     identity (not compressed, Vary still there)
 *   - not compressed: small bodies, other types, HEAD, 204, a body the
 *     application encoded itself
 *   - ETag weakened, Accept-Ranges dropped
 *   - event streams: left alone by default; with flush_each_write
 *     compressed and still delivered as written
 *   - request bodies: decompressed when enabled (415 for an unknown
 *     coding; a bomb refused), passed through untouched when not
 *
 * Client (hc_gzip_client_decorator on a pooled requester):
 *   - Accept-Encoding added; the response decompressed, Content-Encoding
 *     gone; the application's own Accept-Encoding kept
 *   - raw deflate from a server decoded
 *   - a bomb: ->failed("body transformation failed")
 *
 * Usage: gzip_test <tests/tls dir>
 */
#define _GNU_SOURCE
#include "hc.h"
#include "hc_gzip.h"
#include "hc_support.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>
#include <zlib.h>

#define PORT       9400
#define PORT_TLS   9401
#define PORT_FLUSH 9402
#define PORT_DEC   9403
#define TEXT       100000
#define MIB        (1024 * 1024)

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static hc_reactor *R;
static char       *text;                    /* TEXT bytes of compressible text */
static hc_bytes    bomb_gz, rawdeflate;     /* prepared bodies */

static void
sleep_ms(int ms)
{
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };

    nanosleep(&ts, NULL);
}

static void
make_text(char *p, size_t n)
{
    static const char *words[] = { "alpha ", "beta ", "gamma ", "delta ", "epsilon\n" };
    size_t             i = 0, w = 0;

    while (i < n) {
        const char *s = words[(w * 7 + w / 5) % 5];

        for (; *s && i < n; s++) {
            p[i++] = *s;
        }

        w++;
    }
}

/* Run all of in through a transform, in pieces of step bytes (0: one go). */
static int
run(hc_transform *t, const void *in, size_t len, size_t step, hc_bytes *out, int finish)
{
    const char *p = in;
    int         rv = 0;

    while (len > 0 && rv == 0) {
        size_t n = step && step < len ? step : len;

        rv   = t->ops->update(t->state, p, n, out);
        p   += n;
        len -= n;
    }

    if (rv == 0 && finish) {
        rv = t->ops->finish(t->state, out);
    }

    t->ops->free(t->state);
    return rv;
}

static int
round_trip(hc_coding coding, size_t step)
{
    hc_transform enc, dec;
    hc_bytes     z = { 0 }, back = { 0 };
    int          ok;

    ok = hc_gzip_encoder(&enc, coding, 0) == 0 && run(&enc, text, TEXT, 0, &z, 1) == 0 &&
         z.len < TEXT / 5 &&
         hc_gzip_decoder(&dec, coding, 0, 0) == 0 && run(&dec, z.data, z.len, step, &back, 1) == 0 &&
         back.len == TEXT && memcmp(back.data, text, TEXT) == 0;
    hc_bytes_free(&z);
    hc_bytes_free(&back);
    return ok;
}

/* zlib directly: windowBits 15 (zlib), -15 (raw), 31 (gzip) */
static void
zcompress(const void *in, size_t len, int bits, hc_bytes *out)
{
    z_stream z;

    memset(&z, 0, sizeof(z));
    deflateInit2(&z, 9, Z_DEFLATED, bits, 8, Z_DEFAULT_STRATEGY);
    z.next_in  = (Bytef *)(uintptr_t)in;
    z.avail_in = (uInt)len;

    do {
        z.next_out  = hc_bytes_reserve(out, 65536);
        z.avail_out = 65536;
        deflate(&z, Z_FINISH);
        hc_bytes_commit(out, 65536 - z.avail_out);
    } while (z.avail_out == 0);

    deflateEnd(&z);
}

static void
codec_cases(void)
{
    hc_transform t;
    hc_bytes     z = { 0 }, out = { 0 };
    char        *zeros;

    puts("codecs:");
    CHECK(round_trip(HC_CODING_GZIP, 0));
    CHECK(round_trip(HC_CODING_DEFLATE, 0));
    CHECK(round_trip(HC_CODING_GZIP, 1));       /* a byte at a time */
    CHECK(round_trip(HC_CODING_DEFLATE, 1));
    puts("  round trips: gzip, deflate (also fed a byte at a time)");

    zcompress(text, TEXT, -15, &z);
    CHECK(hc_gzip_decoder(&t, HC_CODING_DEFLATE, 0, 0) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == 0 && out.len == TEXT &&
          memcmp(out.data, text, TEXT) == 0);
    z.len = out.len = 0;
    zcompress(text, TEXT, 15, &z);
    CHECK(hc_gzip_decoder(&t, HC_CODING_DEFLATE, 0, 0) == 0 &&
          run(&t, z.data, z.len, 1, &out, 1) == 0 && out.len == TEXT);
    z.len = out.len = 0;
    zcompress("first,", 6, 31, &z);
    zcompress("second", 6, 31, &z);
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 0) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == 0 && out.len == 12 &&
          memcmp(out.data, "first,second", 12) == 0);
    puts("  zlib's own: raw and zlib-wrapped deflate, two gzip members");

    z.len = out.len = 0;
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 0) == 0 && run(&t, "", 0, 0, &out, 1) == 0);
    zcompress(text, TEXT, 31, &z);
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 0) == 0 &&
          run(&t, z.data, z.len - 10, 0, &out, 1) == -1);      /* truncated */
    out.len = 0;
    z.data[z.len / 2] ^= 0x55;
    z.data[z.len / 2 + 1] ^= 0x55;
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 0) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == -1);           /* corrupt */
    puts("  empty body accepted; truncated and corrupt refused");

    /* limits */
    zeros = calloc(1, 10 * MIB);
    z.len = out.len = 0;
    zcompress(zeros, 10 * MIB, 31, &z);         /* ratio ~1000 */
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 2 * MIB, 100000) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == -1 && out.len <= 2 * MIB + 16384);
    out.len = 0;
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 100) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == -1 && out.len < 2 * MIB);
    out.len = 0;
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 2000) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == 0 && out.len == 10 * MIB);
    z.len = out.len = 0;
    zcompress(zeros, 512 * 1024, 31, &z);       /* high ratio, under the floor */
    CHECK(hc_gzip_decoder(&t, HC_CODING_GZIP, 0, 0) == 0 &&
          run(&t, z.data, z.len, 0, &out, 1) == 0 && out.len == 512 * 1024);
    printf("  limits: max_size stops at %d MiB, ratio past 1 MiB; 512 KiB of zeros fine\n", 2);
    free(zeros);

    /* flush: a prefix decodes before the end */
    {
        hc_transform enc, dec;
        hc_bytes     part = { 0 }, plain = { 0 };

        hc_gzip_encoder(&enc, HC_CODING_GZIP, 0);
        hc_gzip_decoder(&dec, HC_CODING_GZIP, 0, 0);
        enc.ops->update(enc.state, "data: hello\n\n", 13, &part);
        enc.ops->flush(enc.state, &part);
        dec.ops->update(dec.state, part.data, part.len, &plain);
        CHECK(plain.len == 13 && memcmp(plain.data, "data: hello\n\n", 13) == 0);
        enc.ops->free(enc.state);
        dec.ops->free(dec.state);
        hc_bytes_free(&part);
        hc_bytes_free(&plain);
        puts("  flush: written data decodes before the end");
    }

    CHECK(hc_gzip_coding("gzip") == HC_CODING_GZIP && hc_gzip_coding(" X-GZIP ") == HC_CODING_GZIP &&
          hc_gzip_coding("Deflate") == HC_CODING_DEFLATE && hc_gzip_coding("identity") == 0 &&
          hc_gzip_coding("gzip, br") == 0 && hc_gzip_coding(NULL) == 0);
    CHECK(hc_gzip_encoder(&t, HC_CODING_GZIP, 10) == -1 && hc_gzip_encoder(&t, 0, 1) == -1);

    hc_bytes_free(&z);
    hc_bytes_free(&out);
}

/* ------------------------------------------------------------------------ */
/* The server application                                                   */
/* ------------------------------------------------------------------------ */

static volatile int delivered, released;
static char         last_ae[128], last_ce[64];

typedef struct app_req {
    hc_timer        *timer;
    hc_data_channel *ch;
    int              events;
} app_req;

static void
event_cb(void *arg)
{
    app_req *a = arg;
    char     ev[64];

    snprintf(ev, sizeof(ev), "data: event %d\n\n", ++a->events);
    hc_data_write(a->ch, ev, strlen(ev));
    hc_timer_start(a->timer, 50);
}

static void
send_body(hc_response_channel *rc, int status, const char *type, const void *body, size_t len,
          const char *n1, const char *v1, const char *n2, const char *v2)
{
    hc_headers      *h  = hc_headers_new();
    hc_response_head rh = { status, 11, h };
    hc_entity        e  = { (int64_t)len, type };
    hc_data_channel *ch;

    if (n1) {
        hc_headers_add(h, n1, v1);
    }

    if (n2) {
        hc_headers_add(h, n2, v2);
    }

    if ((ch = hc_response_send(rc, &rh, len || type ? &e : NULL)) != NULL) {
        hc_data_write(ch, body, len);
        hc_data_end(ch, NULL);
    }

    hc_headers_free(h);
}

static void
app_request(void *arg, hc_basic_request *r)
{
    hc_response_channel *rc     = hc_basic_request_channel(r);
    const char          *target = hc_basic_request_target(r);
    const hc_headers    *h      = hc_basic_request_headers(r);
    const char          *ae     = hc_headers_get(h, "Accept-Encoding");
    const char          *ce     = hc_headers_get(h, "Content-Encoding");
    app_req             *a      = calloc(1, sizeof(*a));
    size_t               len;
    const char          *body   = hc_basic_request_body(r, &len);
    char                 msg[128];

    (void)arg;
    delivered++;
    hc_basic_request_set_data(r, a);
    snprintf(last_ae, sizeof(last_ae), "%s", ae ? ae : "-");
    snprintf(last_ce, sizeof(last_ce), "%s", ce ? ce : "-");

    if (strcmp(target, "/text") == 0) {
        send_body(rc, 200, "text/plain; charset=utf-8", text, TEXT, NULL, NULL, NULL, NULL);
    } else if (strcmp(target, "/small") == 0) {
        send_body(rc, 200, "text/plain", text, 100, NULL, NULL, NULL, NULL);
    } else if (strcmp(target, "/png") == 0) {
        send_body(rc, 200, "image/png", text, TEXT, NULL, NULL, NULL, NULL);
    } else if (strcmp(target, "/etag") == 0) {
        send_body(rc, 200, "application/json", text, TEXT, "ETag", "\"v1\"", "Accept-Ranges", "bytes");
    } else if (strcmp(target, "/vary") == 0) {
        send_body(rc, 200, "text/html", text, TEXT, "Vary", "Origin", NULL, NULL);
    } else if (strcmp(target, "/bomb") == 0) {
        send_body(rc, 200, "text/plain", bomb_gz.data, bomb_gz.len, "Content-Encoding", "gzip",
                  NULL, NULL);
    } else if (strcmp(target, "/rawdeflate") == 0) {
        send_body(rc, 200, "text/plain", rawdeflate.data, rawdeflate.len, "Content-Encoding",
                  "deflate", NULL, NULL);
    } else if (strcmp(target, "/nocontent") == 0) {
        hc_response_send_simple(rc, 204, NULL, NULL, 0);
    } else if (strcmp(target, "/sse") == 0) {
        hc_response_head rh = { 200, 11, NULL };
        hc_entity        e  = { -1, "text/event-stream" };

        a->ch    = hc_response_send(rc, &rh, &e);
        a->timer = hc_timer_new(R, event_cb, a);
        hc_timer_start(a->timer, 0);
    } else if (strcmp(target, "/upload") == 0) {
        unsigned long sum = 0;

        for (size_t i = 0; i < len; i++) {
            sum = sum * 31 + (unsigned char)body[i];
        }

        snprintf(msg, sizeof(msg), "len=%zu sum=%lu ce=%s", len, sum, last_ce);
        hc_response_send_simple(rc, 200, "application/octet-stream", msg, 0);
    } else {
        hc_response_send_simple(rc, 404, "text/plain", "?", 0);
    }
}

static void
app_release(void *arg, hc_basic_request *r)
{
    app_req *a = hc_basic_request_data(r);

    (void)arg;
    released++;

    if (a->timer) {
        hc_timer_free(a->timer);
    }

    free(a);
}

static hc_basic_server_config APP = {
    .request = app_request, .release = app_release, .max_body = 32 * MIB
};

/* ------------------------------------------------------------------------ */

static int
curl(const char *args, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd), "curl -sS -k --max-time 20 %s 2>/dev/null", args);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';

    return WEXITSTATUS(pclose(p));
}

/* The headers part of curl -D - output, and where the body starts. */
static const char *
body_of(const char *out)
{
    const char *b = strstr(out, "\r\n\r\n");

    return b ? b + 4 : out;
}

static int
has(const char *out, const char *header_line)
{
    char   needle[128];
    size_t hlen = (size_t)(body_of(out) - out);
    char  *head = strndup(out, hlen);
    int    rv;

    snprintf(needle, sizeof(needle), "\n%s", header_line);
    rv = strcasestr(head, needle) != NULL;
    free(head);
    return rv;
}

static int
coding_for(const char *proto, const char *scheme, int port, const char *ae)
{
    static char out[4 * TEXT];
    char        args[512];

    snprintf(args, sizeof(args), "%s -D - -o /dev/null -H 'Accept-Encoding: %s' %s://127.0.0.1:%d/text",
             proto, ae, scheme, port);
    curl(args, out, sizeof(out));
    return has(out, "content-encoding: gzip") ? 'g' : has(out, "content-encoding: deflate") ? 'd' : '-';
}

static void
server_cases(const char *proto, const char *scheme, int port)
{
    static char out[4 * TEXT];
    char        args[512];

    printf("%s:\n", proto);

    snprintf(args, sizeof(args), "%s --compressed -i %s://127.0.0.1:%d/text", proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0);
    CHECK(has(out, "content-encoding: gzip") && has(out, "vary: accept-encoding"));
    CHECK(!has(out, "content-length:"));
    CHECK(strlen(body_of(out)) == TEXT && memcmp(body_of(out), text, TEXT) == 0);
    printf("  --compressed: gzip, Vary, length unknown, body %s\n",
           memcmp(body_of(out), text, TEXT) == 0 ? "intact" : "WRONG");

    CHECK(coding_for(proto, scheme, port, "deflate") == 'd');
    CHECK(coding_for(proto, scheme, port, "gzip;q=0, deflate") == 'd');
    CHECK(coding_for(proto, scheme, port, "gzip;q=0.5, deflate;q=0.8") == 'd');
    CHECK(coding_for(proto, scheme, port, "deflate;q=0.5, gzip") == 'g');
    CHECK(coding_for(proto, scheme, port, "*;q=0.3") == 'g');
    CHECK(coding_for(proto, scheme, port, "br, *;q=0") == '-');
    CHECK(coding_for(proto, scheme, port, "identity") == '-');
    CHECK(coding_for(proto, scheme, port, "GZIP ; Q=1.0") == 'g');
    puts("  Accept-Encoding: deflate, q=0, q-values, \"*\", identity, case");

    snprintf(args, sizeof(args), "%s -D - -o /dev/null %s://127.0.0.1:%d/text", proto, scheme, port);
    curl(args, out, sizeof(out));
    CHECK(!has(out, "content-encoding:") && has(out, "vary: accept-encoding"));
    puts("  no Accept-Encoding: not compressed, Vary sent");

    snprintf(args, sizeof(args), "%s --compressed -D - -o /dev/null %s://127.0.0.1:%d/small", proto,
             scheme, port);
    curl(args, out, sizeof(out));
    CHECK(!has(out, "content-encoding:") && has(out, "content-length: 100"));
    snprintf(args, sizeof(args), "%s --compressed -D - -o /dev/null %s://127.0.0.1:%d/png", proto,
             scheme, port);
    curl(args, out, sizeof(out));
    CHECK(!has(out, "content-encoding:") && !has(out, "vary:"));
    snprintf(args, sizeof(args), "%s --compressed -I %s://127.0.0.1:%d/text", proto, scheme, port);
    curl(args, out, sizeof(out));
    CHECK(!has(out, "content-encoding:"));
    snprintf(args, sizeof(args), "%s --compressed -o /dev/null -w '%%{http_code}' %s://127.0.0.1:%d/nocontent",
             proto, scheme, port);
    CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, "204") == 0);
    snprintf(args, sizeof(args), "%s -H 'Accept-Encoding: gzip' -D - %s://127.0.0.1:%d/rawdeflate",
             proto, scheme, port);
    curl(args, out, sizeof(out));
    CHECK(has(out, "content-encoding: deflate") && !has(out, "content-encoding: gzip"));
    puts("  left alone: small, image/png (no Vary), HEAD, 204, already encoded");

    snprintf(args, sizeof(args), "%s --compressed -D - -o /dev/null %s://127.0.0.1:%d/etag", proto,
             scheme, port);
    curl(args, out, sizeof(out));
    CHECK(has(out, "etag: W/\"v1\"") && !has(out, "accept-ranges:"));
    snprintf(args, sizeof(args), "%s --compressed -D - -o /dev/null %s://127.0.0.1:%d/vary", proto,
             scheme, port);
    curl(args, out, sizeof(out));
    CHECK(has(out, "vary: Origin, Accept-Encoding"));
    puts("  ETag weakened, Accept-Ranges dropped, Vary extended");

    snprintf(args, sizeof(args), "%s --compressed -D - -N --max-time 0.3 %s://127.0.0.1:%d/sse",
             proto, scheme, port);
    curl(args, out, sizeof(out));
    CHECK(!has(out, "content-encoding:") && strstr(out, "data: event 1") != NULL);
    puts("  event stream, default config: not compressed");
}

/* ------------------------------------------------------------------------ */
/* Client                                                                   */
/* ------------------------------------------------------------------------ */

typedef struct client {
    hc_client_handler base;
    const char       *target;
    const char       *ae;           /* the application's own Accept-Encoding */
    int               status;
    int               had_ce;
    hc_bytes          got;
    char              reason[64];
    int               failures;
    volatile int      released;
} client;

static void
cl_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    client         *c  = (client *)self;
    hc_headers     *h  = hc_headers_new();
    hc_request_head rh = { "GET", c->target, "127.0.0.1", 11, h };

    if (c->ae) {
        hc_headers_add(h, "Accept-Encoding", c->ae);
    }

    hc_request_send(rc, &rh, NULL);
    hc_headers_free(h);
}

static void
cl_consume_response(hc_client_handler *self, const hc_response_head *head, const hc_entity *entity)
{
    client *c = (client *)self;

    (void)entity;
    c->status = head->status;
    c->had_ce = hc_headers_get(head->headers, "Content-Encoding") != NULL;
}

static void
cl_consume(hc_client_handler *self, const void *data, size_t len)
{
    hc_bytes_append(&((client *)self)->got, data, len);
}

static void
cl_stream_end(hc_client_handler *self, const hc_headers *trailers)
{
    (void)self;
    (void)trailers;
}

static void
cl_failed(hc_client_handler *self, const char *reason)
{
    client *c = (client *)self;

    c->failures++;
    snprintf(c->reason, sizeof(c->reason), "%s", reason);
}

static void
cl_release(hc_client_handler *self)
{
    ((client *)self)->released++;
}

static const hc_client_handler_vtbl CL_VTBL = {
    cl_produce_request, cl_consume_response, cl_consume, cl_stream_end,
    NULL, cl_failed, cl_release, NULL
};

static hc_requester *RQ;

static void
start_client(evutil_socket_t fd, short what, void *arg)
{
    client *c = arg;
    hc_host h;

    (void)fd;
    (void)what;
    hc_host_init(&h, "http", "127.0.0.1", PORT);

    if (hc_requester_execute(RQ, &h, 10000, &c->base) != 0) {
        c->released = 1;
    }
}

static void
run_client(struct event_base *base, client *c, const char *target, const char *ae)
{
    hc_bytes_free(&c->got);
    memset(c, 0, sizeof(*c));
    c->base.vtbl = &CL_VTBL;
    c->target    = target;
    c->ae        = ae;
    event_base_once(base, -1, EV_TIMEOUT, start_client, c, NULL);

    for (int i = 0; i < 200 && !c->released; i++) {
        sleep_ms(50);
    }

    sleep_ms(50);
}

/* ------------------------------------------------------------------------ */

static void *
loop_main(void *arg)
{
    event_base_dispatch(arg);
    return NULL;
}

static void
stop_cb(evutil_socket_t fd, short what, void *arg)
{
    (void)fd;
    (void)what;
    event_base_loopbreak(arg);
}

int
main(int argc, char **argv)
{
    static char        out[4 * TEXT];
    char               cert[512], key[512], args[512], file[64], want[128];
    struct event_base *base;
    hc_server         *s, *s_tls, *s_flush, *s_dec;
    hc_gzip           *gz, *gz_flush, *gz_dec, *gz_cli;
    hc_gzip_config     flush_cfg = { 0 }, dec_cfg = { 0 }, cli_cfg = { 0 };
    pthread_t          th;
    client             c;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);

    text = malloc(TEXT);
    make_text(text, TEXT);
    codec_cases();

    {
        char *zeros = calloc(1, 8 * MIB);

        zcompress(zeros, 8 * MIB, 31, &bomb_gz);
        free(zeros);
        zcompress(text, TEXT, -15, &rawdeflate);
    }

    flush_cfg.flush_each_write   = 1;
    dec_cfg.decompress_requests  = 1;
    dec_cfg.max_size             = 2 * MIB;
    cli_cfg.max_size             = 2 * MIB;
    gz       = hc_gzip_server_new(NULL);
    gz_flush = hc_gzip_server_new(&flush_cfg);
    gz_dec   = hc_gzip_server_new(&dec_cfg);
    gz_cli   = hc_gzip_client_new(&cli_cfg);
    CHECK(gz && gz_flush && gz_dec && gz_cli);
    {
        hc_gzip_config bad = { 0 };

        bad.level = 12;
        CHECK(hc_gzip_server_new(&bad) == NULL);
    }

    evthread_use_pthreads();
    base    = event_base_new();
    R       = hc_reactor_new_on_base(base);
    s       = hc_server_new(R, hc_basic_server_factory, &APP);
    s_tls   = hc_server_new(R, hc_basic_server_factory, &APP);
    s_flush = hc_server_new(R, hc_basic_server_factory, &APP);
    s_dec   = hc_server_new(R, hc_basic_server_factory, &APP);
    hc_server_add_decorator_last(s, "gzip", hc_gzip_server_decorator, gz);
    hc_server_add_decorator_last(s_tls, "gzip", hc_gzip_server_decorator, gz);
    hc_server_add_decorator_last(s_flush, "gzip", hc_gzip_server_decorator, gz_flush);
    hc_server_add_decorator_last(s_dec, "gzip", hc_gzip_server_decorator, gz_dec);
    RQ = hc_requester_new(R, NULL);
    hc_requester_add_decorator_last(RQ, "gzip", hc_gzip_client_decorator, gz_cli);

    if (hc_server_listen(s, "127.0.0.1", PORT) != 0 ||
        hc_server_enable_tls(s_tls, cert, key) != 0 ||
        hc_server_listen(s_tls, "127.0.0.1", PORT_TLS) != 0 ||
        hc_server_listen(s_flush, "127.0.0.1", PORT_FLUSH) != 0 ||
        hc_server_listen(s_dec, "127.0.0.1", PORT_DEC) != 0) {
        fprintf(stderr, "setup failed\n");
        return 1;
    }

    pthread_create(&th, NULL, loop_main, base);

    server_cases("--http1.1", "http", PORT);

    snprintf(args, sizeof(args), "--http2 -o /dev/null -w '%%{http_version}' https://127.0.0.1:%d/small",
             PORT_TLS);
    if (curl(args, out, sizeof(out)) == 0 && strcmp(out, "2") == 0) {
        server_cases("--http2", "https", PORT_TLS);
    } else {
        puts("--http2: skipped (no HTTP/2 in curl or libevhtp)");
    }

    puts("flush_each_write:");
    snprintf(args, sizeof(args), "--compressed -D - -N --max-time 0.3 http://127.0.0.1:%d/sse", PORT_FLUSH);
    curl(args, out, sizeof(out));
    CHECK(has(out, "content-encoding: gzip") && strstr(out, "data: event 1\n\n") != NULL);
    puts("  event stream compressed, events delivered as written");
    sleep_ms(300);

    puts("request bodies:");
    snprintf(file, sizeof(file), "/tmp/hc_gz_%d", (int)getpid());
    {
        hc_transform  enc;
        hc_bytes      z = { 0 };
        FILE         *f;
        unsigned long sum = 0;

        hc_gzip_encoder(&enc, HC_CODING_GZIP, 0);
        run(&enc, text, TEXT, 0, &z, 1);
        f = fopen(file, "wb");
        fwrite(z.data, 1, z.len, f);
        fclose(f);

        for (size_t i = 0; i < TEXT; i++) {
            sum = sum * 31 + (unsigned char)text[i];
        }

        snprintf(want, sizeof(want), "len=%d sum=%lu ce=-", TEXT, sum);
        snprintf(args, sizeof(args), "-H 'Content-Encoding: gzip' --data-binary @%s "
                 "http://127.0.0.1:%d/upload", file, PORT_DEC);
        CHECK(curl(args, out, sizeof(out)) == 0 && strcmp(out, want) == 0);
        printf("  gzip request decompressed: %s\n", strcmp(out, want) == 0 ? "whole, no Content-Encoding" : out);

        snprintf(args, sizeof(args), "-H 'Content-Encoding: gzip' --data-binary @%s "
                 "http://127.0.0.1:%d/upload", file, PORT);
        snprintf(want, sizeof(want), "len=%zu", z.len);
        CHECK(curl(args, out, sizeof(out)) == 0 && strncmp(out, want, strlen(want)) == 0 &&
              strstr(out, "ce=gzip") != NULL);
        puts("  decompression off (the default): passed through as sent");

        snprintf(args, sizeof(args), "-o /dev/null -w '%%{http_code}' -H 'Content-Encoding: br' "
                 "--data-binary @%s http://127.0.0.1:%d/upload", file, PORT_DEC);
        curl(args, out, sizeof(out));
        CHECK(strcmp(out, "415") == 0);
        printf("  unknown coding: %s\n", out);
        hc_bytes_free(&z);

        f = fopen(file, "wb");
        fwrite(bomb_gz.data, 1, bomb_gz.len, f);
        fclose(f);
        {
            int before = delivered;

            snprintf(args, sizeof(args), "-o /dev/null -w '%%{http_code}' -H 'Content-Encoding: gzip' "
                     "--data-binary @%s http://127.0.0.1:%d/upload", file, PORT_DEC);
            curl(args, out, sizeof(out));
            sleep_ms(100);
            CHECK(strcmp(out, "200") != 0 && delivered == before);
            printf("  bomb (8 MiB of zeros, limit 2 MiB): refused (%s), application not called\n", out);
        }
        unlink(file);
    }

    puts("client:");
    memset(&c, 0, sizeof(c));
    run_client(base, &c, "/text", NULL);
    CHECK(c.status == 200 && !c.had_ce && c.got.len == TEXT && memcmp(c.got.data, text, TEXT) == 0);
    CHECK(strcmp(last_ae, "gzip, deflate") == 0);
    printf("  Accept-Encoding added (%s); response decompressed, Content-Encoding gone\n", last_ae);

    run_client(base, &c, "/text", "identity");
    CHECK(c.status == 200 && c.got.len == TEXT && strcmp(last_ae, "identity") == 0);
    puts("  the application's own Accept-Encoding kept");

    run_client(base, &c, "/rawdeflate", NULL);
    CHECK(c.status == 200 && !c.had_ce && c.got.len == TEXT && memcmp(c.got.data, text, TEXT) == 0);
    puts("  raw deflate decoded");

    run_client(base, &c, "/bomb", NULL);
    CHECK(c.released == 1 && c.failures == 1 && strcmp(c.reason, "body transformation failed") == 0);
    CHECK(c.got.len <= 2 * MIB + 16384);
    printf("  bomb: failed(\"%s\") after %zu bytes\n", c.reason, c.got.len);
    hc_bytes_free(&c.got);

    sleep_ms(200);
    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    hc_requester_free(RQ);
    hc_server_free(s);
    hc_server_free(s_tls);
    hc_server_free(s_flush);
    hc_server_free(s_dec);
    hc_reactor_free(R);
    event_base_free(base);
    hc_gzip_free(gz);
    hc_gzip_free(gz_flush);
    hc_gzip_free(gz_dec);
    hc_gzip_free(gz_cli);
    hc_bytes_free(&bomb_gz);
    hc_bytes_free(&rawdeflate);
    free(text);

    CHECK(released == delivered);
    printf("gzip_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
