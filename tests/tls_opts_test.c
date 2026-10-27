/*
 * tls_opts_test.c - hc_server_enable_tls_opts(), checked with openssl
 * s_client against real servers:
 *
 *   - TLS 1.3 only, with one TLS 1.3 cipher suite: a TLS 1.2 client is
 *     refused; a TLS 1.3 client gets that suite, and one that offers only
 *     another suite is refused
 *   - TLS 1.2 only, with one cipher: a TLS 1.3 client is refused, a TLS
 *     1.2 client gets that cipher, and one offering only another cipher is
 *     refused
 *   - client certificates required (mutual TLS): no certificate, or one not
 *     from the CA, gets no response; the CA's certificate gets 200
 *   - client certificates optional: without one, 200
 *   - defaults (hc_server_enable_tls): TLS 1.2 and 1.3, not 1.1
 *   - errors, each with its reason, leaving the server able to try again:
 *     a missing certificate file, a key that does not match, an invalid
 *     cipher list, invalid TLS 1.3 suites, a missing CA file, client
 *     verification without a CA file, no protocol version
 *
 * Usage: tls_opts_test <tests/tls dir>; needs the openssl command (skipped
 * without it).
 */
#define _GNU_SOURCE
#include "hc.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

#include <event2/event.h>
#include <event2/thread.h>

#define PORT_13   9370
#define PORT_12   9371
#define PORT_REQ  9372
#define PORT_OPT  9373
#define PORT_DEF  9374

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

static const char *dir;

/* ------------------------------------------------------------------------ */

static void
h_request(hc_server_handler *self, hc_server_exchange *ex, const hc_request_head *head,
          const hc_entity *entity, hc_response_channel *rc)
{
    (void)self;
    (void)ex;
    (void)head;
    (void)entity;
    hc_response_send_simple(rc, 200, "text/plain", "tls ok", 1);
}

static void h_consume(hc_server_handler *s, const void *d, size_t n) { (void)s; (void)d; (void)n; }
static void h_end(hc_server_handler *s, const hc_headers *t) { (void)s; (void)t; }
static void h_release(hc_server_handler *self) { free(self); }

static const hc_server_handler_vtbl vt = {
    .handle_request = h_request,
    .consume        = h_consume,
    .stream_end     = h_end,
    .release        = h_release,
};

static hc_server_handler *
factory(void *arg)
{
    hc_server_handler *h = calloc(1, sizeof(*h));

    (void)arg;
    h->vtbl = &vt;
    return h;
}

/* ------------------------------------------------------------------------ */

/* openssl s_client: one request (HTTP/1.1, "Connection: close"), the
 * transcript in out. */
static void
s_client(int port, const char *opts, char *out, size_t cap)
{
    char   cmd[1024];
    FILE  *p;
    size_t n;

    snprintf(cmd, sizeof(cmd),
             "printf 'GET / HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' | "
             "timeout 10 openssl s_client -connect 127.0.0.1:%d -servername localhost "
             "-alpn http/1.1 -ign_eof %s 2>&1", port, opts);
    p      = popen(cmd, "r");
    n      = fread(out, 1, cap - 1, p);
    out[n] = '\0';
    pclose(p);
}

static int
served(const char *out)
{
    return strstr(out, "HTTP/1.1 200") != NULL && strstr(out, "tls ok") != NULL;
}

/* "Protocol  : TLSv1.3" / "New, TLSv1.3, Cipher is X" */
static int
has(const char *out, const char *what)
{
    return strstr(out, what) != NULL;
}

/* ------------------------------------------------------------------------ */

static hc_server *
tls_server(hc_reactor *r, int port, hc_server_tls_options *o)
{
    hc_server *s = hc_server_new(r, factory, NULL);
    char       err[256];

    hc_server_set_http2(s, 0);      /* s_client speaks HTTP/1.1 here */

    if (hc_server_enable_tls_opts(s, o, err, sizeof(err)) != 0 ||
        hc_server_listen(s, "127.0.0.1", (uint16_t)port) != 0) {
        fprintf(stderr, "server on %d: %s\n", port, err);
        exit(1);
    }

    return s;
}

static void
error_cases(hc_reactor *r, const char *cert, const char *key, const char *other_key,
            const char *ca)
{
    hc_server            *s = hc_server_new(r, factory, NULL);
    hc_server_tls_options o;
    char                  err[256];

    puts("errors:");

#define TRY(what, want) do {                                                \
        int rc_ = hc_server_enable_tls_opts(s, &o, err, sizeof(err));       \
        CHECK(rc_ == -1);                                                   \
        CHECK(strstr(err, want) != NULL);                                   \
        printf("  %-38s -> %s\n", what, err);                               \
    } while (0)

    memset(&o, 0, sizeof(o));
    o.cert_file = "/nonexistent/cert.pem";
    o.key_file  = key;
    TRY("missing certificate file", "certificate /nonexistent/cert.pem: No such file");

    memset(&o, 0, sizeof(o));
    o.cert_file = cert;
    o.key_file  = other_key;
    TRY("key of another certificate", "private key does not match the certificate");

    memset(&o, 0, sizeof(o));
    o.cert_file = cert;
    o.key_file  = key;
    o.ciphers   = "NOT-A-CIPHER";
    TRY("invalid cipher list", "invalid cipher list: NOT-A-CIPHER");

    memset(&o, 0, sizeof(o));
    o.cert_file    = cert;
    o.key_file     = key;
    o.ciphersuites = "TLS_NOT_A_SUITE";
    TRY("invalid TLS 1.3 suites", "invalid TLS 1.3 cipher suites: TLS_NOT_A_SUITE");

    memset(&o, 0, sizeof(o));
    o.cert_file     = cert;
    o.key_file      = key;
    o.verify_client = HC_TLS_VERIFY_REQUIRED;
    o.ca_file       = "/nonexistent/ca.pem";
    TRY("missing CA file", "CA file /nonexistent/ca.pem: No such file");

    memset(&o, 0, sizeof(o));
    o.cert_file     = cert;
    o.key_file      = key;
    o.verify_client = HC_TLS_VERIFY_REQUIRED;
    TRY("client verification without CA", "needs a CA file");

    memset(&o, 0, sizeof(o));
    o.cert_file = cert;
    o.key_file  = key;
    o.protocols = 1u << 7;
    TRY("no protocol version", "no TLS protocol version");

    /* after all that, the server can still be set up */
    memset(&o, 0, sizeof(o));
    o.cert_file = cert;
    o.key_file  = key;
    o.ca_file   = ca;
    CHECK(hc_server_enable_tls_opts(s, &o, err, sizeof(err)) == 0);
    CHECK(err[0] == '\0');
    CHECK(hc_server_enable_tls_opts(s, &o, err, sizeof(err)) == -1);
    CHECK(strstr(err, "already enabled") != NULL);
    printf("  then valid options: %s\n", "ok");

    hc_server_free(s);
#undef TRY
}

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
    char                  cert[512], key[512], other[512], other_key[512], out[16384], opts[1200];
    struct event_base    *base;
    hc_reactor           *r;
    hc_server            *s13, *s12, *sreq, *sopt, *sdef;
    hc_server_tls_options o;
    pthread_t             th;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    if (system("openssl version >/dev/null 2>&1") != 0) {
        puts("tls_opts_test: skipped (no openssl command)");
        return 0;
    }

    dir = argv[1];
    snprintf(cert, sizeof(cert), "%s/localhost.pem", dir);
    snprintf(key, sizeof(key), "%s/localhost.key", dir);
    snprintf(other, sizeof(other), "%s/other.pem", dir);
    snprintf(other_key, sizeof(other_key), "%s/other.key", dir);

    evthread_use_pthreads();
    base = event_base_new();
    r    = hc_reactor_new_on_base(base);

    error_cases(r, cert, key, other_key, other);

    memset(&o, 0, sizeof(o));
    o.cert_file    = cert;
    o.key_file     = key;
    o.protocols    = HC_TLS_1_3;
    o.ciphersuites = "TLS_CHACHA20_POLY1305_SHA256";
    s13            = tls_server(r, PORT_13, &o);

    memset(&o, 0, sizeof(o));
    o.cert_file = cert;
    o.key_file  = key;
    o.protocols = HC_TLS_1_2;
    o.ciphers   = "ECDHE-RSA-AES128-GCM-SHA256";
    s12         = tls_server(r, PORT_12, &o);

    memset(&o, 0, sizeof(o));
    o.cert_file     = cert;
    o.key_file      = key;
    o.verify_client = HC_TLS_VERIFY_REQUIRED;
    o.ca_file       = other;        /* "other" is a self-signed CA */
    sreq            = tls_server(r, PORT_REQ, &o);

    o.verify_client = HC_TLS_VERIFY_OPTIONAL;
    sopt            = tls_server(r, PORT_OPT, &o);

    sdef = hc_server_new(r, factory, NULL);
    hc_server_set_http2(sdef, 0);
    CHECK(hc_server_enable_tls(sdef, cert, key) == 0 &&
          hc_server_listen(sdef, "127.0.0.1", PORT_DEF) == 0);

    pthread_create(&th, NULL, loop_main, base);

    puts("TLS 1.3 only, TLS_CHACHA20_POLY1305_SHA256:");
    s_client(PORT_13, "-tls1_2", out, sizeof(out));
    CHECK(!served(out));
    printf("  TLS 1.2 client: %s\n", served(out) ? "served" : "refused");
    s_client(PORT_13, "-tls1_3", out, sizeof(out));
    CHECK(served(out) && has(out, "TLSv1.3") && has(out, "TLS_CHACHA20_POLY1305_SHA256"));
    printf("  TLS 1.3 client: %s\n", served(out) && has(out, "TLS_CHACHA20_POLY1305_SHA256") ?
           "served with TLS_CHACHA20_POLY1305_SHA256" : "not as expected");
    s_client(PORT_13, "-tls1_3 -ciphersuites TLS_AES_128_GCM_SHA256", out, sizeof(out));
    CHECK(!served(out));
    printf("  TLS 1.3 client offering only TLS_AES_128_GCM_SHA256: %s\n",
           served(out) ? "served" : "refused");

    puts("TLS 1.2 only, ECDHE-RSA-AES128-GCM-SHA256:");
    s_client(PORT_12, "-tls1_3", out, sizeof(out));
    CHECK(!served(out));
    printf("  TLS 1.3 client: %s\n", served(out) ? "served" : "refused");
    s_client(PORT_12, "-tls1_2", out, sizeof(out));
    CHECK(served(out) && has(out, "TLSv1.2") && has(out, "ECDHE-RSA-AES128-GCM-SHA256"));
    printf("  TLS 1.2 client: %s\n", served(out) && has(out, "ECDHE-RSA-AES128-GCM-SHA256") ?
           "served with ECDHE-RSA-AES128-GCM-SHA256" : "not as expected");
    s_client(PORT_12, "-tls1_2 -cipher ECDHE-RSA-AES256-GCM-SHA384", out, sizeof(out));
    CHECK(!served(out));
    printf("  TLS 1.2 client offering only ECDHE-RSA-AES256-GCM-SHA384: %s\n",
           served(out) ? "served" : "refused");

    puts("client certificate required (CA: other):");
    s_client(PORT_REQ, "", out, sizeof(out));
    CHECK(!served(out));
    printf("  no certificate: %s\n", served(out) ? "served" : "refused");
    snprintf(opts, sizeof(opts), "-cert %s -key %s", cert, key);
    s_client(PORT_REQ, opts, out, sizeof(out));
    CHECK(!served(out));
    printf("  certificate from another CA: %s\n", served(out) ? "served" : "refused");
    snprintf(opts, sizeof(opts), "-cert %s -key %s", other, other_key);
    s_client(PORT_REQ, opts, out, sizeof(out));
    CHECK(served(out));
    CHECK(has(out, "Acceptable client certificate CA names") && has(out, "CN = other"));
    printf("  certificate from the CA: %s\n", served(out) ? "served" : "refused");

    puts("client certificate optional:");
    s_client(PORT_OPT, "", out, sizeof(out));
    CHECK(served(out));
    printf("  no certificate: %s\n", served(out) ? "served" : "refused");

    puts("defaults (hc_server_enable_tls):");
    s_client(PORT_DEF, "-tls1_3", out, sizeof(out));
    CHECK(served(out));
    s_client(PORT_DEF, "-tls1_2", out, sizeof(out));
    CHECK(served(out));
    s_client(PORT_DEF, "-tls1_1", out, sizeof(out));
    CHECK(!served(out));
    printf("  TLS 1.3 and 1.2 served, TLS 1.1 refused\n");

    event_base_once(base, -1, EV_TIMEOUT, stop_cb, base, NULL);
    pthread_join(th, NULL);

    hc_server_free(s13);
    hc_server_free(s12);
    hc_server_free(sreq);
    hc_server_free(sopt);
    hc_server_free(sdef);
    hc_reactor_free(r);
    event_base_free(base);

    printf("tls_opts_test: %d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
