/*
 * tls_name_test.c - hc_host.tls_name: connect to an address, present (SNI)
 * and verify a host name. The way a reverse proxy reaches an origin by IP
 * that hosts many sites (htm8's upstream "ssl: sni:").
 *
 * The origin is a plain OpenSSL server in a thread, with the test
 * certificate for "localhost". Its response body is the server name the
 * client sent, or "(none)".
 */
#define _GNU_SOURCE     /* strcasestr */
#include "hc.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

#define PORT 9330

static int passed, failed;

#define CHECK(c) do {                                                      \
        if (c) { passed++; }                                               \
        else { failed++; fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } \
    } while (0)

/* ======================================================================== */
/* Origin: one connection at a time, answers with the SNI it received       */
/* ======================================================================== */

static int listen_fd = -1;

static void *
origin_main(void *arg)
{
    SSL_CTX *ctx = arg;

    for (;;) {
        int  fd = accept(listen_fd, NULL, NULL);
        SSL *ssl;

        if (fd < 0) {
            break;                      /* listening socket closed: done */
        }

        ssl = SSL_new(ctx);
        SSL_set_fd(ssl, fd);

        if (SSL_accept(ssl) == 1) {
            const char *sni = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
            char        req[4096], resp[512];
            size_t      have = 0;
            int         n;

            /* read the request head */
            while (have < sizeof(req) - 1 &&
                   (n = SSL_read(ssl, req + have, (int)(sizeof(req) - 1 - have))) > 0) {
                have += (size_t)n;
                req[have] = '\0';

                if (strstr(req, "\r\n\r\n")) {
                    break;
                }
            }

            sni = sni ? sni : "(none)";
            n   = snprintf(resp, sizeof(resp),
                           "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
                           strlen(sni), sni);
            SSL_write(ssl, resp, n);
            SSL_shutdown(ssl);
        }

        SSL_free(ssl);
        close(fd);
    }

    return NULL;
}

/* ======================================================================== */
/* Client                                                                   */
/* ======================================================================== */

static hc_reactor *R;

typedef struct rec {
    hc_client_handler base;
    int               status;
    char              body[256];
    size_t            len;
    int               failed;
    char              reason[256];
} rec;

#define REC(self) ((rec *)(void *)(self))

static void
rec_produce_request(hc_client_handler *self, hc_request_channel *rc)
{
    hc_headers     *h    = hc_headers_new();
    hc_request_head head = { "GET", "/", "site", 11, h };

    (void)self;
    hc_request_send(rc, &head, NULL);
    hc_headers_free(h);
}

static void
rec_consume_response(hc_client_handler *self, const hc_response_head *head, const hc_entity *e)
{
    (void)e;
    REC(self)->status = head->status;
}

static void
rec_consume(hc_client_handler *self, const void *data, size_t len)
{
    rec *r = REC(self);

    if (r->len + len < sizeof(r->body)) {
        memcpy(r->body + r->len, data, len);
        r->len += len;
        r->body[r->len] = '\0';
    }
}

static void rec_stream_end(hc_client_handler *s, const hc_headers *t) { (void)s; (void)t; }

static void
rec_failed(hc_client_handler *self, const char *reason)
{
    REC(self)->failed = 1;
    snprintf(REC(self)->reason, sizeof(REC(self)->reason), "%s", reason);
}

static void
rec_release(hc_client_handler *self)
{
    (void)self;
    hc_reactor_stop(R);
}

static const hc_client_handler_vtbl rec_vtbl = {
    .produce_request  = rec_produce_request,
    .consume_response = rec_consume_response,
    .consume          = rec_consume,
    .stream_end       = rec_stream_end,
    .failed           = rec_failed,
    .release          = rec_release,
};

/* One request to name:PORT presenting tls_name (NULL: none set). */
static rec
fetch(hc_requester *rq, const char *name, const char *tls_name)
{
    hc_host target;
    rec     r;

    memset(&r, 0, sizeof(r));
    r.base.vtbl = &rec_vtbl;
    hc_host_init(&target, "https", name, PORT);

    if (tls_name) {
        CHECK(hc_host_set_tls_name(&target, tls_name) == 0);
    }

    CHECK(hc_requester_execute(rq, &target, 5000, &r.base) == 0);
    hc_reactor_run(R);

    if (r.failed) {
        printf("    -> failed: %s\n", r.reason);
    } else {
        printf("    -> %d, origin saw SNI \"%s\"\n", r.status, r.body);
    }

    return r;
}

int
main(int argc, char **argv)
{
    char                cert[512], key[512];
    struct sockaddr_in  sin;
    int                 one = 1;
    SSL_CTX            *sctx;
    pthread_t           origin;
    hc_requester_config verify   = { 0 };
    hc_requester_config noverify = { 0 };
    hc_requester       *rq_verify, *rq_noverify;
    rec                 r;

    if (argc < 2) {
        fprintf(stderr, "usage: %s <tests/tls dir>\n", argv[0]);
        return 2;
    }

    snprintf(cert, sizeof(cert), "%s/localhost.pem", argv[1]);
    snprintf(key, sizeof(key), "%s/localhost.key", argv[1]);

    /* origin */
    sctx = SSL_CTX_new(TLS_server_method());

    if (!sctx || SSL_CTX_use_certificate_chain_file(sctx, cert) != 1 ||
        SSL_CTX_use_PrivateKey_file(sctx, key, SSL_FILETYPE_PEM) != 1) {
        fprintf(stderr, "origin TLS setup failed\n");
        return 1;
    }

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    memset(&sin, 0, sizeof(sin));
    sin.sin_family      = AF_INET;
    sin.sin_port        = htons(PORT);
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(listen_fd, (struct sockaddr *)&sin, sizeof(sin)) != 0 || listen(listen_fd, 8) != 0) {
        perror("origin listen");
        return 1;
    }

    pthread_create(&origin, NULL, origin_main, sctx);

    /* client: one requester verifying against the test CA, one not */
    R                    = hc_reactor_new();
    verify.tls_ca_file   = cert;
    noverify.tls_no_verify = 1;
    rq_verify            = hc_requester_new(R, &verify);
    rq_noverify          = hc_requester_new(R, &noverify);
    CHECK(R && rq_verify && rq_noverify);

    puts("by IP, tls_name \"localhost\": SNI sent, certificate verified against it");
    r = fetch(rq_verify, "127.0.0.1", "localhost");
    CHECK(!r.failed && r.status == 200 && strcmp(r.body, "localhost") == 0);

    puts("by IP, no tls_name: no SNI, certificate checked against the IP -> fails");
    r = fetch(rq_verify, "127.0.0.1", NULL);
    CHECK(r.failed && strstr(r.reason, "IP address mismatch") != NULL);

    puts("by IP, tls_name \"other.example\": certificate is for localhost -> fails");
    r = fetch(rq_verify, "127.0.0.1", "other.example");
    CHECK(r.failed && strcasestr(r.reason, "hostname mismatch") != NULL);

    puts("same, verification off: the origin sees SNI \"other.example\"");
    r = fetch(rq_noverify, "127.0.0.1", "other.example");
    CHECK(!r.failed && r.status == 200 && strcmp(r.body, "other.example") == 0);

    puts("by IP, no tls_name, verification off: no SNI at all");
    r = fetch(rq_noverify, "127.0.0.1", NULL);
    CHECK(!r.failed && r.status == 200 && strcmp(r.body, "(none)") == 0);

    puts("by name, no tls_name: SNI is the name, as before");
    r = fetch(rq_verify, "localhost", NULL);
    CHECK(!r.failed && r.status == 200 && strcmp(r.body, "localhost") == 0);

    puts("tls_name an IP literal: verified as an IP, never sent as SNI");
    r = fetch(rq_noverify, "localhost", "127.0.0.1");
    CHECK(!r.failed && r.status == 200 && strcmp(r.body, "(none)") == 0);

    hc_requester_free(rq_verify);
    hc_requester_free(rq_noverify);
    hc_reactor_free(R);

    shutdown(listen_fd, SHUT_RDWR);
    close(listen_fd);
    pthread_join(origin, NULL);
    SSL_CTX_free(sctx);

    printf("%d checks, %d failed\n", passed + failed, failed);
    return failed ? 1 : 0;
}
