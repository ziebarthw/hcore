/*
 * hc_gzip.c - gzip and deflate through zlib (hc_gzip.h): the codecs as
 * hc_transform, and the server and client policies on top of the
 * transform handlers (hc_transform.h). Built only with ZLIB=1.
 *
 * Engine-independent: hc.h and hc_transform.h only.
 */
#include "hc_gzip.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <zlib.h>

#define CHUNK            16384
#define DEFAULT_MIN_SIZE 1024
#define DEFAULT_MAX_SIZE ((int64_t)64 * 1024 * 1024)
#define DEFAULT_RATIO    100u
#define RATIO_FLOOR      ((uint64_t)1024 * 1024)    /* ratio checked past this */

/* ======================================================================== */
/* Codings                                                                  */
/* ======================================================================== */

/* s[0..n) equals word, ignoring case */
static int
token_is(const char *s, size_t n, const char *word)
{
    return strlen(word) == n && strncasecmp(s, word, n) == 0;
}

static void
trim(const char **s, size_t *n)
{
    while (*n > 0 && (**s == ' ' || **s == '\t')) {
        (*s)++;
        (*n)--;
    }

    while (*n > 0 && ((*s)[*n - 1] == ' ' || (*s)[*n - 1] == '\t')) {
        (*n)--;
    }
}

hc_coding
hc_gzip_coding(const char *v)
{
    size_t n;

    if (v == NULL) {
        return 0;
    }

    n = strlen(v);
    trim(&v, &n);

    if (token_is(v, n, "gzip") || token_is(v, n, "x-gzip")) {
        return HC_CODING_GZIP;
    }

    return token_is(v, n, "deflate") ? HC_CODING_DEFLATE : 0;
}

/* ======================================================================== */
/* Codecs                                                                   */
/* ======================================================================== */

/* struct zcodec: its flags (HC_SET_FLAG / HC_CLEAR_FLAG / HC_TEST_FLAG) */
enum {
    ZC_ENCODER   = 1u << 0,
    ZC_STARTED   = 1u << 1,     /* deflateInit2 / inflateInit2 done      */
    ZC_ENDED     = 1u << 2,     /* decoder: end of stream seen           */
    ZC_SAW_INPUT = 1u << 3,     /* decoder: at least one byte came in    */
};

typedef struct zcodec {
    z_stream      z;
    hc_coding     coding;
    unsigned      flags;        /* ZC_*, above */
    unsigned char hdr[2];       /* deflate decoder: the first two bytes, */
    size_t        nhdr;         /* to tell zlib-wrapped from raw         */
    uint64_t      in_total;
    uint64_t      out_total;
    uint64_t      max_size;
    unsigned      max_ratio;
} zcodec;

/* Compress with the given flush mode until zlib has nothing more to say. */
static int
enc_run(zcodec *c, const void *in, size_t len, int flush, hc_bytes *out)
{
    const unsigned char *p = in;

    do {
        uInt n = len > UINT_MAX ? UINT_MAX : (uInt)len;
        int  last = n == len;       /* the flush goes with the last piece */
        int  rc;

        c->z.next_in  = (Bytef *)(uintptr_t)p;
        c->z.avail_in = n;

        do {
            unsigned char *o = hc_bytes_reserve(out, CHUNK);

            if (o == NULL) {
                return -1;
            }

            c->z.next_out  = o;
            c->z.avail_out = CHUNK;
            rc             = deflate(&c->z, last ? flush : Z_NO_FLUSH);

            if (rc == Z_STREAM_ERROR) {
                return -1;
            }

            hc_bytes_commit(out, CHUNK - c->z.avail_out);
        } while (c->z.avail_out == 0 || (last && flush == Z_FINISH && rc != Z_STREAM_END));

        p   += n;
        len -= n;
    } while (len > 0);

    return 0;
}

static int
enc_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    return len ? enc_run(state, in, len, Z_NO_FLUSH, out) : 0;
}

static int
enc_flush(void *state, hc_bytes *out)
{
    return enc_run(state, "", 0, Z_SYNC_FLUSH, out);
}

static int
enc_finish(void *state, hc_bytes *out)
{
    return enc_run(state, "", 0, Z_FINISH, out);
}

static void
codec_free(void *state)
{
    zcodec *c = state;

    if (HC_TEST_FLAG(c->flags, ZC_STARTED)) {
        if (HC_TEST_FLAG(c->flags, ZC_ENCODER)) {
            deflateEnd(&c->z);
        } else {
            inflateEnd(&c->z);
        }
    }

    free(c);
}

static const hc_transform_ops ENCODER = { enc_update, enc_finish, enc_flush, codec_free };

int
hc_gzip_encoder(hc_transform *t, hc_coding coding, int level)
{
    zcodec *c;
    int     bits = coding == HC_CODING_GZIP ? 15 + 16 : 15;    /* gzip / zlib wrapper */

    if (t == NULL || level < 0 || level > 9 ||
        (coding != HC_CODING_GZIP && coding != HC_CODING_DEFLATE) ||
        (c = calloc(1, sizeof(*c))) == NULL) {
        return -1;
    }

    if (deflateInit2(&c->z, level ? level : Z_DEFAULT_COMPRESSION, Z_DEFLATED, bits, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
        free(c);
        return -1;
    }

    c->coding = coding;
    HC_SET_FLAG(c->flags, ZC_ENCODER | ZC_STARTED);
    t->ops   = &ENCODER;
    t->state = c;
    return 0;
}

/* Over a limit: too much output in all, or too much per input byte. */
static int
dec_over(const zcodec *c)
{
    return c->out_total > c->max_size ||
           (c->out_total > RATIO_FLOOR && c->out_total / c->max_ratio > c->in_total);
}

static int
dec_run(zcodec *c, const unsigned char *in, size_t len, hc_bytes *out)
{
    while (len > 0 && !HC_TEST_FLAG(c->flags, ZC_ENDED)) {
        uInt n = len > UINT_MAX ? UINT_MAX : (uInt)len;

        c->z.next_in  = (Bytef *)(uintptr_t)in;
        c->z.avail_in = n;

        for (;;) {
            unsigned char *o = hc_bytes_reserve(out, CHUNK);
            int            rc;
            size_t         made;

            if (o == NULL) {
                return -1;
            }

            c->z.next_out  = o;
            c->z.avail_out = CHUNK;
            rc             = inflate(&c->z, Z_NO_FLUSH);
            made           = CHUNK - c->z.avail_out;
            hc_bytes_commit(out, made);
            c->out_total  += made;

            if (dec_over(c)) {
                return -1;
            }

            if (rc == Z_STREAM_END) {
                if (c->coding == HC_CODING_GZIP && c->z.avail_in > 0) {
                    inflateReset(&c->z);    /* the next gzip member */
                    continue;
                }

                HC_SET_FLAG(c->flags, ZC_ENDED);    /* the rest is ignored */
                return 0;
            }

            if (rc == Z_BUF_ERROR) {
                break;                      /* needs more input */
            }

            if (rc != Z_OK) {
                return -1;                  /* corrupt */
            }

            if (c->z.avail_in == 0 && c->z.avail_out != 0) {
                break;
            }
        }

        in  += n;
        len -= n;
    }

    return 0;
}

static int
dec_start(zcodec *c, int bits)
{
    if (inflateInit2(&c->z, bits) != Z_OK) {
        return -1;
    }

    HC_SET_FLAG(c->flags, ZC_STARTED);
    return 0;
}

static int
dec_update(void *state, const void *in, size_t len, hc_bytes *out)
{
    zcodec              *c = state;
    const unsigned char *p = in;

    if (len == 0) {
        return 0;
    }

    HC_SET_FLAG(c->flags, ZC_SAW_INPUT);
    c->in_total += len;

    /* deflate: a zlib header (RFC 1950: CM 8, CINFO <= 7, FCHECK) or raw */
    if (!HC_TEST_FLAG(c->flags, ZC_STARTED)) {
        while (c->nhdr < 2 && len > 0) {
            c->hdr[c->nhdr++] = *p++;
            len--;
        }

        if (c->nhdr < 2) {
            return 0;
        }

        if (dec_start(c, ((c->hdr[0] & 0x0f) == 8 && (c->hdr[0] >> 4) <= 7 &&
                          ((c->hdr[0] << 8) | c->hdr[1]) % 31 == 0) ? 15 : -15) != 0 ||
            dec_run(c, c->hdr, 2, out) != 0) {
            return -1;
        }
    }

    return dec_run(c, p, len, out);
}

static int
dec_finish(void *state, hc_bytes *out)
{
    zcodec *c = state;

    (void)out;

    if (!HC_TEST_FLAG(c->flags, ZC_SAW_INPUT)) {
        return 0;       /* no body at all */
    }

    return HC_TEST_FLAG(c->flags, ZC_ENDED) ? 0 : -1;   /* truncated */
}

static const hc_transform_ops DECODER = { dec_update, dec_finish, NULL, codec_free };

int
hc_gzip_decoder(hc_transform *t, hc_coding coding, int64_t max_size, unsigned max_ratio)
{
    zcodec *c;

    if (t == NULL || (coding != HC_CODING_GZIP && coding != HC_CODING_DEFLATE) ||
        (c = calloc(1, sizeof(*c))) == NULL) {
        return -1;
    }

    c->coding    = coding;
    c->max_size  = (uint64_t)(max_size > 0 ? max_size : DEFAULT_MAX_SIZE);
    c->max_ratio = max_ratio ? max_ratio : DEFAULT_RATIO;

    /* gzip starts at once; deflate waits for two bytes to pick its window */
    if (coding == HC_CODING_GZIP && dec_start(c, 15 + 16) != 0) {
        free(c);
        return -1;
    }

    t->ops   = &DECODER;
    t->state = c;
    return 0;
}

/* ======================================================================== */
/* Policies                                                                 */
/* ======================================================================== */

struct hc_gzip {
    hc_gzip_config             cfg;     /* defaults filled in */
    hc_server_transform_config server;
    hc_client_transform_config client;
};

static const char *const DEFAULT_TYPES[] = {
    "text/*", "application/json", "application/javascript", "application/xml",
    "image/svg+xml", NULL
};

static int
type_ok(const hc_gzip *gz, const char *type)
{
    const char *const *list = gz->cfg.types ? gz->cfg.types : DEFAULT_TYPES;
    size_t             n;

    if (type == NULL) {
        return 0;
    }

    n = strcspn(type, ";");
    trim(&type, &n);

    if (gz->cfg.types == NULL && !gz->cfg.flush_each_write &&
        token_is(type, n, "text/event-stream")) {
        return 0;       /* would be held back: see hc_gzip_config.types */
    }

    for (; *list; list++) {
        size_t m = strlen(*list);

        if (m >= 2 && strcmp(*list + m - 2, "/*") == 0) {
            if (n > m - 1 && strncasecmp(type, *list, m - 1) == 0) {
                return 1;   /* "text/" prefix */
            }
        } else if (token_is(type, n, *list)) {
            return 1;
        }
    }

    return 0;
}

/* A q-value: "1", "0.5", "1.000"; anything else counts as 0. Thousandths. */
static int
qvalue(const char *s, size_t n)
{
    int    q = 0, scale = 1000;
    size_t i = 0;

    trim(&s, &n);

    if (n == 0 || (s[0] != '0' && s[0] != '1')) {
        return 0;
    }

    q = (s[0] - '0') * 1000;

    if (n > 1 && s[1] == '.') {
        for (i = 2; i < n && i < 5 && s[i] >= '0' && s[i] <= '9'; i++) {
            scale /= 10;
            q += (s[i] - '0') * scale;
        }
    }

    return q > 1000 ? 1000 : q;
}

/* The coding to use for an Accept-Encoding value, or 0. */
static hc_coding
accepted(const char *ae)
{
    int q_gzip = -1, q_deflate = -1, q_star = -1;

    while (ae && *ae) {
        size_t      len  = strcspn(ae, ",");
        size_t      nlen = strcspn(ae, ";,");
        const char *name = ae;
        int         q    = 1000;

        if (nlen < len) {   /* parameters: look for q= */
            const char *p = ae + nlen;

            while (p < ae + len) {
                size_t plen = strcspn(p + 1, ";,");
                const char *v = p + 1;
                size_t      vl = plen;

                trim(&v, &vl);

                if (vl >= 2 && (v[0] == 'q' || v[0] == 'Q') && v[1] == '=') {
                    q = qvalue(v + 2, vl - 2);
                }

                p += plen + 1;
            }
        }

        trim(&name, &nlen);

        if (token_is(name, nlen, "gzip") || token_is(name, nlen, "x-gzip")) {
            q_gzip = q;
        } else if (token_is(name, nlen, "deflate")) {
            q_deflate = q;
        } else if (token_is(name, nlen, "*")) {
            q_star = q;
        }

        ae += len + (ae[len] == ',');
    }

    if (q_gzip < 0) {
        q_gzip = q_star < 0 ? 0 : q_star;
    }

    if (q_deflate < 0) {
        q_deflate = q_star < 0 ? 0 : q_star;
    }

    if (q_gzip > 0 && q_gzip >= q_deflate) {
        return HC_CODING_GZIP;
    }

    return q_deflate > 0 ? HC_CODING_DEFLATE : 0;
}

/* Add Accept-Encoding to Vary unless it is there already (or "*"). */
static void
add_vary(hc_headers *h)
{
    const char *v = hc_headers_get(h, "Vary");
    char       *joined;
    size_t      n;

    if (v == NULL) {
        hc_headers_add(h, "Vary", "Accept-Encoding");
        return;
    }

    for (const char *p = v; *p;) {
        size_t      len = strcspn(p, ",");
        const char *t   = p;
        size_t      tl  = len;

        trim(&t, &tl);

        if (token_is(t, tl, "accept-encoding") || token_is(t, tl, "*")) {
            return;
        }

        p += len + (p[len] == ',');
    }

    n = strlen(v) + sizeof(", Accept-Encoding");

    if ((joined = malloc(n)) != NULL) {     /* v points into h: copy first */
        snprintf(joined, n, "%s, Accept-Encoding", v);
        hc_headers_remove(h, "Vary");
        hc_headers_add(h, "Vary", joined);
        free(joined);
    }
}

/* A strong ETag names these exact bytes: the compressed ones differ. */
static void
weaken_etag(hc_headers *h)
{
    const char *v = hc_headers_get(h, "ETag");
    char       *weak;
    size_t      n;

    if (v == NULL || v[0] != '"' || (weak = malloc(n = strlen(v) + 3)) == NULL) {
        return;
    }

    snprintf(weak, n, "W/%s", v);
    hc_headers_remove(h, "ETag");
    hc_headers_add(h, "ETag", weak);
    free(weak);
}

static int
srv_choose_response(void *arg, const hc_request_head *request, int *status,
                    hc_headers *headers, const hc_entity *entity, hc_transform *t)
{
    hc_gzip    *gz = arg;
    const char *type;
    hc_coding   coding;

    if (entity == NULL || *status < 200 || *status == 204 || *status == 206 ||
        *status == 304 || hc_headers_get(headers, "Content-Encoding")) {
        return 0;
    }

    type = entity->content_type ? entity->content_type : hc_headers_get(headers, "Content-Type");

    if (!type_ok(gz, type)) {
        return 0;
    }

    add_vary(headers);  /* this response could be compressed: caches must know */

    if (strcasecmp(request->method, "HEAD") == 0 ||
        (gz->cfg.min_size > 0 && entity->content_length >= 0 &&
         entity->content_length < gz->cfg.min_size) ||
        (coding = accepted(hc_headers_get(request->headers, "Accept-Encoding"))) == 0 ||
        hc_gzip_encoder(t, coding, gz->cfg.level) != 0) {
        return 0;       /* sent as it is */
    }

    hc_headers_add(headers, "Content-Encoding", coding == HC_CODING_GZIP ? "gzip" : "deflate");
    weaken_etag(headers);
    hc_headers_remove(headers, "Accept-Ranges");
    return 0;
}

static int
srv_choose_request(void *arg, const hc_request_head *request, hc_headers *headers,
                   const hc_entity *entity, hc_transform *t)
{
    hc_gzip    *gz = arg;
    const char *ce = hc_headers_get(headers, "Content-Encoding");
    const char *v  = ce;
    size_t      n  = ce ? strlen(ce) : 0;
    hc_coding   coding;

    (void)request;

    if (!gz->cfg.decompress_requests || ce == NULL || entity == NULL) {
        return 0;
    }

    trim(&v, &n);

    if (token_is(v, n, "identity")) {
        hc_headers_remove(headers, "Content-Encoding");
        return 0;
    }

    if ((coding = hc_gzip_coding(ce)) == 0) {
        return 415;     /* Unsupported Media Type (RFC 9110 15.5.16) */
    }

    if (hc_gzip_decoder(t, coding, gz->cfg.max_size, gz->cfg.max_ratio) != 0) {
        return 503;
    }

    hc_headers_remove(headers, "Content-Encoding");
    return 0;
}

static int
cli_choose_request(void *arg, const hc_request_head *request, hc_headers *headers,
                   const hc_entity *entity, hc_transform *t)
{
    (void)arg;
    (void)request;
    (void)entity;
    (void)t;

    if (hc_headers_get(headers, "Accept-Encoding") == NULL) {
        hc_headers_add(headers, "Accept-Encoding", "gzip, deflate");
    }

    return 0;
}

static int
cli_choose_response(void *arg, const hc_request_head *request, int status,
                    hc_headers *headers, const hc_entity *entity, hc_transform *t)
{
    hc_gzip  *gz = arg;
    hc_coding coding;

    (void)request;
    (void)status;

    if (entity == NULL || (coding = hc_gzip_coding(hc_headers_get(headers, "Content-Encoding"))) == 0) {
        return 0;
    }

    if (hc_gzip_decoder(t, coding, gz->cfg.max_size, gz->cfg.max_ratio) != 0) {
        return 502;
    }

    hc_headers_remove(headers, "Content-Encoding");
    return 0;
}

static hc_gzip *
gzip_new(const hc_gzip_config *cfg)
{
    hc_gzip *gz;

    if ((cfg && (cfg->level < 0 || cfg->level > 9)) || (gz = calloc(1, sizeof(*gz))) == NULL) {
        return NULL;
    }

    if (cfg) {
        gz->cfg = *cfg;
    }

    if (gz->cfg.min_size == 0) {
        gz->cfg.min_size = DEFAULT_MIN_SIZE;
    }

    if (gz->cfg.max_size <= 0) {
        gz->cfg.max_size = DEFAULT_MAX_SIZE;
    }

    if (gz->cfg.max_ratio == 0) {
        gz->cfg.max_ratio = DEFAULT_RATIO;
    }

    gz->server.choose_request   = srv_choose_request;
    gz->server.choose_response  = srv_choose_response;
    gz->server.arg              = gz;
    gz->server.high_water       = gz->cfg.high_water;
    gz->server.flush_each_write = gz->cfg.flush_each_write;
    gz->client.choose_request   = cli_choose_request;
    gz->client.choose_response  = cli_choose_response;
    gz->client.arg              = gz;
    gz->client.high_water       = gz->cfg.high_water;
    gz->client.flush_each_write = gz->cfg.flush_each_write;
    return gz;
}

hc_gzip *
hc_gzip_server_new(const hc_gzip_config *cfg)
{
    return gzip_new(cfg);
}

hc_gzip *
hc_gzip_client_new(const hc_gzip_config *cfg)
{
    return gzip_new(cfg);
}

void
hc_gzip_free(hc_gzip *gz)
{
    free(gz);
}

hc_server_handler *
hc_gzip_server_decorator(void *gz, hc_server_handler *inner)
{
    return hc_server_transform_handler_new(inner, &((hc_gzip *)gz)->server);
}

hc_client_handler *
hc_gzip_client_decorator(void *gz, hc_client_handler *inner)
{
    return hc_client_transform_handler_new(inner, &((hc_gzip *)gz)->client);
}
