/*
 * hc_gzip.h - gzip and deflate content coding through zlib (an optional
 * module: build hcore with ZLIB=1, link with -lz).
 *
 *   Java (httpcore5 / httpclient5)        C (this header)
 *   ------------------------------------  ------------------------------------
 *   ContentCompressionAsyncExec           hc_gzip_client_new() +
 *                                         hc_gzip_client_decorator
 *   InflatingAsyncDataConsumer etc.       hc_gzip_decoder()
 *   DeflatingAsyncEntityProducer etc.     hc_gzip_encoder()
 *   (server response compression)         hc_gzip_server_new() +
 *                                         hc_gzip_server_decorator
 *
 * Two levels:
 *
 * - Ready-made decorators (hc.h, "Handler decorators") for a server or a
 *   requester:
 *
 *     hc_gzip *gz = hc_gzip_server_new(NULL);          defaults
 *     hc_server_add_decorator_last(s, "gzip", hc_gzip_server_decorator, gz);
 *     ...
 *     hc_server_free(s);
 *     hc_gzip_free(gz);                                 after the server
 *
 *   server   compresses responses the client accepts (Accept-Encoding)
 *            and that are worth it (type, size, status: see below); adds
 *            Content-Encoding and Vary: Accept-Encoding. Optionally
 *            decompresses request bodies (off by default).
 *   client   adds "Accept-Encoding: gzip, deflate" to requests that have no
 *            Accept-Encoding, and decompresses gzip and deflate responses
 *            (Content-Encoding removed, as HttpClient does).
 *
 * - The codecs alone, as hc_transform (hc_transform.h), for choose
 *   functions of your own: hc_gzip_encoder(), hc_gzip_decoder().
 *
 * Decompression is limited, against "zip bombs": a body that would
 * decompress past max_size bytes, or (beyond its first MiB) past max_ratio
 * times its compressed size, fails the exchange ("body transformation
 * failed") instead of being buffered.
 *
 * Deflate: "deflate" means zlib-wrapped data (RFC 9110 8.4.1.2), which is
 * what the encoder writes; the decoder also accepts raw deflate data, as
 * some servers send it.
 */
#ifndef HC_GZIP_H
#define HC_GZIP_H

#include "hc_transform.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum hc_coding {
    HC_CODING_GZIP    = 1,    /* "gzip" (also accepted: "x-gzip") */
    HC_CODING_DEFLATE = 2     /* "deflate" */
} hc_coding;

/* The coding for a Content-Encoding value: gzip, x-gzip or deflate (any
 * case, surrounding spaces ignored), else 0 - "identity", several codings
 * ("gzip, br") and anything else. */
hc_coding hc_gzip_coding(const char *content_encoding);

/* ------------------------------------------------------------------------ */
/* Codecs                                                                   */
/* ------------------------------------------------------------------------ */

/* Set t to a compressor. level: 1 (fastest) to 9 (smallest); 0: zlib's
 * default (6). It has flush() (a sync flush) for streamed responses.
 * 0, or -1 (out of memory, bad level). */
int hc_gzip_encoder(hc_transform *t, hc_coding coding, int level);

/* Set t to a decompressor. max_size: most bytes it will produce (0: 64
 * MiB); max_ratio: most output per input byte once past the first MiB
 * (0: 100). An empty body is accepted (no data, e.g. a HEAD response); a
 * truncated or corrupt one fails. Concatenated gzip members are decoded
 * one after another. 0, or -1. */
int hc_gzip_decoder(hc_transform *t, hc_coding coding, int64_t max_size, unsigned max_ratio);

/* ------------------------------------------------------------------------ */
/* Decorators                                                               */
/* ------------------------------------------------------------------------ */

typedef struct hc_gzip_config {
    /* Compression (server responses). */
    int                level;       /* 1..9; 0: zlib's default (6)           */
    int64_t            min_size;    /* compress bodies of at least this many
                                     * bytes - and every one of unknown
                                     * length; 0: 1024; -1: any size         */
    /* Content types to compress, NULL-terminated. An entry that is a type
     * followed by a slash and an asterisk (the string "text/" "*") matches
     * every subtype of it. Parameters (";charset=...") are ignored. NULL:
     * every text subtype, application/json, application/javascript,
     * application/xml and image/svg+xml - except text/event-stream, which
     * is compressed only when listed explicitly or with flush_each_write.
     * The array must outlive the hc_gzip. */
    const char *const *types;
    int                flush_each_write;    /* sync-flush after every write */

    /* Decompression (client responses; server requests if enabled). */
    int                decompress_requests; /* server: 0: off (the default) */
    int64_t            max_size;    /* 0: 64 MiB */
    unsigned           max_ratio;   /* 0: 100   */
    size_t             high_water;  /* hc_server/client_transform_config's;
                                     * 0: 64 KiB */
} hc_gzip_config;

typedef struct hc_gzip hc_gzip;

/* cfg is copied (NULL: defaults). Free with hc_gzip_free() once the server
 * or requester using it has been freed. NULL when out of memory. */
hc_gzip *hc_gzip_server_new(const hc_gzip_config *cfg);
hc_gzip *hc_gzip_client_new(const hc_gzip_config *cfg);
void     hc_gzip_free(hc_gzip *gz);

/* hc_server_decorator / hc_client_decorator: arg is the hc_gzip from
 * hc_gzip_server_new() / hc_gzip_client_new(). */
hc_server_handler *hc_gzip_server_decorator(void *gz, hc_server_handler *inner);
hc_client_handler *hc_gzip_client_decorator(void *gz, hc_client_handler *inner);

/* Server, compressing a response, when all of these hold:
 *   - the request is not HEAD, and its Accept-Encoding gives gzip (or
 *     deflate, or "*") a q-value above 0 - gzip is preferred;
 *   - the status is 200..599 other than 204, 206 and 304, the response
 *     has a body and no Content-Encoding yet;
 *   - its content type is on the list, and its length is unknown or at
 *     least min_size.
 * It then adds Content-Encoding, weakens a strong ETag (W/"..."), and
 * drops Accept-Ranges. Vary: Accept-Encoding is added to every response
 * whose type and status would allow compression, compressed or not, so
 * that caches keep the versions apart.
 *
 * Server, request bodies (decompress_requests): gzip and deflate are
 * decompressed, with Content-Encoding removed; "identity" is removed;
 * any other coding is answered 415 Unsupported Media Type. */

#ifdef __cplusplus
}
#endif

#endif /* HC_GZIP_H */
