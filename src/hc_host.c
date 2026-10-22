/*
 * hc_host.c - hc_host (Java: HttpHost) and URI helpers. Engine-independent.
 */
#include "hc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

static uint16_t
default_port(const char *scheme)
{
    if (strcasecmp(scheme, "http") == 0) {
        return 80;
    }

    if (strcasecmp(scheme, "https") == 0) {
        return 443;
    }

    return 0;
}

int
hc_host_init(hc_host *h, const char *scheme, const char *name, uint16_t port)
{
    uint16_t dp = default_port(scheme);
    size_t   nl = strlen(name);

    if (dp == 0 || nl == 0 || nl >= sizeof(h->name)) {
        return -1;
    }

    /* scheme is known to fit: "http" / "https", stored lower-case */
    snprintf(h->scheme, sizeof(h->scheme), "%s", dp == 443 ? "https" : "http");
    memcpy(h->name, name, nl + 1);
    h->port = port ? port : dp;

    return 0;
}

/* Parse "[scheme://]authority" up to end (exclusive). */
static int
parse_authority(hc_host *h, const char *s, const char *end)
{
    char        scheme[8] = "http";
    char        name[256];
    const char *sep = strstr(s, "://");
    const char *p, *colon = NULL, *name_end;
    long        port = 0;

    if (sep && sep < end) {
        size_t sl = (size_t)(sep - s);

        if (sl == 0 || sl >= sizeof(scheme)) {
            return -1;
        }

        memcpy(scheme, s, sl);
        scheme[sl] = '\0';
        s = sep + 3;
    }

    if (*s == '[') {                     /* [IPv6]:port */
        const char *rb = memchr(s, ']', (size_t)(end - s));

        if (rb == NULL) {
            return -1;
        }

        p        = s + 1;
        name_end = rb;
        colon    = rb + 1 < end && rb[1] == ':' ? rb + 1 : NULL;

        if (rb + 1 < end && rb[1] != ':') {
            return -1;
        }
    } else {
        p        = s;
        colon    = memchr(s, ':', (size_t)(end - s));
        name_end = colon ? colon : end;
    }

    if (colon) {
        char *e;

        port = strtol(colon + 1, &e, 10);

        if (e != end || port <= 0 || port > 65535) {
            return -1;
        }
    }

    if (name_end <= p || (size_t)(name_end - p) >= sizeof(name)) {
        return -1;
    }

    memcpy(name, p, (size_t)(name_end - p));
    name[name_end - p] = '\0';

    return hc_host_init(h, scheme, name, (uint16_t)port);
}

int
hc_host_parse(hc_host *h, const char *s)
{
    return parse_authority(h, s, s + strlen(s));
}

int
hc_uri_parse(const char *uri, hc_host *h, char *target, size_t target_len)
{
    const char *sep = strstr(uri, "://");
    const char *auth, *slash;

    if (sep == NULL) {
        return -1;
    }

    auth  = sep + 3;
    slash = auth + strcspn(auth, "/?#");

    if (parse_authority(h, uri, slash) != 0) {
        return -1;
    }

    if (*slash == '/' || *slash == '\0') {
        const char *t = *slash ? slash : "/";
        size_t      n = strcspn(t, "#");       /* never send a fragment */

        if (n >= target_len) {
            return -1;
        }

        memcpy(target, t, n);
        target[n] = '\0';
    } else if (*slash == '?') {                /* "http://h?q" -> "/?q" */
        size_t n = strcspn(slash, "#");

        if (n + 2 > target_len) {
            return -1;
        }

        target[0] = '/';
        memcpy(target + 1, slash, n);
        target[n + 1] = '\0';
    } else {                                   /* "http://h#frag" */
        if (target_len < 2) {
            return -1;
        }

        strcpy(target, "/");
    }

    return 0;
}

int
hc_host_is_tls(const hc_host *h)
{
    return strcmp(h->scheme, "https") == 0;
}

int
hc_host_equals(const hc_host *a, const hc_host *b)
{
    return a->port == b->port && strcmp(a->scheme, b->scheme) == 0 &&
           strcasecmp(a->name, b->name) == 0;
}

size_t
hc_host_authority(const hc_host *h, char *buf, size_t len)
{
    int ipv6 = strchr(h->name, ':') != NULL;
    int n;

    if (h->port == default_port(h->scheme)) {
        n = snprintf(buf, len, ipv6 ? "[%s]" : "%s", h->name);
    } else {
        n = snprintf(buf, len, ipv6 ? "[%s]:%u" : "%s:%u", h->name, (unsigned)h->port);
    }

    return n < 0 ? 0 : (size_t)n;
}

size_t
hc_host_to_string(const hc_host *h, char *buf, size_t len)
{
    int n = snprintf(buf, len, "%s://", h->scheme);

    if (n < 0 || (size_t)n >= len) {
        return 0;
    }

    return (size_t)n + hc_host_authority(h, buf + n, len - (size_t)n);
}
