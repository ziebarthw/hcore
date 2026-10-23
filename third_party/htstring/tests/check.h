#ifndef HTSTRING_TESTS_CHECK_H
#define HTSTRING_TESTS_CHECK_H

/* A minimal test harness: CHECK() counts and reports, check_done() prints
 * the totals and gives main()'s exit status. */
#include <stdio.h>
#include <string.h>
#include "htstring/htstring.h"

static int check_passed, check_failed;

#define CHECK(cond) do {                                                 \
        if (cond) {                                                      \
            check_passed++;                                              \
        } else {                                                         \
            check_failed++;                                              \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                \
    } while (0)

/* An htstring_t equals a C string literal. */
#define CHECK_STR(s, lit) do {                                           \
        htstring_t s_ = (s);                                             \
        if (htstring_equals(s_, HTSTRING_LIT(lit))) {                    \
            check_passed++;                                              \
        } else {                                                         \
            check_failed++;                                              \
            fprintf(stderr, "FAIL %s:%d: %s is \"%.*s\", expected \"%s\"\n", \
                    __FILE__, __LINE__, #s, (int)s_.len,                 \
                    s_.startp ? s_.startp : "(null)", lit);              \
        }                                                                \
    } while (0)

static int
check_done(const char *name)
{
    printf("%s: passed=%d failed=%d\n", name, check_passed, check_failed);
    return check_failed ? 1 : 0;
}

#endif
