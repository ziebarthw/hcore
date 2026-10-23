#ifndef HTSTRING_LOG_H
#define HTSTRING_LOG_H

/*
 * Logging for the library's .c files (not installed). Same output as
 * libevhtp's log_info/log_debug, which this code used while it lived there:
 *
 *   log_info   always printed to stderr; -DHTSTRING_NO_LOG removes it
 *   log_debug  compiled out unless -DHTSTRING_DEBUG
 */
#include <stdio.h>
#include <string.h>

#define __ht_filename \
    (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#if defined(HTSTRING_NO_LOG)
#define log_info(M, ...)  ((void)0)
#else
#define log_info(M, ...)                                        \
    fprintf(stderr, "[\x1b[32mINFO\x1b[0;39m]  "                \
            "\x1b[33m%4s:%-9d\x1b[39m"                          \
            "\x1b[94m" M "\x1b[39m\n",                          \
            __ht_filename, __LINE__, ## __VA_ARGS__)
#endif

#if !defined(HTSTRING_DEBUG)
#define log_debug(M, ...) ((void)0)
#else
#define log_debug(M, ...)                                       \
    fprintf(stderr, "[\x1b[1;36mDEBUG\x1b[0;39m] "              \
            "\x1b[33m%s/%s:%d \x1b[39m"                         \
            "\x1b[94m" M "\x1b[39m\n",                          \
            __ht_filename, __func__, __LINE__, ## __VA_ARGS__)
#endif

#endif
