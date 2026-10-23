#include <stdio.h>

//#define HTSTRING_DEBUG 1

#include "ht_log.h"
#include "htstring/tokenizer.h"


bool
parse_parameterized_header(htstring_t header_value, element_handler_t handler, void * ctx)
{
    log_debug("(\"%.*s\", %p, %p)", (int)header_value.len, header_value.startp, handler, ctx);

    httokenizer_t t_elements = httokenizer_ctor(header_value);

    htstring_t element_raw;
    while (httokenizer_next(&t_elements, &p_comma, &element_raw)) {
        ht_element_t element = {0};
        httokenizer_t t_params = httokenizer_ctor(element_raw);

        htstring_t part;
        bool is_first = true;

        while (httokenizer_next(&t_params, &p_semi, &part)) {
            if (is_first) {
                // The first part is the primary value
                element.name = part;
                is_first = false;
            } else if (element.param_count < HT_MAX_PARAMS) {
                // Subsequent parts are parameters (key=value)
                httokenizer_t t_kv = httokenizer_ctor(part);

                htstring_t key, val;
                if (httokenizer_next(&t_kv, &p_eq, &key) &&
                    httokenizer_next(&t_kv, &p_eq, &val)) {

                    element.params[element.param_count].name = key;
                    element.params[element.param_count].value = val;
                    element.param_count++;
                }
            } else {
                log_info("exceeded %u params", HT_MAX_PARAMS);
                return false;
            }
        }

        // Dispatch to the user's logic
        if (element.name.len > 0) {
            if (handler(&element, ctx) != 0) {
                log_debug("breaking");
                break;
            }
        }
    }
    return true;
}

bool
parse_set_cookie(htstring_t value, cookie_handler_t handler, void *ctx)
{
    log_debug("(\"%.*s\", %p, %p)", (int)value.len, value.startp, handler, ctx);

    httokenizer_t t = httokenizer_ctor(value);

    ht_cookie_t cookie = {0};
    htstring_t part;
    bool first = true;

    while (httokenizer_next(&t, &p_semi, &part)) {
        if (first) {
            /* cookie-pair: split on FIRST '=' only — values may contain '=' */
            const char * eq = memchr(part.startp, '=', part.len);
            if (!eq) {
                log_info("missing equals sign");
                return false;              /* malformed: no '=' in pair */
            }
            cookie.name.startp  = part.startp;
            cookie.name.len   = (size_t)(eq - part.startp);
            cookie.value.startp = eq + 1;
            cookie.value.len  = (size_t)(part.startp + part.len - cookie.value.startp);/*part.len - cookie.name.len - 1;*/
            first = false;
        } else if (cookie.attr_count < HT_MAX_PARAMS) {
            /* cookie-av: attribute, optionally = value */
            const char * eq = memchr(part.startp, '=', part.len);
            ht_cookie_av_t *av = &cookie.attrs[cookie.attr_count];
            if (eq) {
                av->name.startp  = part.startp;
                av->name.len   = (size_t)(eq - part.startp);
                av->value.startp = eq + 1;
                av->value.len  = part.len - av->name.len - 1;
                av->has_value  = true;
            } else {
                av->name  = part;               /* flag: "secure", "HttpOnly" */
                av->has_value = false;
            }
            cookie.attr_count++;
        }
        else {
            log_info("exceeded %u params", HT_MAX_PARAMS);
            return false;
        }
    }

    if (cookie.name.len > 0)
        handler(&cookie, ctx);
    return true;
}

bool
parse_cookie(htstring_t value, cookie_pair_handler_t handler, void * ctx)
{
    log_debug("(\"%.*s\", %p, %p)", (int)value.len, value.startp, handler, ctx);

    httokenizer_t t = httokenizer_ctor(value);

    htstring_t part;
    while (httokenizer_next(&t, &p_semi, &part)) {
        ht_cookie_pair_t pair;

        const char * eq = memchr(part.startp, '=', part.len);
        if (eq) {
            pair.name.startp  = part.startp;
            pair.name.len   = (size_t)(eq - part.startp);
            pair.value.startp = eq + 1;
            pair.value.len  = part.len - pair.name.len - 1;
        } else {
            pair.name  = part;
            pair.value.startp = NULL;
            pair.value.len  = 0;
        }

        if (handler(&pair, ctx) != 0) {
            log_debug("breaking");
            break;
        }
    }
    return true;
}
