#ifndef RDKAFKA_UTIL_H
#define RDKAFKA_UTIL_H

#include <assert.h>
#include <errno.h>
#include <log.h>
#include <rdkafka.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define RED "\x1b[31m"
#define GREEN "\x1b[32m"
#define YELLOW "\x1b[33m"
#define BLUE "\x1b[34m"
#define MAGENTA "\x1b[35m"
#define CYAN "\x1b[36m"
#define RESET "\x1b[0m"

char *conf_err2str(rd_kafka_conf_res_t err)
{
    switch (err)
    {
    case RD_KAFKA_CONF_UNKNOWN:
        return "unknown property";
    case RD_KAFKA_CONF_INVALID:
        return "invalid or unsupported value";
    default:
        __builtin_unreachable();
    }
}

typedef struct kafka_client_opt_s
{
    char *key;
    char *value;
} kafka_client_opt_t;

// takes ownership of conf only on failure
rd_kafka_conf_res_t set_options(rd_kafka_conf_t **conf)
{
#define OPTIONS_COUNT 9
#define ERR_BUF_SIZE 512
    static kafka_client_opt_t opts[OPTIONS_COUNT] = {
        {"allow.auto.create.topics", "false"},
        {"bootstrap.servers", "localhost:9092"},
        {"client.id", "async_producer"},
        {"debug", "all"},
        {"enable.auto.commit", "false"},
        {"log_level", "7"},
        {"queue.buffering.max.messages", "10"},
        {"queue.buffering.max.ms", "100"},
        {"statistics.interval.ms", "5000"},
    };

    size_t buf_size = ERR_BUF_SIZE;
    char buf[ERR_BUF_SIZE];

    rd_kafka_conf_get(*conf, "queue.buffering.max.messages", buf, &buf_size);
    log_info("queue.buffering.max.messages: %s", buf);
    rd_kafka_conf_get(*conf, "queue.buffering.max.kbytes", buf, &buf_size);
    log_info("queue.buffering.max.kbytes: %s", buf);

    rd_kafka_conf_res_t ret = RD_KAFKA_CONF_OK;
    for (ssize_t i = 0; i < OPTIONS_COUNT; ++i)
    {
        if ((ret = rd_kafka_conf_set(*conf, opts[i].key, opts[i].value, buf,
                                     ERR_BUF_SIZE)) != RD_KAFKA_CONF_OK)
        {
            log_error("cannot set property '%s': %s", opts[i].key,
                      conf_err2str(ret));
            rd_kafka_conf_destroy(*conf);
            *conf = NULL;
            break;
        }
    }

    return ret;
#undef ERR_BUF_SIZE
#undef OPTIONS_COUNT
}

void log_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
    (void)rk;
    switch (level)
    {
    case 0:
    case 1:
    case 2:
        log_fatal("%s: %s", fac, buf);
        break;
    case 3:
        log_error("%s: %s", fac, buf);
        break;
    case 4:
    case 5:
        log_warn("%s: %s", fac, buf);
        break;
    case 6:
        log_info("%s: %s", fac, buf);
        break;
    case 7:
        log_debug("%s: %s", fac, buf);
        break;
    default:
        log_trace("%s: %s", fac, buf);
        break;
    }
}

rd_kafka_conf_res_t conf_prop_to_uint64(const rd_kafka_conf_t *conf,
                                        const char *key, uint64_t *value)
{
#define ERR_BUF_SIZE 512
    size_t buf_size = ERR_BUF_SIZE;
    char buf[ERR_BUF_SIZE], *endptr;
    rd_kafka_conf_res_t ret = RD_KAFKA_CONF_OK;

    if ((ret = rd_kafka_conf_get(conf, key, buf, &buf_size)) !=
        RD_KAFKA_CONF_OK)
    {
        log_error("cannot GET property " BLUE "'%s'" RESET ": " MAGENTA
                  "%s" RESET,
                  key, conf_err2str(ret));
        return ret;
    }

    errno = 0;

    *value = strtoull(buf, &endptr, 10);

    if (errno == ERANGE)
    {
        log_error("cannot convert property to uint64: overflow or underflow "
                  "occurred");
        return RD_KAFKA_CONF_INVALID;
    }

    if (*endptr != '\0')
    {
        log_error("cannot convert property to uint64: invalid characters found "
                  "in input: %s",
                  endptr);
        return RD_KAFKA_CONF_INVALID;
    }

    return ret;
#undef NUM_PROP
#undef ERR_BUF_SIZE
}

void print_current_config(rd_kafka_conf_t *conf)
{
    size_t i, cnt;
    const char **conf_dump = rd_kafka_conf_dump(conf, &cnt);

    printf("\nlibrdkafka config dump:\n");
    for (i = 0; i < cnt; i += 2)
        printf("\t(%lu)  " BLUE "%s" RESET " => " MAGENTA "%s" RESET "\n",
               (i / 2) + 1, conf_dump[i], conf_dump[i + 1]);
    printf("\n");

    rd_kafka_conf_dump_free(conf_dump, cnt);
}

void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque)
{
    static _Atomic uint64_t count = 0;
    (void)rk;
    size_t cnt;

    uint64_t marker = (uint64_t)msg->_private,
             expected =
                 atomic_fetch_add_explicit(&count, 1, memory_order_relaxed);
    log_error("marker: %llu; expected: %llu", marker, expected);
    if (marker != expected)
    {
        log_error("marker: %llu; expected: %llu", marker, expected);
        assert(false);
    }

    cnt = atomic_fetch_add_explicit((_Atomic size_t *)opaque, 1,
                                    memory_order_relaxed) +
          1;
    if (cnt % 10 == 0 && cnt != 0)
        log_info(RED "delivery report" RESET ": message count is %lu", cnt);
}

int create_kafka_producer(rd_kafka_conf_t *conf, rd_kafka_t **rk,
                          _Atomic size_t *cnt)
{
#define ERR_BUF_SIZE 512
    char buf[ERR_BUF_SIZE];

    rd_kafka_conf_set_opaque(conf, cnt);
    rd_kafka_conf_set_log_cb(conf, log_cb);
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    if ((*rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, buf, ERR_BUF_SIZE)) ==
        NULL)
    {
        log_error("cannot initialize kafka producer client: %s", buf);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    return 0;
#undef ERR_BUF_SIZE
}

void create_topic_handle(rd_kafka_t *rk, rd_kafka_topic_conf_t *conf,
                         rd_kafka_topic_t **topic, const char *name)
{
    rd_kafka_topic_conf_t *new_conf = conf;
    if (new_conf == NULL)
        new_conf = rd_kafka_topic_conf_new();

    *topic = rd_kafka_topic_new(rk, name, new_conf);
}

#undef RED
#undef GREEN
#undef YELLOW
#undef BLUE
#undef MAGENTA
#undef CYAN
#undef RESET

#endif
