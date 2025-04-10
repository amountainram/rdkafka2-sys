#include <log.h>
#include <rdkafka.h>

void admin_client_callback(rd_kafka_t *rk, rd_kafka_event_t *rkev,
                           void *opaque);

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

typedef struct kafka_client_opt_s
{
    char *key;
    char *value;
} kafka_client_opt_t;

// takes ownership of conf only on failure
rd_kafka_conf_res_t set_options(rd_kafka_conf_t **conf)
{
#define OPTIONS_COUNT 4
#define ERR_BUF_SIZE 512
    static kafka_client_opt_t opts[OPTIONS_COUNT] = {
        {"bootstrap.servers", "localhost:9092"},
        {"client.id", "admin_client"},
        {"debug", "all"},
        {"log_level", "7"},
    };

    char buf[ERR_BUF_SIZE];
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

int create_kafka_admin_client(rd_kafka_t **rk_out)
{
#define ERR_BUF_SIZE 512
    char buf[ERR_BUF_SIZE];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (set_options(&conf) != RD_KAFKA_CONF_OK)
        return -1;

    rd_kafka_conf_set_background_event_cb(conf, admin_client_callback);
    rd_kafka_conf_set_log_cb(conf, log_cb);

    if ((*rk_out = rd_kafka_new(RD_KAFKA_PRODUCER, conf, buf, ERR_BUF_SIZE)) ==
        NULL)
    {
        log_error("cannot initialize kafka producer client: %s", buf);
        rd_kafka_conf_destroy(conf);
        return -1;
    }

    return 0;
#undef ERR_BUF_SIZE
}

void create_new_topic_instance(const char *topic_name,
                               rd_kafka_NewTopic_t **new_topic)
{
#define ERR_BUF_SIZE 512

    char buf[ERR_BUF_SIZE];
    *new_topic = rd_kafka_NewTopic_new(topic_name, 3, 1, buf, ERR_BUF_SIZE);

#undef ERR_BUF_SIZE
}

void create_delete_topic_instance(const char *topic_name,
                                  rd_kafka_DeleteTopic_t **new_topic)
{
    *new_topic = rd_kafka_DeleteTopic_new(topic_name);
}
