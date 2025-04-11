#include <cJSON.h>
#include <errno.h>
#include <log.h>
#include <rdkafka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

rd_kafka_conf_res_t set_option_in_rd_conf(rd_kafka_conf_t *conf, char *key,
                                          char *value)
{
#define ERR_BUF_SIZE 512
    char buf[ERR_BUF_SIZE];
    rd_kafka_conf_res_t ret = RD_KAFKA_CONF_OK;

    if ((ret = rd_kafka_conf_set(conf, key, value, buf, ERR_BUF_SIZE)) !=
        RD_KAFKA_CONF_OK)
        log_error("cannot set property '%s': %s", key, conf_err2str(ret));

    return ret;
#undef ERR_BUF_SIZE
#undef OPTIONS_COUNT
}

int set_options_from_config_file(const char *path, rd_kafka_conf_t **conf)
{
    int ret = 0;
    char *data;
    FILE *config;
    long file_len;

    if ((config = fopen(path, "r")) == NULL)
    {
        log_error("Cannot open file '%s': %s", path, strerror(errno));
        return -errno;
    }

    fseek(config, 0, SEEK_END);
    file_len = ftell(config);
    rewind(config);

    if ((data = (char *)malloc(file_len * sizeof(char))) == NULL)
    {
        log_error("Cannot allocate buffer: %s", strerror(errno));
        ret = -errno;
        goto cleanup;
    }

    fread(data, sizeof(char), file_len, config);

    cJSON *config_json;
    if ((config_json = cJSON_ParseWithLength(data, file_len)) == NULL ||
        !cJSON_IsObject(config_json))
    {
        log_error("Cannot parse config file as a JSON Object: %s",
                  cJSON_GetErrorPtr());
        ret = -1;
        goto json_cleanup;
    }

    cJSON *item = NULL;
    cJSON_ArrayForEach(item, config_json)
    {
        if (item->string != NULL && cJSON_IsString(item) &&
            set_option_in_rd_conf(*conf, item->string, item->valuestring) !=
                RD_KAFKA_CONF_OK)
        {
            rd_kafka_conf_destroy(*conf);
            *conf = NULL;
            ret = -1;
            break;
        }
    }

    cJSON_Delete(config_json);
json_cleanup:
    free(data);
cleanup:
    fclose(config);
    return ret;
}

// takes ownership of conf only on failure
rd_kafka_conf_res_t set_default_options(rd_kafka_conf_t **conf)
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

int create_kafka_admin_client(rd_kafka_t **rk_out, const char *path)
{
#define ERR_BUF_SIZE 512
    char buf[ERR_BUF_SIZE];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (path == NULL)
    {
        if (set_default_options(&conf) != RD_KAFKA_CONF_OK)
            return -1;
    }
    else if (set_options_from_config_file(path, &conf) != 0)
    {
        return -1;
    }

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

int sort_broker_list(rd_kafka_metadata_broker_t *first,
                     rd_kafka_metadata_broker_t *second)
{
    return first->id - second->id;
}

int sort_topic_list(rd_kafka_metadata_topic_t *first,
                    rd_kafka_metadata_topic_t *second)
{
    return first->topic - second->topic;
}

void print_metadata_table(const rd_kafka_metadata_t *metadata)
{
    printf("=== Kafka Cluster Metadata ===\n");

    qsort(metadata->brokers, metadata->broker_cnt,
          sizeof(rd_kafka_metadata_broker_t),
          (int (*)(const void *, const void *))sort_broker_list);

    // Print Brokers
    printf("\nBrokers (%d):\n", metadata->broker_cnt);
    printf("  %-4s %-20s %s\n", "ID", "Host", "Port");
    for (int i = 0; i < metadata->broker_cnt; ++i)
    {
        const rd_kafka_metadata_broker_t *broker = &metadata->brokers[i];
        printf("  %-4d %-20s %d\n", broker->id, broker->host, broker->port);
    }

    qsort(metadata->topics, metadata->topic_cnt,
          sizeof(rd_kafka_metadata_topic_t),
          (int (*)(const void *, const void *))sort_topic_list);

    // Print Topics
    printf("\nTopics (%d):\n", metadata->topic_cnt);
    for (int i = 0; i < metadata->topic_cnt; ++i)
    {
        const rd_kafka_metadata_topic_t *topic = &metadata->topics[i];
        printf("  Topic: %s %s\n", topic->topic,
               topic->err ? rd_kafka_err2str(topic->err) : "");

        // Print Partitions
        printf("    %-10s %-10s %-10s %s\n", "Partition", "Leader", "Replicas",
               "ISR");
        for (int j = 0; j < topic->partition_cnt; ++j)
        {
            const rd_kafka_metadata_partition_t *part = &topic->partitions[j];

            // Print replicas and ISR as comma-separated lists
            char replicas_str[128] = {0};
            char isr_str[128] = {0};

            for (int r = 0; r < part->replica_cnt; ++r)
                sprintf(replicas_str + strlen(replicas_str), "%s%d",
                        (r > 0 ? "," : ""), part->replicas[r]);

            for (int r = 0; r < part->isr_cnt; ++r)
                sprintf(isr_str + strlen(isr_str), "%s%d", (r > 0 ? "," : ""),
                        part->isrs[r]);

            printf("    %-10d %-10d %-10s %s\n", part->id, part->leader,
                   replicas_str, isr_str);
        }
    }

    printf("\n");
}
