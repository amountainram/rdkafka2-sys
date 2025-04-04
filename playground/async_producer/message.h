#ifndef MESSAGE_H
#define MESSAGE_H

#include <cJSON.h>
#include <errno.h>
#include <rdkafka.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct
{
    rd_kafka_message_t **msgs;
    size_t num;
} messages_t;

size_t uint64_to_str(uint64_t value, char *buf, size_t buf_size)
{
    snprintf(buf, sizeof(buf_size), "%" PRIu64, value);
    return strlen(buf);
}

int create_messages(messages_t **out, rd_kafka_topic_t *topic, size_t num)
{
#define MAX_UINT64_DIGITS 20
    static _Atomic uint64_t marker = 0;

    int ret = 0;
    size_t i;
    messages_t *msgs;
    rd_kafka_message_t **arr;
    errno = 0;
    if ((arr = (rd_kafka_message_t **)malloc(
             num * sizeof(rd_kafka_message_t))) == NULL)
        return -errno;
    if ((msgs = (messages_t *)malloc(sizeof(messages_t))) == NULL)
    {
        free(arr);
        return -errno;
    }

    for (i = 0; i < num; ++i)
    {
        char *marker_str;
        uint64_t this_marker;
        if ((marker_str = (char *)malloc(MAX_UINT64_DIGITS)) == NULL)
        {
            ret = -errno;
            break;
        }

        cJSON *key = cJSON_CreateObject();
        cJSON *_id = cJSON_CreateObject();
        cJSON_AddStringToObject(_id, "$oid", "656ae16b8383f3bbf0c5a945");
        cJSON_AddItemToObject(key, "_id", _id);

        cJSON *payload = cJSON_CreateObject();
        cJSON *before = cJSON_CreateNull();
        cJSON_AddItemToObject(payload, "before", before);
        cJSON *after = cJSON_CreateObject();
        cJSON *payload_id = cJSON_CreateObject();
        cJSON_AddStringToObject(payload_id, "$oid", "656ae16b8383f3bbf0c5a945");
        cJSON_AddItemToObject(after, "_id", payload_id);

        uint64_to_str(this_marker = atomic_fetch_add(&marker, 1), marker_str,
                      MAX_UINT64_DIGITS + 1);
        cJSON_AddStringToObject(after, "marker", marker_str);

        cJSON_AddItemToObject(after, "firstName", cJSON_CreateString("John"));
        cJSON_AddItemToObject(after, "lastName", cJSON_CreateString("Doe"));
        cJSON_AddItemToObject(payload, "after", after);

        char *key_buf = cJSON_PrintUnformatted(key);
        char *payload_buf = cJSON_PrintUnformatted(payload);

        cJSON_Delete(key);
        cJSON_Delete(payload);
        rd_kafka_message_t *msg;
        errno = 0;
        if ((msg = (rd_kafka_message_t *)malloc(sizeof(rd_kafka_message_t))) ==
            NULL)
        {
            ret = -errno;
            break;
        }
        msg->key = key_buf;
        msg->key_len = strlen(key_buf);
        msg->payload = payload_buf;
        msg->len = strlen(payload_buf);
        msg->partition = RD_KAFKA_PARTITION_UA;
        msg->rkt = topic;
        msg->_private = (void *)this_marker;

        arr[i] = msg;
    }

    if (ret != 0)
    {
        size_t j = i;
        for (i = 0; i <= j; ++i)
            free(msgs->msgs[i]);

        free(arr);
        free(msgs);
        return -1;
    }

    msgs->msgs = arr;
    msgs->num = num;
    *out = msgs;

    return 0;
#undef MAX_UINT64_DIGITS
}

#endif
