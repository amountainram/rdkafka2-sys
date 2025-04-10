#include <errno.h>
#include <log.h>
#include <rdkafka.h>
#include <semaphore.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "kafka-util.h"

static _Atomic bool running = true;

typedef struct signal_completion_s
{
    int ret;
    sem_t s;
} signal_completion_t;

int signal_init(signal_completion_t **s_out)
{
    int ret = 0;
    signal_completion_t *s = NULL;
    if ((s = malloc(sizeof(signal_completion_t))) == NULL)
    {
        *s_out = NULL;
        return -1;
    }

    s->ret = EXIT_FAILURE;
    if ((ret = sem_init(&s->s, 0, 0)) == -1)
    {
        log_error("error while sem posting: %s", strerror(errno));
        free(s);
        *s_out = NULL;
    }

    *s_out = s;
    return ret;
}

void signal_destroy(signal_completion_t *s)
{
    sem_destroy(&s->s);
    free(s);
}

void signal_complete(signal_completion_t *s, int ret)
{
    s->ret = ret;
    if (sem_post(&s->s) == -1)
        log_error("error while sem posting: %s", strerror(errno));
}

void sig_handler(int signal)
{
    (void)signal;
    atomic_store_explicit(&running, false, memory_order_relaxed);
}

int help(const char *command, const char *opts)
{
    fprintf(stderr, "Usage: admin_client %s %s\n", command, opts);
    return EXIT_FAILURE;
}

void handle_create_topics_result(rd_kafka_t *rk, rd_kafka_event_t *rkev)
{
    (void)rk;
    size_t topics_cnt = 0;
    rd_kafka_topic_result_t **topic_results = NULL;
    rd_kafka_resp_err_t err;
    signal_completion_t *opaque =
        (signal_completion_t *)rd_kafka_event_opaque(rkev);
    if ((err = rd_kafka_event_error(rkev)) != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        log_error("create topic returned error: %s", rd_kafka_err2str(err));
        signal_complete(opaque, EXIT_FAILURE);
        goto cleanup;
    }
    else
    {
        topic_results =
            (rd_kafka_topic_result_t **)rd_kafka_CreateTopics_result_topics(
                rkev, &topics_cnt);
    }

    size_t j;
    for (j = 0; j < topics_cnt; ++j)
    {
        if (rd_kafka_topic_result_error(topic_results[j]))
        {
            log_error("Topic '%s' creation failed: %s",
                      rd_kafka_topic_result_name(topic_results[j]),
                      rd_kafka_topic_result_error_string(topic_results[j]));
            signal_complete(opaque, EXIT_FAILURE);
            goto cleanup;
        }
        else
        {
            log_info("Topic '%s' created successfully.",
                     rd_kafka_topic_result_name(topic_results[j]));
        }
    }

    signal_complete(opaque, EXIT_SUCCESS);
cleanup:
    rd_kafka_event_destroy(rkev);
}

void handle_delete_topics_result(rd_kafka_t *rk, rd_kafka_event_t *rkev)
{
    (void)rk;
    size_t topics_cnt = 0;
    rd_kafka_topic_result_t **topic_results = NULL;
    rd_kafka_resp_err_t err;
    signal_completion_t *opaque =
        (signal_completion_t *)rd_kafka_event_opaque(rkev);
    if ((err = rd_kafka_event_error(rkev)) != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        log_error("create topic returned error: %s", rd_kafka_err2str(err));
        signal_complete(opaque, EXIT_FAILURE);
        goto cleanup;
    }
    else
    {
        topic_results =
            (rd_kafka_topic_result_t **)rd_kafka_DeleteTopics_result_topics(
                rkev, &topics_cnt);
    }

    size_t j;
    for (j = 0; j < topics_cnt; ++j)
    {
        if (rd_kafka_topic_result_error(topic_results[j]))
        {
            log_error("Cannot delete topic '%s': %s",
                      rd_kafka_topic_result_name(topic_results[j]),
                      rd_kafka_topic_result_error_string(topic_results[j]));
            signal_complete(opaque, EXIT_FAILURE);
            goto cleanup;
        }
        else
        {
            log_info("Topic '%s' deleted successfully.",
                     rd_kafka_topic_result_name(topic_results[j]));
        }
    }

    signal_complete(opaque, EXIT_SUCCESS);
cleanup:
    rd_kafka_event_destroy(rkev);
}

void admin_client_callback(rd_kafka_t *rk, rd_kafka_event_t *rkev, void *opaque)
{
    (void)opaque;
    rd_kafka_event_type_t type = rd_kafka_event_type(rkev);
    switch (type)
    {
    case RD_KAFKA_EVENT_CREATETOPICS_RESULT:
        handle_create_topics_result(rk, rkev);
        break;
    case RD_KAFKA_EVENT_DELETETOPICS_RESULT:
        handle_delete_topics_result(rk, rkev);
        break;
    default:
        break;
    }
}

int handle_signal(signal_completion_t *channel)
{
    int ret = 0;
    struct timespec t = {.tv_sec = 1, .tv_nsec = 0};
    while (1)
    {
        if (!atomic_load_explicit(&running, memory_order_relaxed))
        {
            ret = EXIT_FAILURE;
            break;
        }

        int lock = sem_timedwait(&channel->s, &t);
        if (lock == -1 && errno == ETIMEDOUT)
            continue;
        if (lock == -1 && errno == EAGAIN)
            continue;

        ret = channel->ret;
        break;
    }

    signal_destroy(channel);
    return ret;
}

int create_topics(int argc, char **topics)
{
    int ret = EXIT_SUCCESS;
    if (argc < 1)
        return help("create-topics", "<topic1> [topic2] ...");

    rd_kafka_t *rk = NULL;
    if (create_kafka_admin_client(&rk) == -1)
        return EXIT_FAILURE;

    rd_kafka_NewTopic_t **new_topics = NULL;
    if ((new_topics = (rd_kafka_NewTopic_t **)malloc(
             argc * sizeof(rd_kafka_NewTopic_t *))) == NULL)
    {
        log_error("cannot allocate new topics: %s", strerror(errno));
        ret = EXIT_FAILURE;
        goto cleanup;
    }

    int i;
    for (i = 0; i < argc; ++i)
        create_new_topic_instance(topics[i], new_topics + i);

    rd_kafka_queue_t *q = rd_kafka_queue_get_background(rk);
    rd_kafka_AdminOptions_t *opts =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);

    signal_completion_t *channel = NULL;
    if (signal_init(&channel) == -1)
        goto cleanup;

    rd_kafka_AdminOptions_set_opaque(opts, (void *)channel);
    rd_kafka_CreateTopics(rk, new_topics, argc, opts, q);

    ret = handle_signal(channel);

    // ******* CLEANUP ******** //
    rd_kafka_queue_destroy(q);
    for (i = 0; i < argc; ++i)
        rd_kafka_NewTopic_destroy(new_topics[i]);
    free(new_topics);
    rd_kafka_AdminOptions_destroy(opts);
cleanup:
    rd_kafka_destroy(rk);
    return ret;
}

int delete_topics(int argc, char **topics)
{
    int ret = EXIT_SUCCESS;
    if (argc < 1)
        return help("delete-topics", "<topic1> [topic2] ...");

    rd_kafka_t *rk = NULL;
    if (create_kafka_admin_client(&rk) == -1)
        return EXIT_FAILURE;

    rd_kafka_DeleteTopic_t **delete_topics = NULL;
    if ((delete_topics = (rd_kafka_DeleteTopic_t **)malloc(
             argc * sizeof(rd_kafka_DeleteTopic_t *))) == NULL)
    {
        log_error("cannot allocate new topics: %s", strerror(errno));
        ret = EXIT_FAILURE;
        goto cleanup;
    }

    int i;
    for (i = 0; i < argc; ++i)
        create_delete_topic_instance(topics[i], delete_topics + i);

    rd_kafka_queue_t *q = rd_kafka_queue_get_background(rk);
    rd_kafka_AdminOptions_t *opts =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETETOPICS);

    signal_completion_t *channel = NULL;
    if (signal_init(&channel) == -1)
        goto cleanup;

    rd_kafka_AdminOptions_set_opaque(opts, (void *)channel);
    rd_kafka_DeleteTopics(rk, delete_topics, argc, opts, q);

    ret = handle_signal(channel);

    // ******* CLEANUP ******** //
    rd_kafka_queue_destroy(q);
    for (i = 0; i < argc; ++i)
        rd_kafka_DeleteTopic_destroy(delete_topics[i]);
    free(delete_topics);
    rd_kafka_AdminOptions_destroy(opts);
cleanup:
    rd_kafka_destroy(rk);
    return ret;
}

int main(int argc, char **argv)
{
    char *log_level = NULL;
    if ((log_level = getenv("LOG_LEVEL")) == NULL)
        log_level = "2";
    log_set_level(atoi(log_level));

    signal(SIGINT, sig_handler);

    if (argc > 1)
    {
        char *command = argv[1];
        if (strcmp(command, "create-topics") == 0 || strcmp(command, "ct") == 0)
            return create_topics(argc - 2, argv + 2);
        else if (strcmp(command, "delete-topics") == 0 ||
                 strcmp(command, "dt") == 0)
            return delete_topics(argc - 2, argv + 2);
    }

    return help("[command]", "[options]");
}
