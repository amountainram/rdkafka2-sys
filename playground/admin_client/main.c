#define _GNU_SOURCE

#include <errno.h>
#include <getopt.h>
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
#include <unistd.h>

#include "kafka-util.h"
#include "signal_completion.h"

int help(const char *command, const char *opts)
{
    fprintf(stderr, "Usage: " BIN_NAME " %s %s\n", command, opts);
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

int create_topics(int num_topics, char **topics, const char *config_path)
{
    int ret = EXIT_SUCCESS;
    if (num_topics < 1)
        return help("create-topics", "<topic1> [topic2] ...");

    rd_kafka_t *rk = NULL;
    if (create_kafka_admin_client(&rk, config_path) == -1)
        return EXIT_FAILURE;

    rd_kafka_NewTopic_t **new_topics = NULL;
    if ((new_topics = (rd_kafka_NewTopic_t **)malloc(
             num_topics * sizeof(rd_kafka_NewTopic_t *))) == NULL)
    {
        log_error("cannot allocate new topics: %s", strerror(errno));
        ret = EXIT_FAILURE;
        goto cleanup;
    }

    int i;
    for (i = 0; i < num_topics; ++i)
        create_new_topic_instance(topics[i], new_topics + i);

    rd_kafka_queue_t *q = rd_kafka_queue_get_background(rk);
    rd_kafka_AdminOptions_t *opts =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);

    signal_completion_t *channel = NULL;
    if (signal_init(&channel) == -1)
        goto cleanup;

    rd_kafka_AdminOptions_set_opaque(opts, (void *)channel);
    rd_kafka_CreateTopics(rk, new_topics, num_topics, opts, q);

    ret = handle_signal(channel);

    // ******* CLEANUP ******** //
    rd_kafka_queue_destroy(q);
    rd_kafka_NewTopic_destroy_array(new_topics, num_topics);
    rd_kafka_AdminOptions_destroy(opts);
    free(new_topics);
cleanup:
    rd_kafka_destroy(rk);
    return ret;
}

int delete_topics(int num_topics, char **topics, const char *config_path)
{
    int ret = EXIT_SUCCESS;
    if (num_topics < 1)
        return help("delete-topics", "<topic1> [topic2] ...");

    rd_kafka_t *rk = NULL;
    if (create_kafka_admin_client(&rk, config_path) == -1)
        return EXIT_FAILURE;

    rd_kafka_DeleteTopic_t **delete_topics = NULL;
    if ((delete_topics = (rd_kafka_DeleteTopic_t **)malloc(
             num_topics * sizeof(rd_kafka_DeleteTopic_t *))) == NULL)
    {
        log_error("cannot allocate new topics: %s", strerror(errno));
        ret = EXIT_FAILURE;
        goto cleanup;
    }

    int i;
    for (i = 0; i < num_topics; ++i)
        create_delete_topic_instance(topics[i], delete_topics + i);

    rd_kafka_queue_t *q = rd_kafka_queue_get_background(rk);
    rd_kafka_AdminOptions_t *opts =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETETOPICS);

    signal_completion_t *channel = NULL;
    if (signal_init(&channel) == -1)
        goto cleanup;

    rd_kafka_AdminOptions_set_opaque(opts, (void *)channel);
    rd_kafka_DeleteTopics(rk, delete_topics, num_topics, opts, q);

    ret = handle_signal(channel);

    // ******* CLEANUP ******** //
    rd_kafka_queue_destroy(q);
    rd_kafka_DeleteTopic_destroy_array(delete_topics, num_topics);
    rd_kafka_AdminOptions_destroy(opts);
    free(delete_topics);
cleanup:
    rd_kafka_destroy(rk);
    return ret;
}

struct metadata_command_opts
{
    bool follow;
};

void clear_screen(void)
{
    printf("\033[2J\033[H");
    fflush(stdout);
}

int metadata(int num_topics, char **topic_names,
             const struct metadata_command_opts *opts, const char *config_path)
{
    (void)num_topics;
    (void)topic_names;

#define LOCAL_TIMEOUT_SEC 2
#define LOCAL_TIMEOUT LOCAL_TIMEOUT_SEC * 5000
    int ret = EXIT_SUCCESS;
    rd_kafka_t *rk = NULL;
    rd_kafka_resp_err_t err;
    if (create_kafka_admin_client(&rk, config_path) == -1)
        return EXIT_FAILURE;

    rd_kafka_metadata_t *meta;

    if (opts->follow)
    {
        time_t now, exec_time;
        while (1)
        {
            if (!atomic_load_explicit(&running, memory_order_relaxed))
                break;
            now = time(NULL);
            if ((err = rd_kafka_metadata(
                     rk, 0, NULL, (const rd_kafka_metadata_t **)&meta,
                     LOCAL_TIMEOUT)) != RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                rd_kafka_metadata_destroy(meta);
                log_error("Cannot retrieve metadata: %s",
                          rd_kafka_err2str(err));
                ret = EXIT_FAILURE;
                break;
            }
            exec_time = time(NULL) - now;

            clear_screen();
            printf("Time: %s\n", ctime(&now));
            print_metadata_table(meta);
            rd_kafka_metadata_destroy(meta);
            fflush(stdout);

            if (exec_time >= 0)
                sleep(LOCAL_TIMEOUT - exec_time);
        }
    }
    else
    {
        if ((err = rd_kafka_metadata(
                 rk, 0, NULL, (const rd_kafka_metadata_t **)&meta,
                 LOCAL_TIMEOUT)) != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            rd_kafka_metadata_destroy(meta);
            log_error("Cannot retrieve metadata: %s", rd_kafka_err2str(err));
            ret = EXIT_FAILURE;
        }
        else
        {
            print_metadata_table(meta);
            rd_kafka_metadata_destroy(meta);
        }
    }

    rd_kafka_destroy(rk);
    return ret;
}

int main(int argc, char **argv)
{
    char *config_path = getenv("ADMIN_CLIENT_CONFIG_FILE");
    char *log_level = NULL;
    if ((log_level = getenv("LOG_LEVEL")) == NULL)
        log_level = "2";
    log_set_level(atoi(log_level));

    signal(SIGINT, sig_handler);

    if (argc > 1)
    {
        char *command = argv[1];
        if (strcmp(command, "create-topics") == 0 || strcmp(command, "ct") == 0)
            return create_topics(argc - 2, argv + 2, config_path);
        else if (strcmp(command, "delete-topics") == 0 ||
                 strcmp(command, "dt") == 0)
            return delete_topics(argc - 2, argv + 2, config_path);
        else if (strcmp(command, "metadata") == 0 || strcmp(command, "m") == 0)
        {
            int opt;
            int option_index = 0;
            struct metadata_command_opts m_opts;
            static struct option long_options[] = {
                {"follow", no_argument, 0, 'f'}, {0, 0, 0, 0}};
            while ((opt = getopt_long(argc, argv, "f", long_options,
                                      &option_index)) != -1)
            {
                switch (opt)
                {
                case 'f':
                    m_opts.follow = true;
                    break;
                default:
                    return help("metadata", "[-f|--follow]");
                }
            }

            return metadata(argc - 2, argv + 2, &m_opts, config_path);
        }
    }

    return help("[command]", "[options]");
}
