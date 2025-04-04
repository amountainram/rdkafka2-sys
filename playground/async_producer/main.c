#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h>

#include "cds/mpsc.h"
#include "kafka-util.h"
#include "log.h"
#include "message.h"
#include "rdkafka.h"

#define ERR_BUF_SIZE 512
#define PRODUCER_CHANNEL_SIZE 65536

static _Atomic bool running = true;

static mpscb_tx_t *tx = NULL;

int pthread_setname_np(pthread_t thread, const char *name);

typedef struct
{
    mpscb_rx_t *rx;
    rd_kafka_t *rk;
} producer_thread_context_t;

void handle_sigint(int sig)
{
    printf("\nCaught signal %d (CTRL+C). Exiting gracefully...\n", sig);
    atomic_store_explicit(&running, false, memory_order_relaxed);
}

void *producer_thread_task(producer_thread_context_t *ctx)
{
    size_t num, retry /*, i*/;
    messages_t *msgs;
    rd_kafka_message_t *msg;
    /*rd_kafka_topic_t *topic;*/
    rd_kafka_resp_err_t err;
    while (mpscb_recv(ctx->rx, (void **)&msgs) != MPSCB_CLOSED)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
        switch ((num = msgs->num))
        {
        case 0:
            continue;
        case 1:
            retry = 0;
            msg = msgs->msgs[0];
            while ((err = rd_kafka_producev(
                        ctx->rk, RD_KAFKA_V_RKT(msg->rkt),
                        RD_KAFKA_V_PARTITION(msg->partition),
                        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_FREE),
                        RD_KAFKA_V_VALUE(msg->payload, msg->len),
                        RD_KAFKA_V_KEY(msg->key, msg->key_len),
                        RD_KAFKA_V_OPAQUE(msg->_private), RD_KAFKA_V_END)) ==
                       RD_KAFKA_RESP_ERR__QUEUE_FULL &&
                   atomic_load_explicit(&running, memory_order_relaxed))
            {
                rd_kafka_poll(ctx->rk, 0);
                /*log_error("retrying #%lu...", ++retry);*/
            }
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                free(msg->payload);

            free(msg->key);
            free(msg);
            free(msgs->msgs);
            break;
        default:
            /*msg = msgs->msgs[0];*/
            /*topic = msg->rkt;*/
            /*err = rd_kafka_produce_batch(topic, RD_KAFKA_PARTITION_UA,*/
            /*                             RD_KAFKA_MSG_F_FREE, *msgs->msgs,*/
            /*                             msgs->num);*/
            /*for (i = 0; i < num; ++i)*/
            /*{*/
            /*    rd_kafka_resp_err_t err;*/
            /*    if ((err = msgs->msgs[i]->err) !=
             * RD_KAFKA_RESP_ERR_NO_ERROR)*/
            /*    {*/
            /*        free(msgs->msgs[i]->payload);*/
            /*        log_error("msg #%lu got into error while producing: %s",
             * i,*/
            /*                  rd_kafka_err2str(err));*/
            /*    }*/
            /**/
            /*    free(msgs->msgs[i]->key);*/
            /*    free(msgs->msgs[i]);*/
            /*}*/
            /*free(msgs->msgs);*/
            /*break;*/
            __builtin_unreachable();
        }
        /*if (err != RD_KAFKA_RESP_ERR_NO_ERROR)*/
        /*    log_error("error while producing: %s", rd_kafka_err2str(err));*/
#pragma GCC diagnostic pop
        free(msgs);
        rd_kafka_poll(ctx->rk, 0);
    }

    rd_kafka_purge(ctx->rk, 0);

    return NULL;
}

int ingestion_task(mpscb_tx_t *tx, rd_kafka_topic_t *topic)
{
    int ret = 0;
    size_t num = 1;
    messages_t *msgs;
    while (1)
    {
        if (atomic_load_explicit(&running, memory_order_relaxed) == true)
        {
            if ((ret = create_messages(&msgs, topic, num)) != 0 ||
                mpscb_send(tx, (void **)&msgs) == MPSCB_CLOSED)
                break;

            usleep(100);
        }
        else
        {
            mpscb_tx_close(tx);
            break;
        }
    }

    return ret;
}

int main(void)
{
    log_set_level(LOG_INFO);
    signal(SIGINT, handle_sigint);

    /*rd_kafka_t *rk = NULL;*/
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (set_options(&conf) != RD_KAFKA_CONF_OK)
        return EXIT_FAILURE;
    print_current_config(conf);

    int ret;
    rd_kafka_t *producer;
    _Atomic size_t cnt = 0;
    if ((ret = create_kafka_producer(conf, &producer, &cnt)) == -1)
        return EXIT_FAILURE;

    char *topic_name = "test-topic-1";
    rd_kafka_topic_t *topic;
    create_topic_handle(producer, NULL, &topic, topic_name);

    mpscb_rx_t *rx;
    if ((ret = mpscb_alloc(PRODUCER_CHANNEL_SIZE, &tx, &rx)) != 0)
    {
        log_error("cannot create send channel for kafka producer: %s",
                  strerror(-ret));

        rd_kafka_flush(producer, 1000);
        rd_kafka_topic_destroy(topic);
        rd_kafka_destroy(producer);

        return EXIT_FAILURE;
    }

    pthread_t producer_thread;
    producer_thread_context_t ctx = {.rk = producer, .rx = rx};
    if ((ret = pthread_create(&producer_thread, NULL,
                              (void *(*)(void *))producer_thread_task, &ctx)) !=
        0)
    {
        log_error("cannot create thread: %s", strerror(ret));

        rd_kafka_flush(producer, 1000);
        rd_kafka_topic_destroy(topic);
        rd_kafka_destroy(producer);
        mpscb_free(rx);

        return EXIT_FAILURE;
    }
    pthread_setname_np(producer_thread, "ap:producer");

    ingestion_task(tx, topic);

    int exit_code = EXIT_SUCCESS;
    if ((ret = pthread_join(producer_thread, NULL)) != 0)
    {
        log_error("cannot create thread: %s", strerror(ret));
        exit_code = EXIT_FAILURE;
    }

    rd_kafka_flush(producer, 1000);
    rd_kafka_topic_destroy(topic);
    rd_kafka_destroy(producer);
    mpscb_free(rx);

    return exit_code;
}
