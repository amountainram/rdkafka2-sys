#include <rdkafka.h>
#include <stdlib.h>

#define ERR_BUF_SIZE 512

int print_rd_kafka_conf_value(const rd_kafka_conf_t *conf, const char *key)
{
    size_t dest_sz;
    rd_kafka_conf_res_t ret;

    if ((ret = rd_kafka_conf_get(conf, key, NULL, &dest_sz)) !=
        RD_KAFKA_CONF_OK)
        return ret;

    char *val = (char *)malloc(sizeof(dest_sz));
    if ((ret = rd_kafka_conf_get(conf, key, val, &dest_sz)) != RD_KAFKA_CONF_OK)
    {
        free(val);
        return ret;
    }

    printf("(size:%lu) %s: %s\n", dest_sz, key, val);
    free(val);

    return RD_KAFKA_CONF_OK;
}

int main(void)
{
    char buf[ERR_BUF_SIZE];
    rd_kafka_t *rd;

    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    print_rd_kafka_conf_value(conf, "allow.auto.create.topics");

    rd_kafka_conf_res_t ret;
    if ((ret = rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9093",
                                 buf, ERR_BUF_SIZE)) != RD_KAFKA_CONF_OK)
    {
        rd_kafka_conf_destroy(conf);
        return EXIT_FAILURE;
    }

    if ((rd = rd_kafka_new(RD_KAFKA_PRODUCER, conf, buf, ERR_BUF_SIZE)) == NULL)
    {
        rd_kafka_conf_destroy(conf);
        return EXIT_FAILURE;
    }

    rd_kafka_destroy(rd);
    return EXIT_SUCCESS;
}
