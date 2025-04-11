#include <errno.h>
#include <log.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

_Atomic bool running = true;

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
