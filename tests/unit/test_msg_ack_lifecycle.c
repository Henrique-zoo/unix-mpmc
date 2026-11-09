/**
 * @file test_msg_ack_lifecycle.c
 * @brief Critérios de aceite de ACK por operação, ponta a ponta, no modo MSG.
 * @ingroup tests
 * @details Usa a fachada ipc_*, producer_run() e consumer_run() em processos
 * reais sobre filas System V reais, com capacidade reduzida por msgctl(IPC_SET)
 * para forçar contrapressão. O processo de teste faz o papel do supervisor
 * (ipc_poll_events(), ipc_ack_abandon(), remoção das filas). Cobre:
 * - C1: vários produtores ack=1 (inclusive striped) sem troca de ACKs;
 * - C2: produtor ack=1 só conclui depois que todas as suas mensagens foram
 *   consumidas, e não antes;
 * - C3: filas de dados, ACK e eventos lotadas sem deadlock;
 * - C4: SIGTERM, remoção de fila (shutdown) e produtor morto liberam esperas
 *   e não deixam ACKs nem filas para trás.
 * Todo bloqueio tem prazo: um alarm() global mata os filhos e remove as filas.
 */
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Acesso aos identificadores das filas, privados do backend. */
#include "../../src/ipc/msg_backend.c"
#include "../../src/ipc/transport.c"
#include "mpmc/workers/consumer.h"
#include "mpmc/workers/producer.h"

#define DEADLINE_MS 15000U
#define OBSERVE_MS 300U
#define MAX_KIDS 16U

static unsigned checks;
static unsigned failures;
static pid_t kids[MAX_KIDS];
static pid_t owner_pid;
static ipc_context_t *volatile live;

#define CHECK(condition)                                                       \
  do {                                                                         \
    ++checks;                                                                  \
    if (!(condition)) {                                                        \
      ++failures;                                                              \
      fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);    \
    }                                                                          \
  } while (0)

static void remove_live_queues(void) {
  ipc_context_t *context = live;
  if (context == NULL || context->message_backend == NULL)
    return;
  msg_backend_t *backend = context->message_backend;
  for (size_t index = 0u; index < backend->producer_count; ++index)
    (void)msgctl(backend->data_queues[index], IPC_RMID, NULL);
  (void)msgctl(backend->event_queue, IPC_RMID, NULL);
  (void)msgctl(backend->ack_queue, IPC_RMID, NULL);
}

static void on_alarm(int signal_number) {
  (void)signal_number;
  for (size_t index = 0u; index < MAX_KIDS; ++index)
    if (kids[index] > 0)
      (void)kill(kids[index], SIGKILL);
  remove_live_queues();
  static const char message[] = "ALARME: teste travou; filhos mortos\n";
  (void)!write(STDERR_FILENO, message, sizeof(message) - 1u);
  _exit(3);
}

static void on_fatal(int signal_number) {
  if (getpid() == owner_pid) {
    for (size_t index = 0u; index < MAX_KIDS; ++index)
      if (kids[index] > 0)
        (void)kill(kids[index], SIGKILL);
    remove_live_queues();
  }
  _exit(128 + signal_number);
}

static void tick(void) {
  const struct timespec delay = {.tv_sec = 0, .tv_nsec = 2000000};
  (void)nanosleep(&delay, NULL);
}

static unsigned long elapsed_ms(const struct timespec *start) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (unsigned long)((now.tv_sec - start->tv_sec) * 1000L +
                         (now.tv_nsec - start->tv_nsec) / 1000000L);
}

static bool set_qbytes(int queue, unsigned long bytes) {
  struct msqid_ds info;
  if (msgctl(queue, IPC_STAT, &info) == -1)
    return false;
  info.msg_qbytes = bytes;
  return msgctl(queue, IPC_SET, &info) == 0;
}

static unsigned long queued(int queue) {
  struct msqid_ds info;
  if (msgctl(queue, IPC_STAT, &info) == -1)
    return ULONG_MAX;
  return (unsigned long)info.msg_qnum;
}

static msg_backend_t *backend_of(ipc_context_t *context) {
  return context->message_backend;
}

/** @brief Cria o contexto e reduz as filas em número de entradas. */
static ipc_context_t *make_context(size_t producers, unsigned data_slots,
                                   unsigned ack_slots, unsigned event_slots) {
  ipc_context_t *context = NULL;
  if (ipc_init(&context, producers, IPC_MESSAGE_QUEUE).status != MPMC_OK) {
    ++failures;
    return NULL;
  }
  live = context;
  msg_backend_t *backend = backend_of(context);
  for (size_t index = 0u; index < producers; ++index)
    CHECK(set_qbytes(backend->data_queues[index],
                     (unsigned long)data_slots * sizeof(message_t)));
  CHECK(set_qbytes(backend->ack_queue,
                   (unsigned long)ack_slots * sizeof(ack_event_t)));
  CHECK(set_qbytes(backend->event_queue,
                   (unsigned long)event_slots * sizeof(consumption_event_t)));
  return context;
}

/** @brief Remove tudo e confere que nenhuma fila sobrou no kernel. */
static void destroy_and_check(ipc_context_t *context) {
  msg_backend_t *backend = backend_of(context);
  int ids[16];
  size_t count = 0u;
  for (size_t index = 0u; index < backend->producer_count && count < 13u;
       ++index)
    ids[count++] = backend->data_queues[index];
  ids[count++] = backend->event_queue;
  ids[count++] = backend->ack_queue;
  live = NULL;
  ipc_destroy(context);
  for (size_t index = 0u; index < count; ++index)
    CHECK(queued(ids[index]) == ULONG_MAX);
}

static int exit_code(mpmc_result_t result) {
  return result.status == MPMC_OK ? 0 : 100 + (int)result.status;
}

static pid_t spawn_producer(ipc_context_t *context, size_t queue, unsigned n,
                            producer_ack_mode_t ack, uint64_t operation) {
  pid_t pid = fork();
  if (pid == 0) {
    (void)signal(SIGTERM, SIG_DFL);
    producer_job_t job = {.producer_id = queue,
                          .message_count = n,
                          .ack_mode = ack,
                          .operation_id = operation};
    _exit(exit_code(producer_run(context, &job)));
  }
  return pid;
}

static pid_t spawn_consumer(ipc_context_t *context, size_t queue, unsigned n) {
  pid_t pid = fork();
  if (pid == 0) {
    (void)signal(SIGTERM, SIG_DFL);
    consumer_job_t job = {.producer_id = queue,
                          .message_count = n,
                          .ack_mode = CONSUMER_BLOCKING,
                          .operation_id = 1000u + n};
    consumer_result_t result;
    _exit(exit_code(consumer_run(context, &job, &result)));
  }
  return pid;
}

/**
 * @brief Recolhe um filho com prazo, atuando como supervisor.
 * @return Código de saída; 128+sinal se morto por sinal; -1 se o prazo venceu.
 */
static int reap(size_t slot, unsigned deadline_ms, ipc_context_t *context) {
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (;;) {
    if (context != NULL)
      (void)ipc_poll_events(context);
    int status = 0;
    pid_t pid = waitpid(kids[slot], &status, WNOHANG);
    if (pid == kids[slot]) {
      kids[slot] = 0;
      return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    }
    if (pid == -1 && errno != EINTR) {
      kids[slot] = 0;
      return -1;
    }
    if (elapsed_ms(&start) > deadline_ms) {
      (void)kill(kids[slot], SIGKILL);
      (void)waitpid(kids[slot], &status, 0);
      kids[slot] = 0;
      return -1;
    }
    tick();
  }
}

/** @brief Espera passiva; verdadeiro se o filho continua vivo ao fim. */
static bool still_running_after(size_t slot, unsigned ms) {
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (elapsed_ms(&start) < ms) {
    int status = 0;
    pid_t pid = waitpid(kids[slot], &status, WNOHANG);
    if (pid == kids[slot]) {
      kids[slot] = 0;
      return false;
    }
    tick();
  }
  return true;
}

/** @brief Ids cumulativos para que nenhuma operação reuse um operation_id. */
static uint64_t next_op = 1u;

/**
 * @brief C1 + C3: cinco produtores (quatro ack=1, um striped, um ack=2), quatro
 * consumidores, filas de dados de 3 posições, 2 ACKs e 2 eventos.
 */
static void test_many_producers_full_queues(void) {
  ipc_context_t *context = make_context(2u, 3u, 2u, 2u);
  if (context == NULL)
    return;
  const uint64_t base = next_op;
  next_op += 5u;
  kids[0] = spawn_producer(context, 1u, 30u, PRODUCER_ACK_WAIT, base + 0u);
  kids[1] = spawn_producer(context, 1u, 11u, PRODUCER_ACK_WAIT, base + 1u);
  kids[2] = spawn_producer(context, 2u, 25u, PRODUCER_ACK_WAIT, base + 2u);
  kids[3] = spawn_producer(context, 0u, 40u, PRODUCER_ACK_WAIT, base + 3u);
  kids[4] = spawn_producer(context, 2u, 9u, PRODUCER_ACK_NONE, base + 4u);
  /* fila 1: 30 + 11 + 20 (striped) = 61; fila 2: 25 + 20 + 9 = 54. */
  kids[5] = spawn_consumer(context, 1u, 31u);
  kids[6] = spawn_consumer(context, 1u, 30u);
  kids[7] = spawn_consumer(context, 2u, 27u);
  kids[8] = spawn_consumer(context, 2u, 27u);
  for (size_t slot = 0u; slot < 9u; ++slot) {
    CHECK(kids[slot] > 0);
    if (kids[slot] > 0)
      CHECK(reap(slot, DEADLINE_MS, context) == 0);
  }
  CHECK(ipc_poll_events(context).status == MPMC_OK);
  uint64_t first = 0u, second = 0u;
  CHECK(ipc_consumed_count(context, 1u, &first).status == MPMC_OK);
  CHECK(ipc_consumed_count(context, 2u, &second).status == MPMC_OK);
  CHECK(first == 61u && second == 54u);
  /* Cada produtor recebeu exatamente os próprios ACKs: nada sobrou. */
  msg_backend_t *backend = backend_of(context);
  CHECK(queued(backend->ack_queue) == 0u);
  CHECK(queued(backend->data_queues[0]) == 0u);
  CHECK(queued(backend->data_queues[1]) == 0u);
  destroy_and_check(context);
}

/** @brief C2: o produtor ack=1 não conclui antes do consumo de todas as
 * mensagens. */
static void test_producer_waits_for_consumption(void) {
  ipc_context_t *context = make_context(1u, 16u, 16u, 16u);
  if (context == NULL)
    return;
  const uint64_t operation = next_op++;
  kids[0] = spawn_producer(context, 1u, 5u, PRODUCER_ACK_WAIT, operation);
  CHECK(kids[0] > 0);

  /* Inserção concluída, mas nada consumido: o produtor continua esperando. */
  CHECK(still_running_after(0u, OBSERVE_MS));
  msg_backend_t *backend = backend_of(context);
  CHECK(queued(backend->data_queues[0]) == 5u);

  /* Consumo parcial (3 de 5): ainda não pode concluir. */
  kids[1] = spawn_consumer(context, 1u, 3u);
  CHECK(kids[1] > 0 && reap(1u, DEADLINE_MS, context) == 0);
  CHECK(still_running_after(0u, OBSERVE_MS));

  /* As duas últimas liberam o produtor. */
  kids[1] = spawn_consumer(context, 1u, 2u);
  CHECK(kids[1] > 0 && reap(1u, DEADLINE_MS, context) == 0);
  CHECK(reap(0u, DEADLINE_MS, context) == 0);
  CHECK(queued(backend->ack_queue) == 0u);
  destroy_and_check(context);
}

/**
 * @brief C2 (isolamento): consumir só as mensagens de outro produtor não
 * libera o produtor ack=1 que ainda não teve as suas consumidas.
 */
static void test_other_producer_does_not_satisfy_wait(void) {
  ipc_context_t *context = make_context(1u, 16u, 16u, 16u);
  if (context == NULL)
    return;
  const uint64_t first = next_op++;
  const uint64_t second = next_op++;
  kids[0] = spawn_producer(context, 1u, 4u, PRODUCER_ACK_WAIT, first);
  CHECK(kids[0] > 0);
  /* O segundo só começa depois que o primeiro enfileirou suas 4 mensagens. */
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (queued(backend_of(context)->data_queues[0]) < 4u &&
         elapsed_ms(&start) < DEADLINE_MS)
    tick();
  kids[1] = spawn_producer(context, 1u, 3u, PRODUCER_ACK_WAIT, second);
  CHECK(kids[1] > 0);
  while (queued(backend_of(context)->data_queues[0]) < 7u &&
         elapsed_ms(&start) < DEADLINE_MS)
    tick();

  /* FIFO: as 4 primeiras mensagens são do primeiro produtor. O segundo deve
   * continuar esperando depois delas. */
  kids[2] = spawn_consumer(context, 1u, 4u);
  CHECK(kids[2] > 0 && reap(2u, DEADLINE_MS, context) == 0);
  CHECK(reap(0u, DEADLINE_MS, context) == 0);
  CHECK(still_running_after(1u, OBSERVE_MS));
  kids[2] = spawn_consumer(context, 1u, 3u);
  CHECK(kids[2] > 0 && reap(2u, DEADLINE_MS, context) == 0);
  CHECK(reap(1u, DEADLINE_MS, context) == 0);
  destroy_and_check(context);
}

/**
 * @brief C4: SIGTERM libera o produtor em ack_wait; o supervisor abandona a
 * operação e o consumo das mensagens restantes termina com Q_ack de 2 posições.
 */
static void test_sigterm_and_abandon(void) {
  ipc_context_t *context = make_context(1u, 16u, 2u, 16u);
  if (context == NULL)
    return;
  const uint64_t operation = next_op++;
  kids[0] = spawn_producer(context, 1u, 6u, PRODUCER_ACK_WAIT, operation);
  CHECK(kids[0] > 0);
  msg_backend_t *backend = backend_of(context);
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (queued(backend->data_queues[0]) < 6u &&
         elapsed_ms(&start) < DEADLINE_MS)
    tick();
  CHECK(still_running_after(0u, 100u));
  CHECK(kill(kids[0], SIGTERM) == 0);
  CHECK(reap(0u, DEADLINE_MS, context) == 128 + SIGTERM);

  /* Sem abandon os ACKs tardios lotariam Q_ack; o supervisor os descarta. */
  CHECK(ipc_ack_abandon(context, operation).status == MPMC_OK);
  kids[1] = spawn_consumer(context, 1u, 6u);
  CHECK(kids[1] > 0 && reap(1u, DEADLINE_MS, context) == 0);
  CHECK(ipc_poll_events(context).status == MPMC_OK);
  CHECK(queued(backend->ack_queue) == 0u);
  CHECK(queued(backend->data_queues[0]) == 0u);
  destroy_and_check(context);
}

/** @brief C4: remover a fila de ACK (shutdown) libera ack_wait com erro. */
static void test_ack_queue_removal_releases_ack_wait(void) {
  ipc_context_t *context = make_context(1u, 16u, 16u, 16u);
  if (context == NULL)
    return;
  kids[0] = spawn_producer(context, 1u, 3u, PRODUCER_ACK_WAIT, next_op++);
  CHECK(kids[0] > 0);
  msg_backend_t *backend = backend_of(context);
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (queued(backend->data_queues[0]) < 3u &&
         elapsed_ms(&start) < DEADLINE_MS)
    tick();
  CHECK(still_running_after(0u, 100u));
  CHECK(msgctl(backend->ack_queue, IPC_RMID, NULL) == 0);
  CHECK(reap(0u, DEADLINE_MS, NULL) == 100 + (int)MPMC_ERR_SHUTDOWN);
  backend->ack_queue = -1;
  destroy_and_check(context);
}

/** @brief C4: remover a fila de dados libera o produtor ack=1 preso na fila
 * cheia (laço de envio com drenagem e backoff). */
static void test_data_queue_removal_releases_send_loop(void) {
  ipc_context_t *context = make_context(1u, 2u, 16u, 16u);
  if (context == NULL)
    return;
  kids[0] = spawn_producer(context, 1u, 10u, PRODUCER_ACK_WAIT, next_op++);
  CHECK(kids[0] > 0);
  msg_backend_t *backend = backend_of(context);
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (queued(backend->data_queues[0]) < 2u &&
         elapsed_ms(&start) < DEADLINE_MS)
    tick();
  CHECK(still_running_after(0u, 100u));
  int removed = backend->data_queues[0];
  CHECK(msgctl(removed, IPC_RMID, NULL) == 0);
  /* O laço usa IPC_NOWAIT: o Linux devolve EINVAL (e não EIDRM) a msgsnd() numa
   * fila já removida, que o wrapper classifica como INVALID_ARG. O que importa
   * ao critério é que o produtor termine logo e com falha. */
  int rc = reap(0u, DEADLINE_MS, NULL);
  CHECK(rc == 100 + (int)MPMC_ERR_SHUTDOWN ||
        rc == 100 + (int)MPMC_ERR_INVALID_ARG);
  backend->data_queues[0] = -1;
  destroy_and_check(context);
}

/** @brief C4: remover a fila de ACK libera consumidor preso em msgsnd(Q_ack
 * cheia). */
static void test_ack_queue_removal_releases_consumer(void) {
  ipc_context_t *context = make_context(1u, 16u, 1u, 16u);
  if (context == NULL)
    return;
  msg_backend_t *backend = backend_of(context);
  const uint64_t operation = next_op++;
  const uint64_t foreign = next_op++;
  CHECK(ipc_ack_begin(context, operation, 1u).status == MPMC_OK);
  message_t message = {
      .sender_pid = getpid(), .sequence = 1u, .operation_id = operation};
  CHECK(ipc_send(context, 1u, &message).status == MPMC_OK);
  /* Enche Q_ack com ACK de outra operação que ninguém drena. */
  ack_event_t occupant = {.operation_id = foreign};
  CHECK(sysv_msg_send(backend->ack_queue, (long)foreign, &occupant,
                      sizeof(occupant), true)
            .status == MPMC_OK);

  kids[0] = spawn_consumer(context, 1u, 1u);
  CHECK(kids[0] > 0);
  CHECK(still_running_after(0u, OBSERVE_MS));
  CHECK(queued(backend->data_queues[0]) == 0u);
  CHECK(msgctl(backend->ack_queue, IPC_RMID, NULL) == 0);
  CHECK(reap(0u, DEADLINE_MS, NULL) == 100 + (int)MPMC_ERR_SHUTDOWN);
  backend->ack_queue = -1;
  (void)ipc_ack_end(context, operation);
  destroy_and_check(context);
}

static bool install_handlers(void) {
  struct sigaction alarm_action = {.sa_handler = on_alarm};
  struct sigaction fatal_action = {.sa_handler = on_fatal};
  sigemptyset(&alarm_action.sa_mask);
  sigemptyset(&fatal_action.sa_mask);
  return sigaction(SIGALRM, &alarm_action, NULL) == 0 &&
         sigaction(SIGINT, &fatal_action, NULL) == 0 &&
         sigaction(SIGTERM, &fatal_action, NULL) == 0 &&
         sigaction(SIGHUP, &fatal_action, NULL) == 0;
}

int main(void) {
  owner_pid = getpid();
  if (!install_handlers()) {
    perror("sigaction");
    return 1;
  }
  alarm(120u);
  test_many_producers_full_queues();
  test_producer_waits_for_consumption();
  test_other_producer_does_not_satisfy_wait();
  test_sigterm_and_abandon();
  test_ack_queue_removal_releases_ack_wait();
  test_data_queue_removal_releases_send_loop();
  test_ack_queue_removal_releases_consumer();
  alarm(0u);
  printf("%u verificações de ciclo de vida de ACK MSG, %u falhas\n", checks,
         failures);
  if (failures != 0u)
    return 1;
  puts("Message ACK lifecycle tests passed.");
  return 0;
}
