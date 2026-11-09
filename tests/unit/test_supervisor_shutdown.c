/**
 * @file test_supervisor_shutdown.c
 * @brief Testes do prazo de shutdown com filhos reais e falhas controladas.
 * @ingroup tests
 * @details A fachada IPC é substituída: nenhum objeto System V é criado.
 * Pipes confirmam que cada filho já configurou SIGTERM antes do despacho voltar.
 * O relógio e poll() podem simular vencimento/interrupção sem sleeps. Um cenário
 * também executa a espera real de dois segundos, com alarm() como watchdog.
 */
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "mpmc/app/supervisor.h"

/** @brief Contexto substituto sem recursos IPC globais. */
struct ipc_context {
    bool live; /**< Indica que a instância simulada ainda não foi destruída. */
};

/** @brief Modos de execução do substituto de poll(). */
typedef enum {
    WAIT_NATIVE,    /**< Usa poll() real. */
    WAIT_EXPIRE,    /**< Avança o relógio até o timeout informado. */
    WAIT_INTERRUPT, /**< Avança 1,2 s, retorna EINTR e depois simula vencimento. */
    WAIT_FAIL,      /**< Retorna EBADF sem consumir o prazo. */
    WAIT_BAD_PIPE,  /**< Retorna POLLNVAL sem consumir o prazo. */
} wait_mode_t;

/** @brief Estado restrito aos filhos e substitutos desta suíte. */
static struct {
    struct ipc_context ipc;
    supervisor_t *supervisor;
    pid_t children[8];
    size_t child_count;
    pid_t external;
    bool ignore_term;
    bool fake_clock;
    bool fail_clock;
    bool fail_term;
    bool fail_kill;
    bool exit_after_ready;
    bool fail_fork;
    bool fail_pipe;
    bool fail_init;
    bool shutdown_active;
    bool fail_install;
    bool fail_unblock;
    bool fail_restore_action;
    bool fail_restore_mask;
    bool pipe_eof;
    bool signal_esrch;
    bool invalid_wait_status;
    unsigned fail_term_count;
    unsigned fail_kill_count;
    unsigned fail_read_count;
    unsigned fail_wait_count;
    int wait_errno;
    unsigned clock_failure_at;
    unsigned clock_calls;
    unsigned action_calls;
    unsigned init_calls;
    unsigned destroy_calls;
    unsigned pipe_closes;
    unsigned ipc_failures;
    mpmc_result_t ipc_error;
    int event_pipe[2];
    struct sigaction original_action;
    sigset_t original_mask;
    pid_t parent_pid;
    supervisor_shutdown_error_t errors[64];
    size_t error_count;
    int64_t elapsed_ns;
    wait_mode_t wait_mode;
    unsigned poll_calls;
    unsigned term_calls;
    unsigned kill_calls;
    int first_timeout;
    int second_timeout;
    short bad_poll_revents;
} fixture;

/** @brief Totais de verificações e falhas no processo pai. */
static unsigned checks, failures;

/** @brief Registra falha sem desabilitar as verificações em release. */
#define CHECK(condition)                                                                           \
    do {                                                                                           \
        ++checks;                                                                                  \
        if (!(condition)) {                                                                        \
            ++failures;                                                                            \
            fprintf(stderr, "FALHA %s:%d: %s\n", __FILE__, __LINE__, #condition);                  \
        }                                                                                          \
    } while (0)

static void watchdog(int signal_number);
static void previous_child_handler(int signal_number);
static pid_t create_child(void);
static supervisor_t *prepare(bool ignore_term, unsigned count);
static void finish(supervisor_t *supervisor);
static void check_reports(supervisor_t *supervisor, unsigned count, int signal_number);
static void reset_fixture(void);
static mpmc_result_t run_shutdown(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *reporter
);
static void collect_error(const supervisor_shutdown_error_t *error, void *user_data);
static void check_error(size_t index, supervisor_shutdown_operation_t operation, int system_errno);

/** @brief Reporter que copia diagnósticos e altera errno para testar sua preservação. */
static const supervisor_shutdown_reporter_t reporter = {
    .on_error = collect_error,
    .user_data = &fixture,
};

/**
 * @brief Fornece ao supervisor um filho já pronto para receber sinais.
 * @return PID positivo no pai, ou -1 em falha de pipe()/fork().
 */
pid_t mpmc_shutdown_test_fork(void) {
    if (fixture.fail_fork) {
        fixture.fail_fork = false;
        errno = EAGAIN;
        return -1;
    }
    return create_child();
}

/**
 * @brief Observa sinais enviados aos filhos e injeta falhas de permissão.
 * @param pid PID positivo de um filho registrado no supervisor.
 * @param signal_number SIGTERM ou SIGKILL.
 * @return Resultado de kill(), ou -1 com EPERM em uma falha programada.
 */
int mpmc_shutdown_test_kill(pid_t pid, int signal_number) {
    CHECK(pid > 0 && pid != fixture.external);
    if (signal_number == SIGTERM) ++fixture.term_calls;
    if (signal_number == SIGKILL) ++fixture.kill_calls;
    if (signal_number == SIGTERM && fixture.signal_esrch) {
        fixture.signal_esrch = false;
        errno = ESRCH;
        return -1;
    }
    bool fail = signal_number == SIGTERM ? fixture.fail_term : fixture.fail_kill;
    unsigned *remaining =
        signal_number == SIGTERM ? &fixture.fail_term_count : &fixture.fail_kill_count;
    if (*remaining > 0) {
        --*remaining;
        fail = true;
    }
    if (fail) {
        if (signal_number == SIGTERM)
            fixture.fail_term = false;
        else
            fixture.fail_kill = false;
        errno = EPERM;
        return -1;
    }
    return kill(pid, signal_number);
}

/**
 * @brief Consulta o relógio real ou o monotônico simulado e injeta EIO.
 * @param clock_id Relógio solicitado pelo supervisor.
 * @param[out] out Recebe o instante somente em sucesso.
 * @return Zero em sucesso; -1 na falha programada ou se clock_gettime() falhar.
 */
int mpmc_shutdown_test_clock_gettime(clockid_t clock_id, struct timespec *out) {
    if (fixture.shutdown_active) ++fixture.clock_calls;
    if (fixture.fail_clock
        || (fixture.shutdown_active && fixture.clock_failure_at != 0
            && fixture.clock_calls == fixture.clock_failure_at)) {
        fixture.fail_clock = false;
        errno = EIO;
        return -1;
    }
    if (!fixture.fake_clock) return clock_gettime(clock_id, out);
    CHECK(clock_id == CLOCK_MONOTONIC);
    *out = (struct timespec){
        .tv_sec = 100 + fixture.elapsed_ns / 1000000000,
        .tv_nsec = fixture.elapsed_ns % 1000000000,
    };
    return 0;
}

/**
 * @brief Injeta falhas distintas na instalação e restauração de SIGCHLD.
 * @param signal_number Sinal solicitado pelo supervisor.
 * @param action Ação a instalar, ou NULL para consulta.
 * @param[out] previous Recebe a ação anterior em sucesso, quando não nulo.
 * @return Resultado real de sigaction(), ou -1 com EINVAL/EACCES na falha programada.
 */
int mpmc_shutdown_test_sigaction(
    int signal_number,
    const struct sigaction *action,
    struct sigaction *previous
) {
    if (fixture.shutdown_active && action != NULL) {
        ++fixture.action_calls;
        if (fixture.action_calls == 1 && fixture.fail_install) {
            fixture.fail_install = false;
            errno = EINVAL;
            return -1;
        }
        if (fixture.action_calls == 2 && fixture.fail_restore_action) {
            fixture.fail_restore_action = false;
            errno = EACCES;
            return -1;
        }
    }
    return sigaction(signal_number, action, previous);
}

/**
 * @brief Injeta falhas de desbloqueio e restauração da máscara durante o shutdown.
 * @param how Operação de máscara solicitada.
 * @param set Máscara recebida, ou NULL para consulta.
 * @param[out] previous Recebe a máscara anterior em sucesso, quando não nulo.
 * @return Resultado real de sigprocmask(), ou -1 com EPERM/EINVAL na falha programada.
 */
int mpmc_shutdown_test_sigprocmask(int how, const sigset_t *set, sigset_t *previous) {
    if (fixture.shutdown_active && how == SIG_UNBLOCK && fixture.fail_unblock) {
        fixture.fail_unblock = false;
        errno = EPERM;
        return -1;
    }
    if (fixture.shutdown_active && how == SIG_SETMASK && fixture.fail_restore_mask) {
        fixture.fail_restore_mask = false;
        errno = EINVAL;
        return -1;
    }
    return sigprocmask(how, set, previous);
}

/**
 * @brief Injeta falhas de coleta e estado inesperado sem criar PIDs fictícios.
 * @param pid Filho a consultar.
 * @param[out] status Recebe o estado somente quando o filho é recolhido.
 * @param options Opções repassadas a waitpid().
 * @return Resultado real de waitpid(), ou -1 com o errno programado.
 */
pid_t mpmc_shutdown_test_waitpid(pid_t pid, int *status, int options) {
    if (fixture.shutdown_active && fixture.fail_wait_count > 0) {
        --fixture.fail_wait_count;
        errno = fixture.wait_errno == 0 ? EIO : fixture.wait_errno;
        return -1;
    }
    pid_t waited = waitpid(pid, status, options);
    if (fixture.shutdown_active && waited > 0 && fixture.invalid_wait_status) {
        fixture.invalid_wait_status = false;
        *status = 0x7f;
    }
    return waited;
}

/**
 * @brief Injeta falha ou EOF exclusivamente na leitura do self-pipe.
 * @param fd Descritor solicitado pelo supervisor.
 * @param[out] buffer Destino da leitura real.
 * @param size Quantidade máxima de bytes a ler.
 * @return Resultado de read(), -1 com EIO ou zero no EOF programado.
 */
ssize_t mpmc_shutdown_test_read(int fd, void *buffer, size_t size) {
    if (fixture.shutdown_active && fd == fixture.event_pipe[0]) {
        if (fixture.fail_read_count > 0) {
            --fixture.fail_read_count;
            errno = EIO;
            return -1;
        }
        if (fixture.pipe_eof) {
            fixture.pipe_eof = false;
            return 0;
        }
    }
    return read(fd, buffer, size);
}

/**
 * @brief Observa os descritores próprios do supervisor e injeta falha de criação.
 * @param[out] descriptors Recebe os dois descritores em sucesso.
 * @return Resultado real de pipe(), ou -1 com EMFILE na falha programada.
 */
int mpmc_shutdown_test_pipe(int descriptors[2]) {
    if (fixture.fail_pipe) {
        fixture.fail_pipe = false;
        errno = EMFILE;
        return -1;
    }
    int result = pipe(descriptors);
    if (result == 0) {
        fixture.event_pipe[0] = descriptors[0];
        fixture.event_pipe[1] = descriptors[1];
    }
    return result;
}

/**
 * @brief Observa o fechamento dos descritores próprios do supervisor.
 * @param fd Descritor a fechar.
 * @return Resultado original de close().
 */
int mpmc_shutdown_test_close(int fd) {
    if (fd == fixture.event_pipe[0] || fd == fixture.event_pipe[1]) ++fixture.pipe_closes;
    return close(fd);
}

/**
 * @brief Aguarda eventos reais ou simula interrupção, vencimento e falhas.
 * @param[in,out] fds Descritor do self-pipe e eventos observados.
 * @param count Quantidade de descritores, esperada igual a um.
 * @param timeout Tempo restante positivo, em milissegundos.
 * @return Resultado real de poll(), zero no vencimento simulado, um para
 * POLLNVAL ou -1 com EINTR/EBADF nos cenários programados.
 */
int mpmc_shutdown_test_poll(struct pollfd *fds, nfds_t count, int timeout) {
    ++fixture.poll_calls;
    CHECK(count == 1 && timeout > 0 && timeout <= 2000);
    if (fixture.poll_calls == 1u) fixture.first_timeout = timeout;
    if (fixture.poll_calls == 2u) fixture.second_timeout = timeout;
    command_t command = {.kind = COMMAND_PRODUCE};
    CHECK(supervisor_dispatch(fixture.supervisor, &command, NULL).status == MPMC_ERR_SHUTDOWN);
    switch (fixture.wait_mode) {
        case WAIT_NATIVE:
            return poll(fds, count, timeout);
        case WAIT_INTERRUPT:
            if (fixture.poll_calls == 1u) {
                fixture.elapsed_ns += 1200000000;
                errno = EINTR;
                return -1;
            }
            // Segue para o vencimento do tempo restante.
            [[fallthrough]];
        case WAIT_EXPIRE:
            fixture.elapsed_ns += (int64_t)timeout * 1000000;
            return 0;
        case WAIT_FAIL:
            errno = EBADF;
            return -1;
        case WAIT_BAD_PIPE:
            fds[0].revents = fixture.bad_poll_revents == 0 ? POLLNVAL : fixture.bad_poll_revents;
            return 1;
    }
    return -1;
}

/**
 * @brief Inicializa somente o contexto substituto da fixture.
 * @param[out] out Recebe o contexto local simulado.
 * @param producer_count Quantidade de estruturas, não utilizada pela fixture.
 * @param mode Paradigma solicitado, não utilizado pela fixture.
 * @return Estrutura mpmc_result_t com MPMC_OK e system_errno zero, ou MPMC_ERR_SYS
 * com EIO quando a inicialização foi programada para falhar.
 */
mpmc_result_t mpmc_shutdown_test_ipc_init(
    ipc_context_t **out,
    size_t producer_count,
    ipc_mode_t mode
) {
    (void)producer_count;
    (void)mode;
    ++fixture.init_calls;
    if (fixture.fail_init) {
        *out = NULL;
        errno = EIO;
        return (mpmc_result_t){.status = MPMC_ERR_SYS, .system_errno = EIO};
    }
    fixture.ipc.live = true;
    *out = &fixture.ipc;
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Confere a propriedade e encerra o contexto local substituto.
 * @param context Instância simulada que deve continuar viva até esta chamada.
 */
void mpmc_shutdown_test_ipc_destroy(ipc_context_t *context) {
    CHECK(context == &fixture.ipc && fixture.ipc.live);
    ++fixture.destroy_calls;
    fixture.ipc.live = false;
}

/**
 * @brief Simula uma drenagem sem eventos e sem acesso a recursos globais.
 * @param context Contexto simulado ainda vivo.
 * @return Estrutura mpmc_result_t com MPMC_OK e system_errno zero quando não há
 * falha programada; nos demais casos, cópia de fixture.ipc_error, incluindo o
 * status e o system_errno configurados pelo cenário.
 */
mpmc_result_t mpmc_shutdown_test_ipc_poll_events(ipc_context_t *context) {
    CHECK(context == &fixture.ipc && fixture.ipc.live);
    if (fixture.ipc_failures > 0) {
        --fixture.ipc_failures;
        return fixture.ipc_error;
    }
    return (mpmc_result_t){.status = MPMC_OK};
}

/**
 * @brief Fornece contador zero à apresentação de relatórios, sem IPC real.
 * @param context Contexto não consultado por este substituto.
 * @param producer_id Índice não consultado por este substituto.
 * @param[out] out Recebe zero.
 * @return Estrutura mpmc_result_t com status MPMC_OK e system_errno zero.
 */
mpmc_result_t mpmc_shutdown_test_ipc_consumed_count(
    ipc_context_t *context,
    size_t producer_id,
    uint64_t *out
) {
    (void)context;
    (void)producer_id;
    *out = 0;
    return (mpmc_result_t){.status = MPMC_OK};
}

/** @brief Verifica término cooperativo, restauração de sinais e idempotência. */
static void test_cooperative(void) {
    supervisor_t *supervisor = prepare(false, 2);
    struct sigaction action = {0}, previous, restored;
    action.sa_handler = previous_child_handler;
    sigemptyset(&action.sa_mask);
    CHECK(sigaction(SIGCHLD, &action, &previous) == 0);
    sigset_t child_signal, old_mask, restored_mask;
    sigemptyset(&child_signal);
    sigaddset(&child_signal, SIGCHLD);
    CHECK(sigprocmask(SIG_BLOCK, &child_signal, &old_mask) == 0);

    CHECK(run_shutdown(supervisor, NULL).status == MPMC_OK);
    CHECK(fixture.kill_calls == 0);
    check_reports(supervisor, 2, SIGTERM);
    CHECK(sigaction(SIGCHLD, NULL, &restored) == 0);
    CHECK(restored.sa_handler == previous_child_handler);
    CHECK(sigprocmask(SIG_SETMASK, NULL, &restored_mask) == 0);
    CHECK(sigismember(&restored_mask, SIGCHLD) == 1);
    CHECK(sigaction(SIGCHLD, &previous, NULL) == 0);
    CHECK(sigprocmask(SIG_SETMASK, &old_mask, NULL) == 0);

    CHECK(run_shutdown(supervisor, NULL).status == MPMC_OK);
    CHECK(supervisor_report_count(supervisor) == 2);
    finish(supervisor);
}

/** @brief Executa o prazo real comum a dois filhos que ignoram SIGTERM. */
static void test_native_deadline(void) {
    supervisor_t *supervisor = prepare(true, 2);
    fixture.external = create_child();
    CHECK(fixture.external > 0);
    CHECK(run_shutdown(supervisor, NULL).status == MPMC_OK);
    CHECK(fixture.term_calls == 2 && fixture.kill_calls == 2);
    CHECK(fixture.poll_calls == 1 && fixture.first_timeout > 0);
    check_reports(supervisor, 2, SIGKILL);
    CHECK(kill(fixture.external, 0) == 0);
    finish(supervisor);
}

/** @brief Comprova que EINTR não reinicia o prazo de dois segundos. */
static void test_interrupted_deadline(void) {
    supervisor_t *supervisor = prepare(true, 1);
    fixture.fake_clock = true;
    fixture.wait_mode = WAIT_INTERRUPT;
    CHECK(run_shutdown(supervisor, NULL).status == MPMC_OK);
    CHECK(fixture.poll_calls == 2);
    CHECK(fixture.first_timeout == 2000 && fixture.second_timeout == 800);
    check_reports(supervisor, 1, SIGKILL);
    finish(supervisor);
}

/** @brief Confere liberação dos filhos e preservação do primeiro erro. */
static void test_wait_errors(void) {
    for (unsigned scenario = 0; scenario < 4; ++scenario) {
        supervisor_t *supervisor = prepare(true, 1);
        fixture.fake_clock = true;
        if (scenario == 0) fixture.fail_clock = true;
        if (scenario == 1) fixture.wait_mode = WAIT_FAIL;
        if (scenario == 2) fixture.wait_mode = WAIT_BAD_PIPE;
        if (scenario == 3) fixture.fail_term = true;
        const int expected[] = {EIO, EBADF, 0, EPERM};
        mpmc_result_t result = run_shutdown(supervisor, NULL);
        CHECK(result.status == MPMC_ERR_SYS && result.system_errno == expected[scenario]);
        check_reports(supervisor, 1, SIGKILL);
        CHECK(run_shutdown(supervisor, NULL).status == MPMC_OK);
        finish(supervisor);
    }
}

/** @brief Uma falha de SIGKILL retorna sem espera ilimitada e permite repetir. */
static void test_kill_failure(void) {
    supervisor_t *supervisor = prepare(true, 1);
    fixture.fake_clock = true;
    fixture.wait_mode = WAIT_EXPIRE;
    fixture.fail_kill = true;
    mpmc_result_t result = run_shutdown(supervisor, NULL);
    CHECK(result.status == MPMC_ERR_SYS && result.system_errno == EPERM);
    CHECK(supervisor_report_count(supervisor) == 0);
    CHECK(run_shutdown(supervisor, NULL).status == MPMC_OK);
    check_reports(supervisor, 1, SIGKILL);
    finish(supervisor);
}

/** @brief Cada erro local produz um diagnóstico, com a união apropriada e sem duplicação. */
static void test_reporter_errors(void) {
    const supervisor_shutdown_operation_t operations[] = {
        SUPERVISOR_READ_CLOCK,
        SUPERVISOR_WAIT_EVENTS,
        SUPERVISOR_WAIT_EVENTS,
        SUPERVISOR_SEND_SIGNAL,
    };
    const int causes[] = {EIO, EBADF, 0, EPERM};
    for (unsigned scenario = 0; scenario < 4; ++scenario) {
        supervisor_t *supervisor = prepare(true, 1);
        fixture.fake_clock = true;
        if (scenario == 0) fixture.fail_clock = true;
        if (scenario == 1) fixture.wait_mode = WAIT_FAIL;
        if (scenario == 2) fixture.wait_mode = WAIT_BAD_PIPE;
        if (scenario == 3) fixture.fail_term = true;

        mpmc_result_t result = run_shutdown(supervisor, &reporter);
        CHECK(result.status == MPMC_ERR_SYS && result.system_errno == causes[scenario]);
        CHECK(fixture.error_count == 1);
        check_error(0, operations[scenario], causes[scenario]);
        if (scenario == 1) CHECK(fixture.errors[0].poll_revents == 0);
        if (scenario == 2) CHECK(fixture.errors[0].poll_revents == POLLNVAL);
        if (scenario == 3) {
            CHECK(fixture.errors[0].send_signal.child_pid == fixture.children[0]);
            CHECK(fixture.errors[0].send_signal.signal_number == SIGTERM);
        }
        check_reports(supervisor, 1, SIGKILL);
        finish(supervisor);
    }
}

/** @brief Preserva falhas de vários filhos e permite repetir após SIGKILL malsucedido. */
static void test_multiple_signal_errors(void) {
    supervisor_t *supervisor = prepare(true, 2);
    fixture.fake_clock = true;
    fixture.wait_mode = WAIT_EXPIRE;
    fixture.fail_term_count = 2;
    fixture.fail_kill_count = 1;
    mpmc_result_t result = run_shutdown(supervisor, &reporter);
    CHECK(result.status == MPMC_ERR_SYS && result.system_errno == EPERM);
    CHECK(fixture.error_count == 3);
    for (size_t index = 0; index < 3; ++index) {
        check_error(index, SUPERVISOR_SEND_SIGNAL, EPERM);
        CHECK(fixture.errors[index].send_signal.signal_number == (index < 2 ? SIGTERM : SIGKILL));
        CHECK(fixture.errors[index].send_signal.child_pid == fixture.children[index == 1 ? 1 : 0]);
    }
    CHECK(run_shutdown(supervisor, &reporter).status == MPMC_OK);
    CHECK(fixture.error_count == 3);
    check_reports(supervisor, 2, SIGKILL);
    finish(supervisor);
}

/** @brief Reporta falhas secundárias de restauração sem substituir a causa original. */
static void test_signal_configuration_errors(void) {
    for (unsigned scenario = 0; scenario < 6; ++scenario) {
        supervisor_t *supervisor = prepare(true, 1);
        fixture.fake_clock = true;
        fixture.wait_mode = WAIT_EXPIRE;
        if (scenario == 0) fixture.fail_install = true;
        if (scenario == 1) fixture.fail_unblock = true;
        if (scenario == 2) fixture.wait_mode = WAIT_FAIL;
        if (scenario == 5) fixture.clock_failure_at = 2;
        fixture.fail_restore_action = scenario != 0 && scenario != 4;
        fixture.fail_restore_mask = scenario == 2 || scenario == 4 || scenario == 5;
        const int causes[] = {EINVAL, EPERM, EBADF, EACCES, EINVAL, EIO};
        const supervisor_shutdown_operation_t operations[] = {
            SUPERVISOR_INSTALL_SIGCHLD,
            SUPERVISOR_UNBLOCK_SIGCHLD,
            SUPERVISOR_WAIT_EVENTS,
            SUPERVISOR_RESTORE_SIGCHLD,
            SUPERVISOR_RESTORE_SIGNAL_MASK,
            SUPERVISOR_READ_CLOCK,
        };

        mpmc_result_t result = run_shutdown(supervisor, &reporter);
        CHECK(result.status == MPMC_ERR_SYS && result.system_errno == causes[scenario]);
        check_error(0, operations[scenario], causes[scenario]);
        size_t expected = scenario == 1 ? 2 : (scenario == 2 || scenario == 5 ? 3 : 1);
        CHECK(fixture.error_count == expected);
        if (expected > 1) check_error(1, SUPERVISOR_RESTORE_SIGCHLD, EACCES);
        if (expected > 2) check_error(2, SUPERVISOR_RESTORE_SIGNAL_MASK, EINVAL);
        check_reports(supervisor, 1, SIGKILL);
        finish(supervisor);
    }
}

/** @brief Distingue erro/EOF do self-pipe de falha retornada pelo backend IPC. */
static void test_event_errors(void) {
    for (unsigned scenario = 0; scenario < 3; ++scenario) {
        supervisor_t *supervisor = prepare(true, 1);
        if (scenario == 0) fixture.fail_read_count = 1;
        if (scenario == 1) fixture.pipe_eof = true;
        if (scenario == 2) {
            fixture.ipc_failures = 1;
            fixture.ipc_error = (mpmc_result_t){.status = MPMC_ERR_STATE};
        }
        mpmc_result_t result = run_shutdown(supervisor, &reporter);
        CHECK(result.status == (scenario == 2 ? MPMC_ERR_STATE : MPMC_ERR_SYS));
        CHECK(result.system_errno == (scenario == 0 ? EIO : 0));
        CHECK(fixture.error_count == 1);
        check_error(
            0,
            scenario == 2 ? SUPERVISOR_POLL_IPC_EVENTS : SUPERVISOR_DRAIN_SELF_PIPE,
            scenario == 0 ? EIO : 0
        );
        check_reports(supervisor, 1, SIGKILL);
        finish(supervisor);
    }
}

/** @brief Um erro anterior de IPC prevalece sobre ECHILD e ambos são diagnosticados. */
static void test_lost_child(void) {
    supervisor_t *supervisor = prepare(true, 1);
    pid_t pid = fixture.children[0];
    CHECK(kill(pid, SIGKILL) == 0);
    int status;
    pid_t waited;
    do {
        waited = waitpid(pid, &status, 0);
    } while (waited == -1 && errno == EINTR);
    CHECK(waited == pid);
    fixture.ipc_failures = 1;
    fixture.ipc_error = (mpmc_result_t){.status = MPMC_ERR_SHUTDOWN, .system_errno = EIDRM};

    mpmc_result_t result = run_shutdown(supervisor, &reporter);
    CHECK(result.status == MPMC_ERR_SHUTDOWN && result.system_errno == EIDRM);
    CHECK(fixture.error_count == 2);
    check_error(0, SUPERVISOR_POLL_IPC_EVENTS, EIDRM);
    check_error(1, SUPERVISOR_WAIT_CHILD, ECHILD);
    CHECK(fixture.errors[1].child_pid == pid);
    child_report_t report;
    CHECK(supervisor_report_get(supervisor, 0, &report).status == MPMC_OK);
    CHECK(report.pid == pid && report.end == CHILD_END_LOST);
    finish(supervisor);
}

/** @brief Estado inesperado gera diagnóstico com PID e não deixa filho zumbi. */
static void test_unexpected_child_status(void) {
    supervisor_t *supervisor = prepare(true, 1);
    fixture.fake_clock = true;
    fixture.wait_mode = WAIT_EXPIRE;
    fixture.invalid_wait_status = true;
    mpmc_result_t result = run_shutdown(supervisor, &reporter);
    CHECK(result.status == MPMC_ERR_SYS && result.system_errno == 0);
    CHECK(fixture.error_count == 1);
    check_error(0, SUPERVISOR_WAIT_CHILD, 0);
    CHECK(fixture.errors[0].child_pid == fixture.children[0]);
    finish(supervisor);
}

/** @brief ESRCH, EINTR e timeout não produzem diagnósticos de falha. */
static void test_expected_conditions(void) {
    for (unsigned scenario = 0; scenario < 3; ++scenario) {
        supervisor_t *supervisor = prepare(true, 1);
        fixture.fake_clock = true;
        fixture.wait_mode = scenario == 2 ? WAIT_INTERRUPT : WAIT_EXPIRE;
        if (scenario == 0) fixture.signal_esrch = true;
        if (scenario == 1) {
            fixture.fail_wait_count = 1;
            fixture.wait_errno = EINTR;
        }
        CHECK(run_shutdown(supervisor, &reporter).status == MPMC_OK);
        CHECK(fixture.error_count == 0);
        check_reports(supervisor, 1, SIGKILL);
        finish(supervisor);
    }
}

/** @brief Callback nula e user_data nulo respeitam o contrato opcional do reporter. */
static void test_optional_reporter(void) {
    for (unsigned scenario = 0; scenario < 2; ++scenario) {
        supervisor_t *supervisor = prepare(true, 1);
        fixture.fail_clock = true;
        const supervisor_shutdown_reporter_t destination = {
            .on_error = scenario == 0 ? NULL : collect_error,
            .user_data = NULL,
        };
        mpmc_result_t result = run_shutdown(supervisor, &destination);
        CHECK(result.status == MPMC_ERR_SYS && result.system_errno == EIO);
        CHECK(fixture.error_count == scenario);
        check_reports(supervisor, 1, SIGKILL);
        finish(supervisor);
    }
}

/** @brief Preserva os bits originais de erro do descritor sem inventar errno. */
static void test_poll_events_context(void) {
    const short events[] = {POLLERR, POLLHUP, POLLNVAL | POLLIN};
    for (size_t index = 0; index < sizeof events / sizeof events[0]; ++index) {
        supervisor_t *supervisor = prepare(true, 1);
        fixture.fake_clock = true;
        fixture.wait_mode = WAIT_BAD_PIPE;
        fixture.bad_poll_revents = events[index];
        mpmc_result_t result = run_shutdown(supervisor, &reporter);
        CHECK(result.status == MPMC_ERR_SYS && result.system_errno == 0);
        CHECK(fixture.error_count == 1);
        check_error(0, SUPERVISOR_WAIT_EVENTS, 0);
        CHECK(fixture.errors[0].poll_revents == events[index]);
        check_reports(supervisor, 1, SIGKILL);
        finish(supervisor);
    }
}

/** @brief Recolhe filho já encerrado normalmente sem enviar sinais e aceita novo shutdown. */
static void test_natural_exit(void) {
    supervisor_t *supervisor = prepare(false, 0);
    struct sigaction action = {.sa_handler = previous_child_handler};
    sigemptyset(&action.sa_mask);
    CHECK(sigaction(SIGCHLD, &action, NULL) == 0);
    sigset_t child_signal, pending;
    sigemptyset(&child_signal);
    sigaddset(&child_signal, SIGCHLD);
    CHECK(sigprocmask(SIG_BLOCK, &child_signal, NULL) == 0);
    CHECK(sigpending(&pending) == 0);
    if (sigismember(&pending, SIGCHLD)) {
        int received;
        CHECK(sigwait(&child_signal, &received) == 0);
    }
    fixture.exit_after_ready = true;
    command_t command = {
        .kind = COMMAND_PRODUCE,
        .producer_id = 1,
        .message_count = 1,
        .ack_mode = 2,
    };
    CHECK(supervisor_dispatch(supervisor, &command, NULL).status == MPMC_OK);
    int received;
    CHECK(sigwait(&child_signal, &received) == 0 && received == SIGCHLD);
    CHECK(run_shutdown(supervisor, &reporter).status == MPMC_OK);
    CHECK(fixture.term_calls == 0 && fixture.kill_calls == 0 && fixture.error_count == 0);
    child_report_t report;
    CHECK(supervisor_report_get(supervisor, 0, &report).status == MPMC_OK);
    CHECK(report.pid == fixture.children[0] && report.end == CHILD_END_EXITED && report.value == 0);
    command.kind = COMMAND_SHUTDOWN;
    CHECK(supervisor_dispatch(supervisor, &command, &reporter).status == MPMC_OK);
    CHECK(supervisor_report_count(supervisor) == 1);
    finish(supervisor);
}

/** @brief Falhas antes e depois de criar o pipe não destroem IPC que não foi adquirido. */
static void test_partial_initialization(void) {
    for (unsigned scenario = 0; scenario < 3; ++scenario) {
        reset_fixture();
        fixture.fail_clock = scenario == 0;
        fixture.fail_pipe = scenario == 1;
        fixture.fail_init = scenario == 2;
        supervisor_t *supervisor = NULL;
        mpmc_result_t result = supervisor_create(&supervisor, 1, IPC_SHARED_MEMORY);
        CHECK(result.status == MPMC_ERR_SYS);
        CHECK(result.system_errno == (scenario == 1 ? EMFILE : EIO));
        CHECK(supervisor == NULL && fixture.destroy_calls == 0 && !fixture.ipc.live);
        CHECK(fixture.init_calls == (scenario == 2 ? 1u : 0u));
        CHECK(fixture.pipe_closes == (scenario == 2 ? 2u : 0u));
        if (scenario == 2) {
            CHECK(fcntl(fixture.event_pipe[0], F_GETFD) == -1 && errno == EBADF);
            CHECK(fcntl(fixture.event_pipe[1], F_GETFD) == -1 && errno == EBADF);
        }
        CHECK(run_shutdown(supervisor, &reporter).status == MPMC_ERR_INVALID_ARG);
        supervisor_destroy(supervisor);
        CHECK(fixture.error_count == 0);
    }
}

/** @brief Uma falha de despacho preserva o filho e os recursos já pertencentes ao supervisor. */
static void test_failed_dispatch(void) {
    supervisor_t *supervisor = prepare(true, 1);
    fixture.fail_fork = true;
    command_t command = {
        .kind = COMMAND_PRODUCE,
        .producer_id = 1,
        .message_count = 1,
        .ack_mode = 2,
    };
    mpmc_result_t result = supervisor_dispatch(supervisor, &command, &reporter);
    CHECK(result.status == MPMC_ERR_SYS && result.system_errno == EAGAIN);
    CHECK(fixture.child_count == 1 && fixture.ipc.live);
    fixture.fake_clock = true;
    fixture.wait_mode = WAIT_EXPIRE;
    CHECK(run_shutdown(supervisor, &reporter).status == MPMC_OK);
    check_reports(supervisor, 1, SIGKILL);
    finish(supervisor);
}

/**
 * @brief Executa os cenários com watchdog e restaura a configuração de SIGALRM.
 * @return Zero se todas as verificações passarem; um em falha.
 */
int main(void) {
    struct sigaction action = {0}, previous;
    action.sa_handler = watchdog;
    sigemptyset(&action.sa_mask);
    CHECK(sigaction(SIGALRM, &action, &previous) == 0);
    sigset_t alarm_signal, old_mask;
    sigemptyset(&alarm_signal);
    sigaddset(&alarm_signal, SIGALRM);
    CHECK(sigprocmask(SIG_UNBLOCK, &alarm_signal, &old_mask) == 0);
    alarm(20);
    CHECK(run_shutdown(NULL, NULL).status == MPMC_ERR_INVALID_ARG);
    test_cooperative();
    test_native_deadline();
    test_interrupted_deadline();
    test_wait_errors();
    test_kill_failure();
    test_reporter_errors();
    test_multiple_signal_errors();
    test_signal_configuration_errors();
    test_event_errors();
    test_lost_child();
    test_unexpected_child_status();
    test_expected_conditions();
    test_optional_reporter();
    test_poll_events_context();
    test_natural_exit();
    test_partial_initialization();
    test_failed_dispatch();
    alarm(0);
    CHECK(sigaction(SIGALRM, &previous, NULL) == 0);
    CHECK(sigprocmask(SIG_SETMASK, &old_mask, NULL) == 0);
    printf("%u verificações de shutdown, %u falhas\n", checks, failures);
    return failures == 0 ? 0 : 1;
}

/**
 * @brief Encerra somente filhos próprios se o teste ultrapassar o watchdog.
 * @param signal_number Número do sinal recebido, não utilizado.
 */
static void watchdog(int signal_number) {
    (void)signal_number;
    for (size_t index = 0; index < fixture.child_count; ++index) {
        int status;
        pid_t pid = fixture.children[index];
        if (pid > 0 && waitpid(pid, &status, WNOHANG) == 0) (void)kill(pid, SIGKILL);
    }
    _exit(1);
}

/**
 * @brief Ação anterior usada para verificar a restauração de SIGCHLD.
 * @param signal_number Número do sinal recebido, não utilizado.
 */
static void previous_child_handler(int signal_number) {
    (void)signal_number;
}

/**
 * @brief Cria filho real e aguarda sua confirmação de configuração de sinais.
 * @return PID positivo em sucesso; -1 em falha de pipe() ou fork().
 * Falha de sincronização registra uma verificação malsucedida; o PID permanece
 * disponível para que a limpeza possa recolher o filho criado.
 */
static pid_t create_child(void) {
    int ready[2];
    if (pipe(ready) == -1) return -1;
    pid_t pid = fork();
    if (pid == 0) {
        close(ready[0]);
        struct sigaction action = {0};
        action.sa_handler = fixture.ignore_term ? SIG_IGN : SIG_DFL;
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGTERM, &action, NULL) == -1) _exit(2);
        sigset_t empty;
        sigemptyset(&empty);
        if (sigprocmask(SIG_SETMASK, &empty, NULL) == -1) _exit(2);
        const char byte = 'r';
        if (write(ready[1], &byte, 1) != 1) _exit(2);
        close(ready[1]);
        if (fixture.exit_after_ready) _exit(0);
        for (;;) pause();
    }
    close(ready[1]);
    if (pid < 0) {
        close(ready[0]);
        return -1;
    }
    fixture.children[fixture.child_count++] = pid;
    char byte;
    ssize_t received;
    do {
        received = read(ready[0], &byte, 1);
    } while (received == -1 && errno == EINTR);
    close(ready[0]);
    CHECK(received == 1 && byte == 'r');
    return pid;
}

/**
 * @brief Prepara supervisor e filhos com comportamento de SIGTERM conhecido.
 * @param ignore_term Se true, os filhos ignoram SIGTERM.
 * @param count Quantidade de trabalhos a registrar.
 * @return Instância criada para o cenário.
 */
static supervisor_t *prepare(bool ignore_term, unsigned count) {
    reset_fixture();
    fixture.ignore_term = ignore_term;
    supervisor_t *supervisor = NULL;
    CHECK(supervisor_create(&supervisor, 1, IPC_SHARED_MEMORY).status == MPMC_OK);
    fixture.supervisor = supervisor;
    command_t command = {
        .kind = COMMAND_PRODUCE,
        .producer_id = 1,
        .message_count = 1,
        .ack_mode = 2,
    };
    for (unsigned index = 0; index < count; ++index) {
        CHECK(supervisor_dispatch(supervisor, &command, NULL).status == MPMC_OK);
    }
    return supervisor;
}

/**
 * @brief Confere relatório de término de todos os filhos supervisionados.
 * @param supervisor Instância ainda viva com relatórios já coletados.
 * @param count Quantidade esperada de relatórios.
 * @param signal_number Sinal esperado em cada relatório.
 */
static void check_reports(supervisor_t *supervisor, unsigned count, int signal_number) {
    CHECK(supervisor_report_count(supervisor) == count);
    for (unsigned index = 0; index < count; ++index) {
        child_report_t report;
        CHECK(supervisor_report_get(supervisor, index, &report).status == MPMC_OK);
        CHECK(report.end == CHILD_END_SIGNALED && report.value == signal_number);
    }
}

/**
 * @brief Confere a coleta e libera exclusivamente os filhos e o contexto do teste.
 * @param supervisor Instância cujos trabalhos já deveriam estar recolhidos.
 */
static void finish(supervisor_t *supervisor) {
    CHECK(sigaction(SIGCHLD, &fixture.original_action, NULL) == 0);
    CHECK(sigprocmask(SIG_SETMASK, &fixture.original_mask, NULL) == 0);
    for (size_t index = 0; index < fixture.child_count; ++index) {
        pid_t pid = fixture.children[index];
        int status;
        errno = 0;
        pid_t waited = waitpid(pid, &status, WNOHANG);
        if (pid != fixture.external) CHECK(waited == -1 && errno == ECHILD);
        if (waited == 0) {
            CHECK(kill(pid, SIGKILL) == 0);
            do {
                waited = waitpid(pid, &status, 0);
            } while (waited == -1 && errno == EINTR);
            CHECK(waited == pid);
        }
        fixture.children[index] = 0;
    }
    supervisor_destroy(supervisor);
    CHECK(!fixture.ipc.live);
    CHECK(fixture.destroy_calls == 1 && fixture.pipe_closes == 2);
}

/** @brief Reinicializa a fixture e guarda a configuração de sinais do chamador. */
static void reset_fixture(void) {
    fixture = (typeof(fixture)){.event_pipe = {-1, -1}, .parent_pid = getpid()};
    CHECK(sigaction(SIGCHLD, NULL, &fixture.original_action) == 0);
    CHECK(sigprocmask(SIG_SETMASK, NULL, &fixture.original_mask) == 0);
}

/**
 * @brief Habilita injeções somente durante o shutdown.
 * @param supervisor Instância a encerrar, inclusive NULL para testar validação.
 * @param destination Reporter opcional recebido pelo supervisor.
 * @return Resultado original de supervisor_shutdown(), sem alteração.
 */
static mpmc_result_t run_shutdown(
    supervisor_t *supervisor,
    const supervisor_shutdown_reporter_t *destination
) {
    fixture.shutdown_active = true;
    mpmc_result_t result = supervisor_shutdown(supervisor, destination);
    fixture.shutdown_active = false;
    return result;
}

/**
 * @brief Copia o diagnóstico antes de alterar errno na callback.
 * @param error Registro temporário recebido no fluxo do processo pai.
 * @param user_data Endereço da fixture ou NULL, ambos permitidos pelo contrato.
 */
static void collect_error(const supervisor_shutdown_error_t *error, void *user_data) {
    CHECK(user_data == &fixture || user_data == NULL);
    CHECK(getpid() == fixture.parent_pid);
    CHECK(fixture.error_count < sizeof fixture.errors / sizeof fixture.errors[0]);
    if (fixture.error_count < sizeof fixture.errors / sizeof fixture.errors[0]) {
        fixture.errors[fixture.error_count++] = *error;
    }
    errno = ERANGE;
}

/**
 * @brief Confere operação e errno preservados num diagnóstico copiado.
 * @param index Índice válido no vetor de diagnósticos.
 * @param operation Operação esperada.
 * @param system_errno Causa original esperada, inclusive zero.
 */
static void check_error(size_t index, supervisor_shutdown_operation_t operation, int system_errno) {
    CHECK(index < fixture.error_count);
    if (index >= fixture.error_count) return;
    CHECK(fixture.errors[index].operation == operation);
    CHECK(fixture.errors[index].result.status != MPMC_OK);
    CHECK(fixture.errors[index].result.system_errno == system_errno);
}
