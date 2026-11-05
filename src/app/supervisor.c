/**
 * @file supervisor.c
 * @brief Implementação do ciclo de vida de filhos e canal de despertar.
 * @ingroup application
 *
 * @details Mantém os processos criados pelo REPL, recolhe filhos encerrados,
 * inicia shutdown e fornece um self-pipe não bloqueante para acordar o loop de
 * controle em resposta a SIGCHLD. A integração dos backends IPC permanece
 * como ponto de extensão em supervisor_poll_events().
 */

#include "mpmc/app/supervisor.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/** @brief Registro local do PID de um processo filho supervisionado. */
typedef struct {
	pid_t pid;
} child_record_t;

/** @brief Estado privado mantido exclusivamente pelo processo supervisor. */
struct supervisor {
	size_t producer_count;        /**< Quantidade de estruturas configuradas. */
	child_record_t *children;     /**< Vetor dinâmico de filhos ativos. */
	size_t child_count;           /**< Número de registros ocupados. */
	size_t child_capacity;        /**< Capacidade alocada do vetor. */
	bool shutting_down;           /**< Impede novos despachos após shutdown. */
	int event_pipe[2];            /**< Self-pipe: leitura em [0], escrita em [1]. */
};

/** @brief Descritor usado pelo tratador para acordar o loop ativo. */
static volatile sig_atomic_t wake_write_fd = -1;

/**
 * @brief Define um descritor como não bloqueante e close-on-exec.
 * @param fd Descritor válido a configurar.
 * @retval MPMC_OK Flags aplicadas.
 * @retval MPMC_ERR_SYS Falha em fcntl().
 */
static mpmc_status_t set_nonblocking_cloexec(int fd) {
	int flags = fcntl(fd, F_GETFL);
	if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
		return MPMC_ERR_SYS;
	}
	flags = fcntl(fd, F_GETFD);
	if (flags == -1 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
		return MPMC_ERR_SYS;
	}
	return MPMC_OK;
}

/**
 * @brief Fecha as extremidades do self-pipe e invalida o descritor global.
 * @param supervisor Estado cujo canal será fechado; não pode ser nulo.
 * @details Só invalida wake_write_fd quando ele corresponde à extremidade
 * de escrita desta instância.
 */
static void close_event_pipe(supervisor_t *supervisor) {
	if (wake_write_fd == supervisor->event_pipe[1]) {
		wake_write_fd = -1;
	}
	for (size_t end = 0u; end < 2u; ++end) {
		if (supervisor->event_pipe[end] != -1) {
			(void)close(supervisor->event_pipe[end]);
			supervisor->event_pipe[end] = -1;
		}
	}
}

/**
 * @brief Cria e configura o canal não bloqueante de despertar.
 * @param supervisor Instância a receber os descritores.
 * @retval MPMC_OK Pipe criado e registrado como canal ativo.
 * @retval MPMC_ERR_SYS Falha ao criar o pipe ou configurar suas flags.
 * @post Em falha, os descritores parcialmente criados são fechados.
 */
static mpmc_status_t open_event_pipe(supervisor_t *supervisor) {
	supervisor->event_pipe[0] = -1;
	supervisor->event_pipe[1] = -1;
	if (pipe(supervisor->event_pipe) == -1) {
		supervisor->event_pipe[0] = -1;
		supervisor->event_pipe[1] = -1;
		return MPMC_ERR_SYS;
	}

	for (size_t end = 0u; end < 2u; ++end) {
		if (set_nonblocking_cloexec(supervisor->event_pipe[end]) != MPMC_OK) {
			close_event_pipe(supervisor);
			return MPMC_ERR_SYS;
		}
	}
	wake_write_fd = supervisor->event_pipe[1];
	return MPMC_OK;
}

/**
 * @brief Garante espaço para registrar mais um filho.
 * @param supervisor Instância cujo vetor pode crescer.
 * @retval MPMC_OK Já havia espaço ou a realocação foi concluída.
 * @retval MPMC_ERR_SYS Overflow de tamanho ou falha de alocação.
 */
static mpmc_status_t ensure_child_capacity(supervisor_t *supervisor) {
	if (supervisor->child_count < supervisor->child_capacity) {
		return MPMC_OK;
	}

	size_t capacity = supervisor->child_capacity == 0u ? 8u : supervisor->child_capacity * 2u;
	if (capacity < supervisor->child_capacity || capacity > SIZE_MAX / sizeof(*supervisor->children)) {
		return MPMC_ERR_SYS;
	}
	child_record_t *children = realloc(supervisor->children, capacity * sizeof(*children));
	if (children == NULL) {
		return MPMC_ERR_SYS;
	}
	supervisor->children = children;
	supervisor->child_capacity = capacity;
	return MPMC_OK;
}

/** @details Prepara a lista local de filhos e o self-pipe. A inicialização dos
 * backends IPC ainda não está conectada; o contrato público está em supervisor.h.
 */
mpmc_status_t supervisor_create(supervisor_t **out, size_t producer_count, ipc_mode_t mode) {
	if (out == NULL) {
		return MPMC_ERR_INVALID_ARG;
	}
	*out = NULL;
	if (producer_count == 0u
		|| (mode != IPC_SHARED_MEMORY && mode != IPC_MESSAGE_QUEUE)) {
		return MPMC_ERR_INVALID_ARG;
	}

	supervisor_t *supervisor = calloc(1u, sizeof(*supervisor));
	if (supervisor == NULL) {
		return MPMC_ERR_SYS;
	}

	supervisor->producer_count = producer_count;
	if (open_event_pipe(supervisor) != MPMC_OK) {
		free(supervisor);
		return MPMC_ERR_SYS;
	}
	*out = supervisor;
	return MPMC_OK;
}

/** @details Consulta simples do valor armazenado na criação da instância. */
size_t supervisor_producer_count(const supervisor_t *supervisor) {
	return supervisor == NULL ? 0u : supervisor->producer_count;
}

/** @details Expõe o descritor para que o REPL o inclua em poll(). */
int supervisor_event_fd(const supervisor_t *supervisor) {
	return supervisor == NULL ? -1 : supervisor->event_pipe[0];
}

/** @details Pode ser chamada em tratador de sinal: usa write() no descritor
 * não bloqueante, preserva errno e descarta falha por pipe cheio, pois nesse
 * caso já existe um despertar pendente. O contrato público está em supervisor.h.
 */
void supervisor_notify_signal(void) {
	int saved_errno = errno;
	int fd = wake_write_fd;
	if (fd != -1) {
		const unsigned char wake = 1u;
		ssize_t written;
		do {
			written = write(fd, &wake, sizeof(wake));
		} while (written == -1 && errno == EINTR);
	}
	errno = saved_errno;
}

/** @details Os bytes são notificações agregadas sem payload. A integração da
 * drenagem dos backends IPC deve ocorrer neste ponto quando estiver disponível.
 */
mpmc_status_t supervisor_poll_events(supervisor_t *supervisor) {
	if (supervisor == NULL) {
		return MPMC_ERR_INVALID_ARG;
	}

	unsigned char sink[64];
	for (;;) {
		ssize_t count = read(supervisor->event_pipe[0], sink, sizeof(sink));
		if (count > 0) {
			continue;
		}
		if (count == 0) {
			return MPMC_ERR_SYS;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return MPMC_OK;
		}
		return MPMC_ERR_SYS;
	}
}

/**
 * @brief Restaura ações padrão de sinais no processo filho.
 * @details Remove os tratadores herdados do REPL para que sinais de término,
 * em particular SIGTERM, tenham o comportamento esperado no worker.
 */
static void reset_child_signals(void) {
	static const int signals[] = {SIGCHLD, SIGINT, SIGTERM};
	struct sigaction action = {0};
	action.sa_handler = SIG_DFL;
	sigemptyset(&action.sa_mask);
	for (size_t index = 0u; index < sizeof(signals) / sizeof(signals[0]); ++index) {
		(void)sigaction(signals[index], &action, NULL);
	}
}

/** @details O pai registra o PID sem aguardar; o filho fecha o self-pipe,
 * restaura os sinais e inicia o worker (ainda não conectado nesta versão).
 */
mpmc_status_t supervisor_dispatch(supervisor_t *supervisor, const command_t *command) {
	if (supervisor == NULL || command == NULL) {
		return MPMC_ERR_INVALID_ARG;
	}
	if (supervisor->shutting_down) {
		return MPMC_ERR_SHUTDOWN;
	}
	if (command->kind == COMMAND_SHUTDOWN) {
		return supervisor_shutdown(supervisor);
	}
	if (command->kind != COMMAND_PRODUCE && command->kind != COMMAND_CONSUME) {
		return MPMC_ERR_INVALID_ARG;
	}

	mpmc_status_t status = ensure_child_capacity(supervisor);
	if (status != MPMC_OK) {
		return status;
	}

	pid_t child = fork();
	if (child == -1) {
		return MPMC_ERR_SYS;
	}
	if (child == 0) {
		close_event_pipe(supervisor);
		reset_child_signals();
#ifdef DEBUG
		if (getenv("MPMC_TEST_WORKER_HANG") != NULL) {
			for (;;) {
				(void)pause();
			}
		}
#endif
		(void)dprintf(STDERR_FILENO,
					  "mpmc: comando recebido, mas a execução de workers/IPC ainda não está implementada.\n");
		_exit(EXIT_FAILURE);
	}

	supervisor->children[supervisor->child_count++] = (child_record_t){
		.pid = child,
	};
	return MPMC_OK;
}

/**
 * @brief Remove um registro do vetor compactando-o com o último elemento.
 * @param supervisor Instância proprietária do vetor.
 * @param index Índice válido do registro a remover.
 */
static void remove_child(supervisor_t *supervisor, size_t index) {
	supervisor->children[index] = supervisor->children[supervisor->child_count - 1u];
	--supervisor->child_count;
}

/** @details EINTR em waitpid() é repetido; ECHILD remove registros obsoletos.
 * Eventos do canal são drenados antes da coleta, fora do contexto de sinal.
 */
mpmc_status_t supervisor_reap(supervisor_t *supervisor, bool blocking) {
	if (supervisor == NULL) {
		return MPMC_ERR_INVALID_ARG;
	}

	mpmc_status_t event_status = supervisor_poll_events(supervisor);

	size_t index = 0u;
	while (index < supervisor->child_count) {
		int wait_options = blocking ? 0 : WNOHANG;
		int wait_status;
		pid_t waited;
		do {
			waited = waitpid(supervisor->children[index].pid, &wait_status, wait_options);
		} while (waited == -1 && errno == EINTR);

		if (waited == 0) {
			++index;
			continue;
		}
		if (waited == -1) {
			if (errno == ECHILD) {
				remove_child(supervisor, index);
				continue;
			}
			return MPMC_ERR_SYS;
		}

		if (WIFEXITED(wait_status) && WEXITSTATUS(wait_status) != EXIT_SUCCESS) {
			fprintf(stderr, "mpmc: processo filho %ld terminou com código %d.\n",
					(long)waited, WEXITSTATUS(wait_status));
		} else if (WIFSIGNALED(wait_status) && !supervisor->shutting_down) {
			fprintf(stderr, "mpmc: processo filho %ld terminou pelo sinal %d.\n",
					(long)waited, WTERMSIG(wait_status));
		}
		remove_child(supervisor, index);
	}
	return event_status;
}

/** @details Marca a instância como encerrando, envia SIGTERM aos filhos e
 * espera sua saída. Recursos globais IPC ainda dependem dos backends pendentes.
 */
mpmc_status_t supervisor_shutdown(supervisor_t *supervisor) {
	if (supervisor == NULL) {
		return MPMC_ERR_INVALID_ARG;
	}
	if (supervisor->shutting_down) {
		return supervisor_reap(supervisor, true);
	}
	supervisor->shutting_down = true;

	mpmc_status_t signal_status = MPMC_OK;
	for (size_t index = 0u; index < supervisor->child_count; ++index) {
		if (kill(supervisor->children[index].pid, SIGTERM) == -1 && errno != ESRCH) {
			signal_status = MPMC_ERR_SYS;
		}
	}

	mpmc_status_t reap_status = supervisor_reap(supervisor, true);
	mpmc_status_t event_status = supervisor_poll_events(supervisor);
	if (signal_status != MPMC_OK) {
		return signal_status;
	}
	return reap_status != MPMC_OK ? reap_status : event_status;
}

/** @details Libera o vetor de filhos e fecha as extremidades do self-pipe. */
void supervisor_destroy(supervisor_t *supervisor) {
	if (supervisor == NULL) {
		return;
	}
	close_event_pipe(supervisor);
	free(supervisor->children);
	free(supervisor);
}
