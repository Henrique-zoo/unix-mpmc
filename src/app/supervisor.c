#include "mpmc/app/supervisor.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct {
	pid_t pid;
} child_record_t;

struct supervisor {
	size_t producer_count;
	child_record_t *children;
	size_t child_count;
	size_t child_capacity;
	bool shutting_down;
};

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
	*out = supervisor;
	return MPMC_OK;
}

size_t supervisor_producer_count(const supervisor_t *supervisor) {
	return supervisor == NULL ? 0u : supervisor->producer_count;
}

mpmc_status_t supervisor_poll_events(supervisor_t *supervisor) {
	return supervisor == NULL ? MPMC_ERR_INVALID_ARG : MPMC_OK;
}

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
		/* Worker/IPC implementations are separate modules and are not present yet. */
		(void)dprintf(STDERR_FILENO,
					  "mpmc: comando recebido, mas a execução de workers/IPC ainda não está implementada.\n");
		_exit(EXIT_FAILURE);
	}

	supervisor->children[supervisor->child_count++] = (child_record_t){
		.pid = child,
	};
	return MPMC_OK;
}

static void remove_child(supervisor_t *supervisor, size_t index) {
	supervisor->children[index] = supervisor->children[supervisor->child_count - 1u];
	--supervisor->child_count;
}

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

void supervisor_destroy(supervisor_t *supervisor) {
	if (supervisor == NULL) {
		return;
	}
	free(supervisor->children);
	free(supervisor);
}
