#include "mpmc/app/repl.h"

#include "mpmc/app/command.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#define REPL_MAX_LINE 4096u
#define REPL_POLL_INTERVAL_MS 100

static volatile sig_atomic_t termination_requested;

static void handle_child_signal(int signal_number) {
	(void)signal_number;
	supervisor_notify_signal();
}

static void handle_termination_signal(int signal_number) {
	(void)signal_number;
	termination_requested = 1;
}

static mpmc_status_t install_signal_handler(int signal_number,
											void (*handler)(int),
											struct sigaction *previous) {
	struct sigaction action = {0};
	action.sa_handler = handler;
	sigemptyset(&action.sa_mask);
	if (sigaction(signal_number, &action, previous) == -1) {
		return MPMC_ERR_SYS;
	}
	return MPMC_OK;
}

static mpmc_status_t check_foreground_stdin(void) {
	if (!isatty(STDIN_FILENO)) {
		return MPMC_OK;
	}

	pid_t foreground_group = tcgetpgrp(STDIN_FILENO);
	if (foreground_group == -1) {
		return MPMC_ERR_SYS;
	}
	if (foreground_group != getpgrp()) {
		fprintf(stderr,
				"mpmc: stdin pertence a um terminal, mas este processo está em background; "
				"use-o em primeiro plano ou redirecione stdin.\n");
		return MPMC_ERR_SYS;
	}
	return MPMC_OK;
}

static mpmc_status_t service_supervisor(supervisor_t *supervisor) {
	return supervisor_reap(supervisor, false);
}

static void dispatch_line(supervisor_t *supervisor, char *line, bool *shutdown) {
	command_t command;
	mpmc_status_t status = command_parse(line, &command);
	if (status == MPMC_OK) {
		status = command_validate(&command, supervisor_producer_count(supervisor));
	}
	if (status == MPMC_OK) {
		status = supervisor_dispatch(supervisor, &command);
	}

	if (status != MPMC_OK) {
		fprintf(stderr, "mpmc: %s\n", mpmc_status_str(status));
		return;
	}

	*shutdown = command.kind == COMMAND_SHUTDOWN;
}

mpmc_status_t repl_run(supervisor_t *supervisor) {
	struct sigaction previous_chld;
	struct sigaction previous_int;
	struct sigaction previous_term;
	char line[REPL_MAX_LINE + 1u];
	size_t line_length = 0u;
	bool line_too_long = false;
	bool need_prompt = true;
	bool shutdown = false;
	mpmc_status_t result;

	if (supervisor == NULL) {
		return MPMC_ERR_INVALID_ARG;
	}

	result = check_foreground_stdin();
	if (result != MPMC_OK) {
		return result;
	}

	termination_requested = 0;
	result = install_signal_handler(SIGCHLD, handle_child_signal, &previous_chld);
	if (result != MPMC_OK) {
		return result;
	}
	result = install_signal_handler(SIGINT, handle_termination_signal, &previous_int);
	if (result != MPMC_OK) {
		(void)sigaction(SIGCHLD, &previous_chld, NULL);
		return result;
	}
	result = install_signal_handler(SIGTERM, handle_termination_signal, &previous_term);
	if (result != MPMC_OK) {
		(void)sigaction(SIGINT, &previous_int, NULL);
		(void)sigaction(SIGCHLD, &previous_chld, NULL);
		return result;
	}

	result = MPMC_OK;
	while (!shutdown && !termination_requested) {
		if (need_prompt) {
			fputs(">> ", stdout);
			fflush(stdout);
			need_prompt = false;
		}

		mpmc_status_t service_status = service_supervisor(supervisor);
		if (service_status != MPMC_OK) {
			fprintf(stderr, "mpmc: falha ao processar eventos dos processos filhos: %s\n",
					mpmc_status_str(service_status));
			result = service_status;
			break;
		}

		struct pollfd fds[2] = {
			{.fd = STDIN_FILENO, .events = POLLIN | POLLHUP, .revents = 0},
			{.fd = supervisor_event_fd(supervisor), .events = POLLIN, .revents = 0},
		};
		struct pollfd *input = &fds[0];
		const struct pollfd *wake = &fds[1];
		int ready = poll(fds, 2u, REPL_POLL_INTERVAL_MS);
		if (ready == -1) {
			if (errno == EINTR) {
				continue;
			}
			result = MPMC_ERR_SYS;
			break;
		}
		if (ready == 0) {
			continue;
		}
		if (((input->revents | wake->revents) & (POLLERR | POLLNVAL)) != 0) {
			result = MPMC_ERR_SYS;
			break;
		}
		if (input->revents == 0) {
			continue;
		}

		char buffer[512];
		ssize_t bytes_read = read(STDIN_FILENO, buffer, sizeof(buffer));
		if (bytes_read == -1) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			result = MPMC_ERR_SYS;
			break;
		}
		if (bytes_read == 0) {
			if (line_length != 0u || line_too_long) {
				if (line_too_long) {
					fprintf(stderr, "mpmc: linha excede o limite de %u caracteres.\n",
							(unsigned)REPL_MAX_LINE);
				} else {
					line[line_length] = '\0';
					dispatch_line(supervisor, line, &shutdown);
				}
			}
			break;
		}

		for (ssize_t index = 0; index < bytes_read && !shutdown; ++index) {
			char character = buffer[index];
			if (character == '\n') {
				if (line_too_long) {
					fprintf(stderr, "mpmc: linha excede o limite de %u caracteres.\n",
							(unsigned)REPL_MAX_LINE);
				} else {
					line[line_length] = '\0';
					dispatch_line(supervisor, line, &shutdown);
				}
				line_length = 0u;
				line_too_long = false;
				need_prompt = !shutdown;
				if (need_prompt) {
					fputs(">> ", stdout);
					fflush(stdout);
					need_prompt = false;
				}
				continue;
			}

			if (!line_too_long) {
				if (line_length < REPL_MAX_LINE) {
					line[line_length++] = character;
				} else {
					line_too_long = true;
					line_length = 0u;
				}
			}
		}
	}

	(void)sigaction(SIGTERM, &previous_term, NULL);
	(void)sigaction(SIGINT, &previous_int, NULL);
	(void)sigaction(SIGCHLD, &previous_chld, NULL);

	mpmc_status_t shutdown_status = supervisor_shutdown(supervisor);
	if (result == MPMC_OK && shutdown_status != MPMC_OK) {
		result = shutdown_status;
	}
	return result;
}
