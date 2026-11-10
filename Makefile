# unix-mpmc — múltiplos produtores/consumidores com IPC System V
# Uso: make | make debug | make release | make test | make unit | make docs | make docs-clean | make clean

CC := clang
MODE ?= debug

CPPFLAGS += -Iinclude -D_POSIX_C_SOURCE=200809L
CFLAGS   += -std=c23 -Wall -Wextra -Wpedantic -Wshadow -Werror

ifeq ($(MODE),debug)
  CFLAGS += -O0 -g3 -DDEBUG
else ifeq ($(MODE),release)
  CFLAGS += -O2 -DNDEBUG
else
	$(error Invalid MODE '$(MODE)'; use debug or release)
endif

BUILD_DIR := build/$(MODE)
SOURCES   := $(shell find src -type f -name '*.c' | sort)
OBJECTS   := $(patsubst src/%.c,$(BUILD_DIR)/%.o,$(SOURCES))
DEPS      := $(OBJECTS:.o=.d)
BINARY    := $(BUILD_DIR)/mpmc

.DEFAULT_GOAL := all
.PHONY: all debug release test unit docs docs-clean clean

all: $(BINARY)
	@ln -sfn $(BINARY) mpmc

$(BINARY): $(OBJECTS)
	@mkdir -p $(dir $@)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@

$(BUILD_DIR)/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

debug:
	$(MAKE) MODE=debug all

release:
	$(MAKE) MODE=release all

# Testes unitários. supervisor.c é compilado com -Dfork=mpmc_test_fork para que o teste
# injete falhas de fork() sem depender de LD_PRELOAD (portável entre Linux e macOS).
UNIT_DIR := $(BUILD_DIR)/unit
SHM_BACKEND_DIR := src/ipc/shm_backend
WORKERS_DIR := src/workers
APP_DIR := src/app
DIAGNOSTIC_OBJECT := $(BUILD_DIR)/diagnostic.o

$(patsubst tests/unit/%.c,$(UNIT_DIR)/%,$(wildcard tests/unit/test_*.c)): $(DIAGNOSTIC_OBJECT)

SHM_RING_OBJECT := $(BUILD_DIR)/ipc/shm_backend/shm_ring.o
SHM_OPERATIONS_OBJECT := $(BUILD_DIR)/ipc/shm_backend/operations.o

$(UNIT_DIR)/supervisor_for_test.o: src/app/supervisor.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dfork=mpmc_test_fork -c $< -o $@

# Fixture isolada do relatório: substitui processos, relógio e fachada IPC.
$(UNIT_DIR)/supervisor_report_for_test.o: src/app/supervisor.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) \
		-Dfork=mpmc_test_fork -Dwaitpid=mpmc_test_waitpid \
        -Dproducer_run_with_result=mpmc_test_producer_run -Dconsumer_run=mpmc_test_consumer_run \
        -Dipc_worker_close=mpmc_test_worker_close -Dipc_worker_cancel=mpmc_test_worker_cancel \
		-Dclock_gettime=mpmc_test_clock_gettime \
		-Dipc_init=mpmc_test_ipc_init -Dipc_destroy=mpmc_test_ipc_destroy \
        -Dipc_quiesce=mpmc_test_quiesce \
		-Dipc_poll_events=mpmc_test_ipc_poll_events \
		-Dipc_ack_abandon=mpmc_test_ipc_ack_abandon \
		-Dipc_consumed_count=mpmc_test_ipc_consumed_count \
		-c $< -o $@

$(UNIT_DIR)/test_supervisor_report: tests/unit/test_supervisor_report.c \
		$(UNIT_DIR)/supervisor_report_for_test.o src/app/job_outcome.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

# Filhos reais, IPC substituto e falhas controladas no prazo de shutdown.
$(UNIT_DIR)/supervisor_shutdown_for_test.o: src/app/supervisor.c \
		include/mpmc/app/supervisor.h Makefile
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) \
		-Dfork=mpmc_shutdown_test_fork -Dkill=mpmc_shutdown_test_kill \
        -Dproducer_run_with_result=mpmc_test_producer_run -Dconsumer_run=mpmc_test_consumer_run \
        -Dipc_worker_close=mpmc_test_worker_close -Dipc_worker_cancel=mpmc_test_worker_cancel \
		-Dclock_gettime=mpmc_shutdown_test_clock_gettime -Dpoll=mpmc_shutdown_test_poll \
		'-Dsigaction(...)=mpmc_shutdown_test_sigaction(__VA_ARGS__)' \
		-Dsigprocmask=mpmc_shutdown_test_sigprocmask -Dwaitpid=mpmc_shutdown_test_waitpid \
		-Dread=mpmc_shutdown_test_read -Dpipe=mpmc_shutdown_test_pipe -Dclose=mpmc_shutdown_test_close \
		-Dipc_init=mpmc_shutdown_test_ipc_init -Dipc_destroy=mpmc_shutdown_test_ipc_destroy \
        -Dipc_quiesce=mpmc_test_quiesce \
		-Dipc_poll_events=mpmc_shutdown_test_ipc_poll_events \
		-Dipc_ack_abandon=mpmc_shutdown_test_ipc_ack_abandon \
		-Dipc_consumed_count=mpmc_shutdown_test_ipc_consumed_count \
		-c $< -o $@

$(UNIT_DIR)/test_supervisor_shutdown: tests/unit/test_supervisor_shutdown.c \
		$(UNIT_DIR)/supervisor_shutdown_for_test.o src/app/job_outcome.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_command: tests/unit/test_command.c src/app/command.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_supervisor: tests/unit/test_supervisor.c \
		$(UNIT_DIR)/supervisor_for_test.o src/app/command.c src/app/job_outcome.c src/error.c \
		src/ipc/transport.c src/ipc/msg_backend.c $(SHM_BACKEND_DIR)/lifecycle.c \
		$(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c \
        src/workers/producer.c src/workers/producer_routing.c src/workers/consumer.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

# Apresentação de resultados de trabalhos (#54): formatador puro e REPL/supervisor com
# eventos controlados e fork() substituído; nenhum worker é necessário.
$(UNIT_DIR)/test_job_outcome: tests/unit/test_job_outcome.c $(APP_DIR)/job_outcome.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(APP_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_outcome_repl: tests/unit/test_outcome_repl.c \
		$(UNIT_DIR)/supervisor_for_test.o $(APP_DIR)/repl.c $(APP_DIR)/command.c \
		$(APP_DIR)/job_outcome.c src/error.c src/ipc/transport.c src/ipc/msg_backend.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c \
        src/workers/producer.c src/workers/producer_routing.c src/workers/consumer.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(APP_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_msg: tests/unit/test_msg.c src/ipc/msg.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/sem_for_test.o: src/ipc/sem.c include/mpmc/ipc/sem.h include/mpmc/error.h
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dsemop=mpmc_test_semop -c $< -o $@

$(UNIT_DIR)/test_sem: tests/unit/test_sem.c $(UNIT_DIR)/sem_for_test.o
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_error: tests/unit/test_error.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_shm: tests/unit/test_shm.c src/ipc/shm.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/shm_backend_for_test.o: $(SHM_BACKEND_DIR)/lifecycle.c \
		$(SHM_BACKEND_DIR)/shm_backend_internal.h $(SHM_BACKEND_DIR)/shm_ring.h \
		include/mpmc/ipc/shm_backend.h include/mpmc/message.h Makefile
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) \
		-Dmalloc=mpmc_test_shm_malloc -Dcalloc=mpmc_test_shm_calloc -Dfree=mpmc_test_shm_free \
		-Dsysv_shm_create=mpmc_test_shm_create -Dsysv_shm_attach=mpmc_test_shm_attach \
		-Dsysv_shm_detach=mpmc_test_shm_detach -Dsysv_shm_remove=mpmc_test_shm_remove \
		-Dsysv_sem_create=mpmc_test_sem_create -Dsysv_sem_set=mpmc_test_sem_set \
		-Dsysv_sem_remove=mpmc_test_sem_remove \
		-Dshm_backend_ack_resources_create=mpmc_test_ack_resources_create \
		-Dshm_backend_ack_resources_cleanup=mpmc_test_ack_resources_cleanup \
		-c $< -o $@

$(UNIT_DIR)/test_shm_backend: tests/unit/test_shm_backend.c \
		$(UNIT_DIR)/shm_backend_for_test.o $(SHM_BACKEND_DIR)/ack.c \
		$(SHM_RING_OBJECT) src/ipc/shm.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/shm_operations_for_test.o: $(SHM_BACKEND_DIR)/operations.c \
		$(SHM_BACKEND_DIR)/shm_backend_internal.h $(SHM_BACKEND_DIR)/shm_ring.h \
		include/mpmc/ipc/shm_backend.h include/mpmc/ipc/sem.h include/mpmc/error.h Makefile
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) \
		-Dsysv_sem_apply=mpmc_test_sem_apply -Dsysv_sem_change=mpmc_test_sem_change \
		-c $< -o $@

$(UNIT_DIR)/ack_for_test.o: $(SHM_BACKEND_DIR)/ack.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) \
		-Dsysv_sem_change=mpmc_test_sem_change -c $< -o $@

$(UNIT_DIR)/test_shm_backend_ack: tests/unit/test_shm_backend_ack.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(UNIT_DIR)/ack_for_test.o \
		$(SHM_RING_OBJECT) src/ipc/shm.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

# Cleanup ACK: substitui somente as primitivas de liberação para injetar falhas.
$(UNIT_DIR)/ack_cleanup_for_test.o: $(SHM_BACKEND_DIR)/ack.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) \
		-Dsysv_shm_detach=mpmc_test_shm_detach \
		-Dsysv_shm_remove=mpmc_test_shm_remove \
		-Dsysv_sem_remove=mpmc_test_sem_remove \
		-c $< -o $@

$(UNIT_DIR)/test_shm_ack_lifecycle: tests/unit/test_shm_ack_lifecycle.c \
        $(UNIT_DIR)/ack_for_test.o $(SHM_BACKEND_DIR)/lifecycle.c $(SHM_OPERATIONS_OBJECT) \
        $(SHM_RING_OBJECT) src/ipc/shm.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_shm_backend_ack_create: tests/unit/test_shm_backend_ack_create.c \
		$(SHM_BACKEND_DIR)/ack.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) \
		-Dsysv_shm_create=mpmc_test_shm_create \
		-Dsysv_shm_attach=mpmc_test_shm_attach \
		-Dsysv_shm_detach=mpmc_test_shm_detach \
		-Dsysv_shm_remove=mpmc_test_shm_remove \
		-Dsysv_sem_create=mpmc_test_sem_create \
		-Dsysv_sem_set=mpmc_test_sem_set \
		-Dsysv_sem_remove=mpmc_test_sem_remove \
		-Dsysv_sem_change=mpmc_test_sem_change \
		-Dsysv_sem_apply=mpmc_test_sem_apply \
		$^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_shm_backend_ack_cleanup: tests/unit/test_shm_backend_ack_cleanup.c \
		$(UNIT_DIR)/ack_cleanup_for_test.o src/ipc/shm.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_shm_operations: tests/unit/test_shm_operations.c \
		$(UNIT_DIR)/shm_operations_for_test.o $(SHM_BACKEND_DIR)/lifecycle.c \
		$(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) src/ipc/shm.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_shm_stress: tests/unit/test_shm_stress.c $(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c \
		$(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) src/ipc/shm.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_shm_ring: tests/unit/test_shm_ring.c $(SHM_RING_OBJECT)
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_message_metadata: tests/unit/test_message_metadata.c src/workers/producer.c \
		$(WORKERS_DIR)/producer_routing.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_producer: tests/unit/test_producer.c $(WORKERS_DIR)/producer.c \
		$(WORKERS_DIR)/producer_routing.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dclock_gettime=mpmc_test_clock_gettime $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_consumer: tests/unit/test_consumer.c $(WORKERS_DIR)/consumer.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_consumer_integration: tests/unit/test_consumer_integration.c \
		$(WORKERS_DIR)/consumer.c src/ipc/transport.c src/ipc/msg_backend.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_consumer_concurrency: tests/unit/test_consumer_concurrency.c \
		$(WORKERS_DIR)/consumer.c $(WORKERS_DIR)/producer.c $(WORKERS_DIR)/producer_routing.c \
		src/ipc/transport.c src/ipc/msg_backend.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_consumer_acceptance: tests/unit/test_consumer_acceptance.c \
		$(WORKERS_DIR)/consumer.c $(WORKERS_DIR)/producer.c $(WORKERS_DIR)/producer_routing.c \
		src/ipc/transport.c src/ipc/msg_backend.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_producer_routing: tests/unit/test_producer_routing.c \
		$(WORKERS_DIR)/producer_routing.c $(WORKERS_DIR)/producer.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(WORKERS_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_protocol_properties: tests/unit/test_protocol_properties.c \
		src/error.c $(WORKERS_DIR)/producer.c $(WORKERS_DIR)/producer_routing.c $(SHM_RING_OBJECT)
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(WORKERS_DIR) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/msg_backend_for_test.o: src/ipc/msg_backend.c \
		include/mpmc/ipc/msg_backend.h include/mpmc/ipc/msg.h \
		include/mpmc/message.h include/mpmc/error.h
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) \
		-Dsysv_msg_create=mpmc_test_msg_create \
		-Dsysv_msg_remove=mpmc_test_msg_remove \
		-Dsysv_msg_send=mpmc_test_msg_send \
		-Dsysv_msg_receive=mpmc_test_msg_receive \
		-Dsysv_msg_receive_typed=mpmc_test_msg_receive_typed \
		-Dnanosleep=mpmc_test_nanosleep \
		-c $< -o $@

$(UNIT_DIR)/test_msg_backend: tests/unit/test_msg_backend.c \
        $(UNIT_DIR)/msg_backend_for_test.o src/ipc/msg.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_transport_contract: tests/unit/test_transport_contract.c \
		src/ipc/transport.c src/ipc/msg_backend.c $(SHM_BACKEND_DIR)/lifecycle.c \
		$(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

# transport.c é compilado com calloc()/free() redirecionados para que test_transport injete
# falhas de alocação e detecte vazamentos. O teste fornece os backends substitutos; nenhum
# backend real é ligado a ele.
$(UNIT_DIR)/transport_for_test.o: src/ipc/transport.c include/mpmc/ipc/transport.h \
		include/mpmc/ipc/msg_backend.h include/mpmc/ipc/shm_backend.h include/mpmc/message.h
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dcalloc=mpmc_test_calloc -Dfree=mpmc_test_free -c $< -o $@

$(UNIT_DIR)/test_transport: tests/unit/test_transport.c $(UNIT_DIR)/transport_for_test.o
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_msg_stress: tests/unit/test_msg_stress.c src/ipc/msg_backend.c src/ipc/msg.c \
		src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) tests/unit/test_msg_stress.c src/ipc/msg.c src/error.c \
		$(DIAGNOSTIC_OBJECT) $(LDLIBS) -o $@

# Correlação de ACK com filas System V reais: o teste inclui msg_backend.c para ler os
# identificadores privados das filas, por isso o objeto do backend não é ligado de novo.
$(UNIT_DIR)/test_msg_ack_concurrency: tests/unit/test_msg_ack_concurrency.c \
		src/ipc/msg_backend.c src/ipc/msg.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) tests/unit/test_msg_ack_concurrency.c src/ipc/msg.c src/error.c \
		$(DIAGNOSTIC_OBJECT) $(LDLIBS) -o $@

$(UNIT_DIR)/test_msg_ack_lifecycle: tests/unit/test_msg_ack_lifecycle.c \
		src/ipc/msg_backend.c src/ipc/transport.c $(WORKERS_DIR)/producer.c \
		$(WORKERS_DIR)/producer_routing.c $(WORKERS_DIR)/consumer.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(WORKERS_DIR) $(CFLAGS) tests/unit/test_msg_ack_lifecycle.c \
		$(WORKERS_DIR)/producer.c $(WORKERS_DIR)/producer_routing.c $(WORKERS_DIR)/consumer.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c $(DIAGNOSTIC_OBJECT) $(LDLIBS) -o $@

$(UNIT_DIR)/test_transport_parity: tests/unit/test_transport_parity.c \
		src/ipc/msg_backend.c src/ipc/transport.c $(WORKERS_DIR)/producer.c \
		$(WORKERS_DIR)/producer_routing.c $(WORKERS_DIR)/consumer.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(WORKERS_DIR) -I$(SHM_BACKEND_DIR) $(CFLAGS) \
		tests/unit/test_transport_parity.c \
		$(WORKERS_DIR)/producer.c $(WORKERS_DIR)/producer_routing.c $(WORKERS_DIR)/consumer.c \
		$(SHM_BACKEND_DIR)/lifecycle.c $(SHM_BACKEND_DIR)/ack.c $(SHM_RING_OBJECT) $(SHM_OPERATIONS_OBJECT) \
		src/ipc/msg.c src/ipc/shm.c src/ipc/sem.c src/error.c $(DIAGNOSTIC_OBJECT) $(LDLIBS) -o $@

$(UNIT_DIR)/test_isolation: tests/unit/test_isolation.c src/app/command.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_diagnostic: tests/unit/test_diagnostic.c src/app/diagnostics.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

unit: $(UNIT_DIR)/test_diagnostic $(UNIT_DIR)/test_command $(UNIT_DIR)/test_supervisor $(UNIT_DIR)/test_msg \
		$(UNIT_DIR)/test_error $(UNIT_DIR)/test_sem $(UNIT_DIR)/test_shm \
		$(UNIT_DIR)/test_shm_backend $(UNIT_DIR)/test_shm_backend_ack \
		$(UNIT_DIR)/test_shm_backend_ack_cleanup $(UNIT_DIR)/test_shm_backend_ack_create \
        $(UNIT_DIR)/test_shm_ack_lifecycle \
		$(UNIT_DIR)/test_shm_operations $(UNIT_DIR)/test_shm_stress \
		$(UNIT_DIR)/test_shm_ring $(UNIT_DIR)/test_message_metadata \
		$(UNIT_DIR)/test_msg_backend $(UNIT_DIR)/test_msg_stress \
		$(UNIT_DIR)/test_msg_ack_concurrency $(UNIT_DIR)/test_msg_ack_lifecycle \
		$(UNIT_DIR)/test_isolation \
		$(UNIT_DIR)/test_producer_routing $(UNIT_DIR)/test_producer $(UNIT_DIR)/test_consumer \
		$(UNIT_DIR)/test_consumer_integration $(UNIT_DIR)/test_consumer_concurrency \
		$(UNIT_DIR)/test_consumer_acceptance \
		$(UNIT_DIR)/test_transport_contract \
		$(UNIT_DIR)/test_transport_parity \
		$(UNIT_DIR)/test_transport $(UNIT_DIR)/test_protocol_properties \
		$(UNIT_DIR)/test_supervisor_report $(UNIT_DIR)/test_supervisor_shutdown \
		$(UNIT_DIR)/test_job_outcome $(UNIT_DIR)/test_outcome_repl
	@$(UNIT_DIR)/test_diagnostic
	@$(UNIT_DIR)/test_command
	@$(UNIT_DIR)/test_supervisor
	@$(UNIT_DIR)/test_supervisor_report
	@$(UNIT_DIR)/test_supervisor_shutdown
	@$(UNIT_DIR)/test_job_outcome
	@$(UNIT_DIR)/test_outcome_repl
	@$(UNIT_DIR)/test_msg
	@$(UNIT_DIR)/test_sem
	@$(UNIT_DIR)/test_error
	@$(UNIT_DIR)/test_shm
	@$(UNIT_DIR)/test_shm_backend
	@$(UNIT_DIR)/test_shm_backend_ack
	@$(UNIT_DIR)/test_shm_ack_lifecycle
	@$(UNIT_DIR)/test_shm_backend_ack_cleanup
	@$(UNIT_DIR)/test_shm_backend_ack_create
	@$(UNIT_DIR)/test_shm_operations
	@$(UNIT_DIR)/test_shm_stress
	@$(UNIT_DIR)/test_shm_ring
	@$(UNIT_DIR)/test_message_metadata
	@$(UNIT_DIR)/test_producer
	@$(UNIT_DIR)/test_consumer
	@$(UNIT_DIR)/test_consumer_integration
	@$(UNIT_DIR)/test_consumer_concurrency
	@$(UNIT_DIR)/test_consumer_acceptance
	@$(UNIT_DIR)/test_msg_backend
	@$(UNIT_DIR)/test_msg_stress
	@$(UNIT_DIR)/test_msg_ack_concurrency
	@$(UNIT_DIR)/test_msg_ack_lifecycle
	@$(UNIT_DIR)/test_isolation
	@$(UNIT_DIR)/test_producer_routing
	@$(UNIT_DIR)/test_transport_contract
	@$(UNIT_DIR)/test_transport_parity
	@$(UNIT_DIR)/test_transport
	@$(UNIT_DIR)/test_protocol_properties

# Teste do REPL com pseudo-terminal (POSIX; não exige -lutil nem ferramentas externas).
INTEGRATION_DIR := $(BUILD_DIR)/integration

$(INTEGRATION_DIR)/test_repl: tests/integration/test_repl.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LDLIBS) -o $@

# Executável de prova: mesmos módulos da aplicação, syscalls observadas somente nos wrappers.
PROBE_BINARY := $(INTEGRATION_DIR)/mpmc_probe
PROBE_OBJECTS := $(filter-out $(BUILD_DIR)/ipc/sem.o $(BUILD_DIR)/ipc/shm.o $(BUILD_DIR)/ipc/msg.o,$(OBJECTS))

$(INTEGRATION_DIR)/probe_sem.o: src/ipc/sem.c include/mpmc/diagnostic.h Makefile
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dsemget=mpmc_probe_semget -Dsemop=mpmc_probe_semop -c $< -o $@

$(INTEGRATION_DIR)/probe_shm.o: src/ipc/shm.c include/mpmc/diagnostic.h Makefile
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dshmget=mpmc_probe_shmget -Dshmdt=mpmc_probe_shmdt -c $< -o $@

$(INTEGRATION_DIR)/probe_msg.o: src/ipc/msg.c include/mpmc/diagnostic.h Makefile
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dmsgget=mpmc_probe_msgget -Dmsgctl=mpmc_probe_msgctl -Dmsgrcv=mpmc_probe_msgrcv -Dmsgsnd=mpmc_probe_msgsnd -c $< -o $@

$(PROBE_BINARY): $(PROBE_OBJECTS) $(INTEGRATION_DIR)/probe_sem.o $(INTEGRATION_DIR)/probe_shm.o $(INTEGRATION_DIR)/probe_msg.o tests/integration/probe_syscalls.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(INTEGRATION_DIR)/resource_inspect: tests/integration/resource_inspect.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LDLIBS) -o $@

# Testes básicos: o loop interativo deve aceitar o comando shutdown.
test: all unit $(INTEGRATION_DIR)/test_repl $(PROBE_BINARY) $(INTEGRATION_DIR)/resource_inspect
	@printf 'shutdown\n' | $(BINARY) 2 1 > /dev/null
	@printf 'shutdown\n' | $(BINARY) 2 2 > /dev/null
	@sh tests/integration/test_args.sh $(BINARY)
	@sh tests/integration/test_invalid_commands.sh $(BINARY)
	@sh tests/integration/test_repl.sh $(BINARY)
	@$(INTEGRATION_DIR)/test_repl $(BINARY)
	@python3 tests/integration/test_application.py $(BINARY)
	@python3 tests/integration/test_cooperative_shutdown.py $(PROBE_BINARY)
	@python3 tests/integration/test_shutdown_resources.py $(PROBE_BINARY) $(INTEGRATION_DIR)/resource_inspect
	@python3 tests/integration/test_diagnostics.py $(PROBE_BINARY) $(INTEGRATION_DIR)/resource_inspect
	@echo 'Unit, startup and REPL tests passed.'

docs:
	@mkdir -p build/docs
	doxygen Doxyfile
	@echo "HTML documentation: build/docs/html/index.html"

docs-clean:
	$(RM) -r build/docs

clean:
	$(RM) -r build mpmc

-include $(DEPS)
