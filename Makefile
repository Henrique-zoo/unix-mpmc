# unix-mpmc — múltiplos produtores/consumidores com IPC System V
# Uso: make | make debug | make release | make test | make unit | make docs | make docs-clean | make clean

CC := clang
MODE ?= debug

CPPFLAGS += -Iinclude -D_POSIX_C_SOURCE=200809L
CFLAGS   += -std=c23 -Wall -Wextra -Wpedantic -Wshadow

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
SHM_RING_OBJECT := $(BUILD_DIR)/ipc/shm_backend/shm_ring.o

$(UNIT_DIR)/supervisor_for_test.o: src/app/supervisor.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dfork=mpmc_test_fork -c $< -o $@

$(UNIT_DIR)/test_command: tests/unit/test_command.c src/app/command.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_supervisor: tests/unit/test_supervisor.c \
		$(UNIT_DIR)/supervisor_for_test.o src/app/command.c src/error.c \
		src/ipc/transport.c src/ipc/msg_backend.c $(SHM_BACKEND_DIR)/lifecycle.c \
		$(SHM_RING_OBJECT) src/ipc/msg.c src/ipc/shm.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

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
		include/mpmc/ipc/shm_backend.h include/mpmc/message.h
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) \
		-Dmalloc=mpmc_test_malloc -Dcalloc=mpmc_test_calloc -Dfree=mpmc_test_free \
		-Dsysv_shm_create=mpmc_test_shm_create -Dsysv_shm_attach=mpmc_test_shm_attach \
		-Dsysv_shm_detach=mpmc_test_shm_detach -Dsysv_shm_remove=mpmc_test_shm_remove \
		-c $< -o $@

$(UNIT_DIR)/test_shm_backend: tests/unit/test_shm_backend.c \
		$(UNIT_DIR)/shm_backend_for_test.o $(SHM_RING_OBJECT)
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

$(UNIT_DIR)/test_producer_routing: tests/unit/test_producer_routing.c \
		$(WORKERS_DIR)/producer_routing.c $(WORKERS_DIR)/producer.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(WORKERS_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_protocol_properties: tests/unit/test_protocol_properties.c \
		src/error.c $(WORKERS_DIR)/producer.c $(WORKERS_DIR)/producer_routing.c $(SHM_RING_OBJECT)
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) -I$(WORKERS_DIR) -I$(SHM_BACKEND_DIR) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_msg_backend: tests/unit/test_msg_backend.c src/ipc/msg_backend.c src/ipc/msg.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_transport_contract: tests/unit/test_transport_contract.c \
		src/ipc/transport.c src/ipc/msg_backend.c $(SHM_BACKEND_DIR)/lifecycle.c \
		$(SHM_RING_OBJECT) src/ipc/msg.c src/ipc/shm.c src/error.c
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

$(UNIT_DIR)/test_isolation: tests/unit/test_isolation.c src/app/command.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

unit: $(UNIT_DIR)/test_command $(UNIT_DIR)/test_supervisor $(UNIT_DIR)/test_msg \
		$(UNIT_DIR)/test_error $(UNIT_DIR)/test_sem $(UNIT_DIR)/test_shm \
		$(UNIT_DIR)/test_shm_backend $(UNIT_DIR)/test_shm_ring $(UNIT_DIR)/test_message_metadata \
		$(UNIT_DIR)/test_msg_backend $(UNIT_DIR)/test_isolation \
		$(UNIT_DIR)/test_producer_routing $(UNIT_DIR)/test_producer $(UNIT_DIR)/test_transport_contract \
		$(UNIT_DIR)/test_transport $(UNIT_DIR)/test_protocol_properties
	@$(UNIT_DIR)/test_command
	@$(UNIT_DIR)/test_supervisor
	@$(UNIT_DIR)/test_msg
	@$(UNIT_DIR)/test_sem
	@$(UNIT_DIR)/test_error
	@$(UNIT_DIR)/test_shm
	@$(UNIT_DIR)/test_shm_backend
	@$(UNIT_DIR)/test_shm_ring
	@$(UNIT_DIR)/test_message_metadata
	@$(UNIT_DIR)/test_producer
	@$(UNIT_DIR)/test_msg_backend
	@$(UNIT_DIR)/test_isolation
	@$(UNIT_DIR)/test_producer_routing
	@$(UNIT_DIR)/test_transport_contract
	@$(UNIT_DIR)/test_transport
	@$(UNIT_DIR)/test_protocol_properties

# Teste do REPL com pseudo-terminal (POSIX; não exige -lutil nem ferramentas externas).
INTEGRATION_DIR := $(BUILD_DIR)/integration

$(INTEGRATION_DIR)/test_repl: tests/integration/test_repl.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $< $(LDLIBS) -o $@

# Testes básicos: o loop interativo deve aceitar o comando shutdown.
test: all unit $(INTEGRATION_DIR)/test_repl
	@printf 'shutdown\n' | ./mpmc 2 1 > /dev/null
	@printf 'shutdown\n' | ./mpmc 2 2 > /dev/null
	@sh tests/integration/test_args.sh ./mpmc
	@sh tests/integration/test_invalid_commands.sh ./mpmc
	@sh tests/integration/test_repl.sh ./mpmc
	@$(INTEGRATION_DIR)/test_repl ./mpmc
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
