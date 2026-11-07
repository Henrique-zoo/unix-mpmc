# unix-mpmc — múltiplos produtores/consumidores com IPC System V
# Uso: make | make debug | make release | make test | make unit | make docs | make docs-clean | make clean

CC   := clang
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

$(UNIT_DIR)/supervisor_for_test.o: src/app/supervisor.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) -Dfork=mpmc_test_fork -c $< -o $@

$(UNIT_DIR)/test_command: tests/unit/test_command.c src/app/command.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_supervisor: tests/unit/test_supervisor.c \
		$(UNIT_DIR)/supervisor_for_test.o src/app/command.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_msg: tests/unit/test_msg.c src/ipc/msg.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_sem: tests/unit/test_sem.c src/ipc/sem.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_error: tests/unit/test_error.c src/error.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

$(UNIT_DIR)/test_message_metadata: tests/unit/test_message_metadata.c src/workers/producer.c
	@mkdir -p $(dir $@)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDLIBS) -o $@

unit: $(UNIT_DIR)/test_command $(UNIT_DIR)/test_supervisor $(UNIT_DIR)/test_msg \
		$(UNIT_DIR)/test_error $(UNIT_DIR)/test_sem $(UNIT_DIR)/test_message_metadata
	@$(UNIT_DIR)/test_command
	@$(UNIT_DIR)/test_supervisor
	@$(UNIT_DIR)/test_msg
	@$(UNIT_DIR)/test_sem
	@$(UNIT_DIR)/test_error
	@$(UNIT_DIR)/test_message_metadata

# Testes básicos: o loop interativo deve aceitar o comando shutdown.
test: all unit
	@printf 'shutdown\n' | ./mpmc 2 1 > /dev/null
	@printf 'shutdown\n' | ./mpmc 2 2 > /dev/null
	@sh tests/integration/test_repl.sh ./mpmc
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
