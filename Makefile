# unix-mpmc — multiple producers/consumers with System V IPC
# Usage: make | make debug | make release | make test | make docs | make docs-clean | make clean

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
.PHONY: all debug release test docs docs-clean clean

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

# Smoke tests: the interactive loop must accept the shutdown command.
test: all
	@printf 'shutdown\n' | ./mpmc 2 1 > /dev/null
	@printf 'shutdown\n' | ./mpmc 2 2 > /dev/null
	@echo 'Smoke tests passed (shared memory and message queues).'

docs:
	@mkdir -p build/docs
	doxygen Doxyfile
	@echo "HTML documentation: build/docs/html/index.html"

docs-clean:
	$(RM) -r build/docs

clean:
	$(RM) -r build mpmc

-include $(DEPS)
