#!/usr/bin/env python3
"""Exercita o CLI real por eventos, sem sleeps nem ordem global de resultados."""

import os
import re
import selectors
import signal
import subprocess
import sys
import time
import tempfile
from pathlib import Path


class Application:
    """Mantém somente o grupo de processos criado por este cenário."""

    def __init__(self, binary, producers, mode, environment=None, resource_helper=None):
        self.resource_helper = resource_helper
        self.resource_directory = tempfile.TemporaryDirectory(prefix="mpmc-owned-") if resource_helper else None
        self.manifest = Path(self.resource_directory.name) / "resources" if resource_helper else None
        extra = {"MPMC_PROBE_RESOURCES": str(self.manifest)} if resource_helper else {}
        self.process = subprocess.Popen(
            [binary, str(producers), str(mode)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            bufsize=0,
            env={**os.environ, "NO_COLOR": "1", **(environment or {}), **extra},
        )
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.output = b""

    def send(self, commands):
        self.process.stdin.write(commands.encode())
        self.process.stdin.flush()

    def wait_for(self, text, count=1, timeout=15):
        expected = text.encode()
        deadline = time.monotonic() + timeout
        while self.output.count(expected) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"Prazo excedido esperando {text!r}:\n{self.text}")
            for key, _ in self.selector.select(remaining):
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    raise AssertionError(f"Programa encerrou antes de {text!r}:\n{self.text}")
                self.output += chunk

    @property
    def text(self):
        return self.output.decode(errors="replace")

    def finish(self, expected_counts, expected_children):
        self.send("shutdown\n")
        self.output += self.process.communicate(timeout=8)[0]
        assert self.process.returncode == 0, self.text
        assert f"Filhos recolhidos: {expected_children}\n" in self.text, self.text
        counts = {
            int(index): int(count)
            for index, count in re.findall(r"Consumidos pela estrutura (\d+): (\d+)", self.text)
        }
        assert counts == expected_counts, self.text
        assert "DURAÇÃO" in self.text, self.text
        durations = re.findall(r"^\d+\s+\d+\s+(?:produz|consome).*?([0-9]+\.[0-9]+) s$", self.text, re.MULTILINE)
        assert len(durations) == expected_children, self.text
        assert all(float(value) >= 0 for value in durations), self.text
        assert re.search(r"Tempo total de operação do mpmc: [0-9]+\.[0-9]+ s", self.text), self.text
        assert "ainda não está implementada" not in self.text, self.text
        assert "\x1b[" not in self.text, self.text
        if self.manifest and self.manifest.exists():
            for kind, resource, pid in (line.split() for line in self.manifest.read_text().splitlines()):
                assert int(pid) == self.process.pid
                assert subprocess.run([self.resource_helper, "exists", kind, resource], timeout=3).returncode == 1

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.communicate()
        if self.manifest and self.manifest.exists():
            for kind, resource, pid in (line.split() for line in self.manifest.read_text().splitlines()):
                assert int(pid) == self.process.pid
                subprocess.run([self.resource_helper, "remove", kind, resource], timeout=3, check=False)
        if self.resource_directory:
            self.resource_directory.cleanup()
        self.selector.close()
        for stream in (self.process.stdin, self.process.stdout):
            if stream and not stream.closed:
                stream.close()


def scenario(binary, mode, producers, commands, outcomes, counts, children, partial=False):
    app = Application(binary, producers, mode)
    try:
        app.send(commands)
        app.wait_for("concluída:", outcomes)
        if partial:
            app.send("consome 1 5 2\n")
            app.wait_for("apenas 2 de 5 mensagens consumidas")
        app.finish(counts, children)
    finally:
        app.close()


def ack_after_last_message(binary, mode):
    app = Application(binary, 1, mode)
    try:
        app.send("produz 1 10 1\nconsome 1 9 1\n")
        app.wait_for("9 de 9 mensagens consumidas")
        assert not re.search(r"operação 1 .*concluída", app.text), app.text
        app.send("consome 1 1 1\n")
        app.wait_for("concluída:", 3)
        app.finish({1: 10}, 3)
    finally:
        app.close()


def produced_before_consumed(binary, mode):
    app = Application(binary, 3, mode)
    try:
        # ACK=2 conclui antes de existir consumidor; striped não divisível por p.
        app.send("produz 0 8 2\n")
        app.wait_for("8 de 8 mensagens produzidas")
        app.send("produz 4 1 2\nconsome 0 1 2\n")
        app.wait_for("inválido", 2)
        app.send("consome 1 3 1\nconsome 2 3 1\nconsome 3 2 1\n")
        app.wait_for("concluída:", 4)
        app.send("consome 1 1 2\n")
        app.wait_for("apenas 0 de 1 mensagens consumidas")
        app.finish({1: 3, 2: 3, 3: 2}, 5)
    finally:
        app.close()


def main():
    binary = os.path.abspath(sys.argv[1])
    for mode in (1, 2):
        ack_after_last_message(binary, mode)
        produced_before_consumed(binary, mode)
        scenario(
            binary, mode, 3,
            "consome 1 3 1\nconsome 2 3 1\nconsome 3 3 1\nproduz 0 9 1\n",
            4, {1: 3, 2: 3, 3: 3}, 4,
        )
        scenario(binary, mode, 1, "produz 1 2 2\n", 1, {1: 2}, 2, partial=True)
        scenario(binary, mode, 1, "consome 1 1024 1\nproduz 1 1024 1\n", 2, {1: 1024}, 2)
        scenario(
            binary, mode, 1,
            "consome 1 60 1\nconsome 1 60 1\n" + "produz 1 20 1\n" * 6,
            8, {1: 120}, 8,
        )
        for _ in range(10):
            app = Application(binary, 1, mode)
            try:
                app.wait_for(">> ")
                app.send("produz 1 1 2\n" * 3)
                app.finish({1: 0}, 3)
            finally:
                app.close()
    print("CLI real: ACK, striped, consumo parcial, saturação e concorrência aprovados nos dois modos.")


if __name__ == "__main__":
    main()
