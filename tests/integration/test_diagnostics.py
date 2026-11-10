#!/usr/bin/env python3
"""Falhas reais observáveis, propagação filho/pai e cleanup restrito aos IDs da fixture."""
import os
import pty
import signal
import selectors
import time
import subprocess
import sys
import tempfile
from pathlib import Path


def inspect(helper, action, kind, resource):
    return subprocess.run([helper, action, kind, resource], timeout=3).returncode


def run_case(binary, helper, mode, flags, commands="shutdown\n", terminal=False):
    with tempfile.TemporaryDirectory(prefix="mpmc-diagnostic-") as directory:
        manifest = Path(directory) / "resources"
        environment = {**os.environ, **flags, "MPMC_PROBE_RESOURCES": str(manifest), "TERM": "xterm"}
        environment.pop("NO_COLOR", None)
        if not terminal:
            environment["NO_COLOR"] = "1"
        master = slave = None
        if terminal:
            master, slave = pty.openpty()
        process = subprocess.Popen(
            [binary, "2", str(mode)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=slave if terminal else subprocess.STDOUT, env=environment,
            start_new_session=True,
        )
        if slave is not None:
            os.close(slave)
        try:
            if commands is None:
                # Mantém stdin aberto: a saída deve ser provocada pela falha fatal, não por EOF.
                process.stdin.write(b"produz 1 1 2\n")
                process.stdin.flush()
                selector = selectors.DefaultSelector()
                selector.register(process.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + 8
                output = b""
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    assert remaining > 0, output.decode(errors="replace")
                    for key, _ in selector.select(remaining):
                        chunk = os.read(key.fd, 65536)
                        if chunk: output += chunk
                        else: selector.unregister(key.fileobj)
                selector.close()
                process.wait(timeout=3)
            else:
                output = process.communicate(commands.encode(), timeout=8)[0]
            if master is not None:
                while True:
                    try:
                        data = os.read(master, 65536)
                    except OSError:
                        break
                    if not data:
                        break
                    output += data
            text = output.decode(errors="replace")
            assert process.returncode != 0, text
            assert "mpmc: falha em " in text, text
            if terminal:
                assert "\x1b[31m" in text and "\x1b[0m" in text, text
            else:
                assert "\x1b[" not in text, text
            resources = [line.split() for line in manifest.read_text().splitlines()]
            assert resources, text
            assert all(int(pid) == process.pid for _, _, pid in resources), resources
            if "MPMC_PROBE_FAIL_REMOVE" not in flags:
                assert all(inspect(helper, "exists", kind, resource) == 1 for kind, resource, _ in resources), resources
            return text
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
            # O manifest contém exclusivamente recursos retornados por este processo.
            if manifest.exists():
                for kind, resource, pid in (line.split() for line in manifest.read_text().splitlines()):
                    assert int(pid) == process.pid
                    result = inspect(helper, "remove", kind, resource)
                    assert result in (0, 1), (kind, resource, result)
            for stream in (process.stdin, process.stdout):
                if stream and not stream.closed:
                    stream.close()
            if master is not None:
                os.close(master)


def main():
    binary, helper = map(os.path.abspath, sys.argv[1:3])
    for terminal in (False, True):
        text = run_case(binary, helper, 1,
            {"MPMC_PROBE_FAIL_CREATE_AT": "3", "MPMC_PROBE_FAIL_DETACH": "1"}, terminal=terminal)
        assert "shmget()" in text and "shmdt()" in text, text
        assert "Ao finalizar/liberar recursos" in text, text
    text = run_case(binary, helper, 2,
        {"MPMC_PROBE_FAIL_CREATE_AT": "4", "MPMC_PROBE_FAIL_REMOVE": "1"})
    assert text.count("msgctl(IPC_RMID)") == 3, text
    assert "msgget()" in text and "Ao finalizar/liberar recursos" in text, text
    text = run_case(binary, helper, 1,
        {"MPMC_PROBE_FAIL_RELEASE": "1", "MPMC_PROBE_FAIL_DETACH": "1"}, commands=None)
    assert "falha em producer_run (operação 1" in text, text
    assert "semop()" in text and "Filhos recolhidos: 1" in text, text
    # Três associações no filho e três no pai, todas observadas.
    assert text.count("shmdt()") == 6, text
    assert "falha em supervisor_destroy" in text, text
    text = run_case(binary, helper, 1,
        {"MPMC_PROBE_FAIL_RELEASE": "1", "MPMC_PROBE_FAIL_DETACH": "1", "MPMC_PROBE_DIAGNOSTIC_FLOOD": "1"}, commands=None)
    assert text.count("detalhe de cleanup injetado") == 1200, text
    print("Diagnósticos reais: rollback, múltiplas falhas, pipe do worker, cores e isolamento aprovados.")


if __name__ == "__main__":
    main()
