#!/usr/bin/env python3
"""Shutdown após readiness real, sem sleeps, e auditoria de recursos exclusivamente próprios."""
import os
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def resource_exists(helper, kind, resource):
    result = subprocess.run([helper, "exists", kind, resource], timeout=3).returncode
    assert result in (0, 1), (kind, resource, result)
    return result == 0


def scenario(binary, helper, mode, command, waiting, ending):
    with tempfile.TemporaryDirectory(prefix="mpmc-shutdown-") as directory:
        manifest = Path(directory) / "resources"
        read_fd, write_fd = os.pipe()
        process = subprocess.Popen([binary, "1", str(mode)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
            pass_fds=(write_fd,), env={**os.environ, "NO_COLOR": "1",
                "MPMC_PROBE_RESOURCES": str(manifest), "MPMC_PROBE_WAIT_FD": str(write_fd)})
        os.close(write_fd)
        selector = selectors.DefaultSelector()
        selector.register(read_fd, selectors.EVENT_READ, "wait")
        selector.register(process.stdout, selectors.EVENT_READ, "output")
        output = b""
        notices = b""
        child_pid = None
        try:
            process.stdin.write(command.encode())
            process.stdin.flush()
            deadline = time.monotonic() + 8
            while child_pid is None:
                remaining = deadline - time.monotonic()
                assert remaining > 0, (command, output)
                for key, _ in selector.select(remaining):
                    chunk = os.read(key.fd, 65536)
                    assert chunk, (command, output)
                    if key.data == "output":
                        output += chunk
                    else:
                        notices += chunk
                        found = re.search(rb"(?:" + waiting.encode() + rb") (\d+) (\d+)\n", notices)
                        if found:
                            child_pid = int(found[1])
            assert child_pid != process.pid and child_pid > 0
            # Só prossegue depois de observar o worker tentando aguardar o recurso indisponível.
            if ending == "shutdown":
                process.stdin.write(b"shutdown\n")
                process.stdin.flush()
                output += process.communicate(timeout=8)[0]
            elif ending == "eof":
                output += process.communicate(timeout=8)[0]
            else:
                process.send_signal(ending)
                process.wait(timeout=8)  # stdin permanece aberto: o sinal deve provocar o término.
                output += process.stdout.read()
            text = output.decode(errors="replace")
            assert process.returncode == 0, text
            assert "Filhos recolhidos: 1" in text and "Consumidos pela estrutura 1: 0" in text, text
            assert f"{child_pid}" in text, text
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                pass
            else:
                raise AssertionError(f"Filho ainda existe depois do shutdown: {child_pid}")
            resources = [line.split() for line in manifest.read_text().splitlines()]
            assert resources and all(int(pid) == process.pid for _, _, pid in resources)
            assert not any(resource_exists(helper, kind, resource) for kind, resource, _ in resources), resources
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
            # Nunca usa ipcrm global: somente IDs registrados para este PID.
            if manifest.exists():
                for kind, resource, pid in (line.split() for line in manifest.read_text().splitlines()):
                    assert int(pid) == process.pid
                    subprocess.run([helper, "remove", kind, resource], check=False, timeout=3)
            selector.close()
            os.close(read_fd)
            for stream in (process.stdin, process.stdout):
                if stream and not stream.closed:
                    stream.close()


def main():
    binary, helper = map(os.path.abspath, sys.argv[1:3])
    spectators = []
    try:
        for kind in ("sem", "shm", "msg"):
            resource = subprocess.check_output([helper, "create", kind, "0"], timeout=3).decode().strip()
            spectators.append((kind, resource))
        for mode in (1, 2):
            for repeat in range(3):
                # Consumidor sem dados, produtor aguardando ACK, produtor sem espaço para enviar.
                cases = [("consome 1 1 1\n", "sem" if mode == 1 else "receive"),
                    ("produz 1 1 1\n", "sem" if mode == 1 else "receive"),
                    ("produz 1 1024 2\n", "sem" if mode == 1 else "send")]
                for command, waiting in cases:
                    scenario(binary, helper, mode, command, waiting, "shutdown")
            for ending in ("eof", signal.SIGINT, signal.SIGTERM):
                scenario(binary, helper, mode, "consome 1 1 1\n",
                    "sem" if mode == 1 else "receive", ending)
        assert all(resource_exists(helper, kind, resource) for kind, resource in spectators)
    finally:
        for kind, resource in spectators:
            subprocess.run([helper, "remove", kind, resource], timeout=3, check=False)
    print("Shutdown real: 24 cenários de dados/espaço/ACK, EOF/sinais e ausência de recursos/filhos aprovados.")


if __name__ == "__main__":
    main()
