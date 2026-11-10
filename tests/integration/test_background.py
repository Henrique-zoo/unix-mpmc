#!/usr/bin/env python3
"""Job em background pode ser suspenso por SIGTTIN e retomado com o equivalente a fg."""
import errno
import fcntl
import os
import pty
import selectors
import signal
import sys
import termios
import time
import tempfile
import subprocess
from pathlib import Path


def scenario(binary, helper, mode, manifest):
    master, slave = pty.openpty()
    control_read, control_write = os.pipe()
    controller = os.fork()
    if controller == 0:
        os.close(master)
        os.close(control_read)
        os.setsid()
        fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
        os.tcsetpgrp(slave, os.getpgrp())
        signal.alarm(10)
        child = os.fork()
        if child == 0:
            os.close(control_write)
            os.setpgid(0, 0)
            for fd in (0, 1, 2):
                os.dup2(slave, fd)
            if slave > 2:
                os.close(slave)
            os.environ["MPMC_PROBE_RESOURCES"] = str(manifest)
            os.execv(binary, [binary, "1", str(mode)])
        os.write(control_write, f"pid {child}\n".encode())
        _, status = os.waitpid(child, os.WUNTRACED)
        if not os.WIFSTOPPED(status) or os.WSTOPSIG(status) != signal.SIGTTIN:
            os.write(control_write, b"failed\n")
            os._exit(2)
        os.write(control_write, b"stopped\n")
        os.tcsetpgrp(slave, child)
        os.kill(child, signal.SIGCONT)
        _, status = os.waitpid(child, 0)
        os.write(control_write, f"exit {os.waitstatus_to_exitcode(status)}\n".encode())
        os.close(slave)
        os._exit(0)
    os.close(slave)
    os.close(control_write)
    selector = selectors.DefaultSelector()
    selector.register(master, selectors.EVENT_READ, "terminal")
    selector.register(control_read, selectors.EVENT_READ, "control")
    output = b""
    control = b""
    sent = False
    child = None
    collected = False
    deadline = time.monotonic() + 12
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            assert remaining > 0, (output, control)
            for key, _ in selector.select(remaining):
                try:
                    chunk = os.read(key.fd, 65536)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    chunk = b""
                if not chunk:
                    selector.unregister(key.fd)
                    continue
                if key.data == "terminal":
                    output += chunk
                else:
                    control += chunk
                    if not sent and b"\n" in control:
                        child = int(control.splitlines()[0].split()[1])
                        os.write(master, b"shutdown\n")
                        sent = True
        _, status = os.waitpid(controller, 0)
        collected = True
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, (output, control)
        assert b"stopped\n" in control and b"exit 0\n" in control, (output, control)
        assert b">> " in output and b"Filhos recolhidos: 0" in output, output
        assert b"use-o em primeiro plano" not in output, output
        for kind, resource, pid in (line.split() for line in manifest.read_text().splitlines()):
            assert int(pid) == child
            assert subprocess.run([helper, "exists", kind, resource], timeout=3).returncode == 1
    finally:
        if not collected:
            if child:
                try:
                    os.killpg(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            try:
                os.kill(controller, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(controller, 0)
        if manifest.exists():
            for kind, resource, pid in (line.split() for line in manifest.read_text().splitlines()):
                assert child is not None and int(pid) == child
                subprocess.run([helper, "remove", kind, resource], timeout=3, check=False)
        selector.close()
        os.close(master)
        os.close(control_read)


if __name__ == "__main__":
    for mode in (1, 2):
        with tempfile.TemporaryDirectory(prefix="mpmc-background-") as directory:
            scenario(os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2]), mode, Path(directory) / "resources")
    print("Background: SIGTTIN, retomada em foreground e shutdown aprovados nos dois modos.")
