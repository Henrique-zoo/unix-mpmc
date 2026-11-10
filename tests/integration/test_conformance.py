#!/usr/bin/env python3
"""Admissão de mais de 64 ACKs, p sem teto fixo e falha explícita do relógio final."""
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from test_application import Application


def main():
    binary, probe, helper = map(os.path.abspath, sys.argv[1:4])
    for mode in (1, 2):
        app = Application(probe, 1, mode, resource_helper=helper)
        try:
            # O consumidor fica depois dos 65 comandos na entrada. O REPL não pode
            # bloquear com a tabela cheia nem rejeitar a operação adicional.
            app.send("produz 1 1 1\n" * 65 + "consome 1 65 1\n")
            app.wait_for("concluída:", 66)
            assert "Não há espaço disponível" not in app.text, app.text
            app.finish({1: 65}, 66)
        finally:
            app.close()
        # Falha de criação controlada prova que p=1025 passou pelo parser e chegou
        # ao backend, sem exigir milhares de objetos em sistemas com limites baixos.
        failed = subprocess.run([probe, "1025", str(mode)], input="shutdown\n", text=True,
            capture_output=True, timeout=5, env={**os.environ, "MPMC_PROBE_FAIL_CREATE_AT": "1"})
        assert failed.returncode != 0, failed
        assert "Uso:" not in failed.stderr and "falha em supervisor_create" in failed.stderr, failed.stderr
    with tempfile.TemporaryDirectory(prefix="mpmc-clock-") as directory:
        marker = Path(directory) / "fail-clock"
        app = Application(probe, 1, 1, {"MPMC_PROBE_FAIL_CLOCK_MARKER": str(marker)}, resource_helper=helper)
        try:
            app.send("consome 1 1024 1\nproduz 1 1024 1\n")
            app.wait_for("terminou com código 0", 2)
            marker.write_text("EIO")
            app.send("shutdown\n")
            app.output += app.process.communicate(timeout=8)[0]
            assert app.process.returncode != 0, app.text
            assert "Tempo total de operação do mpmc: indisponível" in app.text, app.text
            assert "clock_gettime(CLOCK_MONOTONIC)" in app.text, app.text
            assert "Consumidos pela estrutura 1: 1024" in app.text, app.text
        finally:
            app.close()
    print("Enunciado: 65 ACKs, p=1025 e erro do relógio final aprovados.")


if __name__ == "__main__":
    main()
