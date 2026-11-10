#!/usr/bin/env python3
"""SIGTERM entre retirada/publicação deve conservar consumo e permitir fechamento local."""
import os
import sys
from test_application import Application


def main():
    binary = os.path.abspath(sys.argv[1])
    for mode in (1, 2):
        app = Application(binary, 1, mode, {"MPMC_PROBE_CANCEL_AFTER_RECEIVE": "1", "MPMC_PROBE_DEFER_EVENTS": "1"})
        try:
            app.send("produz 1 2 2\n")
            app.wait_for("2 de 2 mensagens produzidas")
            app.send("consome 1 2 2\n")
            app.wait_for("1 de 2 mensagens consumidas")
            assert "cancelada antes de terminar" in app.text, app.text
            assert "falha em consumer_run" in app.text, app.text
            app.finish({1: 1}, 2)
        finally:
            app.close()
        # ACK devido localmente e evento acumulado devem ser publicados no fechamento.
        app = Application(binary, 1, mode, {"MPMC_PROBE_CANCEL_AFTER_RECEIVE": "1",
            "MPMC_PROBE_DEFER_EVENTS": "1", "MPMC_PROBE_DEFER_ACK": "1",
            "MPMC_PROBE_SIGNAL_DURING_CLOSE": "1"})
        try:
            app.send("produz 1 1 1\nconsome 1 2 2\n")
            app.wait_for("1 de 2 mensagens consumidas")
            app.wait_for("1 de 1 mensagens produzidas")
            app.finish({1: 1}, 2)
        finally:
            app.close()
        # Um envio já aceito continua sendo contabilizado se o sinal chega antes da publicação.
        app = Application(binary, 1, mode, {"MPMC_PROBE_CANCEL_AFTER_SEND": "1"})
        try:
            app.send("produz 1 2 2\n")
            app.wait_for("1 de 2 mensagens produzidas")
            assert "cancelada antes de terminar" in app.text, app.text
            app.send("consome 1 1 1\n")
            app.wait_for("1 de 1 mensagens consumidas")
            app.finish({1: 1}, 2)
        finally:
            app.close()
    print("Cancelamento cooperativo: retirada/publicação e totais preservados nos dois modos.")


if __name__ == "__main__":
    main()
