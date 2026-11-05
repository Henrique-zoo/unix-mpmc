#!/bin/sh
## @file test_repl.sh
## @brief Testes de integração do terminal interativo do mpmc.
##
## @details Verifica a recuperação após comandos inválidos, a exibição do prompt,
## o encerramento por EOF (com entrada vazia ou comando sem newline final), o
## sufixo opcional `&` e a coleta de um filho terminado enquanto stdin permanece
## aberto e ocioso. Esse último cenário confirma que SIGCHLD desperta o REPL pelo
## self-pipe e que a coleta não depende de o usuário enviar outra linha.
##
## Dois cenários adicionais mantêm um filho bloqueado por meio da variável
## MPMC_TEST_WORKER_HANG. Eles verificam que o REPL continua aceitando comandos,
## que `shutdown` recolhe o filho bloqueado e que SIGTERM enviado ao próprio REPL
## também encerra o filho. O gancho de bloqueio existe apenas em builds de
## depuração (`-DDEBUG`); em builds release esses cenários são ignorados. O teste
## depende dos utilitários `ps` e `pgrep` para observar os processos.
##
## Para exercitar os casos, execute o script a partir da raiz do repositório,
## informando o binário como primeiro argumento. A entrada é fornecida por pipes
## ou FIFOs para testar o comportamento sem interação manual.
##
## Também devem ser verificados manualmente estes casos de job control:
##
## - iniciar `mpmc` em primeiro plano e enviar vários comandos, inclusive enquanto há filhos;
## - enviar EOF com e sem uma linha final terminada por `\n` e confirmar encerramento ordenado;
## - executar com stdin redirecionado para arquivo/pipe; essa fonte não terminal é permitida mesmo
##   quando o processo foi iniciado em background pelo shell;
## - colocar o processo em background mantendo stdin ligado ao terminal e confirmar diagnóstico
##   imediato, sem suspensão por `SIGTTIN`;
## - deixar a entrada ociosa com filhos ativos e verificar que `waitpid(..., WNOHANG)` e o ponto de
##   drenagem de eventos do supervisor continuam sendo chamados periodicamente.

set -eu

binary=${1:-./mpmc}
tmp_dir=$(mktemp -d)
repl_pid=
cleanup() {
	if [ -n "$repl_pid" ]; then
		pkill -KILL -P "$repl_pid" 2>/dev/null || true
		kill -KILL "$repl_pid" 2>/dev/null || true
	fi
	rm -rf "$tmp_dir"
}
trap cleanup EXIT HUP INT TERM

# Um comando inválido deve informar o erro e manter o REPL disponível.
printf 'invalido\nshutdown\n' | "$binary" 2 1 >"$tmp_dir/out" 2>"$tmp_dir/err"
grep -q 'Argumento inválido' "$tmp_dir/err"
prompt_count=$(grep -o '>>' "$tmp_dir/out" | wc -l | tr -d ' ')
[ "$prompt_count" -ge 2 ]

# EOF é uma forma explícita e válida de encerramento, inclusive com entrada vazia.
: | "$binary" 2 1 >"$tmp_dir/eof_out" 2>"$tmp_dir/eof_err"
grep -q '^>>' "$tmp_dir/eof_out"
printf 'shutdown' | "$binary" 2 1 >"$tmp_dir/eof_line_out" 2>"$tmp_dir/eof_line_err"
grep -q '^>>' "$tmp_dir/eof_line_out"

# O & como sufixo do REPL pertence à sintaxe do comando; não é
# necessário executar o processo em segundo plano pelo shell para este teste.
printf 'shutdown &\n' | "$binary" 2 2 >"$tmp_dir/amp_out" 2>"$tmp_dir/amp_err"
grep -q '^>>' "$tmp_dir/amp_out"

# Um filho que termina enquanto stdin permanece aberto e ocioso deve ser recolhido
# (e seu término informado) sem aguardar outro comando: SIGCHLD desperta o loop pelo self-pipe.
fifo="$tmp_dir/in"
mkfifo "$fifo"
"$binary" 2 1 <"$fifo" >"$tmp_dir/idle_out" 2>"$tmp_dir/idle_err" &
repl_pid=$!
exec 3>"$fifo"
printf 'produz 1 1 2\n' >&3
attempts=0
until grep -q 'terminou com código' "$tmp_dir/idle_err"; do
	attempts=$((attempts + 1))
	if [ "$attempts" -gt 30 ]; then
		kill "$repl_pid" 2>/dev/null || true
		echo 'filho terminado não foi recolhido com stdin ocioso' >&2
		exit 1
	fi
	sleep 0.1
done
printf 'shutdown\n' >&3
exec 3>&-
wait "$repl_pid"

# --- Worker bloqueado -------------------------------------------------------------
# Os cenários abaixo usam MPMC_TEST_WORKER_HANG, gancho disponível apenas em builds
# de depuração (-DDEBUG): o filho fica bloqueado em pause(), como um worker preso em IPC.

# Verdadeiro enquanto o processo existir e não for zumbi (dash não recolhe sozinho).
is_running() {
	state=$(ps -o stat= -p "$1" 2>/dev/null || true)
	[ -n "$state" ] && [ "${state#Z}" = "$state" ]
}

has_child() {
	pgrep -P "$repl_pid" >/dev/null 2>&1
}

# wait_until <décimos de segundo> <comando...>
wait_until() {
	remaining=$1
	shift
	while ! "$@"; do
		remaining=$((remaining - 1))
		[ "$remaining" -gt 0 ] || return 1
		sleep 0.1
	done
}

stopped() {
	! is_running "$1"
}

fail() {
	echo "$1" >&2
	exit 1
}

# start_hung_repl <prefixo>: inicia o REPL com stdin em FIFO (fd 3) e um worker bloqueado.
start_hung_repl() {
	hung_fifo="$tmp_dir/$1.in"
	mkfifo "$hung_fifo"
	MPMC_TEST_WORKER_HANG=1 "$binary" 2 1 <"$hung_fifo" >"$tmp_dir/$1.out" 2>"$tmp_dir/$1.err" &
	repl_pid=$!
	exec 3>"$hung_fifo"
	printf 'produz 1 1 2\n' >&3
}

if ! command -v pgrep >/dev/null 2>&1 || ! command -v ps >/dev/null 2>&1; then
	echo 'SKIP: cenários de worker bloqueado exigem ps e pgrep.'
else
	# Cenário 1: com um worker bloqueado o REPL continua lendo comandos e, no
	# shutdown, o worker recebe SIGTERM e é recolhido (sem travar em waitpid).
	start_hung_repl hang1
	if ! wait_until 30 has_child; then
		exec 3>&-
		echo 'SKIP: worker de teste não iniciou (binário sem -DDEBUG?); cenários de worker bloqueado ignorados.'
	else
		worker_pid=$(pgrep -P "$repl_pid" | head -n 1)
		printf 'invalido\n' >&3
		wait_until 30 grep -q 'Argumento inválido' "$tmp_dir/hang1.err" \
			|| fail 'REPL parou de ler comandos com um worker bloqueado'
		is_running "$worker_pid" || fail 'worker de teste terminou antes do shutdown'
		printf 'shutdown\n' >&3
		exec 3>&-
		wait_until 30 stopped "$repl_pid" || fail 'shutdown travou com worker bloqueado (SIGTERM ignorado pelo filho?)'
		wait "$repl_pid"
		repl_pid=
		is_running "$worker_pid" && fail 'worker bloqueado sobreviveu ao shutdown'

		# Cenário 2: SIGTERM no próprio REPL, com o worker bloqueado, também encerra tudo.
		start_hung_repl hang2
		wait_until 30 has_child || fail 'worker de teste não iniciou no cenário 2'
		worker_pid=$(pgrep -P "$repl_pid" | head -n 1)
		kill -TERM "$repl_pid"
		exec 3>&-
		wait_until 30 stopped "$repl_pid" || fail 'SIGTERM no REPL não encerrou com worker bloqueado'
		wait "$repl_pid"
		repl_pid=
		is_running "$worker_pid" && fail 'worker bloqueado sobreviveu ao SIGTERM do REPL'
	fi
fi

printf 'REPL integration tests passed.\n'
