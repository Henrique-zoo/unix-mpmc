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
## Três cenários adicionais mantêm um filho bloqueado por meio da variável
## MPMC_TEST_WORKER_HANG. Eles verificam que o REPL continua aceitando comandos,
## que `shutdown` recolhe o filho bloqueado e que SIGTERM enviado ao próprio REPL
## também encerra o filho; o terceiro mata um job com SIGKILL e confirma que o REPL e os
## demais jobs continuam ativos. O gancho de bloqueio existe apenas em builds de
## depuração (`-DDEBUG`); em builds release esses cenários são ignorados. O teste
## depende dos utilitários `ps` e `pgrep` para observar os processos.
##
## Uma linha em branco deve ser ignorada sem mensagem de erro. O redesenho do prompt após avisos
## assíncronos só ocorre com stdout em terminal; por isso não é coberto aqui e deve ser
## verificado manualmente (ou com um pseudo-terminal): inicie `mpmc` em um terminal, envie
## `produz 1 1 2` e confirme que o `>>` reaparece após a mensagem de término do filho.
##
## Dois cenários de regressão cobrem (a) `shutdown`/EOF enviados imediatamente após o
## despacho de workers bloqueados, que não pode travar nem deixar filhos para trás
## (corrida entre SIGTERM e a restauração dos tratadores no filho) e (b) `stdin` fechado,
## que deve terminar com erro em vez de ficar pendurado. Ambos usam um vigia em `sh` puro
## (run_limited), pois `timeout` não existe no macOS.
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

fail_early() {
	echo "$1" >&2
	exit 1
}

# Um comando inválido deve informar o erro e manter o REPL disponível.
printf 'invalido\nshutdown\n' | "$binary" 2 1 >"$tmp_dir/out" 2>"$tmp_dir/err"
grep -q 'Argumento inválido' "$tmp_dir/err"
prompt_count=$(grep -o '>>' "$tmp_dir/out" | wc -l | tr -d ' ')
[ "$prompt_count" -ge 2 ]

# Linhas em branco são ignoradas (sem erro) e o prompt continua sendo exibido.
printf '\n   \t \nshutdown\n' | "$binary" 2 1 >"$tmp_dir/blank_out" 2>"$tmp_dir/blank_err"
[ ! -s "$tmp_dir/blank_err" ] || fail_early 'linha em branco não deveria gerar erro'

# O shutdown imprime o relatório final em stdout com duração e status de cada filho
# e o tempo total de operação (sem filhos, informa que nenhum foi executado).
grep -q 'Nenhum processo filho foi executado' "$tmp_dir/blank_out" \
	|| fail_early 'relatório vazio ausente'
printf 'produz 1 1 2\nconsome 2 1 2\nshutdown\n' | "$binary" 2 1 >"$tmp_dir/rep_out" 2>"$tmp_dir/rep_err"
grep -q 'Relatório de execução' "$tmp_dir/rep_out" || fail_early 'relatório final ausente'
grep -Eq '^[0-9]+ +1 +produz +(código [0-9]+ \((sucesso|falha)\)|sinal [0-9]+) +[0-9]+\.[0-9]{3} s$' "$tmp_dir/rep_out" \
	|| fail_early 'linha do produz fora do formato esperado'
grep -Eq '^[0-9]+ +2 +consome ' "$tmp_dir/rep_out" || fail_early 'linha do consome ausente'
grep -Eq '^Filhos recolhidos: 2$' "$tmp_dir/rep_out" || fail_early 'contagem de filhos incorreta'
grep -Eq '^Tempo total de operação do mpmc: [0-9]+\.[0-9]{3} s$' "$tmp_dir/rep_out" \
	|| fail_early 'tempo total ausente ou fora do formato'
blank_prompts=$(grep -o '>>' "$tmp_dir/blank_out" | wc -l | tr -d ' ')
[ "$blank_prompts" -ge 3 ]

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
grep -Eq 'filho [0-9]+ \(produz, operação [1-9][0-9]*\) terminou com código' "$tmp_dir/idle_err"
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

		# Cenário 3: um job morto por sinal fora do shutdown (kill -KILL) é recolhido e
		# informado, mas o REPL continua lendo comandos e os demais jobs seguem vivos.
		has_two_children() {
			[ "$(pgrep -P "$repl_pid" | wc -l | tr -d ' ')" -ge 2 ]
		}
		start_hung_repl hang3
		printf 'produz 1 1 2\n' >&3
		wait_until 30 has_two_children || fail 'dois workers bloqueados não coexistiram no cenário 3'
		victim_pid=$(pgrep -P "$repl_pid" | head -n 1)
		survivor_pid=$(pgrep -P "$repl_pid" | tail -n 1)
		[ "$victim_pid" != "$survivor_pid" ] || fail 'cenário 3 não encontrou dois PIDs distintos'
		kill -KILL "$victim_pid"
		wait_until 30 grep -q 'terminou pelo sinal 9' "$tmp_dir/hang3.err" \
			|| fail 'filho morto por sinal não foi recolhido/informado'
		is_running "$repl_pid" || fail 'REPL encerrou porque um job morreu por sinal'
		is_running "$survivor_pid" || fail 'o outro job foi derrubado pela morte de um job'
		printf 'invalido\n' >&3
		wait_until 30 grep -q 'Argumento inválido' "$tmp_dir/hang3.err" \
			|| fail 'REPL parou de ler comandos após um job morrer por sinal'
		! grep -q 'falha ao processar eventos' "$tmp_dir/hang3.err" \
			|| fail 'morte de job por sinal foi tratada como erro fatal do REPL'
		printf 'shutdown\n' >&3
		exec 3>&-
		wait_until 30 stopped "$repl_pid" || fail 'shutdown travou no cenário 3'
		wait "$repl_pid"
		repl_pid=
		is_running "$survivor_pid" && fail 'job sobrevivente não foi encerrado pelo shutdown'
		grep -q 'Relatório de execução' "$tmp_dir/hang3.out" || fail 'relatório final ausente no cenário 3'
		grep -q 'sinal 9' "$tmp_dir/hang3.out" || fail 'relatório não mostra o job morto por SIGKILL'
		grep -q 'sinal 15' "$tmp_dir/hang3.out" || fail 'relatório não mostra o job encerrado pelo shutdown'
		grep -q 'Filhos recolhidos: 2' "$tmp_dir/hang3.out" || fail 'relatório deveria ter 2 filhos recolhidos'
	fi
fi

# --- Regressão: shutdown/EOF logo após o fork ---------------------------------------
# O filho herda os tratadores de SIGTERM do REPL até restaurar as ações padrão. Sem
# bloquear os sinais em volta do fork(), o SIGTERM enviado pelo shutdown (ou pelo EOF)
# podia ser engolido e o waitpid() bloqueante do supervisor nunca retornava.
# run_limited <segundos> <arquivo de entrada> <comando...>
# Executa o comando com stdin vindo do arquivo e o mata (junto com seus filhos) se passar do
# limite. Retorna 124 em caso de estouro, ou o código de saída do comando. Não depende de
# `timeout`, ausente no macOS. A entrada vai por arquivo porque jobs em background do sh
# perdem o stdin de pipe.
run_limited() {
	limit=$1
	input_file=$2
	shift 2
	"$@" <"$input_file" >/dev/null 2>&1 &
	limited_pid=$!
	(
		sleep "$limit"
		pkill -KILL -P "$limited_pid" 2>/dev/null || true
		kill -KILL "$limited_pid" 2>/dev/null || true
	) >/dev/null 2>&1 &
	watchdog_pid=$!
	limited_rc=0
	wait "$limited_pid" || limited_rc=$?
	pkill -KILL -P "$watchdog_pid" 2>/dev/null || true
	kill -KILL "$watchdog_pid" 2>/dev/null || true
	wait "$watchdog_pid" 2>/dev/null || true
	# 137 = morto por SIGKILL do vigia (128 + 9).
	[ "$limited_rc" -ne 137 ] || limited_rc=124
	return "$limited_rc"
}

printf 'produz 1 1 2\nproduz 1 1 2\nproduz 1 1 2\nshutdown\n' >"$tmp_dir/race_shutdown.in"
printf 'produz 1 1 2\nproduz 1 1 2\nproduz 1 1 2\n' >"$tmp_dir/race_eof.in"
: >"$tmp_dir/empty.in"

for round in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
	rc=0
	run_limited 5 "$tmp_dir/race_shutdown.in" env MPMC_TEST_WORKER_HANG=1 "$binary" 2 1 || rc=$?
	[ "$rc" -eq 0 ] || fail "shutdown logo após o despacho travou ou falhou (rodada $round, código $rc)"
	rc=0
	run_limited 5 "$tmp_dir/race_eof.in" env MPMC_TEST_WORKER_HANG=1 "$binary" 2 1 || rc=$?
	[ "$rc" -eq 0 ] || fail "EOF logo após o despacho travou ou falhou (rodada $round, código $rc)"
done

# stdin fechado: o self-pipe não pode ocupar o fd 0; o REPL deve falhar, não travar.
rc=0
run_limited 5 "$tmp_dir/empty.in" sh -c 'exec "$1" 2 1 <&-' sh "$binary" || rc=$?
[ "$rc" -ne 124 ] || fail 'stdin fechado deixou o REPL pendurado'
[ "$rc" -ne 0 ] || fail 'stdin fechado deveria terminar com erro'

printf 'REPL integration tests passed.\n'
