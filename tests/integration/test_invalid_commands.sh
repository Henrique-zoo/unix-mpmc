#!/bin/sh
## @file test_invalid_commands.sh
## @brief Comandos inválidos do REPL não criam processos nem objetos IPC.
##
## @details Envia cada comando inválido ao REPL (2 produtores) seguido de
## `shutdown` e confere que o diagnóstico foi emitido em stderr, que o REPL
## encerrou normalmente e que o relatório final informa que nenhum processo
## filho foi executado. Um caso de controle com um comando válido confirma que
## o relatório realmente registra filhos, para que a verificação não passe em
## vão. Ao final, a lista de objetos IPC System V deve ser a mesma do início.
##
## Uso, a partir da raiz do repositório: sh tests/integration/test_invalid_commands.sh ./mpmc
## Termina com código diferente de zero se qualquer caso falhar.

set -u

binary=${1:-./mpmc}
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM

checks=0
failures=0

fail() {
	failures=$((failures + 1))
	echo "FALHA: $*" >&2
}

# Fotografia dos objetos IPC System V existentes (vazia se ipcs não existir).
ipc_snapshot() {
	if command -v ipcs >/dev/null 2>&1; then
		ipcs -m -s -q 2>/dev/null
	fi
}

# run_repl <entrada>: executa o REPL com 2 produtores e a entrada dada.
# Deixa o resultado em $rc, $tmp_dir/out e $tmp_dir/err.
run_repl() {
	rc=0
	printf '%b' "$1" | "$binary" 2 1 >"$tmp_dir/out" 2>"$tmp_dir/err" || rc=$?
}

# expect_rejected <comando inválido>
expect_rejected() {
	checks=$((checks + 1))
	run_repl "$1\nshutdown\n"
	if [ "$rc" -ne 0 ]; then
		fail "[$1] o REPL deveria continuar e encerrar com código zero, obteve $rc"
	fi
	grep -q '^mpmc: ' "$tmp_dir/err" || fail "[$1] deveria emitir diagnóstico em stderr"
	grep -q 'Nenhum processo filho foi executado' "$tmp_dir/out" \
		|| fail "[$1] o relatório deveria informar que nenhum filho foi executado"
	grep -Eq '^Filhos recolhidos: 0$' "$tmp_dir/out" \
		|| fail "[$1] o relatório deveria registrar zero filhos recolhidos"
	if grep -q 'Filhos ainda ativos' "$tmp_dir/out"; then
		fail "[$1] há filhos ativos após um comando inválido"
	fi
}

if [ ! -x "$binary" ]; then
	echo "binário não encontrado: $binary" >&2
	exit 1
fi

ipc_before=$(ipc_snapshot)

# Sintaxe, tokens excedentes e overflow.
expect_rejected 'invalido'
expect_rejected 'PRODUZ 1 1 1'
expect_rejected 'produz'
expect_rejected 'produz 1 5'
expect_rejected 'produz 1 5 abc'
expect_rejected 'produz 1 5 -1'
expect_rejected 'produz 1 5 1 9'
expect_rejected 'consome 1 5 1 9'
expect_rejected 'shutdown 1'
expect_rejected 'produz 1 5 1 \& extra'
expect_rejected 'produz 18446744073709551616 1 1'
expect_rejected 'produz 1 18446744073709551616 1'
expect_rejected 'produz 1 5 18446744073709551616'

# Faixas de n: 0 e 1025 (os limites válidos são 1 e 1024).
expect_rejected 'produz 1 0 1'
expect_rejected 'produz 1 1025 1'
expect_rejected 'consome 1 0 1'
expect_rejected 'consome 1 1025 1'

# Valores de ack: somente 1 e 2.
expect_rejected 'produz 1 5 0'
expect_rejected 'produz 1 5 3'
expect_rejected 'consome 1 5 0'
expect_rejected 'consome 1 5 3'

# Índices inexistentes (com 2 produtores) e consome 0.
expect_rejected 'produz 3 1 1'
expect_rejected 'consome 3 1 1'
expect_rejected 'consome 0 1 1'

# Vários comandos inválidos seguidos, e EOF em vez de shutdown.
checks=$((checks + 1))
run_repl 'invalido\nproduz 1 0 1\nconsome 0 1 1\n'
[ "$rc" -eq 0 ] || fail "[vários inválidos + EOF] código de saída $rc"
grep -q 'Nenhum processo filho foi executado' "$tmp_dir/out" \
	|| fail '[vários inválidos + EOF] o relatório deveria informar que nenhum filho foi executado'

# Nenhum comando inválido pode ter criado ou alterado objetos IPC.
ipc_after=$(ipc_snapshot)
if command -v ipcs >/dev/null 2>&1; then
	[ "$ipc_before" = "$ipc_after" ] || fail 'comandos inválidos alteraram os objetos IPC existentes'
else
	echo 'aviso: ipcs indisponível; verificação de IPC ignorada' >&2
fi

# Controle: um comando válido cria um filho e o relatório registra isso.
checks=$((checks + 1))
run_repl 'produz 1 1 1\nshutdown\n'
grep -Eq '^Filhos recolhidos: 1$' "$tmp_dir/out" \
	|| fail '[controle] um comando válido deveria aparecer no relatório como um filho recolhido'

echo "$checks casos, $failures falhas"

[ "$failures" -eq 0 ]
