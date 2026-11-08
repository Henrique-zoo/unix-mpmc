#!/bin/sh
## @file test_args.sh
## @brief Testes dos argumentos de inicialização do mpmc.
##
## @details Executa o binário com combinações de argumentos válidas e inválidas e
## confere o contrato de main(): `mpmc <produtores> <paradigma>`, com produtores
## inteiro estritamente positivo e paradigma 1 ou 2.
##
## Para argumentos inválidos, o programa deve terminar com código diferente de
## zero, apresentar a sintaxe de uso em stderr, não escrever nada em stdout (nada
## foi iniciado) e não criar objetos IPC. Para argumentos válidos, deve terminar
## com código zero. A entrada padrão vem de /dev/null, então nenhum caso depende
## de interação.
##
## Uso, a partir da raiz do repositório: sh tests/integration/test_args.sh ./mpmc
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

# run_case <rótulo> [argumentos...]: executa o binário e deixa o resultado em
# $rc, $tmp_dir/out e $tmp_dir/err.
run_case() {
	label=$1
	shift
	checks=$((checks + 1))
	rc=0
	"$binary" "$@" </dev/null >"$tmp_dir/out" 2>"$tmp_dir/err" || rc=$?
}

# expect_invalid <rótulo> [argumentos...]
expect_invalid() {
	run_case "$@"
	if [ "$rc" -eq 0 ]; then
		fail "[$label] deveria terminar com código diferente de zero"
	fi
	if [ "$rc" -ge 126 ]; then
		fail "[$label] terminou de forma anormal (código $rc)"
	fi
	grep -q 'Uso:' "$tmp_dir/err" || fail "[$label] stderr deveria mostrar a sintaxe de uso"
	if [ -s "$tmp_dir/out" ]; then
		fail "[$label] stdout deveria estar vazio, pois nada foi iniciado"
	fi
}

# expect_valid <rótulo> [argumentos...]
expect_valid() {
	run_case "$@"
	if [ "$rc" -ne 0 ]; then
		fail "[$label] deveria terminar com código zero, obteve $rc"
	fi
	if grep -q 'Uso:' "$tmp_dir/err"; then
		fail "[$label] não deveria mostrar a sintaxe de uso"
	fi
}

if [ ! -x "$binary" ]; then
	echo "binário não encontrado: $binary" >&2
	exit 1
fi

ipc_before=$(ipc_snapshot)

# Quantidade de argumentos.
expect_invalid 'sem argumentos'
expect_invalid 'só produtores' 2
expect_invalid 'argumento a mais' 2 1 1
expect_invalid 'dois argumentos a mais' 2 1 1 1

# Número de produtores: estritamente positivo, apenas dígitos decimais.
expect_invalid 'produtores = 0' 0 1
expect_invalid 'produtores = 00' 00 1
expect_invalid 'produtores negativo' -1 1
expect_invalid 'produtores com sinal +' +2 1
expect_invalid 'produtores não numérico' abc 1
expect_invalid 'produtores misto' 2x 1
expect_invalid 'produtores decimal' 1.5 1
expect_invalid 'produtores hexadecimal' 0x2 1
expect_invalid 'produtores vazio' '' 1
expect_invalid 'produtores com espaço' '2 ' 1
expect_invalid 'produtores com espaço inicial' ' 2' 1

# Paradigma: somente 1 (memória compartilhada) ou 2 (filas de mensagens).
expect_invalid 'paradigma = 0' 2 0
expect_invalid 'paradigma = 3' 2 3
expect_invalid 'paradigma = 12' 2 12
expect_invalid 'paradigma negativo' 2 -1
expect_invalid 'paradigma não numérico' 2 abc
expect_invalid 'paradigma decimal' 2 1.0
expect_invalid 'paradigma vazio' 2 ''

# Overflow: acima de SIZE_MAX (2^64 - 1) e acima do tamanho de um unsigned.
expect_invalid 'produtores = 2^64' 18446744073709551616 1
expect_invalid 'produtores = 2^64 + 1' 18446744073709551617 1
expect_invalid 'produtores com 32 dígitos' 99999999999999999999999999999999 1
expect_invalid 'paradigma = 2^64' 2 18446744073709551616
expect_invalid 'paradigma = 2^64 + 1 (não pode dar a volta e virar 1)' 2 18446744073709551617
expect_invalid 'paradigma = 2^32 + 1 (não pode ser truncado para 1)' 2 4294967297
expect_invalid 'paradigma = 2^32 + 2 (não pode ser truncado para 2)' 2 4294967298

# Argumentos inválidos não podem deixar objetos IPC para trás.
ipc_after=$(ipc_snapshot)
if command -v ipcs >/dev/null 2>&1; then
	[ "$ipc_before" = "$ipc_after" ] || fail 'argumentos inválidos alteraram os objetos IPC existentes'
else
	echo 'aviso: ipcs indisponível; verificação de IPC ignorada' >&2
fi

# Combinações válidas.
expect_valid 'um produtor, memória compartilhada' 1 1
expect_valid 'um produtor, filas de mensagens' 1 2
expect_valid 'dois produtores, memória compartilhada' 2 1
expect_valid 'dois produtores, filas de mensagens' 2 2
expect_valid 'vários produtores' 4 2
expect_valid 'zeros à esquerda são permitidos' 02 01

echo "$checks casos, $failures falhas"

[ "$failures" -eq 0 ]
