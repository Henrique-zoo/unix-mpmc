/**
 * @file shm_backend.h
 * @brief Backend de buffer circular em memória compartilhada com semáforos.
 * @ingroup backends
 * @note Estão implementados o ciclo de vida dos recursos SHM, o envio, o
 * recebimento, a leitura sincronizada dos totais e o mecanismo interno de ACK:
 * registro, correlação, espera e finalização por operação. O recebimento
 * confirma cada retirada uma vez e notifica somente a conclusão do lote.
 *
 * @anchor shm_operation_failures
 * @par Falhas nas operações sincronizadas
 * Em shm_backend_send(), shm_backend_receive() e shm_backend_consumed_count(),
 * rejeições locais de argumentos (system_errno zero), ausência de itens e
 * interrupção da aquisição não alteram o buffer nem deixam reservas parciais.
 * Esses retornos não invalidam o contexto por si mesmos.
 *
 * As demais falhas de semop() são fatais ao contexto, inclusive argumentos
 * rejeitados pelo kernel, perda de permissão e remoção do conjunto. O resultado
 * não identifica a etapa da falha: após adquirir o mutex, a memória pode já ter
 * sido alterada e o mutex pode continuar adquirido. O chamador deve suspender
 * todas as operações de dados e estatísticas desse contexto, propagar a falha
 * para o supervisor e encaminhar o encerramento coordenado dos workers. Cada
 * filho fecha apenas seu estado local; o supervisor recolhe todos os filhos
 * antes de destruir os recursos globais. Não se deve repetir a operação inteira
 * nem tentar reparar os contadores com outros workers ativos.
 *
 * A inutilização é uma obrigação do chamador: o backend não registra uma marca
 * de falha, não rejeita automaticamente chamadas posteriores e não notifica
 * outros processos. A integração desse encerramento pertence à issue #20.
 * Recuperar um contexto após término de processo entre aquisição e publicação
 * não faz parte da garantia destas operações; nesse caso, o contexto também
 * deve ser descartado por encerramento coordenado.
 */
#ifndef MPMC_IPC_SHM_BACKEND_H
#define MPMC_IPC_SHM_BACKEND_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "mpmc/error.h"
#include "mpmc/message.h"

/**
 * @defgroup backends Backends dos paradigmas
 * @ingroup ipc
 * @brief Implementações concretas da API de transporte, selecionadas pela fachada.
 * @{ */

/**
 * @brief Estado opaco do backend por memória compartilhada.
 * @details Mantém os segmentos System V de 2048 bytes por estrutura lógica,
 * seus conjuntos de semáforos e os recursos globais de ACK: um segmento de
 * controle de 2048 bytes e um conjunto com 66 semáforos (mutex, vagas e conclusões).
 * Os mapeamentos são locais a cada processo; os objetos IPC são compartilhados.
 * Apenas `ipc/transport.c` deve acessar diretamente esta interface.
 */
typedef struct shm_backend shm_backend_t;

/** @brief Operação de liberação que originou um diagnóstico de cleanup. */
typedef enum {
    SHM_CLEANUP_DETACH,     /**< Falha ao desassociar um mapeamento com shmdt(). */
    SHM_CLEANUP_REMOVE,     /**< Falha ao remover um segmento com shmctl(IPC_RMID). */
    SHM_CLEANUP_SEM_REMOVE, /**< Falha ao remover um conjunto com semctl(IPC_RMID). */
} shm_cleanup_operation_t;

/**
 * @brief Diagnóstico de uma falha individual na liberação de um segmento.
 * @details Os campos são copiados por valor e não dependem do estado do backend
 * após sua liberação. O registro recebido pela callback só é válido durante a
 * chamada; copie o conteúdo de *error, não o ponteiro error, para armazenar o
 * diagnóstico. A cópia pertence ao chamador e não exige liberação adicional.
 * resource_id identifica o segmento em DETACH/REMOVE e o conjunto em SEM_REMOVE;
 * esses identificadores pertencem a classes diferentes de objetos System V.
 * @see @ref shm_cleanup_diagnostics para o fluxo e um exemplo de coleta.
 */
typedef struct {
    shm_cleanup_operation_t operation; /**< Operação de liberação que falhou. */
    size_t producer_id;                /**< Índice lógico da estrutura, no intervalo 1..p. */
    int resource_id;      /**< ID passado à liberação, inclusive zero ou -1 no rollback. */
    mpmc_result_t result; /**< Falha individual, com o errno original preservado. */
} shm_cleanup_error_t;

/**
 * @brief Recebe uma falha individual durante a limpeza do backend.
 * @param error Diagnóstico válido somente durante a chamada.
 * @param user_data Ponteiro fornecido pelo chamador, sem interpretação pelo backend;
 * pode ser nullptr se a callback não precisar de estado adicional.
 * @details Este tipo declara um ponteiro para função. O chamador atribui sua
 * função ao campo on_error de shm_cleanup_reporter_t. A cada falha de liberação,
 * o backend executa on_error(error, user_data), no mesmo fluxo da chamada que
 * está realizando a limpeza.
 *
 * A callback não é chamada para operações bem-sucedidas. Em rollback, recebe
 * somente as falhas de liberação; a falha que impediu a criação é retornada por
 * shm_backend_create(). O retorno da callback é void: ela registra ou apresenta
 * o diagnóstico e retorna para que a limpeza prossiga.
 *
 * Recursos globais de ACK não pertencem a um producer_id específico e, por isso,
 * suas falhas de limpeza não são enviadas por esta callback. Elas participam
 * do primeiro erro retornado pela operação de cleanup.
 * @note A callback não deve destruir o backend que já está sendo limpo, nem
 * invalidar o reporter ou seus dados antes que a chamada de limpeza termine.
 * @see @ref shm_cleanup_diagnostics para um exemplo de implementação.
 */
typedef void (*shm_cleanup_error_fn)(const shm_cleanup_error_t *error, void *user_data);

/**
 * @brief Destino opcional dos diagnósticos individuais de cleanup.
 * @details O backend não retém este objeto nem adquire a propriedade de user_data.
 * Ambos devem permanecer válidos durante a operação que recebe o reporter.
 * O campo on_error informa qual função executar; user_data informa onde essa
 * função encontra o estado do chamador, como um buffer de diagnósticos.
 *
 * O reporter vale somente para a chamada que o recebe. Passá-lo a create()
 * habilita os diagnósticos de rollback dessa criação; passe-o novamente a
 * worker_close() ou destroy() para receber os diagnósticos dessas operações.
 *
 * Não há alocação de uma lista de erros pelo backend. Um reporter nulo ou uma
 * callback nula desabilita os diagnósticos, preservando o resultado de retorno.
 * @see @ref shm_cleanup_diagnostics para armazenamento, duração dos dados e resultados.
 */
typedef struct {
    shm_cleanup_error_fn on_error; /**< Callback chamada uma vez para cada falha de liberação. */
    void *user_data;               /**< Dados repassados à callback, pertencentes ao chamador. */
} shm_cleanup_reporter_t;

/**
 * @brief Cria e inicializa um buffer e seu conjunto de semáforos por estrutura lógica.
 * @param[out] out Recebe o backend criado; recebe nullptr em falha, quando o ponteiro é válido.
 * @param producer_count Número positivo de estruturas independentes.
 * @param reporter Destino dos diagnósticos de rollback; pode ser nullptr.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: segmentos de dados e recursos globais de ACK criados e associados,
 *   buffers vazios e semáforos inicializados; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: out nulo ou producer_count zero, com system_errno zero;
 *   ou shmget(), shmat(), semget() ou semctl(SETVAL) rejeitou um argumento;
 *   system_errno preserva o errno capturado nessa chamada.
 * - MPMC_ERR_SHUTDOWN: shmat() ou semctl(SETVAL) retornou EIDRM,
 *   preservado em system_errno.
 * - MPMC_ERR_SYS: falha em malloc(), calloc(), shmget(), shmat(), semget(),
 *   semctl(SETVAL) ou nas operações de inicialização dos recursos de ACK sem
 *   categoria específica; system_errno preserva o errno capturado na operação
 *   que falhou. Erros de rollback são comunicados separadamente, sem substituir a causa.
 * @post Em sucesso, *out contém o backend com producer_count segmentos de dados
 * associados, cada um com buffer vazio, índices e total consumido zerados e
 * capacidade calculada. Cada conjunto de dados contém mutex=1, empty=capacity
 * e full=0. Os recursos globais de ACK também estão prontos para registrar
 * operações, com 64 posições livres.
 * Em falha, *out permanece nullptr quando out é válido, as alocações locais são liberadas
 * e a limpeza dos recursos das entradas visitadas, inclusive a parcial, é tentada.
 * @note Falhas de rollback são comunicadas por reporter e não substituem o erro
 * original de criação ou associação retornado pela função. Sem reporter habilitado,
 * essas falhas secundárias não ficam disponíveis após o retorno.
 */
mpmc_result_t shm_backend_create(
    shm_backend_t **out,
    size_t producer_count,
    const shm_cleanup_reporter_t *reporter
);

/**
 * @brief Insere mensagem no buffer circular do índice escolhido.
 * @param backend Instância criada com sucesso, herdada pelo processo produtor.
 * @param producer_id Índice lógico de destino, no intervalo 1..p.
 * @param[in] message Mensagem copiada integralmente, sem alteração de seus metadados.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem inserida e disponibilizada aos consumidores; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou message nulo, com system_errno zero; ou
 *   semop() rejeitou os argumentos/limites da operação, com errno em system_errno.
 * - MPMC_ERR_INVALID_INDEX: producer_id fora de 1..p, com system_errno zero; ou
 *   semop() retornou EFBIG por índice de semáforo inexistente, preservado em system_errno.
 * - MPMC_ERR_INTERRUPTED: aquisição interrompida em semop(); system_errno contém EINTR.
 * - MPMC_ERR_SHUTDOWN: semop() retornou EIDRM por remoção do conjunto;
 *   system_errno preserva EIDRM.
 * - MPMC_ERR_SYS: outra falha em semop(); system_errno contém o errno capturado.
 * @pre Para backend não nulo e producer_id válido, o segmento está associado,
 * o buffer inicializado e o conjunto de três semáforos disponível.
 * @pre message não nulo aponta para uma mensagem legível durante a chamada.
 * @details Aguarda espaço e exclusão mútua sem manter uma reserva parcial durante
 * a espera. O envio não aguarda consumo nem consulta o registro de ACK; a
 * correlação é feita pela operação de notificação após a retirada da mensagem.
 * @post Em sucesso, a mensagem ocupa a posição FIFO seguinte; o total consumido não muda.
 * @warning Se a aquisição falhar, o buffer não é alterado. Se a publicação posterior
 * falhar, a inserção já ocorreu e não é desfeita; o contexto deve ser considerado
 * inutilizável e o envio não deve ser repetido. Aplicar a política de
 * @ref shm_operation_failures, inclusive para falhas de recursos na aquisição.
 */
mpmc_result_t shm_backend_send(
    shm_backend_t *backend,
    size_t producer_id,
    const message_t *message
);

/**
 * @brief Retira mensagem de um buffer circular.
 * @param backend Instância criada com sucesso, herdada pelo processo consumidor.
 * @param producer_id Índice de origem 1..p.
 * @param[out] out Recebe a mensagem retirada, incluindo todos os metadados originais.
 * @param blocking Se true, aguarda mensagens; se false, não espera itens futuros.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: mensagem retirada, consumo contado e vaga disponibilizada; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou out nulo, com system_errno zero; ou semop()
 *   rejeitou os argumentos/limites da operação, com errno em system_errno.
 * - MPMC_ERR_INVALID_INDEX: producer_id fora de 1..p, com system_errno zero; ou
 *   semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_WOULD_BLOCK: nenhum item disponível com blocking=false;
 *   system_errno contém EAGAIN capturado em semop().
 * - MPMC_ERR_INTERRUPTED: aquisição interrompida em semop(); system_errno contém EINTR.
 * - MPMC_ERR_SHUTDOWN: semop() retornou EIDRM por remoção do conjunto;
 *   system_errno preserva EIDRM.
 * - MPMC_ERR_SYS: outra falha em semop(); system_errno contém o errno capturado.
 * - MPMC_ERR_STATE: tabela ACK indisponível ou inconsistente; system_errno é zero.
 * @pre Para backend não nulo e producer_id válido, o segmento está associado,
 * o buffer inicializado e o conjunto de três semáforos disponível.
 * @pre out não nulo aponta para uma mensagem gravável durante a chamada.
 * @post Em sucesso, out contém a mensagem mais antiga e consumed_total aumenta em um.
 * @note blocking=false aplica IPC_NOWAIT apenas à espera por itens. A chamada ainda
 * pode aguardar a exclusão mútua do buffer.
 * @note Após publicar a vaga, confirma a retirada para a operação produtora.
 * Operações sem registro ACK não geram notificação. A confirmação final é
 * publicada somente quando todas as mensagens do lote forem retiradas.
 * @warning Falhas anteriores à retirada preservam out e o buffer. Se a publicação
 * da vaga falhar depois da retirada, out já contém a mensagem e a contagem já foi
 * incrementada. A função não reinsere o item nem desfaz essa contagem; o contexto
 * deve ser considerado inutilizável e o recebimento não deve ser repetido.
 * Qualquer falha da confirmação após a retirada, inclusive EINTR, também exige
 * encerrar o contexto: a mensagem já foi consumida e não pode ser retirada novamente.
 * Aplicar a política de @ref shm_operation_failures, inclusive para falhas de
 * recursos na aquisição.
 */
mpmc_result_t shm_backend_receive(
    shm_backend_t *backend,
    size_t producer_id,
    message_t *out,
    bool blocking
);

/**
 * @brief Registra uma operação produtora que aguarda confirmações de consumo.
 * @param backend Backend do filho produtor.
 * @param operation_id ID único e não zero de trabalho, gerado pelo supervisor.
 * @param expected Quantidade total de mensagens a confirmar, de 1 a 1024.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: operação registrada; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo, operation_id zero ou expected fora de
 *   1..1024, com system_errno zero; ou argumento rejeitado por semop(), com errno
 *   preservado.
 * - MPMC_ERR_STATE: operation_id já registrado ou controle ACK não inicializado/
 *   inconsistente; system_errno é zero.
 * - MPMC_ERR_INTERRUPTED: aquisição de mutex ou espera por vaga interrompida;
 *   system_errno contém EINTR.
 * - MPMC_ERR_SHUTDOWN: recurso de semáforo removido; system_errno contém EIDRM.
 * - MPMC_ERR_SYS: outra falha em operação de semáforo; system_errno preserva errno.
 * @details Se não houver vaga, aguarda sem manter o mutex. A espera pode retornar
 * INTERRUPTED/EINTR. Ao finalizar uma operação, ack_end devolve uma vaga.
 * Registra operation_id, PID do produtor e quantidade esperada. A tabela
 * é compartilhada entre os processos do backend. A mesma operação
 * pode abranger mensagens destinadas a diferentes estruturas lógicas.
 * @note A política de ACK não é um campo de message_t. O registro precede o
 * primeiro envio; shm_backend_receive() confirma cada retirada efetiva uma vez.
 */
mpmc_result_t shm_backend_ack_begin(
    shm_backend_t *backend,
    uint64_t operation_id,
    unsigned expected
);

/**
 * @brief Aguarda as confirmações de consumo de uma operação registrada.
 * @param backend Backend do filho produtor.
 * @param operation_id Trabalho registrado anteriormente.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: todos os consumos esperados foram confirmados; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo ou operation_id zero; system_errno é zero.
 * - MPMC_ERR_STATE: operation_id não está registrado ou já possui outro waiter
 *   ativo; system_errno é zero.
 * - MPMC_ERR_INTERRUPTED: espera no semáforo interrompida; system_errno contém EINTR.
 * - MPMC_ERR_SHUTDOWN: recurso de semáforo removido; system_errno contém EIDRM.
 * - MPMC_ERR_SYS: outra falha em operação de semáforo; system_errno preserva errno.
 * @details Apenas um processo pode aguardar cada operação. O registro permanece
 * protegido contra reutilização durante a espera. Consulta o contador sob mutex
 * e bloqueia no semáforo específico da operação sem manter o mutex adquirido.
 * Existe uma única notificação final; conclusão anterior à espera retorna imediatamente.
 * @note Notificações anteriores à chamada permanecem contabilizadas e não se perdem.
 * @warning Falhas de semop() diferentes de EINTR na aquisição ou na liberação
 * do mutex, ou na espera pela notificação, podem comprometer o contexto SHM.
 * O chamador deve aplicar @ref shm_operation_failures e não realizar novas
 * operações de dados, estatísticas ou ACK nesse contexto após uma falha fatal.
 */
mpmc_result_t shm_backend_ack_wait(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Encerra e libera o registro de uma operação ACK.
 * @details A eventual notificação final é drenada por ack_begin() antes da reutilização do slot.
 * @param backend Backend do filho produtor.
 * @param operation_id ID da operação previamente registrada.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: registro liberado; também retorna sucesso se a operação for
 *   desconhecida ou já tiver sido encerrada; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend nulo ou operation_id zero; system_errno é zero.
 * - MPMC_ERR_STATE: existe um waiter ativo para a operação; o registro permanece
 *   intacto e pode ser encerrado após o término da espera; system_errno é zero.
 * - MPMC_ERR_INTERRUPTED: operação de semáforo interrompida; system_errno contém EINTR.
 * - MPMC_ERR_SHUTDOWN: recurso de semáforo removido; system_errno contém EIDRM.
 * - MPMC_ERR_SYS: outra falha em operação de semáforo; system_errno preserva errno.
 */
mpmc_result_t shm_backend_ack_end(shm_backend_t *backend, uint64_t operation_id);

/**
 * @brief Obtém total de mensagens consumidas, com sincronização adequada.
 * @param backend Instância no supervisor.
 * @param producer_id Índice lógico 1..p.
 * @param[out] out_count Recebe uma cópia do total acumulado de retiradas deste segmento.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: total lido sob exclusão mútua e escrito em out_count; system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: backend ou out_count nulo, com system_errno zero; ou
 *   semop() rejeitou os argumentos/limites da operação, com errno em system_errno.
 * - MPMC_ERR_INVALID_INDEX: producer_id fora de 1..p, com system_errno zero; ou
 *   semop() retornou EFBIG, preservado em system_errno.
 * - MPMC_ERR_INTERRUPTED: aquisição do mutex interrompida em semop();
 *   system_errno contém EINTR.
 * - MPMC_ERR_SHUTDOWN: semop() retornou EIDRM por remoção do conjunto;
 *   system_errno preserva EIDRM.
 * - MPMC_ERR_SYS: outra falha em semop(); system_errno contém o errno capturado.
 * @pre Para backend não nulo e producer_id válido, o segmento está associado,
 * o buffer inicializado e o conjunto de três semáforos disponível.
 * @pre out_count não nulo aponta para um uint64_t gravável.
 * @post Em erro, out_count permanece inalterado quando válido.
 * @note A leitura pode aguardar o mutex. Seu resultado é uma fotografia do contador;
 * retiradas concorrentes posteriores podem aumentar o total antes do retorno.
 * @note O contador inclui cada retirada executada, mesmo se a liberação posterior
 * da vaga retornar erro. Não é calculado a partir dos ACKs.
 * @warning Uma falha na liberação do mutex torna o contexto inutilizável mesmo
 * preservando out_count. Aplicar a política de @ref shm_operation_failures;
 * não tentar novas leituras após uma falha fatal nesse contexto.
 */
mpmc_result_t shm_backend_consumed_count(
    shm_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
);

/**
 * @brief Copia o total final após todos os workers terem sido recolhidos.
 * @param backend Backend ainda associado no processo proprietário.
 * @param producer_id Estrutura lógica 1..p.
 * @param out_count Destino da contagem final.
 * @pre Não existem workers vivos nem novos despachos neste contexto.
 * @return Estrutura mpmc_result_t com um dos seguintes estados:
 * - MPMC_OK: total copiado; system_errno zero.
 * - MPMC_ERR_INVALID_ARG: backend ou saída nulo; system_errno zero.
 * - MPMC_ERR_INVALID_INDEX: índice fora de 1..p; system_errno zero.
 * @details A quiescência permite ler sem adquirir o mutex que um worker
 * encerrado abruptamente pode ter deixado adquirido.
 */
mpmc_result_t shm_backend_final_count(
    shm_backend_t *backend,
    size_t producer_id,
    uint64_t *out_count
);

/**
 * @brief Desanexa mapeamentos herdados pelo processo filho.
 * @param backend Instância herdada após fork(); nullptr é permitido.
 * @param reporter Destino dos diagnósticos de liberação; pode ser nullptr.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: associações locais desfeitas e estado local liberado, ou backend nulo;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: primeira falha de shmdt() retornou EINVAL, preservado
 *   em system_errno.
 * - MPMC_ERR_SYS: primeira falha de shmdt() tem outra causa; system_errno contém
 *   o errno capturado nessa chamada.
 * @pre Se backend não for nulo, aponta para uma instância válida.
 * @details Tenta todas as desassociações, mesmo após uma falha, e libera o estado
 * local. Os segmentos globais permanecem sob responsabilidade do supervisor.
 * Os conjuntos de semáforos também são preservados. Cada falha é comunicada por
 * reporter, quando habilitado, com o identificador do segmento envolvido.
 * @note Backend nulo não executa liberação nem chama a callback.
 * @post O backend é liberado mesmo em erro e não pode ser reutilizado após o retorno.
 */
mpmc_result_t shm_backend_worker_close(
    shm_backend_t *backend,
    const shm_cleanup_reporter_t *reporter
);

/**
 * @brief Remove objetos IPC e libera estado pertencente ao processo pai.
 * @param backend Instância criada pelo supervisor; nullptr é permitido.
 * @param reporter Destino dos diagnósticos de liberação; pode ser nullptr.
 * @return Estrutura mpmc_result_t com um dos seguintes estados do campo status:
 * - MPMC_OK: desassociações e solicitações de remoção concluídas, ou backend nulo;
 *   system_errno é zero.
 * - MPMC_ERR_INVALID_ARG: primeira falha de shmdt(), semctl(IPC_RMID) ou
 *   shmctl(IPC_RMID) retornou EINVAL, preservado em system_errno.
 * - MPMC_ERR_SHUTDOWN: primeira falha de semctl(IPC_RMID) ou shmctl(IPC_RMID)
 *   retornou EIDRM, preservado em system_errno.
 * - MPMC_ERR_SYS: primeira falha de shmdt(), semctl(IPC_RMID) ou shmctl(IPC_RMID)
 *   tem outra causa; system_errno contém o errno capturado nessa chamada.
 * @pre Se backend não for nulo, suas entradas visitáveis estão preparadas.
 * @pre Todos os trabalhadores foram interrompidos/recolhidos.
 * @details Aceita estado parcialmente inicializado. Tenta todas as desassociações
 * e remoções de conjuntos e segmentos, mesmo após falhas, e libera as alocações locais.
 * Cada falha é comunicada por reporter, quando habilitado; operation identifica
 * se resource_id pertence a um segmento ou a um conjunto de semáforos.
 * @note Backend nulo não executa liberação nem chama a callback.
 * @post O backend é liberado mesmo em erro e não pode ser reutilizado após o retorno.
 */
mpmc_result_t shm_backend_destroy(shm_backend_t *backend, const shm_cleanup_reporter_t *reporter);

/** @} */
#endif
