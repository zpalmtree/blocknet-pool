---
title: 'Pagamento enviado, mas ausente da carteira?'
description: 'Acompanhe um pagamento de Blocknet pela transação, endereço de recebimento e sincronização da carteira e reúna os dados para esclarecer o registro ausente.'
tags: ['Pagamentos', 'Carteira', 'Solução de problemas']
faq:
  - q: 'Um pagamento na fila significa que a carteira já recebeu?'
    a: 'Na fila é um estado de processamento do pool. Confira primeiro se o registro possui identificador de transação e qual é o estado atual. O registro do pagamento, a transação chegar à cadeia e a carteira reconhecer sua saída são etapas separadas. Acompanhe a transação específica quando estiver disponível.'
  - q: 'O explorador mostra o saldo da minha carteira privada?'
    a: 'O explorador permite verificar o estado público de uma transação na cadeia. Blocknet usa saídas privadas; uma busca comum por endereço não exibe o saldo da carteira nem comprova que determinada saída é sua. A carteira escaneia a cadeia para reconhecer as próprias saídas.'
  - q: 'O que informar sobre um pagamento que não aparece?'
    a: 'Forneça endereço completo usado na consulta do pool, identificador da transação, horário aproximado, estado do pool e progresso da sincronização. Diga se a transação aparece no histórico. Compartilhe dados de conta somente no contexto de suporte apropriado, nunca frase de recuperação, chaves privadas ou senha da carteira.'
sourceHash: '915075cf4e2b'
translation: draft
---

Acompanhe um pagamento de Blocknet do registro do pool até a carteira destinatária. Este guia começa quando você identifica uma entrada de pagamento no bntpool.com. Se vê apenas recompensas estimadas ou confirmadas aguardando pagamento, consulte primeiro o [guia PPLNS](/blog/pt-br/how-pplns-payouts-work/): essa é uma etapa anterior com regras próprias de contabilização e pagamento.

## Como rastrear um pagamento ausente da carteira?

<Answer>
Encontre o registro para o endereço completo, anote identificador da transação e estado atual e confira a transação na cadeia. Abra a carteira do mesmo destino e deixe a sincronização e o escaneamento alcançarem a cadeia atual. Compare o histórico antes de decidir se o pagamento ou a visão local precisa de atenção.
</Answer>

*Um registro de pagamento conecta-se à transação na cadeia e depois à carteira que escaneia suas próprias saídas recebidas.*

## Comece por um registro específico

Abra My Stats e busque o endereço completo usado quando as recompensas foram obtidas. Localize o pagamento e anote valor, horário, estado e identificador, se existir. Use essa entrada em vez do total histórico pago, que pode reunir vários pagamentos. O [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/) mostra onde ficam os campos.

A página pública Payouts descreve lotes com possíveis múltiplos destinatários e links de transações. Mantenha o registro do seu endereço ao lado da transação investigada. O total do lote não é o valor atribuído à sua carteira. Quando houver vários identificadores, expanda a lista e guarde os relevantes, em vez de copiar apenas um rótulo abreviado.

## O pagamento está na fila, não confirmado ou confirmado?

Leia o estado atual antes de assumir que uma entrada completou todas as etapas. Um pagamento na fila ainda está no processamento do pool; uma transação não confirmada precisa de confirmação na cadeia. Atualize o registro e abra o link quando existir. A [página de status](/status) oferece contexto operacional se várias observações indicam uma interrupção.

Separe a maturidade da recompensa de mineração da confirmação da transação de pagamento. O bloco que gerou a recompensa e a transação posterior têm identificadores e funções diferentes. O [guia de confirmações](/blog/pt-br/block-confirmations-mining-rewards/) explica isso. Um identificador é um objeto concreto para investigar, mas não comprova que a carteira local já escaneou e reconheceu o pagamento.

## Confira a transação na cadeia

Abra o explorador pelo link do pagamento e compare o identificador completo com suas anotações. Leia o estado na cadeia e as informações do bloco disponíveis. Se ainda não for encontrada, compare o registro atualizado e o estado do serviço antes de concluir. Um resultado temporário de busca sozinho não identifica a causa.

A privacidade do Blocknet significa que uma busca comum por endereço no explorador não exibe saldo público do destinatário. O estado da transação e a verificação de propriedade da carteira respondem perguntas diferentes. A carteira reconhece entradas ao escanear a cadeia. O [guia de endereços furtivos](https://seineminer.com/pt-br/blog/stealth-addresses-explained/) explica o processo; uma lista pública não reproduz o histórico privado da carteira.

## Você está olhando a carteira destinatária correta?

Compare o endereço consultado no pool com o endereço de recebimento da carteira aberta. Use valores completos; prefixos abreviados semelhantes não bastam. Se minera com Blocknet ID, mantenha o endereço completo do trabalho original. Uma consulta atual ao nome pode apontar para outro destino após uma alteração, enquanto o pagamento anterior mantém o destino original.

Confira carteiras antigas usadas antes de mudar a configuração. Atualizar o endereço do minerador ou o mapeamento do nome não move um pagamento já atribuído. O [guia de mudança de endereço](/blog/pt-br/change-mining-wallet-address/) explica como acompanhar ambos os históricos. Preserve a carteira original e o backup e diferencie abrir uma carteira existente de criar outra com endereço diferente.

## Deixe a carteira sincronizar e escanear

Confirme que o nó da carteira avança até a cadeia atual e que o escaneamento alcançou o bloco da transação. Sincronização do nó e escaneamento da carteira são progressos separados. Uma carteira recém-aberta ou restaurada pode precisar de tempo para descobrir saídas mesmo quando o explorador já mostra a transação em um bloco.

Mantenha a carteira conectada e siga o fluxo documentado de sincronização. Compare histórico de recebimentos e saldo principal, pois gastos posteriores alteram o total. Se houver uma entrada, confira confirmação ou disponibilidade. Faça essas observações antes de recuperação ou escaneamento completo e preserve backups antes de manutenção. O [guia da carteira](https://seineminer.com/pt-br/blog/blocknet-wallet-for-mining/) cobre acesso e backups.

## Quais dados ajudam a esclarecer uma diferença restante?

Anote endereço completo do pool, identificador da transação, horário aproximado e estado atual do pool. Inclua versão da carteira, progresso do nó e do escaneamento e se a transação recebida aparece no histórico. Descreva as verificações feitas. Isso distingue estado da transação, endereço divergente e carteira ainda se atualizando.

Use o suporte oficial do projeto e compartilhe dados de conta apenas onde apropriado. A investigação não exige frase de recuperação, chaves privadas ou senha. Revise arquivos de diagnóstico antes de publicá-los inteiros. Termine relacionando o mesmo registro, transação e carteira receptora; hashrate recente ou outro reinício do minerador não comprova recebimento de um pagamento anterior.

<CTA target="pool" title="Acompanhe seus pagamentos no bntpool.com" text="Use seu endereço completo para localizar o pagamento e comparar a transação com o histórico da carteira sincronizada." />
