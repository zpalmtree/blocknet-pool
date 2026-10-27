---
title: 'Dá para minerar Blocknet em tempo parcial?'
description: 'Minere Blocknet em sessões curtas: acompanhe shares na janela PPLNS de seis horas do bntpool, volte com o mesmo endereço e confira saldos não pagos.'
tags: ['Guia de mineração', 'PPLNS', 'Horários']
faq:
  - q: 'Preciso minerar seis horas para minhas shares contarem?'
    a: 'Não. Shares elegíveis podem participar da janela de recompensa de um bloco sem esperar seis horas. Sua parte depende do trabalho ponderado pela dificuldade dentro da janela em relação ao total do pool, e um bloco precisa ser encontrado para distribuir uma recompensa de bloco.'
  - q: 'Meu minerador precisa ficar conectado para receber um pagamento existente?'
    a: 'Uma conexão ativa de mineração não é requisito de pagamento. Saldos confirmados seguem as regras de pagamento do pool, incluindo o limite mínimo atual. Desconectar, por si só, não apaga o saldo nem solicita pagamento imediato.'
  - q: 'Reconectar reinicia a idade das shares antigas?'
    a: 'Não. Shares existentes preservam seus horários. Novo trabalho aceito entra nas janelas relevantes após seu retorno; reconectar não renova shares antigas nem adiciona trabalho pelas horas offline. Verifique novas shares aceitas após reiniciar.'
sourceHash: '0a11c393062e'
translation: draft
---

Você pode executar um minerador de Blocknet em horários escolhidos e voltar depois. Vale distinguir a conexão atual, as shares já enviadas e as recompensas já distribuídas. Este guia acompanha uma sessão curta por essas etapas para você parar, reiniciar e consultar sua conta no bntpool.com com expectativas claras.

## Uma sessão curta pode receber recompensas do pool?

<Answer>
Sim. Trabalho elegível de uma sessão curta pode participar de blocos cujas janelas PPLNS incluam essas shares. Não é preciso completar seis horas antes de contribuir. As recompensas dependem de o pool encontrar blocos e da sua participação ponderada pela dificuldade em cada janela. Parar não acrescenta trabalho; shares existentes envelhecem normalmente e saldos existentes seguem as regras de pagamento.
</Answer>

*Uma linha do tempo mostra uma sessão de mineração, um intervalo offline e o retorno, com marcas separadas para a elegibilidade das shares e o saldo existente.*

## Entenda qual relógio está acompanhando

O bntpool publica atualmente, na [página de primeiros passos](/start), uma janela PPLNS de seis horas, 60 blocos antes da elegibilidade de pagamento e pagamento mínimo de 0,1 BNT. São três etapas diferentes: trabalho considerado para um bloco, maturação desse bloco e limite de saldo. Uma janela de shares de seis horas não é um cronômetro que promete pagamento em seis horas.

Para a fórmula de distribuição, consulte o [guia de PPLNS](/blog/pt-br/how-pplns-payouts-work/). Suas shares elegíveis são ponderadas pela dificuldade em relação a todo o trabalho elegível da mesma janela. Só os minutos online não fixam uma porcentagem da próxima recompensa. Dispositivos, participação no pool e resultados das shares podem mudar os pesos mesmo com sessões de igual duração.

## Acompanhe uma share durante a pausa

Considere um horário local ilustrativo: você minera das 18:00 às 20:00 e uma share elegível é registrada às 19:00. Um bloco do pool cuja janela termina às 22:00 olha seis horas para trás, até 16:00; a share está no intervalo mesmo com o minerador agora offline. Isso ilustra elegibilidade, não prevê que o pool encontrará um bloco naquele horário.

Para uma janela que termina às 02:00 do dia seguinte, o início é 20:00, então a share das 19:00 fica de fora. Reconectar às 08:00 não muda seu horário. Trabalho novo passa a contribuir para janelas posteriores após seu retorno. O [guia de sorte do pool](/blog/pt-br/mining-pool-luck-explained/) explica por que os tempos dos blocos variam; não escolha o horário supondo que um bloco está prestes a chegar.

## Pare deixando um registro reconhecível

Antes de uma pausa planejada, anote endereço de recebimento, nome do rig, horário aproximado de parada e últimas mensagens de shares aceitas. Consulte a conta correspondente pelo [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/). Isso cria uma referência para a próxima sessão e ajuda a distinguir um rig parado de outro endereço ou de uma atualização atrasada.

Encerre o seine normalmente com `q` no painel ou `Ctrl+C` no terminal. Guarde o comando de inicialização e a configuração que funcionam para a próxima execução. O [guia de uso do PC](https://seineminer.com/pt-br/blog/mine-while-using-pc/) trata da mineração junto a outras tarefas; parar corretamente não exige apagar configurações, trocar o endereço da carteira ou pedir ao pool que feche uma conta.

## O que acontece com o saldo não pago enquanto você está offline?

Quando uma share sai da janela de um bloco posterior, isso não apaga uma recompensa já distribuída para um bloco anterior. Recompensas estimadas ainda seguem o estado e a maturação do bloco; recompensas confirmadas não pagas ficam na conta até o processamento do pagamento. O [guia de confirmações](/blog/pt-br/block-confirmations-mining-rewards/) diferencia crédito provisório de bloco e saldo maduro.

Um saldo confirmado abaixo de 0,1 BNT continua abaixo do limite automático atual até que novos créditos atinjam o mínimo. Reconectar sozinho não aumenta o saldo nem força um pagamento. Por outro lado, um saldo elegível não exige que o minerador fique online enquanto o pool o processa. Use o [histórico de pagamentos](/payouts) para separar saldo não pago de transação enviada.

## Volte à mesma conta e confira trabalho novo

Reinicie com o comando salvo e confirme pool, endereço de recebimento e nome do rig antes de comparar estatísticas. O endereço identifica a conta cujo saldo você acompanhava. Se escolher outro destino de propósito, siga o [guia de mudança de endereço](/blog/pt-br/change-mining-wallet-address/); o saldo antigo não é transferido automaticamente para a conta nova.

Procure um trabalho atual e novas shares aceitas, depois dê tempo para a janela de medição exibida pelo pool atualizar. Um reinício recente pode aparecer de modo diferente na média da sessão do minerador e na estimativa móvel do pool. O [guia de hashrate](/blog/pt-br/what-is-hashrate/) explica essas janelas. Verifique o trabalho real e a conta correta antes de interpretar uma diferença breve como recompensa perdida.

## Planeje as sessões conforme sua rotina

Escolha os horários disponíveis conforme o uso do computador e o orçamento de eletricidade. Use o [guia de energia](https://seineminer.com/pt-br/blog/mining-electricity-use/) para calcular o consumo no horário real, incluindo intervalos de tarifa diferentes quando necessário. Uma sessão menor reduz o tempo de operação, mas sua energia exata ainda depende dos equipamentos e da carga medidos.

Mantenha um pequeno registro de inícios, paradas, trabalho aceito e saldos em várias sessões. A [calculadora de recompensas](/blog/pt-br/estimate-blocknet-mining-rewards/) fornece uma expectativa sob suas premissas, não uma promessa para uma noite específica. Avalie a rotina em um período representativo e observe separadamente conexão atual, elegibilidade na janela de shares, maturidade dos blocos e pagamentos enviados.

<CTA target="pool" title="Acompanhe suas sessões no bntpool.com" text="Volte com o mesmo endereço de recebimento, verifique shares novas e acompanhe os saldos pelas etapas normais de pagamento." />
