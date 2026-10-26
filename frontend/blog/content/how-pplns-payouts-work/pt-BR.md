---
title: Como funcionam os pagamentos PPLNS no bntpool.com
description: O bntpool.com paga mineradores de Blocknet com PPLNS em uma janela de seis horas. Entenda a divisão das recompensas, as 60 confirmações e o mínimo de 0,1 BNT.
tags: [Pagamentos, PPLNS, bntpool, Guia de mineração]
sourceHash: 'e00112e9e82e'
translation: draft
faq:
  - q: 'Qual é a duração da janela PPLNS no bntpool.com?'
    a: 'São seis horas. Ao encontrar um bloco, o pool considera as seis horas anteriores e divide a recompensa entre quem enviou shares nesse período, proporcionalmente à dificuldade deles. O valor atual está na página Get Started do pool.'
  - q: 'Qual é o pagamento mínimo no bntpool.com?'
    a: 'O mínimo é 0,1 BNT. Quando seu saldo confirmado chega a esse valor, o pool o envia automaticamente ao seu endereço Blocknet, sem solicitação ou resgate. Um saldo menor permanece na conta e cresce a cada bloco confirmado do qual você participou até atingir o limite.'
  - q: 'Por que Estimated Rewards difere de Confirmed Rewards?'
    a: 'Estimated Rewards é sua parte dos blocos encontrados pelo pool que ainda estão acumulando 60 confirmações, portanto o valor pode mudar. Confirmed Rewards vem de blocos com profundidade de 60 blocos e aguarda pagamento. Conforme cada bloco amadurece, o valor passa do primeiro cartão para o segundo.'
  - q: 'Minhas recompensas param assim que interrompo a mineração?'
    a: 'Não. Cada share permanece na janela PPLNS por seis horas após o envio, então você ainda recebe créditos pelos blocos encontrados nesse intervalo. Recompensas já confirmadas continuam no saldo e são pagas quando ele atinge o mínimo de 0,1 BNT.'
  - q: 'O que significa um esforço acima de 100% na página Luck?'
    a: 'O esforço compara o trabalho feito pelo pool na rodada com a dificuldade do bloco, que representa o trabalho esperado. Acima de 100%, a rodada demorou mais que a média; abaixo disso, foi mais rápida. Ambos são normais. Em muitas rodadas, o esforço médio fica perto de 100%.'
---

Este guia explica como o bntpool.com transforma os shares enviados pelo seu minerador em pagamentos de Blocknet. Você entenderá como a recompensa de um bloco é dividida, por que espera cerca de cinco horas antes de poder ser paga, quando seu saldo é enviado e como ler os cartões de saldo na [página de estatísticas](/stats) e a [página Luck](/luck).

Se você está começando, [o que é um pool de mineração?](/blog/pt-br/what-is-a-mining-pool/) apresenta o básico. Se ainda não minera, o [guia do seine](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) leva do download aos primeiros shares em poucos minutos.

## O que é PPLNS?

<Answer>
PPLNS significa "pay per last N shares", pagamento pelos últimos N shares. Quando o bntpool.com encontra um bloco, divide a recompensa entre os shares enviados nas seis horas anteriores, proporcionalmente à dificuldade de cada um. Quanto mais trabalho contínuo você contribui nessa janela, maior sua parte de cada bloco.
</Answer>

No bntpool.com, os "últimos N" são um intervalo de tempo: seis horas. A página [Get Started](/start) mostra sistema de pagamento, janela, mínimo e confirmações. Os mesmos valores são publicados em `https://bntpool.com/api/info`.

| Configuração | Valor no bntpool.com |
|---|---|
| Sistema de pagamento | PPLNS |
| Janela de shares | 6 horas |
| Confirmações antes do pagamento | 60 blocos |
| Pagamento mínimo | 0,1 BNT |

Os shares são ponderados pela dificuldade. Cada conexão recebe sua própria dificuldade, ajustada para que todo minerador envie um fluxo regular de shares. Máquinas mais rápidas recebem shares mais difíceis, com maior peso individual. O que define sua parte é o trabalho total realizado, uma medida justa tanto para um notebook quanto para um servidor.

## Como o bntpool.com divide a recompensa de um bloco?

Sempre que encontra um bloco, o pool segue estas etapas:

1. **Registra o bloco.** Ele aparece na [página de blocos](/blocks) como pendente.
2. **Mede a janela.** O pool soma a dificuldade de todos os shares aceitos de cada endereço nas seis horas que terminam no bloco.
3. **Divide a recompensa.** Cada endereço recebe a mesma fração da recompensa que sua fração do trabalho total da janela.
4. **Aguarda a maturidade.** Sua parte aparece como estimativa até o bloco atingir 60 blocos de profundidade, quando passa ao saldo confirmado.
5. **Paga o saldo.** Ao atingir 0,1 BNT confirmado, o pool envia o saldo ao seu endereço.

## Um exemplo calculado

<Note>
Este exemplo usa números redondos fictícios para mostrar a conta. Os totais reais dependem da hashrate do pool, da sua hashrate e da [recompensa do bloco](/blog/pt-br/blocknet-block-reward-schedule/) naquele momento.
</Note>

Suponha que, nas seis horas anteriores a um bloco, todos os mineradores do pool enviaram shares com dificuldade total de 1.000.000, e os seus somam 20.000. Sua fração é 20.000 ÷ 1.000.000 = 2%. Se o pool distribuir 400 BNT, você recebe 400 × 0,02 = 8 BNT.

Agora imagine outro minerador com exatamente sua hashrate que conectou 30 minutos antes do bloco. Isso é um doze avos de seis horas: ele tem cerca de 1.667 de dificuldade na janela e recebe aproximadamente 0,67 BNT desse bloco. O trabalho dele continua contando: cada share permanece na janela pelas seis horas seguintes e ganha uma parte de todo bloco encontrado nesse período.

| Minerador (exemplo) | Trabalho na janela | Fração | Crédito de um pagamento de 400 BNT |
|---|---|---|---|
| Você, minerando as seis horas inteiras | 20.000 | 2% | 8 BNT |
| Mesma hashrate, conectado 30 minutos antes do bloco | 1.667 | cerca de 0,17% | cerca de 0,67 BNT |
| Minerador pequeno, minerando as seis horas inteiras | 100 | 0,01% | 0,04 BNT |

O minerador pequeno da última linha ganha 0,04 BNT por bloco. Após três blocos assim, seu saldo confirmado ultrapassa o mínimo de 0,1 BNT e é pago.

## Por que o PPLNS recompensa a mineração contínua?

Cada share enviado conta para todo bloco que o pool encontra nas seis horas seguintes. Um minerador que funciona sem parar sempre tem uma janela cheia de trabalho anterior, então cada bloco novo o remunera pela taxa completa. A renda acompanha de perto sua hashrate de bloco a bloco.

Isso também torna o PPLNS tolerante a interrupções. Reiniciar, atualizar um driver ou perder a rede brevemente não zera sua posição, pois os shares já enviados permanecem por seis horas. Trabalho acrescentado depois de um bloco conta para os próximos; hashrate extra sempre é aproveitada.

Para resultados mais estáveis, deixe o minerador ligado e permita que ele se ajuste sozinho. O seine continua ajustando threads e kernel da CPU enquanto minera, sem reconectar ao pool. Assim, o fluxo de shares permanece contínuo. Você pode [baixar o seine](https://seineminer.com/pt-br/#download) para Windows, macOS ou Linux.

## Por que a recompensa espera 60 confirmações?

No Blocknet, moedas recém-mineradas precisam esperar 60 blocos antes de serem gastas. As regras do daemon chamam isso de maturidade da coinbase: cerca de cinco horas com blocos de cinco minutos (60 × 5 minutos = 300 minutos). O bntpool.com usa os mesmos 60 blocos antes de considerar a recompensa confirmada, garantindo que cada pagamento use moedas que o pool pode enviar.

Durante a maturação, o pool confere se o bloco ainda pertence à cadeia principal. Ao atingir 60 blocos de profundidade, ele é marcado como confirmado e a recompensa é adicionada ao saldo confirmado de cada minerador. No caso raro de a rede escolher outro bloco naquela altura, o bloco é marcado como órfão e as recompensas estimadas são removidas.

## Quando chegam os pagamentos do bntpool.com?

<Answer>
Após 60 confirmações, cerca de cinco horas com blocos de cinco minutos, sua parte passa para Confirmed Rewards na página de estatísticas. Quando o saldo confirmado chega a 0,1 BNT, o bntpool.com o envia automaticamente ao seu endereço Blocknet. Não é preciso solicitar nem resgatar.
</Answer>

Para um minerador novo, a primeira estimativa aparece quando o pool encontra um bloco enquanto seus shares estão na janela. O primeiro pagamento vem após a maturação desse bloco e o saldo atingir o mínimo. Pagamentos enviados aparecem na [página de pagamentos](/payouts) e no seu histórico pessoal na página de estatísticas.

## Como as recompensas provisórias são exibidas?

Consulte seu endereço ou `@name` na [página de estatísticas](/stats) para ver três cartões de saldo:

- **Estimated Rewards:** sua parte dos blocos ainda não maduros. São valores provisórios, sujeitos a mudança até cada bloco ser confirmado ou ficar órfão.
- **Confirmed Rewards:** saldo maduro aguardando pagamento.
- **Paid Balance:** tudo que já foi enviado ao seu endereço.

O pool separa recompensas provisórias para que o saldo confirmado contenha apenas valores finais. Abaixo dos cartões, cada bloco não confirmado aparece com sua parte estimada, permitindo acompanhar a aproximação da confirmação. O [guia de acompanhamento do minerador](/blog/pt-br/track-your-miner-on-bntpool/) explica o restante da página.

## O que as páginas de sorte e esforço mostram?

O esforço mede o trabalho do pool para encontrar um bloco: trabalho aceito na rodada dividido pela dificuldade do bloco, em porcentagem. A dificuldade do Blocknet equivale ao número esperado de hashes por bloco. Assim, 100% de esforço significa exatamente o trabalho médio esperado. Abaixo de 100%, o pool encontrou o bloco mais rápido que a média; acima disso, demorou mais.

A [página Luck](/luck) lista cada rodada com altura, esforço, duração, status e horário do bloco. O [painel do pool](/) mostra o esforço crescente da rodada atual e a sorte média. Rodadas individuais oscilam acima e abaixo de 100%, como em qualquer pool de prova de trabalho. Em muitas rodadas, o esforço médio se aproxima de 100%.

A sorte afeta a frequência dos blocos. Sua fração de cada bloco depende apenas da sua parte do trabalho na janela. Minerar continuamente mantém essa fração completa, seja a rodada curta ou longa.

<CTA target="pool" title="Entre no pool" text="Mine continuamente no bntpool.com. Sua parte de cada bloco é paga automaticamente quando você atinge 0,1 BNT." />
