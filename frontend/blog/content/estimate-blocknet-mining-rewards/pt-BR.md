---
title: "Calculadora de mineração Blocknet"
description: "Estime BNT por dia e semana com seu hashrate, dados da rede ao vivo e o cronograma de recompensas, com as taxas mostradas separadamente."
tags: [Recompensas de mineração, Guia de mineração, Hashrate, bntpool]
sourceHash: c8369c1db7c0
translation: draft
faq:
  - q: 'Como calcular as recompensas de mineração de Blocknet?'
    a: 'Divida sua hashrate pela da rede, multiplique por 288, o número diário de blocos, e pela recompensa atual. O resultado é o BNT bruto esperado por dia. Consulte a hashrate da rede no painel do bntpool.com e a sua no seine ou em My Stats.'
  - q: 'Onde vejo a hashrate da rede Blocknet?'
    a: 'O cartão Network Hashrate do painel do bntpool.com a mostra, geralmente em KH/s. Multiplique por 1.000 para obter H/s. O mesmo valor em H/s está no campo network_hashrate da API bntpool.com/api/stats. Ele é medido sobre os dez blocos mais recentes e atualizado a cada bloco.'
  - q: 'A recompensa de bloco muda com o tempo?'
    a: 'Sim. Ela é fixa dentro de cada mês de 30 dias, ou 8.640 blocos, e cai no início do mês seguinte até chegar a 2,0 BNT permanentes por bloco no mês 48. Divida a altura por 8.640, arredonde para baixo e procure o mês no cronograma de recompensas.'
  - q: 'Por que meus ganhos diários no pool diferem da estimativa?'
    a: 'A estimativa é uma média. Em um dia, o pool pode encontrar mais ou menos blocos que o esperado, e a hashrate da rede muda com novos mineradores. Em uma semana ou mais de mineração contínua, as oscilações se compensam. Acompanhe as recompensas creditadas em My Stats.'
  - q: 'Minerar em pool muda quanto BNT posso esperar?'
    a: 'Sua fração esperada das recompensas da rede permanece igual, pois depende apenas da sua participação na hashrate. O pool muda o momento de recebimento: em vez de esperar dias por um bloco inteiro, você recebe uma parte de cada bloco do bntpool.com, em valores pequenos e regulares.'
---

<RewardsCalculator />

Este guia mostra como estimar quanto BNT seu hardware minera por dia. Você aprenderá a fórmula, onde ler cada dado no [painel do bntpool.com](/) e como calcular um exemplo datado com dados reais do pool e as melhores hashrates medidas pelo seine.

## Quanto BNT seu hardware minera por dia?

<Answer>
O BNT diário esperado é sua hashrate dividida pela hashrate da rede, multiplicada por 288 blocos por dia e pela recompensa atual. Sua hashrate vem do seine ou de My Stats; a da rede, do painel do bntpool.com; a recompensa, da altura atual e do cronograma do Blocknet.
</Answer>

Em fórmula:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Cada parte tem uma razão simples:

- **288 blocos por dia.** O Blocknet busca um bloco a cada cinco minutos. 86.400 segundos diários divididos por 300 segundos por bloco dão 288. A dificuldade se ajusta após cada bloco, mantendo a cadeia perto desse ritmo conforme entram mineradores.
- **Sua fração da hashrate é sua fração dos blocos.** A dificuldade é aproximadamente o número esperado de hashes para encontrar um bloco. Cada hash tem a mesma chance, então uma máquina que faz 1% dos hashes da rede pode esperar 1% dos blocos no longo prazo.
- **A recompensa depende da altura.** Todo bloco no mesmo mês de 30 dias, ou 8.640 blocos, paga igual. Dentro desse mês, a recompensa diária é uma constante conhecida.

O resultado é sua recompensa bruta esperada, em média ao longo do tempo.

## Onde encontrar os dados?

### Sua hashrate

O seine mostra a hashrate no painel do terminal desde o início. Dê alguns minutos: ele ajusta threads e kernel da CPU com mineração real, e o valor se estabiliza após encontrar as melhores configurações.

Depois de começar no pool, o cartão **Hashrate** em [My Stats](/stats) mostra a visão do pool, calculada pela última hora. Para estimar, use a taxa estável do seine ou o gráfico de 24 horas do pool.

### Hashrate da rede

O cartão **Network Hashrate** mostra a taxa da rede inteira em KH/s. Uma leitura de 62,33 KH/s significa 62.330 H/s. Converta para H/s antes de dividir; caso contrário, o resultado estará errado por um fator de mil.

O pool mede o valor pelo trabalho e timestamps dos dez blocos mais recentes, então ele varia a cada bloco. [O que é hashrate?](/blog/pt-br/what-is-hashrate/) explica o cálculo e as oscilações. Para um dado mais estável, consulte algumas vezes no dia e use a média. Você também pode obter H/s pela API:

```sh
curl -s https://bntpool.com/api/stats
```

A seção `chain` contém `network_hashrate` em H/s e `current_job_height`, a altura em que o pool está trabalhando.

### Recompensa do bloco

O cartão **Current Block** mostra a altura atual da rede. Divida por 8.640, arredonde para baixo e procure esse mês no [cronograma de recompensas do Blocknet](/blog/pt-br/blocknet-block-reward-schedule/). A [página de blocos](/blocks) permite conferir rapidamente: a coluna Reward mostra a recompensa completa de cada bloco encontrado pelo pool.

## Um exemplo de 25 de outubro de 2026

<Note>
Este exemplo usa dados de https://bntpool.com/api/stats lidos em 25 de outubro de 2026 às 02:03 EDT. A hashrate da rede e a recompensa mudam. Use os valores atuais do painel antes de se apoiar no resultado.
</Note>

| Dado | Valor no momento da leitura |
|---|---|
| Bloco atual | 69.020 (mês 7, pois 69.020 ÷ 8.640 = 7,99) |
| Recompensa do bloco | 467,67 BNT |
| Hashrate da rede | 62.329 H/s (exibida como 62,33 KH/s) |
| Hashrate do bntpool.com | 5.240 H/s |

Considere o servidor AMD EPYC Zen 5 dos benchmarks do seine, com 49,4 H/s:

1. Fração da rede: 49,4 ÷ 62.329 = 0,0793%.
2. Blocos diários esperados: 0,000793 × 288 = 0,2283.
3. BNT diários esperados: 0,2283 × 467,67 = cerca de 106,8 BNT.

O mês 8 começa na altura 69.120, cerca de 100 blocos depois da leitura, pagando 439,46 BNT por bloco. Com a mesma hashrate de rede, a estimativa desse servidor passa a 0,2283 × 439,46 = aproximadamente 100,3 BNT por dia. Por isso vale conferir a recompensa a cada novo mês.

Veja o mesmo cálculo, com a hashrate de rede da leitura, para o melhor resultado de cada geração medido pelo seine:

| Hardware | Threads | Hashrate | Fração da rede | BNT/dia a 467,67 | BNT/dia a 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (MacBook Pro de 48 GB) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Servidor Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

O que importa é o valor da sua máquina. Execute o seine por algum tempo e use sua taxa estável no lugar da tabela. O guia de [hardware e hashrate de Blocknet](https://seineminer.com/pt-br/blog/blocknet-mining-hardware-hashrate/) explica como RAM e largura de banda definem esses resultados. A lista completa está na [página de desempenho do seine](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## Como minerar no bntpool.com muda a estimativa?

Sua participação esperada permanece igual. Muda a chegada da recompensa. O bntpool.com usa PPLNS: cada recompensa é dividida entre os shares enviados nas seis horas anteriores, proporcionalmente à dificuldade. [Como funcionam os pagamentos PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica a divisão.

A conta pela perspectiva do pool dá a mesma resposta. Na leitura, os 5.240 H/s do pool eram 8,4% da rede. Ele esperava 288 × 5.240 ÷ 62.329 = aproximadamente 24,2 blocos diários, um por hora. O painel mostra isso como **Expected Block Time** em Round Progress. Um servidor de 49,4 H/s representa 0,94% do pool. 0,94% de 24,2 blocos a 467,67 BNT novamente dá cerca de 106,8 BNT por dia. A hashrate do pool se cancela na conta.

A diferença é o momento. Solo, esse servidor Zen 5 esperaria um bloco inteiro a cada aproximadamente 4,4 dias. [Pool ou solo para Blocknet](/blog/pt-br/pool-vs-solo-mining-blocknet/) detalha a comparação. No bntpool.com, ele recebe parte de cada um dos cerca de 24 blocos diários do pool, crescendo o saldo em pequenos passos o dia inteiro. A taxa do pool e a taxa de desenvolvimento do seine são descontadas do valor bruto da fórmula.

Dois detalhes afetam seu primeiro dia:

- **A janela enche em seis horas.** A parte de um novo minerador cresce enquanto seus shares preenchem a janela PPLNS. Depois mantém a taxa completa enquanto ele continuar minerando.
- **Recompensas amadurecem antes do pagamento.** Após 60 confirmações, cerca de cinco horas, a recompensa vai para Confirmed Rewards. O pool envia automaticamente o saldo quando chega a 0,1 BNT.

## Como comparar a estimativa com resultados reais?

A estimativa é uma média. Cada dia oscila conforme a sorte do pool varia entre rodadas. Em muitas rodadas, o esforço médio se aproxima de 100%, então uma semana contínua é um bom período para comparar. Anote a soma de Confirmed Rewards e Paid Balance em [My Stats](/stats) no começo e no fim da semana. O aumento é o crédito de blocos que amadureceram naquele período. Compare-o com a estimativa dos mesmos dias, usando as recompensas e hashrates da rede desses dias.

Refaça a fórmula quando algum dado mudar:

- **A cada novo mês**, quando a recompensa cai. As transições ocorrem a cada 8.640 blocos, cerca de 30 dias.
- **Quando mudar a hashrate da rede**, conforme entram mineradores.
- **Quando mudar sua hashrate**, após adicionar uma máquina, reservar huge pages no Linux ou acrescentar uma GPU NVIDIA, que o seine usa junto com a CPU.

<Note>
Estas hashrates descrevem a prova de trabalho Argon2id atual. Segundo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), o Blocknet planeja a atualização Faraday para cerca de 16 de fevereiro de 2027, com mudança na prova de trabalho. Mantenha o seine atualizado: ele verifica novas versões ao iniciar e a cada 12 horas.
</Note>

<CTA target="seine" title="Meça sua própria hashrate" text="Baixe o seine, deixe-o se ajustar por alguns minutos e use sua hashrate na fórmula com os valores atuais do painel." />
