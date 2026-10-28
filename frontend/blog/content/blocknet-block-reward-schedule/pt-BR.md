---
title: Cronograma de recompensas de bloco do Blocknet
description: A recompensa mensal do Blocknet vai de 723,25 BNT no lançamento a 2,0 BNT permanentes no mês 48, com 288 blocos diários e 98,6 milhões de BNT antes da emissão residual.
tags: [Blocknet, Recompensa do bloco, Emissão, Mineração]
sourceHash: 6c38930dfa09
translation: draft
faq:
  - q: 'Qual é a recompensa atual por bloco do Blocknet?'
    a: 'Depende do mês da cadeia, pois a recompensa cai a cada 8.640 blocos. Em 25 de outubro de 2026, a cadeia estava no mês 7, pagando 467,67 BNT por bloco. O mês 8 começa na altura 69.120 com 439,46 BNT. Consulte a altura atual no painel do bntpool.com e localize-a na tabela.'
  - q: 'Quantos blocos de Blocknet são minerados por dia?'
    a: 'O alvo é um bloco a cada cinco minutos, ou 288 por dia e 8.640 em cada mês de 30 dias. A dificuldade se ajusta a cada bloco pela média ponderada dos últimos 60 tempos de bloco, mantendo a média perto de cinco minutos conforme mineradores entram ou saem.'
  - q: 'Qual é a oferta total de Blocknet?'
    a: 'Cerca de 98,56 milhões de BNT são minerados antes da emissão residual no mês 48. O código e o site oficial arredondam para aproximadamente 100 milhões. Depois, cada bloco paga 2,0 BNT para sempre: mais 576 BNT por dia, ou cerca de 210.240 por ano. A oferta continua crescendo lentamente.'
  - q: 'Quando começa a emissão residual do Blocknet?'
    a: 'No mês 48, na altura 414.720. Isso corresponde a 48 meses de 30 dias, ou 1.440 dias de blocos de cinco minutos após o lançamento, aproximadamente quatro anos. A data exata depende dos tempos reais dos blocos. A partir dessa altura, cada bloco paga 2,0 BNT.'
  - q: 'Quando moedas recém-mineradas podem ser gastas?'
    a: 'A recompensa fica disponível após 60 blocos, cerca de cinco horas com o tempo de bloco de cinco minutos. Essa regra se chama maturidade da coinbase. O bntpool.com também espera 60 blocos antes de pagar a recompensa e só paga moedas de blocos maduros.'
---

Acompanhe os BNT recém-emitidos desde a recompensa de bloco até as moedas gastáveis e os pagamentos posteriores. Esta página explica esse fluxo monetário, lista cada mês do cronograma de recompensas e mostra como interpretar os números como minerador ou titular.

## Qual é a recompensa de bloco do Blocknet?

<Answer>
A recompensa começou em 723,25 BNT por bloco e diminui uma vez por mês de 30 dias, ou 8.640 blocos, seguindo uma curva suave de decaimento. Dentro de cada mês, todos os blocos pagam o mesmo valor. A partir do mês 48, na altura 414.720, cada bloco paga uma emissão residual permanente de 2,0 BNT.
</Answer>

O Blocknet busca um bloco a cada cinco minutos, ou 300 segundos. São 288 blocos por dia e 8.640 por mês de 30 dias. A dificuldade se ajusta após cada bloco pela média ponderada dos últimos 60 tempos de bloco, mantendo o ritmo perto de cinco minutos conforme muda a hashrate da rede.

A recompensa depende apenas do mês correspondente à altura. O mês é a altura dividida por 8.640, arredondada para baixo. Assim, alturas de 0 a 8.639 são mês 0, de 8.640 a 17.279 são mês 1, e assim por diante.

## Como uma recompensa de bloco se torna BNT utilizável?

Um bloco minerado cria a recompensa prevista em uma saída coinbase. Essa saída pertence ao destino escolhido para o bloco e primeiro passa pela maturação coinbase. Quando se torna gastável, o proprietário pode usá-la em transações comuns. Se um pool minerou o bloco, sua contabilidade divide as recompensas elegíveis entre os participantes, e os pagamentos posteriores enviam BNT existentes às suas carteiras.

Um pagamento do pool, portanto, não cria uma segunda recompensa. Receber uma transferência também não exige minerar novas moedas. A emissão descreve a origem do BNT; as transferências descrevem as mudanças posteriores de titularidade. O [guia de confirmações](/blog/pt-br/block-confirmations-mining-rewards/) acompanha as etapas do pool, e o [guia da oferta do Blocknet](https://seineminer.com/pt-br/blog/blocknet-supply/) explica o cronograma monetário geral.

## Como a recompensa é calculada?

O daemon calcula a recompensa mensal com uma fórmula:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

O primeiro termo é a parte acima da emissão residual de 2,0 BNT. Ela diminui por um fator de e^-0,75 a cada ano. Após doze meses, fica em cerca de 47% do tamanho inicial. Mensalmente, a parcela acima do residual vale aproximadamente 93,9% da do mês anterior, reduzindo a recompensa em passos pequenos e regulares.

O resultado é arredondado para baixo em unidades atômicas inteiras: 1 BNT equivale a 100.000.000 dessas unidades. A tabela mostra as recompensas com duas casas decimais.

## Tabela mensal de recompensas do Blocknet

| Mês | Primeiro bloco | Recompensa por bloco (BNT) | Total minerado até o fim do mês (milhões de BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| 48 em diante | 414.720 | 2,00 | +0,017 por mês |

O gênese na altura 0 não tem recompensa, então o mês 0 contém 8.639 blocos pagos. Todo BNT vem dessas recompensas, sem pré-mineração, como explica a página sobre o [lançamento justo do Blocknet](/blog/pt-br/blocknet-fair-launch/). Em 25 de outubro de 2026, o pool trabalhava no bloco 69.016, perto do fim do mês 7. O [painel do bntpool.com](/) mostra a altura atual para localizar a linha acima. A [página de blocos](/blocks) lista os blocos encontrados pelo pool.

## O que acontece no mês 48?

<Answer>
O mês 48 começa na altura 414.720. A partir daí, cada bloco paga 2,0 BNT fixos para sempre. É a emissão residual, ou tail emission. Ela mantém uma recompensa em cada bloco enquanto a cadeia funcionar, remunerando continuamente os mineradores pela segurança da rede.
</Answer>

A fórmula de decaimento vale nos meses 0 a 47, terminando em 40,22 BNT por bloco no mês 47. A partir do mês 48, o daemon usa diretamente o valor residual: 2,0 BNT permanentes. A 288 blocos diários, isso acrescenta 576 BNT por dia ou cerca de 210.240 por ano.

O mês 48 começa após 1.440 dias de blocos de cinco minutos desde o lançamento, aproximadamente quatro anos. A data depende dos tempos reais, então a altura é a referência confiável.

## Quanto BNT é minerado antes da emissão residual?

A soma de todos os blocos da altura 1 até 414.719 dá 98.561.905,08 BNT, cerca de 98,6 milhões. O código e o site oficial descrevem cerca de 100 milhões em quatro anos mais a emissão residual. A soma exata fica um pouco abaixo do número arredondado.

Os totais acumulados abaixo medem a emissão prevista dos blocos aceitos. Não são uma soma dos saldos atualmente gastáveis nas carteiras nem uma medida da atividade de negociação. Mover BNT existentes entre titulares não aumenta esses totais de emissão.

A maior parte é minerada cedo. Por ano do cronograma:

| Meses | BNT minerados | Fração do total anterior à emissão residual |
|---|---|---|
| 0-11 (ano 1) | 54,48 milhões | 55,3% |
| 12-23 (ano 2) | 25,84 milhões | 26,2% |
| 24-35 (ano 3) | 12,32 milhões | 12,5% |
| 36-47 (ano 4) | 5,93 milhões | 6,0% |

## Quando você pode gastar BNT minerados?

A recompensa pode ser gasta após 60 blocos adicionais sobre o bloco de origem, cerca de cinco horas depois. Essa regra é a maturidade da coinbase. O bntpool.com espera os mesmos 60 blocos antes de pagar. A [página de pagamentos](/payouts) lista os envios.

## O que o cronograma significa para mineradores?

As recompensas são maiores nos primeiros meses. Um bloco no mês 12 paga 342,69 BNT, cerca de 47% dos 723,25 BNT do mês 0. No mês 24, paga 162,93 BNT. Cada mês novo paga menos por bloco até chegar à emissão residual.

Sua participação segue sua fração da hashrate da rede. Solo, o número esperado de blocos por dia é 288 × (sua hashrate ÷ hashrate da rede), cada um pagando a recompensa daquele mês. Em pool, a mesma conta vale para a hashrate combinada, e cada recompensa é dividida entre quem contribuiu trabalho. Você ganha em cada bloco do pool em vez de esperar um bloco inteiro próprio. O [painel do bntpool.com](/) mostra hashrates atuais da rede e do pool. A [página de estatísticas](/stats) mostra a sua depois que começar a minerar.

Para começar agora:

1. Baixe o seine em [seineminer.com](https://seineminer.com/pt-br/#download). Ele roda em Windows, macOS e Linux e se ajusta ao seu hardware.
2. Execute e informe seu endereço Blocknet ou `@name`.
3. Aceite o pool padrão, `stratum+tcp://bntpool.com:3333`.

O [guia de mineração no bntpool.com](/blog/pt-br/mine-blocknet-on-bntpool/) cobre as etapas do pool, [Get Started](/start) lista suas configurações e o [guia do seine](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) explica a configuração passo a passo. Para comparar processadores, leia [hardware e hashrate de mineração de Blocknet](https://seineminer.com/pt-br/blog/blocknet-mining-hardware-hashrate/).

<Note>
Os valores vêm do código de recompensas do daemon Blocknet, verificado em 25 de outubro de 2026. Segundo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), a atualização Faraday está planejada para cerca de 16 de fevereiro de 2027. Mantenha o seine atualizado: ele procura novas versões ao iniciar e a cada 12 horas.
</Note>

<CTA target="seine" title="Mine Blocknet com o seine" text="O seine é gratuito para Windows, macOS e Linux e minera no bntpool.com por padrão." />
