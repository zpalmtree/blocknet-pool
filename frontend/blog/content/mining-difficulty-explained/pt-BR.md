---
title: 'Dificuldade de mineração: rede e shares'
description: 'Entenda o alvo da dificuldade de mineração, como o Blocknet a ajusta e por que a dificuldade da rede e a das shares afetam estatísticas diferentes.'
tags: ['Conceitos de mineração', 'Dificuldade', 'Blocknet']
faq:
  - q: 'Uma dificuldade maior reduz meu hashrate?'
    a: 'Ela muda o alvo que o resultado precisa cumprir, não diretamente quantos hashes o hardware calcula por segundo. Com hashrate igual, maior dificuldade da rede aumenta o intervalo médio esperado entre blocos. Verifique o desempenho separadamente da frequência de blocos e shares.'
  - q: 'Uma share de dificuldade maior paga mais?'
    a: 'Ela representa mais trabalho esperado, mas é enviada menos frequentemente com o mesmo hashrate. Compare o trabalho ponderado por dificuldade no mesmo período. No bntpool.com, as recompensas também dependem dos blocos aceitos e do PPLNS, não da contagem bruta de shares.'
  - q: 'Com que frequência o Blocknet ajusta a dificuldade?'
    a: 'Após cada bloco, o Blocknet calcula a dificuldade do seguinte com o histórico recente. A regra atual usa média móvel ponderada linearmente de 60 intervalos e busca 300 segundos por bloco. Intervalos recentes pesam mais; blocos individuais ainda podem chegar antes ou depois.'
sourceHash: '5c203e8c7523'
translation: draft
---

A dificuldade liga o trabalho do hardware aos blocos aceitos pela rede. Este guia explica o alvo, o ajuste do Blocknet e a dificuldade separada das shares. Entenda as mudanças antes de comparar hashrate, tempo esperado ou a [calculadora de recompensas](/blog/pt-br/estimate-blocknet-mining-rewards/#mining-calculator).

## O que é dificuldade de mineração?

<Answer>
Ela descreve o trabalho de hashing esperado para encontrar um resultado que cumpra um alvo. Quanto maior, menos frequentes os resultados válidos com o mesmo hashrate. No Blocknet, a dificuldade da rede equivale aproximadamente aos hashes esperados por bloco; a das shares corresponde a outro alvo de envio.
</Answer>

Cada hash produz um número que precisa cumprir um limite. Um alvo menor aceita uma fração menor dos resultados e tem dificuldade maior. O Blocknet divide o maior valor de 256 bits pela dificuldade para obter o alvo de 256 bits. Dobrá-la dobra aproximadamente o trabalho esperado sem mudar a operação de hashing.

## Como o Blocknet ajusta a dificuldade da rede?

O cálculo ocorre após cada bloco. A regra atual da mainnet usa média móvel ponderada linearmente, LWMA, de 60 intervalos recentes, com peso maior para os novos. O alvo é 300 segundos por bloco. Combinando dificuldade anterior e tempos ponderados, chegadas persistentemente rápidas tendem a elevá-la, e lentas a reduzi-la.

A implementação limita os intervalos usados e aplica dificuldade mínima. A janela de 60 intervalos precisa de 61 timestamps; não significa ajuste apenas a cada 60 blocos. As regras aproximam o ritmo da cadeia do alvo quando muda a potência. Cinco minutos é uma média: um bloco individual pode chegar bem antes ou depois.

## Dificuldade da rede e das shares

O pool atribui um alvo de share para observar o trabalho entre descobertas de blocos. Em geral ele é mais fácil que o alvo da rede. Ambos descrevem trabalho esperado, mas respondem a perguntas distintas e são definidos por partes diferentes do sistema:

| Dificuldade | O que controla |
|---|---|
| Rede | O limiar de trabalho do bloco candidato, derivado das regras de consenso. |
| Share | O limiar de envio atribuído pelo pool à conexão ou tarefa de mineração. |

O bntpool.com usa dificuldade variável, vardiff, para ajustar a frequência dos envios. Com hashrate igual, dificuldade maior geralmente produz menos shares, cada uma com mais trabalho esperado. Isso não altera a dificuldade da rede. Um resultado que também cumpra o alvo da rede pode ser candidato a bloco; o daemon ainda o valida. O [guia de shares](/blog/pt-br/accepted-rejected-stale-shares/) explica aceitações e rejeições.

## Como afeta o tempo esperado entre blocos?

No Blocknet, divida a dificuldade da rede pelo hashrate que faz o trabalho para estimar o intervalo médio em segundos. Use H/s e valores do mesmo período. O exemplo mantém as variáveis constantes para mostrar a conta. São números ilustrativos, não medições atuais nem previsão para um minerador específico.

**Segundos esperados por bloco ≈ dificuldade da rede ÷ hashrate em H/s**

| Dificuldade da rede | Hashrate | Intervalo esperado |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Com dificuldade 30.000 e 100 H/s, a média é 300 segundos. Ao dobrar para 60.000, mantendo o hashrate, passa a 600 segundos. Se ele também dobrar para 200 H/s, volta a 300 segundos. São médias de tentativas repetidas. O [guia de sorte do pool](/blog/pt-br/mining-pool-luck-explained/) explica a variação das rodadas reais.

## Por que as recompensas mudam com hashrate estável?

Com seu hashrate fixo, maior dificuldade reduz as descobertas esperadas por unidade de tempo enquanto persistir. Se o hashrate total cresce e a dificuldade acompanha, sua parcela do trabalho diminui. A recompensa por bloco é outro fator: o [cronograma](/blog/pt-br/blocknet-block-reward-schedule/) muda seu valor independentemente. A dificuldade não determina os BNT da recompensa programada.

Use dados recentes na calculadora para explorar os BNT esperados. Confira se o hashrate vem do minerador ou do pool para aplicar corretamente a taxa do minerador, usando a da sua versão. Estimativas são médias sob certas hipóteses. Pagamentos dependem também de blocos encontrados, confirmações e [PPLNS](/blog/pt-br/how-pplns-payouts-work/); a dificuldade sozinha não informa quando chegarão.

## Qual dificuldade você deve consultar?

Comece pela pergunta. Para blocos, use dificuldade da rede e hashrate do minerador ou pool em análise. Para frequência de shares, use a dificuldade dos trabalhos enviados. Para saldo ou pagamento, abra [My Stats](/stats) e o [histórico de pagamentos](/payouts). Guarde horário, escopo e unidades ao comparar valores.

O [painel do pool](/) e o [histórico de blocos](/blocks) dão contexto atual. O hashrate da rede é estimado pelo trabalho recente e tempo decorrido; pode diferir da próxima dificuldade dividida por 300 segundos. O [guia de hashrate](/blog/pt-br/what-is-hashrate/) explica isso. Separe dificuldade, hashrate e sorte no diagnóstico: cada um descreve um aspecto.

<CTA target="pool" title="Conecte seu minerador ao bntpool.com" text="Use a página de configuração e acompanhe shares e atividade no My Stats." />
