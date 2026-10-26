---
title: O que é um pool de mineração?
description: Um pool combina a hashrate de vários mineradores e divide cada recompensa de bloco. Entenda seu funcionamento, PPLNS, PPS, mineração solo e o que avaliar.
tags: [Fundamentos de mineração, Pools de mineração, PPLNS, bntpool]
sourceHash: 'a17a47ab1d73'
translation: draft
faq:
  - q: 'Entrar em um pool aumenta quanto eu minero?'
    a: 'O pool mantém sua hashrate e sua fração do trabalho da rede exatamente iguais. Ele muda como você recebe recompensas. Sozinho, você espera blocos inteiros em intervalos aleatórios. No pool, ganha uma pequena parte de cada bloco encontrado, com pagamentos frequentes proporcionais à sua hashrate.'
  - q: 'Qual é a diferença entre PPLNS e PPS?'
    a: 'PPLNS paga uma parte de cada bloco realmente encontrado pelo pool, dividida pelo trabalho enviado em uma janela recente. PPS paga um valor fixo por share aceito, calculado pelo valor esperado dele, mesmo sem o pool encontrar um bloco. O bntpool.com usa PPLNS com janela de seis horas.'
  - q: 'Preciso executar um nó próprio para minerar em um pool?'
    a: 'Não. O pool executa o nó, cria modelos de bloco, verifica shares e envia blocos encontrados. No bntpool.com, você só precisa do seine e de um endereço Blocknet ou nome Blocknet ID. Inicie o seine, cole o endereço e aceite o pool padrão. Sua parte de cada bloco é paga nesse endereço.'
  - q: 'Como começar a minerar no bntpool.com?'
    a: 'Baixe o seine para Windows, macOS ou Linux em seineminer.com, execute-o e informe seu endereço Blocknet ou @name quando solicitado. O bntpool.com é o pool padrão: aceite e comece a minerar. Consulte o endereço na página de estatísticas para ver hashrate, shares e saldo.'
---

Este guia explica o que é um pool de mineração, por que a maioria dos mineradores usa um e como o pool calcula os ganhos de cada participante. Você conhecerá PPLNS, PPS e mineração solo, além do que verificar antes de conectar seu minerador. O bntpool.com, pool de mineração de Blocknet, será o exemplo ao longo do texto.

## O que é um pool de mineração?

<Answer>
Um pool de mineração é um servidor que combina a [hashrate](/blog/pt-br/what-is-hashrate/) de muitos mineradores para encontrarem blocos juntos. Ele distribui trabalho sobre o mesmo bloco, contabiliza o trabalho enviado e divide a recompensa proporcionalmente. Os mineradores recebem pagamentos pequenos e frequentes em vez de blocos inteiros raros.
</Answer>

Em uma moeda de prova de trabalho, mineradores competem pelo próximo bloco calculando hashes de seu cabeçalho com nonces diferentes até obter um resultado abaixo do alvo da rede. Cada hash é uma tentativa independente com a mesma pequena chance de sucesso. Quem encontra o hash vencedor recebe a recompensa.

O pool transforma essa competição em trabalho em equipe. Todos conectados ao bntpool.com trabalham em blocos criados pelo pool. Quando alguém encontra o hash vencedor, a recompensa é dividida entre os mineradores cujo trabalho contribuiu para encontrá-lo.

## Por que mineradores entram em pools?

<Answer>
Para receber regularmente. Sozinho, um minerador típico detém uma fração minúscula da hashrate da rede e pode esperar dias entre blocos, em intervalos aleatórios. No pool, a hashrate combinada encontra blocos com frequência e cada minerador ganha uma parte de todos eles. A renda chega regularmente e acompanha sua hashrate.
</Answer>

No Blocknet, a conta é fácil de verificar. A rede busca um bloco a cada cinco minutos, ou 288 por dia. Um minerador sozinho espera 288 × (sua hashrate ÷ hashrate da rede) blocos diariamente.

Considere números redondos hipotéticos: um rig de 50 H/s em uma rede de 50.000 H/s tem 0,1% da rede e encontra, em média, 0,288 bloco por dia, ou um a cada três dias e meio. É apenas uma média. Encontrar blocos é aleatório, e o intervalo real pode ser muito menor ou maior.

No mesmo rig em um pool de 5.000 H/s, o pool encontra cerca de 28,8 blocos por dia, um a cada 50 minutos em média. O rig realizou 1% do trabalho do pool e ganha 1% de cada bloco. Sua contribuição é igual; a recompensa agora chega em pequenas parcelas várias vezes ao dia. O guia de [mineração em pool ou solo de Blocknet](/blog/pt-br/pool-vs-solo-mining-blocknet/) apresenta a comparação para operações maiores.

## Como funciona um pool de mineração?

Todo pool segue o mesmo ciclo básico. No bntpool.com, ele funciona assim:

1. **O pool cria um modelo de bloco.** O bntpool.com executa seu próprio nó Blocknet e obtém modelos dele. Os mineradores não precisam executar nem sincronizar um nó.
2. **Envia trabalho aos mineradores.** Eles conectam por Stratum, o protocolo de distribuição de trabalho dos pools, em `stratum+tcp://bntpool.com:3333`. Cada conexão recebe um job baseado no modelo atual.
3. **Os mineradores calculam hashes e enviam shares.** Um share é um hash que atende a um alvo mais fácil definido pelo pool. Shares comprovam o trabalho realizado.
4. **Um share que também atende ao alvo da rede é um bloco.** O pool verifica cada share contra os dois alvos. Quando atende à rede, envia o bloco ao nó.
5. **A recompensa é dividida e paga.** O pool divide conforme seu sistema de pagamento e envia cada parte após a confirmação e o saldo atingir o mínimo.

## O que é um share em um pool?

Um share é um hash que supera uma exigência menor que a de um bloco real. O bloco exige um hash abaixo do alvo da rede; o share só precisa ficar abaixo do alvo muito mais fácil do pool. A maioria dos shares nunca vira bloco, mas eles mostram regularmente quanto trabalho cada minerador faz.

Os shares são ponderados pela dificuldade. No Blocknet, dificuldade D significa cerca de D hashes esperados até atingir o alvo. Um share de dificuldade 60 representa aproximadamente 60 hashes de trabalho. O bntpool.com define e ajusta a dificuldade por conexão, buscando cerca de 10 shares a cada cinco minutos e iniciando novas conexões em 60. Um servidor rápido recebe shares mais difíceis e um notebook, mais fáceis. Ambos ganham pelo trabalho realmente realizado. O pool também memoriza a dificuldade de cada rig por uma hora, permitindo retomar o ponto anterior após uma reinicialização rápida.

## Sistemas de pagamento comuns em pools

O sistema de pagamento define como os shares viram ganhos. Estes são os mais comuns.

| Sistema | Pelo que você recebe | Como funciona |
|---|---|---|
| PPLNS (pay per last N shares) | Blocos encontrados pelo pool | A recompensa de cada bloco é dividida entre shares de uma janela recente, por dificuldade |
| PPS (pay per share) | Cada share aceito | O pool paga um valor fixo por share, baseado no valor esperado, mesmo sem encontrar bloco |
| Solo | Blocos encontrados por você | Você minera com seu próprio nó e fica com toda a recompensa de cada bloco |

**PPLNS** olha para trás a partir de cada bloco encontrado. No bntpool.com, os "últimos N shares" são uma janela de seis horas. O pool soma a dificuldade dos shares aceitos nesse período e concede a cada minerador a mesma fração da recompensa que sua fração do trabalho. Cada share conta para todos os blocos encontrados enquanto estiver na janela. Quem minera continuamente sempre tem uma janela cheia, e reiniciar não zera nada. [Como funcionam os pagamentos PPLNS no bntpool.com](/blog/pt-br/how-pplns-payouts-work/) explica a divisão passo a passo.

**PPS** paga cada share ao chegar, pelo seu valor esperado: dificuldade do share dividida pela dificuldade da rede, multiplicada pela recompensa do bloco. O operador do pool assume as oscilações dos blocos e paga do próprio saldo nos intervalos.

**Mineração solo** dispensa o pool. Seu nó cria os modelos e cada bloco encontrado paga a recompensa inteira à sua carteira. Ela combina com operações grandes o suficiente para encontrar blocos frequentemente por conta própria. O seine oferece ambos: minera no bntpool.com por padrão, e `./seine --mode daemon` minera com seu nó Blocknet sincronizado.

## O que avaliar em um pool de mineração?

Ao comparar pools, estes detalhes definem como você recebe e como pode conferir:

- **Sistema de pagamento claro.** O pool deve informar o sistema e as configurações, como a janela PPLNS. O bntpool.com os lista em [Get Started](/start) e publica em `https://bntpool.com/api/info`.
- **Confirmações e mínimo.** Recompensas normalmente esperam o bloco atingir profundidade suficiente; pagamentos saem quando o saldo passa do mínimo. No bntpool.com, a recompensa confirma após 60 blocos, conforme a maturidade da coinbase do Blocknet, e saldos a partir de 0,1 BNT são pagos automaticamente.
- **Números públicos e verificáveis.** Você deve poder ver cada bloco encontrado e pagamento enviado. O bntpool.com mostra tudo em [Blocks](/blocks) e [Payouts](/payouts), o esforço por rodada em [Luck](/luck) e a saúde dos serviços em [Status](/status).
- **Suas estatísticas.** Consulte o endereço para ver hashrate, shares e saldos. No bntpool.com, use a [página de estatísticas](/stats); o [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/) explica cada cartão.
- **Configuração simples.** Quanto menos ajustes, mais cedo você minera. O bntpool.com é o pool padrão do seine, que ajusta threads, kernels e memória sozinho. Você pode usar um [nome Blocknet ID](https://seineminer.com/pt-br/blog/blocknet-id-pay-by-name/) como `@name` no lugar do endereço completo.

## Visão geral do bntpool.com

| Configuração | bntpool.com |
|---|---|
| Sistema de pagamento | PPLNS |
| Janela de shares | 6 horas |
| Confirmações antes do pagamento | 60 blocos |
| Pagamento mínimo | 0,1 BNT |
| Endereço Stratum | `stratum+tcp://bntpool.com:3333` |

O [painel do pool](/) mostra mineradores conectados, hashrate do pool e da rede, blocos encontrados e esforço da rodada atual. Em uma leitura de 25 de outubro de 2026 às 02:25 EDT, o pool tinha aproximadamente 5.200 H/s em uma rede de 44.400 H/s, com 246 mineradores conectados. Isso representa cerca de 12% da rede, suficiente para esperar 34 blocos por dia, um a cada aproximadamente 42 minutos. Os valores mudam constantemente. Consulte o painel para dados atuais e o [guia de estimativas](/blog/pt-br/estimate-blocknet-mining-rewards/) para convertê-los em BNT conforme sua hashrate.

Para participar, [baixe o seine](https://seineminer.com/pt-br/#download), execute-o, informe seu endereço Blocknet ou `@name` e aceite o pool padrão. O [guia de mineração do seine](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) cobre cada etapa. [Minerar Blocknet no bntpool.com](/blog/pt-br/mine-blocknet-on-bntpool/) explica nomes de rigs e como conferir os primeiros shares.

<CTA target="pool" title="Entre no bntpool.com" text="Conecte o seine ao pool e ganhe uma parte de cada bloco encontrado, enviada automaticamente ao seu endereço quando atingir 0,1 BNT." />
