---
title: O que é hashrate na mineração de criptomoedas?
description: Hashrate é o número de hashes calculados por segundo. Entenda as unidades, as diferenças entre sua taxa, a do pool e a da rede e por que cada uma varia.
tags: [Hashrate, Fundamentos de mineração, Blocknet, bntpool]
sourceHash: '5af3a8b3f18b'
translation: draft
faq:
  - q: 'Qual hashrate um computador alcança minerando Blocknet?'
    a: 'Nas medições do seine, os melhores resultados por geração são 49,4 H/s em um servidor AMD EPYC Zen 5, 35,7 H/s em AMD EPYC Zen 4, 30,9 H/s em um notebook Apple M4 Max e 21,5 H/s em um servidor Arm AWS Graviton4. Seu valor depende principalmente da largura de banda da memória e de quantas threads de 2 GB cabem na RAM.'
  - q: 'Por que a hashrate do Blocknet usa H/s em vez de MH/s ou TH/s?'
    a: 'Cada hash é um cálculo Argon2id que grava cerca de 2 GiB de memória e a lê de volta. Mesmo servidores grandes completam só dezenas de hashes por segundo. Provas de trabalho baseadas em hashes rápidos, como SHA-256, atingem números muito maiores e usam unidades maiores. Compare hashrates apenas no mesmo algoritmo.'
  - q: 'Por que o pool mostra uma hashrate diferente do minerador?'
    a: 'Seu minerador conta cada hash concluído. O pool vê apenas os shares enviados e estima a hashrate pela dificuldade deles na última hora. Como os shares chegam em momentos aleatórios, o valor do pool varia mais e se estabiliza com o tempo. Compare o gráfico de 24 horas de My Stats com a média do minerador.'
  - q: 'Hashrate maior significa mais recompensas?'
    a: 'Sim, proporcionalmente. Cada hash tem a mesma chance de encontrar um bloco, então sua fração esperada dos blocos equivale à sua fração da hashrate da rede. Dobrar sua hashrate dobra as recompensas esperadas; uma hashrate maior da rede reduz sua participação. No bntpool.com, ela chega como parte de cada bloco do pool.'
---

Hashrate é o primeiro número exibido por mineradores, pools e exploradores de blocos. Este guia explica o que ela mede, como ler as unidades a partir de H/s, como sua taxa se relaciona com a do pool e a da rede e por que cada valor muda a cada minuto. Os exemplos usam Blocknet e o painel do bntpool.com.

## O que é hashrate?

<Answer>
Hashrate é a quantidade de hashes calculados por segundo. Na mineração por prova de trabalho, cada hash é uma tentativa de resolver um bloco, com a mesma pequena chance de sucesso. Maior hashrate significa mais tentativas por segundo, aumentando sua fração esperada de blocos proporcionalmente à sua participação na hashrate total.
</Answer>

Uma cadeia de prova de trabalho exige um cabeçalho de bloco cujo hash fique abaixo de um alvo. Não há atalho: o minerador muda um número do cabeçalho, o nonce, calcula o hash, compara com o alvo e repete. A hashrate conta quantas dessas tentativas a máquina conclui por segundo.

No Blocknet, cada tentativa é um hash Argon2id. O nonce entra como senha, o restante do cabeçalho como salt, e a função percorre 2 GiB de memória antes de produzir uma resposta de 32 bytes. O seine e o pool informam hashes completos por segundo: 1 H/s equivale a um cálculo Argon2id inteiro por segundo.

## O que significam H/s, kH/s e MH/s?

As unidades seguem os prefixos métricos usuais, cada nível mil vezes maior que o anterior:

| Unidade | Hashes por segundo | Uso típico |
|---|---|---|
| H/s | 1 | Um computador no Blocknet |
| kH/s (KH/s) | 1.000 | Totais do pool e da rede Blocknet |
| MH/s | 1.000.000 | Algoritmos de prova de trabalho mais rápidos |
| GH/s | 1.000.000.000 | Hashes rápidos, como SHA-256 |
| TH/s ou mais | 1.000.000.000.000 ou mais | Totais de redes com hashes rápidos |

O painel do bntpool.com escreve a unidade de milhares como KH/s e sobe uma unidade sempre que o valor passa de 1.000. Assim, 44,44 KH/s equivale a 44.440 H/s. Antes de dividir uma hashrate por outra, converta ambas para H/s.

## Por que as hashrates do Blocknet são tão pequenas?

A prova de trabalho do Blocknet é memory-hard, com uso intensivo de memória. Cada hash grava cerca de 2 GiB e lê cerca de 2 GiB de volta, muito além do cache de qualquer processador. O sistema de memória define o ritmo. Cada thread precisa de seus próprios 2 GiB, então a RAM determina quantas threads podem rodar.

Estes são os melhores resultados medidos pelo seine em cada geração de hardware:

| Hardware | Threads | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (MacBook Pro de 48 GB) | 16 | 30,9 H/s |
| Servidor Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Mesmo o servidor Zen 5, com 32 threads, completa aproximadamente 1,5 hash por thread por segundo. Algoritmos de hashes rápidos produzem números maiores: a wiki do Bitcoin observa que mineradores de Bitcoin, com SHA-256 duplo, já eram medidos em bilhões de hashes por segundo em 2014. Os números representam quantidades diferentes de trabalho por hash. Uma hashrate só é significativa comparada à de outro equipamento no mesmo algoritmo. [Entenda a prova de trabalho Argon2id](https://seineminer.com/pt-br/blog/argon2id-proof-of-work-explained/) aprofunda o algoritmo, e o [guia de hardware de Blocknet](https://seineminer.com/pt-br/blog/blocknet-mining-hardware-hashrate/) mostra o papel da largura de banda e da RAM.

## Sua hashrate, a do pool e a da rede

Três hashrates importam para quem minera em um [pool](/blog/pt-br/what-is-a-mining-pool/), cada uma medida de forma diferente.

### Sua hashrate

O seine conta cada hash concluído e mostra dois valores: **Current**, sua taxa atual, e **Average**, a média da sessão. Para uma leitura estável da capacidade da máquina, use a média depois de alguns minutos de ajuste de threads e kernel com trabalho real de mineração.

O pool não vê seus hashes. Ele vê shares que atingem o alvo mais fácil da sua conexão. O bntpool.com estima a hashrate pela dificuldade total dos shares aceitos na última hora e a mostra em [My Stats](/stats). [Acompanhe seu minerador no bntpool.com](/blog/pt-br/track-your-miner-on-bntpool/) explica essa página.

### Hashrate do pool

O cartão **Pool Hashrate** soma o trabalho dos shares de todos os mineradores conectados na mesma janela de uma hora. Ele mostra quanto do trabalho da rede o bntpool.com realiza e, consequentemente, com que frequência pode esperar um bloco, exibida como **Expected Block Time**.

### Hashrate da rede

Ninguém consegue contar todos os hashes da rede. Por isso, a hashrate é calculada a partir dos blocos. A dificuldade representa o número esperado de hashes para encontrar um bloco, e o Blocknet busca um bloco a cada 300 segundos.

## Como a hashrate da rede é calculada?

<Answer>
A hashrate da rede é estimada pela dificuldade e pelos tempos de bloco. No Blocknet, a dificuldade corresponde aproximadamente aos hashes esperados por bloco. Como o alvo é 300 segundos, a taxa é aproximadamente dificuldade dividida por 300. O painel do bntpool.com usa o trabalho real e os timestamps dos dez blocos mais recentes.
</Answer>

Veja a conta com valores lidos em 25 de outubro de 2026 às 02:26 EDT. O [explorador Blocknet](https://explorer.blocknetcrypto.com) mostrava a próxima dificuldade em 16.943.147, correspondendo a cerca de 16,9 milhões de hashes esperados por bloco. Dividindo por 300 segundos, são aproximadamente 56.477 H/s.

Ao mesmo tempo, o painel do bntpool.com mostrava 44,44 KH/s, ou 44.440 H/s. Ambos estimam a mesma grandeza por perspectivas diferentes. O valor da dificuldade corresponde à taxa que produziria blocos exatamente a cada cinco minutos. O painel divide o trabalho real dos últimos dez blocos pelo tempo real decorrido. Se os blocos recentes demoraram mais de cinco minutos, a leitura fica menor. O Blocknet ajusta a dificuldade após cada bloco, mantendo os dois próximos ao longo do tempo.

## Por que a hashrate oscila?

Cada um dos três valores varia por seus próprios motivos.

- **Sua máquina.** Nos primeiros minutos, o seine compara quantidades de threads e kernels durante a mineração e escolhe os mais rápidos. A taxa cresce durante o ajuste. Depois, outros programas usando memória ou núcleos podem consumir parte do tempo de hashing.
- **A visão do pool sobre você.** Shares são eventos aleatórios. O bntpool.com ajusta a dificuldade por conexão para cerca de 10 shares a cada cinco minutos, ou 120 por hora. Um período de sorte ou de poucos shares muda a estimativa. Rigs recém-conectados são avaliados pelos minutos já minerados até completar uma hora.
- **A rede.** Tempos de bloco também são aleatórios, e dez blocos são uma amostra curta. Em 25 de outubro de 2026, o painel mostrava aproximadamente 50.057 H/s às 00:26 EDT, 62.329 H/s às 02:03 e 44.440 H/s às 02:26. Entradas e saídas de mineradores acrescentam mudanças reais a esse ruído.

Para decidir, use médias: a da sessão do seine, o gráfico de 24 horas de My Stats e a taxa da rede consultada algumas vezes durante o dia.

## O que a hashrate significa para as recompensas?

Como cada hash tem a mesma chance, sua fração da hashrate da rede é sua fração esperada dos blocos. O Blocknet produz 288 blocos por dia. Um minerador com 0,1% da rede espera, no longo prazo, cerca de 0,29 bloco em recompensas por dia. [Como estimar suas recompensas de Blocknet](/blog/pt-br/estimate-blocknet-mining-rewards/) converte isso em BNT diários usando o [painel](/).

No bntpool.com, essa participação chega como parte de cada bloco encontrado pelo pool, creditada pelos shares aceitos. As recompensas crescem em pequenos passos ao longo do dia. Se ainda não começou, o [guia de instalação do seine](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) leva do download aos primeiros shares em poucos minutos.

<Note>
As hashrates deste guia descrevem a prova de trabalho Argon2id atual do Blocknet. Segundo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), a atualização Faraday, planejada para cerca de 16 de fevereiro de 2027, deve mudar a prova de trabalho. Mantenha o seine atualizado: ele procura novas versões ao iniciar e a cada 12 horas.
</Note>

<CTA target="seine" title="Veja sua própria hashrate" text="Baixe o seine, deixe-o se ajustar por alguns minutos e acompanhe sua hashrate no painel e em My Stats." />
