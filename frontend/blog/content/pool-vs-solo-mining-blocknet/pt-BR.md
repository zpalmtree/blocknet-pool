---
title: Mineração em pool ou solo para Blocknet
description: Escolha entre pool e mineração solo de Blocknet. Calcule seus blocos esperados por dia, veja como o bntpool.com oferece pagamentos regulares e configure a mineração solo.
tags: [Guia de mineração, Mineração solo, bntpool, PPLNS]
sourceHash: 171d240d6c4b
translation: draft
faq:
  - q: 'Com que frequência eu encontraria um bloco de Blocknet minerando solo?'
    a: 'Em média, são 288 × (sua hashrate ÷ hashrate da rede) blocos por dia, pois o Blocknet busca um bloco a cada cinco minutos. Com 50 H/s em uma rede de 50.000 H/s, isso dá 0,288 bloco por dia, ou aproximadamente um a cada três dias e meio. Consulte a hashrate atual da rede no painel do bntpool.com.'
  - q: 'Preciso executar um nó Blocknet para minerar no bntpool.com?'
    a: 'Não. O pool executa seu próprio nó, cria os modelos de bloco e envia os blocos encontrados. Você só precisa do seine e de um endereço Blocknet ou nome Blocknet ID. Execute o minerador, cole o endereço e aceite o pool padrão. Sua parte de cada bloco é paga nesse endereço.'
  - q: 'O seine pode minerar Blocknet solo?'
    a: 'Sim. Inicie com ./seine --mode daemon para minerar diretamente com seu próprio nó Blocknet sincronizado, usando o cookie da API para conectar. Cada bloco encontrado paga a recompensa inteira à carteira do nó. O guia de mineração solo no repositório do seine cobre instalações personalizadas e senhas de carteira.'
  - q: 'Posso alternar entre pool e solo depois?'
    a: 'Sim. O mesmo download do seine serve para ambos. Execute normalmente para minerar no bntpool.com ou acrescente --mode daemon para usar seu próprio nó. Reinicie quando quiser mudar. Shares já enviados permanecem na janela PPLNS de seis horas e continuam rendendo enquanto estiverem nela.'
---

Este guia ajuda você a escolher entre minerar Blocknet no bntpool.com ou solo com seu próprio nó. Você calculará quantos blocos sua hashrate encontra sozinha, verá como o pool transforma isso em pagamentos regulares e aprenderá a configurar ambas as opções com o seine.

## É melhor minerar Blocknet em pool ou solo?

<Answer>
A conta é a mesma: sua fração da hashrate da rede define sua fração dos blocos. No bntpool.com, você ganha uma parte de todo bloco encontrado pelo pool, recebendo pagamentos regulares conforme eles chegam. Solo, você fica com a recompensa inteira de cada bloco que encontra, uma opção adequada a grandes operações com nó próprio.
</Answer>

Para a maioria dos mineradores, o bntpool.com é o ponto de partida simples. É o pool padrão do seine: basta configurar o endereço, sem executar ou sincronizar um nó. A mineração solo também é totalmente compatível. O modo daemon do seine trabalha diretamente com seu nó Blocknet. O restante do guia mostra como escolher de acordo com seu hardware.

## Quantos blocos você encontraria solo?

<Answer>
O Blocknet busca um bloco a cada cinco minutos, ou 288 por dia. Minerando sozinho, você pode esperar 288 × (sua hashrate ÷ hashrate da rede) blocos diariamente. Divida um dia por esse número para obter o intervalo médio entre seus blocos. A hashrate atual da rede está no painel do bntpool.com.
</Answer>

A fórmula funciona porque o Blocknet ajusta a dificuldade para a rede encontrar um bloco a cada 300 segundos, em média. A dificuldade representa aproximadamente quantos hashes a rede deve tentar antes de um sucesso. Cada hash tem a mesma chance: a fração dos hashes da rede produzida pela sua máquina é a fração esperada de blocos.

Você precisa de dois números:

- **Sua hashrate.** O seine a mostra no painel durante a mineração. A [página de estatísticas](/stats) do pool também a exibe depois que você começar a minerar nele.
- **Hashrate da rede.** O [painel do bntpool.com](/) a mostra em Network Hashrate, ao lado de Pool Hashrate. Ambas mudam com o tempo; confira os valores ao calcular.

Cada bloco paga a recompensa correspondente ao mês. O [cronograma de recompensas](/blog/pt-br/blocknet-block-reward-schedule/) lista os valores mensais.

## Um exemplo calculado

<Note>
Os números abaixo são valores redondos hipotéticos para demonstrar a conta, não dados atuais. Use a hashrate atual da rede no painel para sua própria estimativa.
</Note>

Suponha uma rede de 50.000 H/s. Um rig de 50 H/s, próximo ao medido pelo seine em um servidor AMD EPYC Zen 5, detém 0,1% da rede. Veja o que ele e duas operações maiores encontrariam sozinhos:

| Sua hashrate (exemplo) | Fração da rede | Blocos esperados por dia | Intervalo médio entre blocos |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | cerca de 83 horas (3,5 dias) |
| 500 H/s | 1% | 2,88 | cerca de 8,3 horas |
| 5.000 H/s | 10% | 28,8 | cerca de 50 minutos |

Agora coloque o rig de 50 H/s em um pool com 5.000 H/s no total. O pool encontra cerca de 28,8 blocos por dia, um a cada 50 minutos em média. A fração de 1% do rig no trabalho do pool rende 1% de cada bloco. O trabalho contabilizado continua equivalente a 0,288 bloco por dia. Muda a forma de recebê-lo: pequenas parcelas a cada aproximadamente 50 minutos, em vez de um bloco inteiro a cada alguns dias.

A tabela também mostra por que a mineração solo combina com operações grandes. A 5.000 H/s, um minerador solo encontra blocos tão frequentemente quanto um pool desse tamanho, tornando sua renda regular por conta própria.

Para converter blocos por dia em BNT para seu hardware, com um exemplo datado de dados reais, veja [como estimar suas recompensas de mineração de Blocknet](/blog/pt-br/estimate-blocknet-mining-rewards/).

## Por que um pool oferece pagamentos mais regulares?

O bntpool.com combina a hashrate de todos os participantes, encontrando blocos muito mais frequentemente que um rig isolado. Cada recompensa é dividida entre os mineradores cujo trabalho ajudou a encontrá-la, usando PPLNS: o pool considera as seis horas anteriores ao bloco e distribui proporcionalmente à dificuldade dos shares.

Isso traz alguns resultados práticos:

- **Ganhos em cada bloco.** Enquanto seus shares estiverem na janela de seis horas, você ganha parte de cada bloco do pool. Acompanhe-os na [página de blocos](/blocks).
- **Pagamentos automáticos.** Quando um bloco chega a 60 blocos de profundidade, sua parte passa ao saldo confirmado. O pool o envia ao seu endereço ao atingir 0,1 BNT. Os envios aparecem na [página de pagamentos](/payouts).
- **Continuidade durante reinicializações.** Shares já enviados permanecem na janela por seis horas. Reiniciar ou atualizar um driver não zera sua posição.
- **Só o minerador precisa rodar.** O pool executa o nó, cria modelos e envia blocos. Você pode usar vários rigs para o mesmo endereço ou Blocknet ID `@name`.

O [guia de pagamentos PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica a divisão passo a passo, com exemplo próprio. A página [Get Started](/start) lista as configurações atuais de pagamento.

## Por que minerar Blocknet solo?

Na mineração solo, todo o processo fica sob seu controle. Seu nó cria cada modelo e verifica todos os blocos. Cada bloco encontrado paga a recompensa completa diretamente à carteira do nó.

A opção funciona melhor quando sua hashrate encontra blocos frequentemente. No exemplo acima, uma operação com 10% da rede encontra um bloco a cada cerca de 50 minutos, recebendo tão regularmente quanto um pool. Se você já executa um nó Blocknet para sua carteira ou outros serviços, basta acrescentar o minerador.

O seine também foi feito para esse modo. Ele minera com os modelos do nó e, se o nó esquecer um modelo, por exemplo após reiniciar, reenvia o bloco resolvido completo para que o bloco encontrado ainda chegue à cadeia.

## Como configurar a mineração solo com o seine?

1. **Instale e sincronize um nó Blocknet.** Com o gerenciador `blocknet`, execute:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Isso grava o cookie da API em `~/.config/bnt/data/mainnet/api.cookie`. Para acelerar a primeira sincronização, o bntpool.com publica `checkpoints.dat` na [página de checkpoints](/checkpoints). [Sincronizar um nó Blocknet com checkpoints](/blog/pt-br/blocknet-checkpoints-fast-sync/) explica como o nó usa o arquivo.
2. **Espere a sincronização terminar.** As mensagens `[sync] progress` chegam a 100%. O nó só fornece modelos de bloco quando alcança a cadeia.
3. **Inicie o seine em modo daemon.**

   ```sh
   ./seine --mode daemon
   ```

   Para uma instalação personalizada, informe a API e o cookie:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

O [guia de mineração solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) explica senhas de carteira e configurações personalizadas. O [guia de mineração do seine](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) cobre download e primeiros shares. Você pode [baixar o seine](https://seineminer.com/pt-br/#download) para Windows, macOS ou Linux.

## Qual opção escolher?

Use sua hashrate e a hashrate atual da rede no [painel](/) na fórmula e observe o intervalo médio entre blocos.

- **Comece no bntpool.com** se minera em desktop, notebook ou poucos servidores, ou se quer pagamentos regulares sem executar um nó. Inicie o seine, cole o endereço ou `@name` e aceite o pool padrão.
- **Mine solo** se sua operação detém uma grande fração da rede, encontrando blocos frequentemente por conta própria, e você já executa ou quer executar um nó sincronizado.

O seine facilita rever a escolha. Pool e modo daemon usam o mesmo download; você muda com uma opção e uma reinicialização conforme seu hardware cresce. Se mantiver um nó local enquanto minera no pool, o seine também mostra o saldo da carteira desse nó ao lado do saldo do pool.

<Note>
Segundo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), o Blocknet planeja uma atualização chamada Faraday por volta de 16 de fevereiro de 2027, que deve mudar a prova de trabalho. Mantenha o seine atualizado: ele verifica novas versões ao iniciar e a cada 12 horas.
</Note>

<CTA target="pool" title="Comece a minerar no bntpool.com" text="Execute o seine, cole seu endereço e ganhe uma parte de cada bloco encontrado pelo pool, paga automaticamente a partir de 0,1 BNT." />
