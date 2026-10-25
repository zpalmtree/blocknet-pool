---
title: Sincronize um nó Blocknet rápido com checkpoints do bntpool.com
description: Nós Blocknet sincronizam rapidamente com checkpoints.dat do bntpool.com. Entenda como checkpoints fixam a cadeia e baixe, verifique e carregue o arquivo.
tags: [Nó Blocknet, Checkpoints, Sincronização rápida, bntpool]
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'De onde vêm os checkpoints de um nó Blocknet?'
    a: 'Do bntpool.com. Na primeira execução, se não existir checkpoints.dat no diretório de dados, o daemon baixa https://bntpool.com/checkpoints.dat para sincronização rápida. Você também pode baixar pela página Checkpoints, conferir o SHA-256 e colocar o arquivo no diretório antes de iniciar o nó.'
  - q: 'Com que frequência o bntpool.com atualiza checkpoints.dat?'
    a: 'O daemon do pool reescreve o arquivo conforme a cadeia cresce, com uma entrada a cada 100 blocos. Cada entrada fica pelo menos 100 blocos abaixo da ponta, além da profundidade máxima de reorganização. Assim, checkpoints cobrem apenas histórico consolidado. A página mostra a última altura e alteração.'
  - q: 'Checkpoints mudam meu saldo ou histórico da carteira?'
    a: 'Não. O nó ainda armazena todos os blocos e registra todas as saídas e key images nos índices. A varredura da carteira e os saldos funcionam como após uma sincronização completa. Os checkpoints apenas permitem pular a reverificação de prova de trabalho e assinaturas do histórico já atestado pelos hashes.'
  - q: 'Posso sincronizar um nó sem checkpoints?'
    a: 'Sim. Use --full-sync ao iniciar o daemon ou defina full_sync como true na configuração do blocknet. O nó pula o download e reverifica prova de trabalho e assinaturas de cada bloco desde o gênese. Isso demora mais, por isso checkpoints são o padrão para novos nós.'
---

Um novo nó Blocknet precisa alcançar todos os blocos desde o gênese antes de atender uma carteira ou minerador solo. Checkpoints aceleram esse processo. Este guia explica o conteúdo do arquivo do bntpool.com, como o daemon pula as verificações mais lentas sem se desviar da cadeia real e como baixar, verificar e carregar o arquivo por conta própria.

## O que são checkpoints do Blocknet?

<Answer>
São uma lista de hashes conhecidos, um a cada 100 blocos, fornecida pelo bntpool.com como checkpoints.dat. O nó verifica se os blocos recebidos correspondem a esses hashes e pula a reverificação da prova de trabalho e das assinaturas até o último checkpoint. A primeira sincronização termina muito mais rápido.
</Answer>

O arquivo é texto simples com um par `height:hash` por linha. Em 25 de outubro de 2026, as primeiras linhas eram:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Cada hash é o identificador do bloco, o SHA3-256 de seu cabeçalho. O daemon do pool gera o arquivo de sua cadeia e só inclui blocos com pelo menos 100 blocos de profundidade. Cem blocos são a finalidade rígida do Blocknet, a maior reorganização permitida. Assim, cada checkpoint fica em histórico consolidado.

Em 25 de outubro de 2026 às 02:04 EDT, o arquivo continha 689 checkpoints, da altura 100 até 68.900, em 48.811 bytes. Ele cresce uma linha aproximadamente a cada 100 blocos, cerca de oito horas com blocos de cinco minutos. Consulte a [página Checkpoints](/checkpoints) para a altura atual.

## Como um nó Blocknet usa checkpoints?

Após carregar checkpoints, o nó usa a última altura como alvo da sincronização rápida. Até chegar lá, confia nos checkpoints publicados para as duas verificações mais caras e executa todas as demais normalmente:

- **A prova de trabalho se apoia nos checkpoints.** Cada bloco usa um hash Argon2id sobre 2 GiB de memória. Recalculá-lo para todo o histórico é caro, e os hashes dos checkpoints já atestam esse histórico.
- **Assinaturas RingCT e provas de intervalo também.** Os comentários do daemon chamam a verificação das provas de intervalo de saídas de maior custo individual da sincronização, pois toda saída carrega uma, inclusive a coinbase de cada bloco.
- **Todas as outras verificações continuam.** Encadeamento, regra de dificuldade LWMA, timestamps, tamanho e estrutura dos blocos, gastos duplos por key images e vínculos entre entradas e assinaturas são verificados normalmente. Saídas e key images entram nos índices como sempre. Varreduras e saldos da carteira ficam iguais aos da sincronização completa.

As verificações só são puladas para blocos que estendem a ponta atual do nó, abaixo do último checkpoint. Depois dessa altura, cada novo bloco é verificado integralmente, incluindo a prova de trabalho.

### Como checkpoints fixam a cadeia confiável

Durante a sincronização até o último checkpoint, todo bloco em uma altura marcada precisa ter exatamente o hash do arquivo. Um hash diferente gera um erro de incompatibilidade de checkpoint e é rejeitado. Como há checkpoints a cada 100 blocos, um par não consegue levar o nó por outro histórico: a próxima altura marcada revelaria o desvio.

Checkpoints também aceleram reinicializações. Com blocos já no disco, o daemon encontra o checkpoint mais alto correspondente à cadeia armazenada e reverifica só os blocos seguintes, sem percorrer tudo desde o gênese.

## Um novo nó baixa checkpoints automaticamente?

<Answer>
Sim. Quando o daemon inicia sem checkpoints.dat no diretório de dados, baixa https://bntpool.com/checkpoints.dat, carrega o arquivo e sincroniza rapidamente até o último checkpoint. O download é uma tentativa: se falhar, o nó inicia mesmo assim e verifica toda a cadeia.
</Answer>

O endereço padrão está no próprio núcleo do Blocknet. Sem ajustes extras, cada novo nó sincroniza com o arquivo publicado pelo bntpool.com. O daemon só o busca quando não existe uma cópia, dá 15 segundos à requisição e limita o download a 32 MiB.

## Como usar os checkpoints do bntpool.com passo a passo?

Se preferir baixar e conferir pessoalmente, ou atualizar uma cópia antiga antes de uma longa ressincronização, siga estas etapas.

### 1. Encontre o diretório de dados

O arquivo fica no diretório de dados do nó, ao lado do banco da cadeia:

- **Gerenciador `blocknet`:** `~/.config/bnt/data/mainnet/` no macOS e Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` no Windows, salvo se você configurar `data_dir`.
- **Core executado diretamente:** `./blocknet-data-mainnet` na pasta de inicialização, ou o caminho passado com `--data`.

### 2. Baixe checkpoints.dat

Abra [Checkpoints](/checkpoints) e pressione **Download checkpoints.dat**, ou use o terminal. Este exemplo grava no diretório padrão do core:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Baixar sobre um arquivo existente o substitui, atualizando a cópia antiga.

### 3. Confira o SHA-256

A página publica o SHA-256 do arquivo atual. Calcule o hash da sua cópia e compare:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

No macOS, use `shasum -a 256`. O hash muda sempre que o pool acrescenta um checkpoint. Compare com a página logo após baixar.

### 4. Confira um checkpoint no explorador

Você pode confirmar qualquer linha no [explorador Blocknet](https://explorer.blocknetcrypto.com). Abra a página de uma altura presente, por exemplo `explorer.blocknetcrypto.com/block/68900`, e compare o hash com a linha de 68.900. Fizemos isso com o último checkpoint em 25 de outubro de 2026 e os hashes coincidiram.

### 5. Inicie o nó

Inicie ou reinicie o daemon. Ele lê `checkpoints.dat` na inicialização e sincroniza rapidamente até o último checkpoint. Se usar o shell interativo do core (`--cli`), `load-checkpoints` carrega o arquivo no nó em execução e informa a altura da sincronização rápida:

```text
> load-checkpoints
```

Checkpoints ajudam enquanto o nó está atrasado. Depois da última altura marcada, ele já verifica cada bloco integralmente e um arquivo mais novo não muda nada.

## Como conferir o arquivo por script?

O pool publica os detalhes em JSON em `https://bntpool.com/api/checkpoints`, sem chave de API:

| Campo | Significado |
|---|---|
| `available` | Se há um arquivo disponível |
| `url` | Onde baixá-lo |
| `entries` | Quantidade de checkpoints |
| `latest_height`, `latest_hash` | Maior checkpoint e hash do bloco |
| `bytes`, `sha256` | Tamanho e SHA-256 do arquivo |
| `updated_at` | Última alteração como timestamp Unix |

O download inclui cabeçalhos `X-Checkpoint-Height` e `X-Checkpoint-Sha256` e um `ETag` baseado no hash. O script pode enviar `If-None-Match` e receber rapidamente `304 Not Modified` quando nada mudar:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

As respostas podem ficar em cache por 60 segundos, suficiente para um arquivo que muda a cada cerca de oito horas.

<Note>
Segundo blocknetcrypto.com, a atualização Faraday trará uma prova de trabalho mais leve para acelerar a sincronização de novos nós. Até lá, checkpoints são o caminho rápido para atualizar um nó.
</Note>

## O que fazer com um nó sincronizado?

Um nó sincronizado atende a carteira Blocknet e a API do nó, e é com ele que o seine conversa na mineração solo com `--mode daemon`. [Pool ou solo para Blocknet](/blog/pt-br/pool-vs-solo-mining-blocknet/) ajuda a decidir se solo combina com seu hardware. Se preferir começar sem nó, o seine minera no bntpool.com por padrão. O [guia de instalação](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) leva aos primeiros shares, e [mineração no bntpool.com](/blog/pt-br/mine-blocknet-on-bntpool/) explica o lado do pool.

<CTA target="pool" title="Mine no pool que fornece checkpoints do Blocknet" text="Entre no bntpool.com em poucos minutos e acompanhe hashrate, shares e pagamentos ao vivo." />
