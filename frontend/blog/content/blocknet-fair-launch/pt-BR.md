---
title: 'Lançamento justo do Blocknet: sem pré-mineração'
description: "Confira o gênese vazio, a primeira recompensa e as regras de emissão do Blocknet. Entenda como BNT entrou em circulação e chegou a outros titulares."
tags: [Blocknet, Lançamento justo, Bloco gênese, Mineração, bntpool]
sourceHash: a283d654ab64
translation: draft
faq:
  - q: 'O Blocknet teve pré-mineração?'
    a: 'Não. O bloco gênese, criado em 15 de fevereiro de 2026, não contém transações. Nenhuma moeda existia antes da mineração. As primeiras vieram da recompensa do bloco 1, e cada BNT desde então foi pago a quem minerou o bloco de origem.'
  - q: 'O Blocknet tem fundo de desenvolvimento ou tesouraria?'
    a: "As regras publicadas não exigem uma alocação para fundo de desenvolvimento ou tesouraria. Cada bloco após o gênese tem uma saída coinbase igual à recompensa prevista. Isso descreve a nova emissão, não a identidade dos titulares posteriores. Qualquer proprietário de BNT pode realizar transferências normais, incluindo pagamentos voluntários a um desenvolvedor."
  - q: 'Quando o bloco gênese do Blocknet foi criado?'
    a: 'Seu timestamp é 15 de fevereiro de 2026 às 20:58:38 EST, equivalente a 16 de fevereiro às 01:58:38 UTC. Ele contém a mensagem "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything", cujo hash SHA3-256 é usado como hash do bloco anterior.'
  - q: 'Como posso verificar o lançamento justo do Blocknet?'
    a: 'Abra o bloco 0 no explorador Blocknet: ele tem zero transações e raiz Merkle só com zeros. No código aberto do daemon, o gênese é definido com uma lista vazia de transações, e a verificação de coinbase aceita uma saída igual à recompensa do bloco.'
  - q: 'Como começar a minerar Blocknet?'
    a: 'Baixe o seine para Windows, macOS ou Linux, execute e informe seu endereço Blocknet ou shortname Blocknet ID. Pressione Enter para aceitar o pool padrão, bntpool.com, e a mineração começa imediatamente. Depois, consulte seu endereço na página de estatísticas do pool.'
---

Veja como BNT entrou em circulação: um bloco gênese vazio, a primeira recompensa minerada e transferências posteriores entre titulares. Este guia verifica os registros do lançamento e as regras de emissão, explica o que eles comprovam e mostra como receber BNT recém-emitido minerando na bntpool.com.

## O Blocknet teve pré-mineração?

<Answer>
Não. O bloco gênese, criado em 15 de fevereiro de 2026, não contém transações. Nenhuma moeda existia antes da mineração. Desde o bloco 1, cada bloco paga a recompensa em uma única saída coinbase de valor exatamente igual ao programado. Todo BNT em circulação foi minerado por alguém executando a prova de trabalho.
</Answer>

O bloco gênese não atribui reservas a fundadores, desenvolvedores, investidores ou fundações. Os blocos seguintes criam a recompensa prevista, sem uma saída adicional de tesouraria exigida pelo protocolo. São regras de emissão: elas não identificam quem minerou uma recompensa nem quem recebeu moedas existentes por uma transferência posterior.

## O que há no bloco gênese do Blocknet?

O bloco gênese é fixado no código do daemon, então todos os nós começam com o mesmo bloco 0. Seus campos são fixos:

| Campo | Valor |
|---|---|
| Altura | 0 |
| Timestamp | 15 de fevereiro de 2026, 20:58:38 EST (16 de fevereiro, 01:58:38 UTC) |
| Mensagem | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Hash anterior | SHA3-256 da mensagem: `a94a7f2b…d909df` |
| Dificuldade | 4, o mínimo da rede |
| Nonce | 0 |
| Transações | nenhuma |
| Raiz Merkle | apenas zeros |

A mensagem é o título de uma matéria da Wired de 15 de fevereiro de 2026, citada no código como fonte. Como o hash anterior deriva dela, o título fica incorporado à identidade do bloco 0 e de tudo construído sobre ele.

O comentário acima da função de gênese é direto: "O gênese não tem transações: nenhuma coinbase, nenhuma moeda queimada. O primeiro bloco real, na altura 1, terá a primeira coinbase." A validação confirma isso. Um nó rejeita o bloco 0 se ele não tiver exatamente zero transações e raiz Merkle composta só de zeros. Mesmo um nó modificado não conseguiria inserir moedas no gênese sem ser rejeitado pelos demais.

## Como o consenso exclui fundo de desenvolvimento e tesouraria?

<Answer>
Após o gênese, cada bloco começa com uma transação coinbase sem entradas, com taxa zero e exatamente uma saída igual à recompensa prevista. As regras publicadas não incluem uma saída separada para fundadores ou tesouraria nessa transação. Os titulares continuam podendo transferir normalmente os BNT que já possuem.
</Answer>

Várias verificações do daemon atuam juntas:

- **Uma coinbase por bloco.** A primeira transação deve ser coinbase, e nenhuma outra pode ser.
- **Uma saída, nenhuma entrada.** Uma coinbase com zero ou duas saídas, ou com uma entrada, é inválida.
- **A recompensa exata.** O Blocknet oculta valores com compromissos de Pedersen, mas a coinbase é especial. Seu fator de cegamento deriva de dados públicos, a chave pública da transação e a altura do bloco. Cada nó pode reconstruir o compromisso da recompensa programada e comparar. Se a saída se comprometer com outro valor, o bloco é rejeitado.
- **Memo vazio.** O memo da coinbase deve estar vazio. Recompensas também não podem carregar dados extras.

Transações comuns não podem criar moedas. Cada uma precisa equilibrar entradas com saídas mais taxa, verificado nos próprios compromissos. Toda saída carrega uma prova de intervalo Bulletproofs que demonstra um valor válido e não negativo. Assim os valores ficam privados, enquanto a emissão segue exatamente o cronograma.

## Como verificar o lançamento justo por conta própria?

Você não precisa confiar na palavra de ninguém. A cadeia e o código são públicos.

**No explorador.** Abra o [bloco 0 no explorador Blocknet](https://explorer.blocknetcrypto.com/block/0). Ele lista 0 transações, raiz Merkle de 64 zeros, nonce 0 e hash anterior começando com `a94a7f2b`. A linha de recompensa mostra o valor programado para a altura, mas, sem transações, não existe saída para recebê-lo. Abra depois o [bloco 1](https://explorer.blocknetcrypto.com/block/1): sua única transação é uma coinbase sem entradas e com uma saída, as primeiras moedas da cadeia.

Você pode confirmar que o hash anterior é o da mensagem gênese. Com OpenSSL 1.1.1 ou posterior, execute:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

A saída termina em `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, o mesmo valor exibido para o bloco 0.

**No código.** O daemon é aberto em [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). Em [`block.go`](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go), procure estas funções:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` cria o bloco 0 com lista vazia de transações; `validateGenesisBlock` rejeita qualquer bloco 0 com transações; `validateCoinbaseConsensus` exige a saída única no valor da recompensa. Em `transaction.go`, `validateCoinbase` e `verifyCommitmentBalance` verificam a estrutura da coinbase e o equilíbrio das transações comuns.

## Como os BNT recém-minerados chegaram a outros titulares?

O explorador registra o gênese em 15 de fevereiro de 2026 às 20:58:38 EST. O bloco 1 tem a data de 20 de fevereiro de 2026 às 14:29:20 EST e contém a primeira transação coinbase. São registros distintos: o gênese define o ponto de partida comum, enquanto o bloco 1 inicia a emissão remunerada. A recompensa exibida pelo explorador para o gênese vazio é o valor previsto no cronograma; nenhuma saída o recebe.

Quando uma recompensa de mineração se torna gastável, seu proprietário pode transferir BNT para outra pessoa. Da mesma forma, um pool envia pagamentos posteriores a partir das recompensas recebidas. Essas transferências redistribuem moedas existentes; não repetem a emissão. Portanto, você pode possuir e usar BNT sem ter minerado o bloco que o criou. O [guia de envio e recebimento](https://seineminer.com/pt-br/blog/send-receive-blocknet/) cobre pagamentos comuns, enquanto o [guia da oferta](https://seineminer.com/pt-br/blog/blocknet-supply/) explica o cronograma monetário geral.

## Por que o lançamento justo importa para mineradores?

Para mineradores, ele significa que as recompensas explicam toda a origem das moedas.

- **Todas as novas moedas vão para mineradores.** Sem pré-mineração nem saída de tesouraria, 100% da emissão é paga a quem encontra blocos. O bloco 1 pagou 723,25093035 BNT em sua única saída coinbase, e todos os seguintes pagaram a recompensa completa da mesma forma.
- **As regras são iguais para todos.** Desde o bloco 1, executar a prova de trabalho é a única forma de ganhar novos BNT. O [cronograma](/blog/pt-br/blocknet-block-reward-schedule/) está no código e vale igualmente para o primeiro minerador e para quem começa hoje.
- **A prova de trabalho atende computadores comuns.** O Blocknet usa Argon2id com 2 GiB por hash, projetado para resistir a ASICs. A mineração fica acessível em desktops, notebooks e servidores já existentes. O [guia de Argon2id](https://seineminer.com/pt-br/blog/argon2id-proof-of-work-explained/) explica por quê.
- **A emissão é fácil de auditar.** Cada moeda vem de uma saída coinbase verificada por todos os nós. O total emitido em qualquer altura decorre diretamente do cronograma.

Para conhecer a moeda, [o que é Blocknet?](https://seineminer.com/pt-br/blog/what-is-blocknet/) explica privacidade, pagamentos por nome e carteiras.

## Como minerar Blocknet por conta própria?

Para receber BNT recém-emitido por meio da mineração, use o minerador seine com a bntpool.com:

1. **Obtenha um endereço.** Instale a [carteira de linha de comando](https://github.com/blocknetprivacy/blocknet/releases/latest) ou a [carteira desktop](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) oficial e copie o endereço, ou use um shortname Blocknet ID como `@name`.
2. **Baixe o seine.** Obtenha o arquivo para Windows, macOS ou Linux em [seineminer.com](https://seineminer.com/pt-br/#download) e descompacte. Não há instalação.
3. **Execute e aceite os padrões.** Informe endereço ou `@name` e pressione Enter para aceitar `stratum+tcp://bntpool.com:3333`. O seine começa imediatamente e ajusta threads e kernels enquanto trabalha, na CPU e em GPUs NVIDIA no Windows e Linux.
4. **Acompanhe o minerador.** Consulte o endereço na [página de estatísticas](/stats) para ver hashrate, shares e saldo. Veja os blocos encontrados na [página de blocos](/blocks).

O [guia de mineração no bntpool.com](/blog/pt-br/mine-blocknet-on-bntpool/) cobre login por endereço ou nome e acompanhamento. [Get Started](/start) lista as configurações de pagamento. O [guia de como minerar Blocknet](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) mostra a configuração do seine passo a passo em cada sistema.

<CTA target="seine" title="Mine sua parte" text="Todo BNT é minerado. Baixe o seine e comece a minerar no bntpool.com em poucos minutos." />
