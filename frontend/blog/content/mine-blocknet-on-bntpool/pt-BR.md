---
title: Mine Blocknet no bntpool.com
description: Entre no bntpool.com, o pool padrão do seine para Blocknet. Conecte-se a stratum+tcp://bntpool.com:3333 com seu endereço ou @name, nomeie os rigs e acompanhe as estatísticas.
tags: [Guia de mineração, bntpool, Blocknet ID, seine]
sourceHash: 'df269126bb76'
translation: draft
faq:
  - q: 'Qual é o endereço Stratum do bntpool.com?'
    a: 'É stratum+tcp://bntpool.com:3333, o pool sugerido pelo seine na primeira execução. Pressione Enter na pergunta sobre o pool ou informe-o com --pool-url. Seu endereço Blocknet ou nome Blocknet ID é o login, e o pool envia suas recompensas para esse endereço.'
  - q: 'Posso minerar no bntpool.com com um nome Blocknet ID?'
    a: 'Sim. Use @name ou $name onde o seine ou o pool pedir um endereço. O pool verifica o mapeamento assinado em blocknet.id e credita shares e pagamentos ao endereço completo associado ao nome. É necessário seine v0.2.16 ou posterior. Na linha de comando, coloque o nome entre aspas para que o shell não o interprete.'
  - q: 'Preciso de uma conta para minerar no bntpool.com?'
    a: 'Não. Não há cadastro, senha ou e-mail. Seu endereço Blocknet, ou um nome Blocknet ID que aponte para ele, é o login e o destino dos pagamentos. Inicie o seine com ele e consulte-o em My Stats para ver hashrate, shares, rigs, saldo e pagamentos.'
  - q: 'Posso minerar de vários computadores para um endereço?'
    a: 'Sim. Execute o seine em cada computador com o mesmo endereço ou nome e um nome de rig diferente. O pool contabiliza shares, saldo e pagamentos por endereço. Todos os rigs contribuem para um saldo e um pagamento, e My Stats mostra cada rig em sua própria linha.'
---

Este guia explica como entrar no pool bntpool.com com o seine: endereço do pool, login com endereço Blocknet ou Blocknet ID `@name`, nome de cada rig e verificação da chegada de shares na página de estatísticas. Se ainda não instalou o seine, siga primeiro o [guia de instalação](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) e volte aqui.

## Qual é o endereço do pool bntpool.com?

<Answer>
O endereço Stratum do bntpool.com é `stratum+tcp://bntpool.com:3333`. O seine o oferece como padrão na primeira execução, então basta pressionar Enter na pergunta sobre o pool. Use seu endereço Blocknet ou Blocknet ID `@name` como login. Não é preciso criar uma conta, e as recompensas vão para esse endereço.
</Answer>

Estas são as configurações informadas pelo endpoint público `/api/info` em 25 de outubro de 2026:

| Configuração | Valor |
|---|---|
| Endereço Stratum | `stratum+tcp://bntpool.com:3333` |
| Login | Endereço Blocknet, `@name` ou `$name` |
| Sistema de pagamento | PPLNS com janela de 6 horas |
| Confirmações antes do pagamento | 60 blocos |
| Pagamento mínimo | 0,1 BNT |

Os mesmos valores estão sempre disponíveis na página [Get Started](/start) do pool.

## Por que o bntpool.com é o pool padrão do seine?

Na primeira execução, o seine pergunta qual pool usar e preenche `stratum+tcp://bntpool.com:3333` como padrão. O pool e o minerador funcionam juntos em vários aspectos práticos:

- **Um protocolo.** O bntpool.com usa o protocolo Stratum do seine, versão 2. A conexão não exige nenhuma configuração além do seu endereço.
- **Nomes nos dois lados.** O seine e o pool aceitam nomes Blocknet ID e os verificam no mesmo registro assinado em blocknet.id.
- **Verificação de versão.** Cada rig informa sua versão do seine junto com os shares. A página de estatísticas marca versões antigas com o aviso "update available", que leva à página de releases do seine.
- **Guia de configuração do seine.** A página Get Started mostra como baixar e executar o minerador, com um comando para copiar.

## Como conectar o seine ao bntpool.com?

Na primeira execução, o seine faz duas perguntas:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Cole seu endereço ou digite seu nome e pressione Enter para aceitar o bntpool.com. O seine salva ambas as respostas em `seine-data/seine.config.json`. As próximas execuções começam a minerar diretamente. Ele também escolhe um nome de rig e o salva no mesmo arquivo, como explicado adiante.

Em um rig sem interface, serviço ou script, não há ninguém para responder. Passe os dois valores como opções na primeira execução, usando o mesmo comando da página Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Para mudar de pool ou endereço depois, edite ou exclua `seine-data/seine.config.json`, ou substitua os valores salvos com `--address` e `--pool-url` na linha de comando.

## É possível minerar para um @name do Blocknet ID?

<Answer>
Sim. O bntpool.com aceita um shortname do Blocknet ID, escrito como `@name` ou `$name`, em qualquer lugar que aceite um endereço. O pool verifica o mapeamento assinado de blocknet.id, valida o endereço associado e credita shares, saldo e pagamentos a esse endereço completo durante toda a conexão.
</Answer>

Nomes não diferenciam maiúsculas de minúsculas: `@Alice` e `@alice` são o mesmo login. O suporte chegou no seine v0.2.16 e está em todas as versões atuais. Antes de minerar, o seine imprime o endereço resolvido para você conferir com sua carteira. Na linha de comando, use aspas simples no nome. Sem elas, o shell tentaria expandir `$name` como variável:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Se depois você apontar o nome para outra carteira, reinicie o seine. Ele resolve o nome salvo a cada inicialização e mantém o endereço durante toda a execução; o pool faz o mesmo por conexão. Um saldo já ganho permanece no endereço em que foi ganho e é pago ali. Mover um nome nunca move suas moedas.

Você pode registrar um nome em [blocknet.id](https://blocknet.id). O [guia do Blocknet ID](https://seineminer.com/pt-br/blog/blocknet-id-pay-by-name/) explica os pagamentos por nome.

## Como dar nomes aos rigs?

Cada conexão informa um nome de rig junto com o endereço. Na primeira execução, o seine escolhe `seine-` seguido de quatro dígitos e salva na configuração. A mesma máquina mantém o nome após reiniciar. Para usar um nome fácil de reconhecer, defina-o na linha de comando:

```sh
./seine --pool-worker rig-01
```

O pool guarda até 64 caracteres. Um nome estável também ajuda após reiniciar: o pool memoriza por uma hora a dificuldade dos shares de cada rig, identificada pelo endereço e nome do rig. Ao reconectar, ele retoma a dificuldade em que já havia se estabilizado.

## É possível minerar de vários rigs para um endereço?

Sim. Inicie o seine em cada máquina com o mesmo endereço ou `@name` e um nome de rig diferente. O pool contabiliza shares, saldos e pagamentos por endereço, somando todos os rigs em um saldo e um pagamento. Na página de estatísticas, cada rig aparece em sua própria linha, facilitando conferir se todas as máquinas estão minerando.

## Como verificar se o minerador está funcionando?

O painel do seine mostra hashrate e shares aceitos na própria máquina. Para ver a perspectiva do pool, abra [My Stats](/stats), informe seu endereço, `@name` ou `$name` e pressione **Lookup**. Ao usar um nome, a página mostra o endereço resolvido ao lado da busca. Ela também lembra a última consulta naquele navegador. O [guia de acompanhamento do minerador](/blog/pt-br/track-your-miner-on-bntpool/) explica cada cartão.

Observe três itens:

- **Hashrate e Shares Accepted.** O cartão de hashrate mostra o que o pool recebe de você, e a contagem de aceitos cresce conforme os rigs enviam trabalho.
- **Uma linha por rig.** Cada linha mostra nome, hashrate, shares aceitos e rejeitados e horário do último share. Um share recente indica um rig conectado e trabalhando.
- **Recent Shares.** Os últimos shares aparecem com o rig que os enviou, sua dificuldade e seu status.

O pool define a dificuldade separadamente por conexão e a ajusta durante a mineração. Até uma CPU pequena envia shares regularmente, e os primeiros aceitos aparecem poucos minutos após conectar. Consulte o [guia de hardware de mineração de Blocknet](https://seineminer.com/pt-br/blog/blocknet-mining-hardware-hashrate/) para saber a hashrate esperada do seu equipamento.

As recompensas vêm depois. Estimated Rewards aparece quando o pool encontra um bloco enquanto seus shares estão na janela de seis horas. Após 60 confirmações, sua parte passa para Confirmed Rewards, e o pool a envia automaticamente quando você tem pelo menos 0,1 BNT. [Como funcionam os pagamentos PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica a divisão de cada recompensa. O histórico geral está em [Blocks](/blocks) e [Payouts](/payouts).

## O que há na página Get Started?

A página [Get Started](/start) reúne tudo em uma tela:

- **Informações do pool.** Endereço Stratum para copiar com um clique, sistema de pagamento, mínimo e confirmações necessárias, lidos ao vivo do pool.
- **Início rápido em três etapas.** Baixe o seine em [seineminer.com](https://seineminer.com/pt-br/#download), execute uma vez, informe endereço ou nome e URL do pool, comece a minerar e acompanhe hashrate e saldo em My Stats.
- **Comando para copiar.** O comando completo `./seine` com a URL preenchida. Insira seu endereço ou `'@yourname'` entre aspas.
- **Notas de configuração.** Onde o seine salva a configuração, como redefini-la e quais opções a substituem.
- **Contabilização das recompensas.** Uma breve explicação de PPLNS: shares anteriores ao bloco ganham sua recompensa; trabalho posterior conta para os próximos blocos. Recompensas de blocos não confirmados aparecem separadamente em My Stats até a confirmação.
- **Capturas de tela** da primeira configuração e do painel de mineração do seine, para você saber o que esperar antes de começar.

<CTA target="pool" title="Comece a minerar no bntpool.com" text="Conecte o seine ao bntpool.com com seu endereço ou @name e acompanhe a chegada dos shares em My Stats." />
