---
title: Minere Blocknet direto para uma corretora
description: 'Minere Blocknet direto para seu endereço de depósito na SafeTrade ou em outra corretora no bntpool.com. Adicione seu memo de depósito no seine e o pool paga na sua conta.'
tags: [Guia de mineração, bntpool, SafeTrade, Pagamentos]
faq:
  - q: 'Posso minerar Blocknet direto para uma corretora?'
    a: 'Sim. Inicie o seine v0.3.1 ou mais recente com seu endereço de depósito da corretora e seu memo de depósito, como em ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. O bntpool.com paga suas recompensas nesse endereço com o memo anexado, e a corretora as credita na sua conta.'
  - q: 'Por que o seine pede um memo de pagamento?'
    a: 'Corretoras como a SafeTrade usam um único endereço de depósito compartilhado por todos os clientes e diferenciam os depósitos pelo memo. O seine conhece esses endereços compartilhados, então, quando você digita um deles, ele pede seu memo e o salva junto com o endereço. Copie o memo exatamente como aparece na página de depósito da corretora.'
  - q: 'Qual é o pagamento mínimo para uma corretora no bntpool.com?'
    a: 'Pagamentos com memo começam em 10 BNT, segundo o pool em 4 de novembro de 2026. Seu saldo confirmado se acumula no pool até atingir esse valor, e então o pool o envia ao endereço da corretora com seu memo. Pagamentos para uma carteira pessoal começam em 1 BNT.'
  - q: 'Como consulto meu saldo de mineração na corretora?'
    a: 'Na página de estatísticas do bntpool.com, digite o endereço de depósito da corretora, escolha Adicionar memo e digite seu memo. Cada par de endereço e memo tem seu próprio saldo, então você vê apenas sua hashrate, seus rigs e seus pagamentos, não os de outros clientes que usam o mesmo endereço da corretora.'
sourceHash: '8f73aae25c7a'
translation: draft
---

Este guia mostra como minerar Blocknet direto para sua conta em uma corretora no bntpool.com. Você informa ao seine o endereço de depósito da corretora e seu memo de depósito, e o pool paga suas recompensas lá com o memo anexado, para que a corretora as credite a você. O guia cobre o comando, o pagamento mínimo e como consultar seu saldo.

## Dá para minerar Blocknet direto para uma corretora?

<Answer>
Sim. O bntpool.com paga recompensas de mineração para um endereço de depósito de corretora com seu memo de depósito anexado. Inicie o seine v0.3.1 ou mais recente com o endereço da corretora e seu memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Pagamentos com memo começam em 10 BNT, e a corretora credita cada um na sua conta.
</Answer>

Minerar para uma corretora economiza uma etapa quando você pretende negociar o que minera. As recompensas chegam direto na sua conta da corretora, prontas para vender no [mercado BNT/USDT](/blog/pt-br/sell-blocknet-bnt-safetrade/), sem transferência de carteira no meio.

Corretoras como a SafeTrade dão a todos os clientes o mesmo endereço de depósito e diferenciam os depósitos por um memo, um código curto exclusivo da sua conta. Por isso o memo é importante: é ele que liga um pagamento a você. O pool mantém um saldo separado para cada par de endereço e memo, então suas recompensas nunca se misturam com as de outros mineradores que usam a mesma corretora.

## Como configurar a mineração para corretora no seine?

<Answer>
Copie o endereço de depósito de BNT e seu memo de depósito na página de depósito da corretora. Inicie o seine com os dois: --address para o endereço e --payout-memo para o memo. Quando você digita um endereço de corretora conhecido na pergunta da primeira execução, o seine pede o memo sozinho e o salva. Confira no log a confirmação do pool e minere normalmente.
</Answer>

1. **Pegue seus dados de depósito.** Na corretora, abra a página de depósito de BNT e copie o endereço de depósito e o memo. Copie o memo exatamente: letras maiúsculas e zeros à esquerda fazem diferença.
2. **Inicie o seine com os dois.** No macOS ou Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   No Windows, no PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Deixe o seine salvar.** O seine salva o memo junto com o endereço, então as próximas execuções continuam pagando na sua conta da corretora. Se você digitar um endereço de corretora conhecido na pergunta da primeira execução, o seine pede o memo e salva os dois.

A página [Get Started](/start) do pool tem uma seção "Minerando para uma corretora?" que monta este comando para você: cole o endereço e o memo e copie o resultado. O seine exige um memo para endereços compartilhados de corretoras conhecidas, como os da SafeTrade. Serviços, scripts e outros mineradores sem supervisão precisam passar `--payout-memo` na linha de comando, porque não há pergunta para responder.

O seine verifica o memo com o pool no login. O bntpool.com o confirma, e então a mineração começa. Para dar nome aos seus rigs, passe `--pool-worker rig-01` como faria com um endereço pessoal.

## Como são feitos os pagamentos para corretoras?

As recompensas seguem o mesmo caminho de qualquer pagamento do bntpool.com. Seus shares ganham uma parte de cada bloco que o pool encontra dentro da janela PPLNS de seis horas, e a recompensa de cada bloco é confirmada após 60 blocos. Depois, seu saldo confirmado se acumula no pool.

O endpoint `/api/info` do pool informava estes pagamentos mínimos em 4 de novembro de 2026:

| Destino | Pagamento mínimo |
|---|---|
| Endereço de corretora com memo | 10 BNT |
| Carteira pessoal | 1 BNT |

Quando seu saldo confirmado atinge 10 BNT, o pool o envia ao endereço da corretora com seu memo. A corretora então credita o depósito após o próprio número de confirmações; a página de depósito dela informa quantas são necessárias. [Como funcionam os pagamentos PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica em detalhes o lado do pool.

A mineração solo também funciona com memo. Coloque `solo:` antes do endereço da corretora e mantenha `--payout-memo`, e o pool paga na sua conta da corretora a recompensa inteira de cada bloco que você encontrar. [Mineração solo no bntpool.com](/blog/pt-br/solo-mine-blocknet-on-bntpool/) explica as recompensas solo.

## Como consultar seu saldo?

Abra a [página de estatísticas](/stats), cole o endereço de depósito da corretora, escolha **Adicionar memo** e digite seu memo. A página então mostra sua hashrate, seus rigs, seu saldo e seus pagamentos para esse par de endereço e memo. Outros clientes da mesma corretora têm seus próprios pares e seus próprios saldos.

Cada pagamento em **Recent Payouts** tem link para a transação no explorador. Compare com o histórico de depósitos da corretora para acompanhar as moedas até sua conta. [Acompanhe seu minerador no bntpool.com](/blog/pt-br/track-your-miner-on-bntpool/) explica o restante da página de estatísticas.

## Como voltar para uma carteira pessoal?

Inicie o seine uma vez com seu novo endereço e `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

O seine salva o novo endereço sem memo, e os novos shares contam para sua carteira pessoal. Um saldo já creditado ao par da corretora fica com esse par e é pago a ele quando atingir o mínimo; você pode acompanhá-lo na página de estatísticas. [Troque o endereço da sua carteira de mineração](/blog/pt-br/change-mining-wallet-address/) explica como o pool credita o trabalho após uma troca de endereço.

## Quais configurações aceitam memos?

Memos exigem a conexão direta do seine com o pool, que é o padrão. O modo daemon do seine minera com seu próprio nó e paga à carteira do nó, então não tem memo. O proxy de fazenda não repassa memos, então conecte os rigs da corretora direto ao pool. O seine só minera em um pool que confirma seu memo, então seu trabalho é sempre pago na conta que você escolheu.

<CTA target="pool" title="Minere para sua conta na corretora" text="Inicie o seine com seu endereço de depósito e seu memo, e o bntpool.com paga suas recompensas direto na sua conta da corretora a partir de 10 BNT." />
