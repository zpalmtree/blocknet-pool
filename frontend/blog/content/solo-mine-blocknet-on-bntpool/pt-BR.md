---
title: Minere Blocknet solo no bntpool.com
description: 'Minere Blocknet solo sem executar um nó. Coloque solo: antes do seu endereço no seine e o bntpool.com paga a recompensa inteira de cada bloco que você encontrar.'
tags: [Guia de mineração, Mineração solo, bntpool, seine]
faq:
  - q: 'Como minerar Blocknet solo no bntpool.com?'
    a: 'Atualize o seine para a v0.3.1 ou mais recente e coloque solo: antes do seu endereço de pagamento, por exemplo ./seine --address solo:YOUR_ADDRESS. O seine se conecta ao bntpool.com como sempre, e o pool paga a você a recompensa inteira de cada bloco que seu rig encontrar pelos pagamentos normais. Você não precisa executar um nó.'
  - q: 'Preciso do meu próprio nó Blocknet para minerar solo?'
    a: 'Não. Com o prefixo solo:, o bntpool.com executa o nó, cria os modelos de bloco e envia os blocos que você encontra. Seu rig só executa o seine. O modo daemon do seine é a outra forma de minerar solo, com seu próprio nó sincronizado, e é indicado para quem já executa um.'
  - q: 'Posso usar um nome Blocknet ID na mineração solo?'
    a: 'Sim. Use solo:@name ou solo:$name no lugar de solo:ADDRESS e coloque entre aspas na linha de comando para que o shell o repasse sem alterações. O pool resolve o nome para o endereço completo, credita seus blocos solo e os paga nesse endereço como qualquer outro pagamento.'
  - q: 'Como volto para as recompensas do pool?'
    a: 'Remova o prefixo solo: do seu endereço e reinicie o seine. Seu rig volta a dividir as recompensas PPLNS de cada bloco encontrado pelo pool. Cada rig escolhe seu modo ao se conectar, então você pode ter alguns rigs solo e outros com recompensas compartilhadas no mesmo endereço.'
sourceHash: '623b1e0b356a'
translation: draft
---

Este guia mostra como minerar Blocknet solo no bntpool.com: adicione um prefixo ao seu endereço no seine e o pool paga a você a recompensa inteira de cada bloco que seus rigs encontrarem. Você não instala nem sincroniza um nó. Você vai configurar a mineração solo, ver como os blocos solo são pagos e saber onde acompanhá-los no pool.

## O que é a mineração solo no bntpool.com?

<Answer>
A mineração solo no bntpool.com paga você apenas pelos blocos que seus próprios rigs encontram, e cada um paga a recompensa de bloco inteira ao seu endereço. O pool continua executando o nó, criando os modelos de bloco e enviando seus blocos, então você recebe recompensas solo com a mesma configuração da mineração em pool. Para ativar, coloque solo: antes do seu endereço.
</Answer>

Na mineração normal em pool, o bntpool.com divide cada bloco entre os mineradores cujos shares estão na janela PPLNS de seis horas, então você ganha uma parte de cada bloco encontrado pelo pool. Um rig solo, em vez disso, fica com a recompensa inteira dos próprios blocos e não ganha nada com blocos encontrados por outros. Os dois usam o mesmo servidor, o mesmo endereço e os mesmos pagamentos. Só o login muda.

Minerar solo pelo pool traz recompensas solo sem o trabalho de manter um nó:

- **Nenhum nó para executar.** O nó do pool acompanha a cadeia, fornece modelos novos e envia seus blocos resolvidos.
- **Pagamentos automáticos.** Seus blocos são pagos ao seu endereço ou nome Blocknet ID como qualquer outro pagamento do bntpool.com.
- **Estatísticas ao vivo.** Seus rigs, sua hashrate e seus blocos aparecem na [página de estatísticas](/stats) do pool.
- **Um prefixo para mudar.** Remova `solo:` e reinicie o seine para voltar às recompensas compartilhadas.

O modo daemon do seine é a outra forma de minerar solo, com seu próprio nó Blocknet sincronizado. [Pool ou mineração solo de Blocknet](/blog/pt-br/pool-vs-solo-mining-blocknet/) compara as três opções e mostra com que frequência sua hashrate encontra um bloco.

## Como começar a minerar solo com o seine?

<Answer>
Instale o seine v0.3.1 ou mais recente e inicie-o com solo: antes do seu endereço Blocknet, por exemplo ./seine --address solo:YOUR_ADDRESS no macOS ou Linux. O seine se conecta ao bntpool.com, seu pool padrão, e o painel identifica sua conta como Solo. Você também pode digitar solo: e seu endereço quando o seine pedir um endereço de pagamento na primeira execução.
</Answer>

1. **Atualize o seine.** Logins solo exigem o seine v0.3.1 ou mais recente. [Baixe o seine](https://seineminer.com/pt-br/#download) ou deixe as atualizações automáticas instalarem.
2. **Inicie o seine com o prefixo.** No macOS ou Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   No Windows, no PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   O seine minera no bntpool.com por padrão. A página [Get Started](/start) do pool mostra o mesmo comando com o endereço do pool por extenso.
3. **Confira o painel.** O painel de saldo identifica sua conta como **Solo**, e o log mostra a confirmação do pool de que você recebe pelos blocos que este endereço encontra.

O prefixo funciona em todo lugar onde o seine aceita um endereço: a pergunta da primeira execução, a configuração salva, a API de controle local e os rigs do proxy de fazenda. O seine o salva junto com seu endereço, então as próximas execuções continuam minerando solo.

## Dá para minerar solo com um nome Blocknet ID?

Sim. Um nome Blocknet ID funciona do mesmo jeito que um endereço completo. Escreva `solo:@name` ou `solo:$name` e coloque entre aspas para que o shell o repasse sem alterações:

```sh
./seine --address 'solo:@yourname'
```

O pool resolve o nome no blocknet.id e paga seus blocos solo ao endereço por trás dele. Para dar nome a um rig, acrescente-o a um endereço completo depois de um ponto, como em `solo:YOUR_ADDRESS.rig1`, ou passe `--pool-worker rig1` com qualquer endereço ou nome. O prefixo aceita maiúsculas e minúsculas, então `SOLO:` também funciona.

## Como os blocos solo são pagos?

Quando seu rig encontra um bloco, o pool o registra como bloco solo. Quando ele chega a 60 blocos de profundidade, cerca de cinco horas, o pool credita ao seu saldo a recompensa de bloco inteira, descontada a taxa do pool. A taxa é de 0% em 4 de novembro de 2026. Depois, seu saldo é pago ao seu endereço pelos pagamentos normais do pool.

Os pagamentos seguem as mesmas regras da mineração em pool. Cada bloco aguarda 60 confirmações para que uma reorganização curta da cadeia não desfaça uma recompensa já paga, e [confirmações de blocos e recompensas de mineração](/blog/pt-br/block-confirmations-mining-rewards/) explica o motivo. O endpoint `/api/info` do pool informava um pagamento mínimo de 1 BNT em 4 de novembro de 2026, e uma recompensa de bloco Blocknet fica muito acima disso, então um bloco solo confirmado é pago no calendário normal de pagamentos do pool. A [página de pagamentos](/payouts) lista cada pagamento enviado pelo pool.

Se você minera para um endereço de depósito de exchange com um memo de pagamento, a mineração solo funciona do mesmo jeito. O memo e seu próprio pagamento mínimo valem para seus blocos solo assim como valem para as recompensas compartilhadas.

O trabalho do seu rig continua contando para a hashrate, o esforço da rodada e a sorte do pool, e um bloco solo encerra a rodada do pool como qualquer outro bloco. Shares solo nunca entram na janela PPLNS, então outros mineradores não ganham nada com seus blocos, e você não ganha nada com os deles.

## Com que frequência um rig solo encontra um bloco?

O Blocknet busca um bloco a cada cinco minutos, ou 288 blocos por dia. Um rig solo encontra em média 288 × (sua hashrate ÷ hashrate da rede) deles por dia. Veja a hashrate da rede na [página inicial do bntpool.com](/) e a sua no painel do seine, depois divida um dia pelo resultado para obter o tempo médio entre seus blocos.

As recompensas solo chegam como blocos inteiros, então combinam com mineradores cuja hashrate encontra blocos com frequência: grandes fazendas, frotas de servidores e quem tem uma grande fração da rede. Rigs menores ganham em média o mesmo com as recompensas PPLNS compartilhadas, pagas em partes regulares de cada bloco encontrado pelo pool. [Pool ou mineração solo de Blocknet](/blog/pt-br/pool-vs-solo-mining-blocknet/) traz exemplos com três hashrates.

## Onde ver seus rigs e blocos solo?

Consulte seu endereço na [página de estatísticas](/stats). Você pode colá-lo com ou sem o prefixo `solo:`. A página então destaca seu trabalho solo:

- **Workers** lista cada rig com uma coluna Tipo que mostra **Solo** para os rigs que enviaram shares solo dentro da janela de hashrate.
- **Estimated rewards** marca seus blocos solo enquanto aguardam as confirmações.
- **Blocks** e **Luck** mostram uma coluna Tipo, Pool ou Solo, antes do status de cada bloco. A [página de blocos](/blocks) lista todos os blocos encontrados pelo pool, os mais recentes primeiro.

[Acompanhe seu minerador no bntpool.com](/blog/pt-br/track-your-miner-on-bntpool/) explica o restante da página de estatísticas.

## Dá para combinar rigs solo e de pool?

Sim. Cada rig escolhe seu modo ao se conectar, então um endereço pode ter rigs solo e rigs de pool ao mesmo tempo. Uma fazenda pode minerar solo nos maiores servidores e manter os desktops nas recompensas compartilhadas, tudo pago em um único saldo. O pool mantém os dois tipos de trabalho separados: um share encontrado em uma conexão solo conta apenas para os blocos solo desse endereço.

Para levar um rig de volta, remova `solo:` do endereço dele, no seu comando ou na configuração salva, e reinicie o seine. Os shares que o rig já enviou no modo solo continuam solo, e os novos shares entram na janela PPLNS imediatamente.

<CTA target="pool" title="Minere solo no bntpool.com" text="Adicione solo: ao seu endereço no seine e fique com a recompensa inteira de cada bloco que seus rigs encontrarem, sem nó para executar." />
