---
title: 'Mudar o endereço da carteira de mineração Blocknet'
description: 'Mude o endereço de recebimento dos mineradores Blocknet, confira novas shares no bntpool.com e acompanhe as recompensas anteriores no endereço antigo.'
tags: ['Guia de mineração', 'Carteira', 'Pagamentos']
faq:
  - q: 'Mudar o endereço transfere o saldo anterior?'
    a: 'A nova configuração direciona o trabalho futuro ao novo endereço. Shares, créditos e pagamentos existentes continuam associados ao endereço original. Consulte ambos separadamente em Minhas estatísticas e mantenha acesso à carteira antiga enquanto acompanha as recompensas restantes.'
  - q: 'Preciso mudar todos os computadores?'
    a: 'Sim. Cada processo usa seu próprio destino configurado. Atualize e confira primeiro um computador e depois os demais, incluindo configuração salva e argumentos de início. Nomes diferentes identificam quais máquinas já enviam trabalho ao novo endereço.'
  - q: 'Mudar meu Blocknet ID atualiza um minerador ativo?'
    a: 'O seine resolve o ID salvo ao iniciar e mantém esse endereço durante a execução, inclusive nas reconexões. Após mudar o vínculo do ID, reinicie cada minerador e confira o endereço completo. Isso não reatribui trabalho ou saldos já registrados no endereço anterior.'
sourceHash: 'fca912d46d55'
translation: draft
---

Mude o destino do trabalho futuro mantendo um registro claro das recompensas anteriores. Este guia parte do seine já conectado ao bntpool.com. Para a primeira conexão, veja o [guia do pool](/blog/pt-br/mine-blocknet-on-bntpool/); para uma nova carteira, o [guia de carteira](https://seineminer.com/pt-br/blog/blocknet-wallet-for-mining/).

## Como mudar o endereço de mineração?

<Answer>
Pare um minerador, atualize o destino na configuração salva ou no comando habitual e reinicie. Confira o endereço completo resolvido e shares aceitas; depois consulte o endereço no bntpool.com. Repita nas outras máquinas. Shares e recompensas anteriores continuam ligadas ao endereço usado quando foram registradas.
</Answer>

*O trabalho novo vai para a segunda carteira; os registros das recompensas anteriores ficam ligados à primeira.*

## Registre os dois destinos antes da mudança

Copie o novo endereço diretamente da carteira escolhida. O minerador precisa do destino público ou Blocknet ID registrado, sem frase de recuperação ou chave privada. Guarde também o endereço completo original: ele continua sendo a chave para consultar a atividade anterior.

Abra Minhas estatísticas no endereço antigo e anote recompensas pendentes, saldo confirmado, pagamentos recentes e máquinas ativas. Esse registro serve de referência; a nova página não contém o histórico antigo. Mantenha acesso à carteira original enquanto acompanha recompensas e pagamentos restantes.

## Atualize um minerador e confira seu comando

Pare um minerador e examine seu início habitual. Em configuração existente, `--address` prevalece sobre o valor salvo sem substituí-lo. O exemplo PowerShell escolhe o destino desta execução. Troque `@yourname` pelo novo ID ou endereço completo e preserve os argumentos testados de pool, dados e hardware.

```powershell
.\seine.exe --address "@yourname"
```

No Linux ou macOS, use a sintaxe abaixo no diretório habitual. Ambos os exemplos iniciam mineração real: escolha o do seu sistema. A instalação existente fornece os demais ajustes. Mantenha o nome da máquina para reconhecer suas shares sob o novo endereço.

```sh
./seine --address "@yourname"
```

## Faça o início normal usar o novo endereço

Com o minerador parado, edite `address` em `seine.config.json`, preservando os demais campos e JSON válido. Confira a pasta: `./seine-data` é relativa ao diretório de trabalho e `--data-dir` pode escolher outra. Edite o arquivo realmente usado pelo inicializador.

Atualize ou remova `--address` antigo do atalho, script, tarefa ou serviço. Um comando manual correto não altera esses outros inícios. Reinicie normalmente e confira. O [guia de configuração](https://seineminer.com/pt-br/blog/configure-seine-miner/) explica prioridades e como outro diretório de trabalho pode selecionar outra configuração.

## O que muda usando um Blocknet ID?

No seine v0.2.36, `@name` é resolvido e validado ao iniciar. O endereço completo fica fixo naquela execução, inclusive nas reconexões ao pool. Se mudar o vínculo do nome, reinicie cada minerador e confira o destino. Reconectar o processo existente não repete a resolução inicial.

Anote os endereços completos antigo e novo, mesmo mantendo o nome legível. A contabilidade usa o endereço resolvido: mudar o vínculo não redireciona shares ou saldos anteriores. Se o nome não resolver, confira o vínculo ou use deliberadamente o endereço completo verificado na carteira.

## Verifique todos os computadores no novo endereço

Confira destino e envios aceitos, depois procure o endereço completo em Minhas estatísticas. Identifique nome da máquina e shares recentes. Uma nova estimativa de hashrate precisa de trabalho e tempo; confirme primeiro os envios. O [guia de estatísticas](/blog/pt-br/track-your-miner-on-bntpool/) explica contadores e máquinas que não aparecem.

Após confirmar a primeira máquina, repita nas restantes. Nomes distintos facilitam a conferência. Teste também o início normal de cada uma, evitando que um inicializador antigo retome o destino anterior. O [guia de vários computadores](/blog/pt-br/mine-multiple-computers-one-wallet/) explica a organização sob um só endereço.

## O que acontece com as recompensas antigas?

Créditos e pagamentos existentes continuam no endereço antigo. Shares elegíveis anteriores ainda podem contribuir nas respectivas janelas PPLNS; recompensas pendentes seguem as confirmações. Hashrate zero não significa contabilidade concluída. Os guias de [PPLNS](/blog/pt-br/how-pplns-payouts-work/) e [confirmações](/blog/pt-br/block-confirmations-mining-rewards/) distinguem trabalho, maturação e pagamentos.

A mudança não combina saldos nem ignora o mínimo de pagamento. Consulte as regras atuais e acompanhe as duas páginas até entender os valores restantes. Renomear uma máquina também não transfere recompensas. Confirme que todos os mineradores previstos enviam trabalho novo ao novo endereço e preserve a carteira antiga com seu histórico.

<CTA target="pool" title="Confira sua configuração no bntpool.com" text="Verifique destino e trabalho aceito em cada máquina e acompanhe separadamente recompensas antigas e novas." />
