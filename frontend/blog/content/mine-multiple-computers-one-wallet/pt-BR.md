---
title: 'Mine Blocknet em vários computadores para uma carteira'
description: 'Adicione outro minerador de Blocknet à mesma carteira no bntpool.com. Nomeie cada máquina, confira shares e entenda recompensas e nomes de rigs duplicados.'
tags: ['Guia de mineração', 'Máquinas', 'Configuração']
faq:
  - q: 'Preciso de uma carteira para cada computador?'
    a: 'Não. Cada computador pode usar o mesmo endereço de recebimento no bntpool.com, com um nome diferente para identificar cada instalação. O pool contabiliza recompensas por endereço. As máquinas precisam apenas desse endereço ou do seu Blocknet ID, não da frase de recuperação.'
  - q: 'O que acontece se dois mineradores usam o mesmo nome?'
    a: 'Sob o mesmo endereço, o histórico de shares fica agrupado nesse nome, dificultando identificar a máquina por trás de uma leitura. Dê um nome diferente a cada instalação e reinicie com ele. Os shares já registrados mantêm a identificação original.'
  - q: 'Adicionar outra máquina divide meu pagamento?'
    a: 'Os nomes organizam a atividade exibida e não criam saldos separados sob o mesmo endereço. O trabalho válido dos computadores contribui para a participação PPLNS desse endereço. O saldo segue as regras normais de confirmação e pagamento do pool.'
sourceHash: 'a07a17533ad2'
translation: draft
---

Adicione um segundo computador ao bntpool.com mantendo uma carteira de recebimento. Nomeie cada instalação, verifique separadamente e acompanhe o endereço conjunto. Este guia cobre a segunda máquina e nomes duplicados. Para o primeiro minerador, conclua antes o [guia de configuração do pool](/blog/pt-br/mine-blocknet-on-bntpool/).

## Vários computadores podem minerar para a mesma carteira?

<Answer>
Sim. Execute o seine em cada computador com o mesmo endereço de recebimento e um nome de máquina distinto. O bntpool.com registra trabalho e recompensas por endereço; os nomes separam a atividade nas estatísticas. Confirme shares aceitos de cada máquina antes de confiar na visão conjunta.
</Answer>

*Computadores separados enviam trabalho pelo pool para um endereço de recebimento, cada um com seu próprio nome.*

## Escolha o endereço e os nomes

Use o endereço de recebimento da carteira ou um Blocknet ID que aponte para ele. Confira o endereço resolvido em cada minerador. O nome facilita a entrada, mas a contabilidade segue o endereço resultante. Se mudar o destino do ID, uma execução existente pode manter a resolução anterior até reiniciar. Recompensas já obtidas permanecem no endereço original.

O [guia de carteira para mineração](https://seineminer.com/pt-br/blog/blocknet-wallet-for-mining/) explica como obter o endereço. As máquinas só precisam desse destino público; mantenha frase de recuperação e chaves privadas com a carteira. Escolha nomes curtos, como `desktop-01` e `server-02`. O pool remove espaços nas extremidades e mantém até 64 caracteres. Coloque a parte distintiva no começo de nomes longos.

## Adicione o segundo minerador com nome próprio

Instale a versão adequada do seine no segundo computador e execute na pasta extraída. O primeiro exemplo de PowerShell define explicitamente nome de recebimento, pool e máquina. Substitua `@yourname` pelo seu ID ou endereço completo antes de executar. Preserve as aspas e acrescente opções de backend ou ajuste já testadas nessa máquina.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

No Linux ou macOS, use o executável do exemplo abaixo com outro nome de máquina. Os sistemas podem ser diferentes; o endereço deve coincidir. Cada exemplo inicia mineração real. Execute o apropriado no computador correspondente e aguarde a inicialização e os ajustes normais. Não é necessário executar os dois exemplos na mesma máquina.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Quais configurações são compartilhadas?

Compartilhe endereço de recebimento e servidor do pool. Mantenha pasta de dados local e nome separados por instalação. O seine salva a conexão em `seine.config.json` no diretório de dados; o padrão `./seine-data` depende da pasta inicial. Iniciar em outra pasta pode encontrar outra configuração. Uma pasta fixa facilita manter os ajustes.

Se copiar a configuração do primeiro computador, defina um nome distinto no comando do segundo e confira na inicialização. Mantenha esse nome explícito nos próximos comandos: a substituição não regrava um nome já salvo. Adapte as opções ao hardware e deixe cada máquina se ajustar localmente. O argumento de endereço define o pagamento e o de nome define a identificação; informe-os separadamente como acima.

## Como conferir a contribuição de ambos?

Abra My Stats no bntpool.com e consulte o endereço comum. Procure os dois nomes e um share recente de cada um. Confira também envios aceitos nos próprios mineradores. Faça isso uma máquina por vez para associar processo local e linha do pool, encontrando erros mais facilmente do que apenas pelo hashrate total.

O pool estima hashrate pelos shares enviados; uma máquina nova precisa de tempo e trabalho aceito para uma leitura útil. Taxa local e estimativa do pool cobrem medidas e intervalos diferentes. Primeiro confirme atividade por shares recentes e contadores aceitos, depois compare períodos maiores. O [guia de estatísticas](/blog/pt-br/track-your-miner-on-bntpool/) explica cartões e linhas sem deduzir dispositivos pelo total de conexões.

## E se faltar uma máquina ou houver nomes iguais?

Nomes iguais sob o mesmo endereço agrupam os shares registrados. Renomeie uma instalação pela opção de nome e reinicie esse minerador. Novos envios usam a nova identificação; os antigos mantêm a anterior. A linha antiga pode aparecer enquanto o histórico estiver no período exibido. Renomear não move o saldo ganho pelo endereço.

Para uma máquina ausente, compare endereço realmente resolvido, servidor e nome com a página aberta. Confira a conexão e os shares aceitos. CPU e GPU dentro do mesmo processo do seine não precisam de nomes separados neste fluxo. Para shares rejeitados ou atrasados, siga o [guia de estados dos shares](/blog/pt-br/accepted-rejected-stale-shares/) em vez de trocar repetidamente o endereço da carteira.

## Como funcionam recompensas e pagamentos conjuntos?

O trabalho válido dos computadores contribui para a participação do mesmo endereço na janela PPLNS. Nomes ajudam a acompanhar contribuições, sem criar saldos separados. Outra máquina soma o trabalho útil que realmente enviar, não uma recompensa fixa por dispositivo ou nome. Hardware e tempo ativo podem gerar contribuições diferentes; mais nomes não aumentam o trabalho por si só.

O [guia PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica a divisão ao encontrar um bloco. As regras de confirmação e pagamento continuam valendo para o saldo conjunto. Não é preciso juntar carteiras nem transferir saldos entre nomes. Antes de deixar funcionando, confira destino, nome distinto e shares aceitos em cada computador. Reutilize essa lista ao adicionar o próximo.

<CTA target="pool" title="Conecte outro computador ao bntpool.com" text="Use um endereço de recebimento, dê um nome distinto a cada instalação e confira seus shares aceitos." />
