---
title: Acompanhe seu minerador no bntpool.com
description: Consulte endereço ou @name no bntpool.com para ver hashrate, shares, saldo e pagamentos. Acompanhe blocos, sorte e disponibilidade ou use a API pública.
tags: [bntpool, Estatísticas de mineração, API do pool, Blocknet]
sourceHash: 'bde59962f8d3'
translation: draft
faq:
  - q: 'Por que minha hashrate no bntpool.com difere da mostrada pelo seine?'
    a: 'O seine conta cada hash calculado. O pool estima sua hashrate pela dificuldade dos shares aceitos na última hora. Shares chegam em momentos aleatórios, então o valor do pool oscila mais e se estabiliza com o tempo. Compare o gráfico de 24 horas de My Stats com a média do seine.'
  - q: 'Posso consultar minhas estatísticas com um @name?'
    a: 'Sim. Digite seu shortname Blocknet ID, como @name ou $name, na busca de My Stats. A página resolve via blocknet.id, mostra as estatísticas e coloca o endereço completo resolvido na caixa de busca. Use esse endereço completo ao chamar a API em scripts.'
  - q: 'A API do bntpool.com exige chave?'
    a: 'Não. Estatísticas, blocos, pagamentos, sorte, status e endpoints individuais são JSON público por HTTPS. curl ou um painel doméstico podem ler sem cadastro. Valores vêm em unidades atômicas: divida por 100.000.000 para BNT e consulte a cada 30 a 60 segundos.'
  - q: 'Por que My Stats mostra zero em Estimated Rewards?'
    a: 'As estimativas aparecem quando o pool encontra um bloco enquanto seus shares estão na janela de pagamento. Para um novo minerador, isso pode levar algumas rodadas. Primeiro aparecem shares aceitos e hashrate, depois recompensas estimadas, que amadurecem para Confirmed Rewards e são pagas automaticamente.'
---

Quando o seine começa a minerar no bntpool.com, o pool mostra tudo que vê: hashrate, cada share aceito, ganhos e pagamentos enviados ao seu endereço. Este guia percorre My Stats, as páginas públicas do pool e a API para scripts e painéis domésticos.

Se ainda não começou, o [guia de instalação do seine](https://seineminer.com/pt-br/blog/how-to-mine-blocknet/) leva aos primeiros shares em poucos minutos.

## Como consultar as estatísticas no bntpool.com?

<Answer>
Abra My Stats em bntpool.com/stats, cole seu endereço Blocknet ou @name e pressione Lookup. A página mostra hashrate, shares aceitos e rejeitados, saldos estimado, confirmado e pago, gráfico de hashrate, uma linha por rig e pagamentos recentes. Ela se atualiza automaticamente enquanto a aba estiver aberta.
</Answer>

Em [My Stats](/stats), informe o mesmo endereço ou [shortname Blocknet ID](https://seineminer.com/pt-br/blog/blocknet-id-pay-by-name/) usado no seine. Nomes como `@name` ou `$name` são resolvidos via blocknet.id, e a busca passa a mostrar o endereço completo correspondente. A página lembra a última consulta naquele navegador; na próxima visita, seus números já estão lá.

## O que My Stats mostra?

A página organiza cartões e tabelas em grupos, cada um respondendo uma pergunta sobre seu minerador.

### Saldo

- **Estimated Rewards:** sua parte dos blocos encontrados que ainda estão amadurecendo.
- **Confirmed Rewards:** saldo maduro aguardando o próximo pagamento.
- **Paid Balance:** tudo que já foi enviado ao seu endereço.

Quando há recompensas pendentes, **Estimated From Recent Blocks** lista cada bloco com altura, crédito estimado, confirmações restantes e horário do encontro. A altura leva ao bloco no [explorador Blocknet](https://explorer.blocknetcrypto.com), permitindo conferir. [Como funcionam os pagamentos PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica a divisão e o momento do pagamento.

### Mineração e shares

Os cartões **Mining** mostram **Hashrate**, quantos **Blocks Found** seus próprios rigs resolveram para o pool e a data de início em **Mining Since**. Abaixo ficam **Shares Accepted**, **Shares Rejected**, **Reject Rate** e **Avg Difficulty**. Um rig saudável envia shares aceitos regularmente, com rejeição perto de zero.

**Hashrate History** mostra a hashrate em 1h, 24h, 7d ou 30d. Intervalos maiores ajudam a ver se o rig funcionou a noite inteira.

### Uma linha por rig

Se usa várias máquinas, dê um nome a cada uma. O pool as lista separadamente, com hashrate, shares aceitos e rejeitados e horário do último share. Na primeira execução, o seine escolhe `seine-` seguido de quatro dígitos. Você pode definir outro nome pela linha de comando:

```sh
./seine --pool-worker rig-01
```

Um rig com versão antiga recebe o aviso **update available**, que leva à [página de releases do seine](https://github.com/zpalmtree/seine/releases). Assim é fácil encontrar uma máquina esquecida nas atualizações.

### Pagamentos e shares recentes

**Recent Payouts** lista pagamentos ao seu endereço com valor, link da transação no explorador e status queued, unconfirmed ou confirmed. **Recent Shares** mostra os últimos 20 shares com job, rig, dificuldade e status. É a forma mais rápida de confirmar que uma máquina recém-iniciada alcança o pool.

## Por que a hashrate do pool difere da do seine?

O seine conta todos os hashes calculados. O pool só vê shares e estima a hashrate pela dificuldade total dos aceitos na última hora. Cada share é aleatório. O bntpool.com define a dificuldade por conexão buscando cerca de 10 shares a cada 5 minutos; uma janela curta contém algum ruído.

Dois hábitos ajudam a comparar:

- Compare médias: o gráfico de 24h de My Stats com a média do seine no mesmo intervalo.
- Dê tempo ao rig novo. No início, o pool calcula a média pelos minutos já minerados, com poucos shares. Após uma hora, a janela completa passa a valer.

A tabela Recent Shares confirma imediatamente a chegada de trabalho, antes de a hashrate se estabilizar.

<span id="zero-hashrate"></span>

## Por que o seine está minerando, mas My Stats mostra hashrate zero?

Comece pelo endereço de pagamento, pela conexão com o pool e pelas shares aceitas. O hashrate local indica que a máquina calcula hashes; a estimativa do pool depende de shares aceitas para o endereço consultado.

1. **Confira o endereço.** Consulte o mesmo endereço de recebimento ou `@name` registrado usado pelo seine. Se mudou o endereço de um nome curto, reinicie o seine para resolvê-lo novamente e consulte o endereço resultante em My Stats.
2. **Confira o pool.** Verifique se o seine usa `stratum+tcp://bntpool.com:3333`. Uma máquina minerando em outro lugar ou no modo solo não envia shares para cá. O [guia de configuração do pool](/blog/pt-br/mine-blocknet-on-bntpool/) explica a conexão.
3. **Confira Recent Shares.** Aguarde shares aceitas e a atualização da página. As shares chegam em intervalos variáveis; um hashrate local ativo não garante uma estimativa imediata do pool. Se há shares aceitas chegando, compare as médias ao longo do tempo.
4. **Leia a mensagem de conexão.** Use a tabela enquanto o minerador ainda não envia shares aceitas. Consulte [Status](/status) para verificar incidentes atuais.

| Mensagem no seine | O que verificar |
|---|---|
| `failed to connect to pool at` | Confira o nome do servidor e a porta, sua conexão e se conexões TCP de saída para a porta 3333 são permitidas. |
| `login rejected:` | Leia o motivo que aparece em seguida; confira o endereço de recebimento ou nome curto e use a versão atual do seine. |
| `stale job` | A share corresponde a uma tarefa antiga. Observe novas tarefas e shares aceitas; se continuar, confira a conexão e a versão do minerador. |
| `submit interrupted during reconnect` | O seine está se reconectando. Observe a aceitação do login e novas shares aceitas depois que a conexão voltar. |

Zero em **Estimated Rewards** é outra questão: shares podem ser aceitas antes que um bloco do pool credite recompensas. O [guia de PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica essa sequência.

## Quais páginas públicas mostram o pool inteiro?

Elas não exigem busca e mostram publicamente toda a atividade. O [painel](/) reúne os números principais: mineradores conectados, hashrates do pool e da rede, blocos encontrados e sorte média.

### Blocos

[Blocks](/blocks) lista todos os blocos encontrados pelo bntpool.com, mais recentes primeiro: altura com link ao explorador, recompensa, esforço, duração da rodada, status e horário. Filtre por Confirmed, Pending ou Orphaned para acompanhar a situação.

### Pagamentos

[Payouts](/payouts) lista cada lote de pagamentos com total, número de mineradores pagos, links das transações e status. O lote aparece como unconfirmed ao ser transmitido e vira confirmed pouco após a primeira confirmação de bloco. Você acompanha o pagamento pelo pool e pelo explorador.

### Sorte

[Luck](/luck) mostra o esforço de cada rodada: trabalho dos shares aceitos dividido pela dificuldade do bloco. 100% significa exatamente o trabalho esperado; menos significa que o pool encontrou o bloco antes. Rodadas individuais variam muito nos dois sentidos, então acompanhe **Average Luck** no painel. A tabela **Pool Luck History** leva a essa página por **View All**.

### Status

[Status](/status) mostra a saúde do pool: disponibilidade do pool, HTTP público, API, Stratum, banco e daemon, idade do modelo de trabalho, altura da cadeia, uptime nos últimos 10 minutos, 6 horas, 24 horas e 7 dias e histórico de incidentes. A disponibilidade é medida por um monitor no servidor e por uma sonda externa na Cloudflare.

## Como usar a API do bntpool.com?

<Answer>
A API é JSON público por HTTPS, sem chave. Chame https://bntpool.com/api/miner/ seguido do seu endereço para hashrate, rigs, shares e pagamentos. Acrescente /balance para saldos e use /api/stats para hashrates do pool e da rede. Valores estão em unidades atômicas: divida por 100.000.000 para BNT.
</Answer>

Todas as páginas acima leem estes endpoints. Qualquer dado mostrado no site pode ser obtido diretamente:

| Endpoint | Retorno |
|---|---|
| `/api/stats` | Mineradores conectados, hashrate do pool, blocos encontrados, hashrate da rede, altura atual |
| `/api/stats/history?range=24h` | Histórico de hashrate do pool (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Rodada atual, esforço médio, rodadas recentes |
| `/api/info` | Configurações: porta Stratum, sistema e janela de pagamento, mínimo |
| `/api/blocks?status=confirmed` | Blocos encontrados, filtráveis por status |
| `/api/payouts/recent` | Lotes de pagamentos com hashes de transação |
| `/api/luck` | Esforço e duração de cada rodada |
| `/api/status` | Saúde dos serviços, janelas de uptime, incidentes |
| `/api/miner/{address}` | Sua hashrate, totais, rigs, últimos shares e pagamentos |
| `/api/miner/{address}/balance` | Saldos estimado, confirmado e pago |
| `/api/miner/{address}/hashrate?range=24h` | Seu histórico de hashrate |

Endpoints de listas aceitam `limit`, padrão 25 e máximo 200, e `offset`, retornando `items` e `total`. Endpoints de mineradores exigem endereço completo. Após buscar um @name em My Stats, copie o endereço da caixa de busca.

Para consultar seu minerador no terminal:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Com `jq`, extraia valores individuais, como hashrate e shares aceitos, ou o total pago em BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Mostre seu minerador em um painel doméstico

O mesmo endpoint funciona como sensor. No Home Assistant, a integração RESTful lê os dados com algumas linhas em `configuration.yaml`:

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Consultas a cada 30 a 60 segundos bastam. A hashrate é uma média horária, e My Stats atualiza seus detalhes a cada 30 segundos. A API limita requisições por cliente. Se um script receber HTTP 429, deve pausar e esperar o tempo do cabeçalho `Retry-After` antes de tentar novamente.

## E o painel do próprio seine?

O pool mostra o que foi creditado. Para acompanhar a máquina ao vivo, o painel do terminal do seine mostra hashrate e shares no momento em que ocorrem. O minerador também oferece API local de controle com REST, eventos enviados pelo servidor e métricas Prometheus em `/metrics`, vinculada a `127.0.0.1:9977` por padrão. O [guia da API de controle](https://github.com/zpalmtree/seine/blob/master/docs/API.md) explica opções e endpoints. Muitos usam ambos: métricas do seine por máquina e API do pool para saldos e pagamentos.

Ainda não conhece o seine? [Baixe-o](https://seineminer.com/pt-br/#download), conecte ao pool padrão e seu endereço aparecerá em My Stats poucos minutos após o primeiro share aceito.

<CTA target="pool" title="Comece a minerar e acompanhe ao vivo" text="Entre no bntpool.com em poucos minutos e acompanhe hashrate, shares e saldo em My Stats." />
