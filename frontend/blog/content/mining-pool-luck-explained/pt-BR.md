---
title: 'Sorte no pool de mineração: esforço e tempo entre blocos'
description: 'Entenda a sorte do pool, o esforço de 100% e rodadas longas. Consulte o histórico da bntpool.com e separe a descoberta de blocos do recebimento de pagamentos.'
tags: ['Pools de mineração', 'Sorte do pool', 'Blocknet']
faq:
  - q: 'Esforço de 100% significa que o pool está prestes a encontrar um bloco?'
    a: 'Não. Significa que o trabalho registrado pelas shares atingiu a quantidade esperada para um bloco na dificuldade usada naquele cálculo. É uma comparação com uma média, não um percentual de conclusão. Cada novo hash ainda precisa atender ao alvo da rede.'
  - q: 'Um percentual de sorte menor é melhor na bntpool.com?'
    a: 'Leia primeiro o rótulo e a definição. A coluna Esforço e o cartão Sorte média da bntpool.com expressam trabalho em relação ao esperado; valores menores significam menos trabalho registrado por bloco. Outros sites podem usar a convenção inversa. Compare métricas e tamanhos de amostra equivalentes, não apenas percentuais.'
  - q: 'Uma rodada longa aumentará meu próximo pagamento?'
    a: 'Uma rodada longa não garante um pagamento individual maior. A descoberta de blocos determina quando uma recompensa fica disponível, enquanto a contabilidade PPLNS do pool determina sua parte pelo trabalho aceito elegível. Depois se aplicam as regras de confirmação e pagamento. Consulte seus saldos em Minhas estatísticas e a sequência no guia de pagamentos.'
sourceHash: 'f7051fa67591'
translation: draft
---

O painel do pool mostra tanto o trabalho de mineração quanto os blocos encontrados por esse trabalho. Este guia explica como ler os percentuais de esforço, o tempo esperado entre blocos e o histórico de rodadas da bntpool.com. Você poderá distinguir uma média estatística de uma contagem regressiva e verificar sua conexão de mineração sem esperar pelo próximo bloco do pool.

## O que é sorte em um pool de mineração?

<Answer>
A sorte de um pool descreve como a descoberta real de blocos se compara à média esperada pelo trabalho realizado. Os blocos chegam aleatoriamente: alguns exigem pouco trabalho, outros mais. Na bntpool.com, o esforço da rodada compara o trabalho das shares aceitas com a dificuldade do bloco; esforço menor significa que a rodada encontrou seu bloco com menos trabalho registrado.
</Answer>

Um pool reúne as tentativas de muitos mineradores, mas ainda precisa encontrar um hash que atenda ao alvo da rede. Shares aceitas medem contribuições em um alvo mais fácil e ajudam o pool a estimar quanto trabalho de hash ocorreu entre descobertas. A [introdução aos pools](/blog/pt-br/what-is-a-mining-pool/) explica essa relação; a sorte descreve a variação dos blocos resultantes.

## Como o pool calcula o esforço?

Para uma rodada concluída, a bntpool.com divide o trabalho das shares aceitas pela dificuldade do bloco e multiplica por 100. O trabalho das shares é ponderado pela dificuldade, não sendo apenas a quantidade enviada. Uma share com dificuldade atribuída maior representa mais trabalho de hash esperado que outra de dificuldade menor.

**Esforço (%) = trabalho das shares aceitas ÷ dificuldade do bloco × 100**

Por exemplo, suponha que um bloco tenha dificuldade 10.000 e o pool registre 5.000 unidades de trabalho aceito durante a rodada. O esforço é de 50%. Com 20.000 unidades de trabalho na mesma dificuldade, seria de 200%. São exemplos aritméticos, não medições de um bloco específico em produção.

| Esforço | Como interpretar |
|---|---|
| 50% | Metade do trabalho esperado para essa dificuldade |
| 100% | A quantidade de trabalho esperada para um bloco |
| 200% | O dobro do trabalho esperado para essa dificuldade |

O esforço da rodada atual usa a dificuldade atual, que pode mudar conforme a cadeia avança. As linhas históricas usam a dificuldade de seus blocos. Portanto, o número compara trabalho; não é um indicador de progresso que precise parar em 100%. Também difere da duração da rodada: um pool mais rápido pode executar o mesmo trabalho em menos tempo.

## O que significa tempo esperado entre blocos?

É o intervalo médio sugerido pela participação estimada do pool no hashrate da rede. O painel da bntpool.com o calcula como 300 segundos multiplicados pelo hashrate da rede e divididos pelo hashrate do pool. Use a mesma unidade para ambos. O [guia de hashrate](/blog/pt-br/what-is-hashrate/) explica por que essas estimativas mudam com a chegada de shares aceitas e novos blocos.

**Segundos esperados = 300 × H/s da rede ÷ H/s do pool**

Se a rede for estimada em 100.000 H/s e o pool em 10.000 H/s, o pool tem um décimo do total estimado. Seu intervalo esperado é de 3.000 segundos, ou 50 minutos. Essa média pode representar uma combinação de rodadas curtas e longas; ela não agenda o próximo bloco para exatamente 50 minutos a partir de agora.

O painel também compara o tempo decorrido com o esperado. Esse percentual baseado em tempo e o esforço baseado em shares podem diferir porque usam dados distintos. Confira o rótulo antes de comparar dois números. Mudanças no hashrate do pool ou na dificuldade da rede podem alterar a estimativa atual mesmo durante a mesma rodada.

## Um bloco fica garantido após 100% de esforço?

<Answer>
O pool não passa a ter direito a um bloco ao chegar a 100% de esforço. Hashes anteriores sem sucesso não tornam o próximo mais propenso a atender a um alvo de rede inalterado. Com hashrate e dificuldade constantes, a espera esperada a partir de agora continua igual, mesmo após uma rodada longa. Mais trabalho gera mais tentativas, não um prazo.
</Answer>

Um modelo simples de probabilidade ajuda a visualizar isso. Com tentativas independentes e uma taxa constante de descoberta de blocos, a espera é aproximadamente exponencial. A chance de ainda não haver bloco após um intervalo esperado é de cerca de 36,8%; após dois, 13,5%; após três, 5,0%. Esses percentuais descrevem muitas rodadas hipotéticas sob condições fixas, não uma previsão da rodada atual.

| Intervalos esperados decorridos | Chance aproximada de ainda não haver bloco |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

O cálculo é `exp(-t / T)`, em que `T` é o intervalo médio e `t` é o tempo decorrido. A [referência do NIST sobre a distribuição exponencial](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) apresenta essa função de sobrevivência. Hashrate e dificuldade reais variam, então trate a tabela como uma explicação da aleatoriedade. Uma sequência de blocos rápidos também mantém inalterada a probabilidade do próximo hash quando o alvo permanece igual.

## Como ler o histórico de sorte da bntpool.com?

Abra [Sorte](/luck) para comparar rodadas concluídas. Cada linha mostra um bloco, esforço, duração da rodada, status e horário da descoberta. Observe várias rodadas juntas, em vez de escolher uma excepcionalmente curta ou longa. O link do bloco permite examinar o registro correspondente na cadeia, e a paginação permite consultar um histórico mais amplo.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Página real do histórico de sorte da bntpool.com com esforço, duração da rodada e status dos blocos" width="1200" height="833" loading="lazy" />

Esta captura foi feita em 26 de outubro de 2026. As linhas e os status são um registro histórico; use a página ao vivo para obter valores atuais. O status pendente ou confirmado descreve a confirmação do bloco. Ele não altera o significado do percentual de esforço registrado e é separado de um pagamento ao seu endereço.

No painel, Sorte média é a média aritmética dos percentuais de esforço registrados para blocos não órfãos até a altura atual da cadeia. Apesar do nome do cartão, segue a convenção de esforço: menor significa menos trabalho registrado por bloco. Médias resumem essa amostra; não prometem que rodadas futuras compensarão algum resultado anterior específico.

## Como a sorte do pool afeta suas recompensas?

A sorte do pool muda quando os blocos são encontrados. O PPLNS determina como distribuir a recompensa de um bloco encontrado entre o trabalho aceito elegível. São etapas distintas. O limite entre rodadas não transforma a janela PPLNS em uma regra de pagamento restrita à rodada; leia [como funcionam os pagamentos PPLNS](/blog/pt-br/how-pplns-payouts-work/) para conhecer a janela, as confirmações e o limite mínimo reais.

Por isso, seu saldo pode mudar em etapas mesmo enquanto seu minerador continua enviando shares aceitas. A [calculadora de recompensas](/blog/pt-br/estimate-blocknet-mining-rewards/#mining-calculator) estima a produção a partir do hashrate e dos dados da rede. É útil para expectativas ao longo do tempo; não prevê o bloco nem o momento em que um pagamento chegará à carteira.

## O que verificar durante uma rodada longa?

Verifique o que você observa diretamente: o minerador está rodando, as shares continuam sendo aceitas e o endereço correto aparece em [Minhas estatísticas](/stats). Use [Status](/status) para consultar a saúde pública do serviço. Se suas próprias shares aceitas pararem, siga o [guia de diagnóstico das estatísticas](/blog/pt-br/track-your-miner-on-bntpool/#zero-hashrate); uma rodada longa do pool e uma máquina desconectada exigem verificações diferentes.

Com a conexão funcionando, interprete a sorte ao longo de um histórico significativo e mantenha as mesmas definições. Assim cada número do painel tem um papel claro: o hashrate estima trabalho por segundo, o esforço compara trabalho com o esperado para um bloco e os saldos mostram a contabilidade das recompensas. Nada disso exige adivinhar quando chegará o próximo hash vencedor.

<CTA target="pool" title="Comece a minerar na bntpool.com" text="Conecte o seine, confirme suas shares aceitas e acompanhe rodadas e recompensas no painel do pool." />
