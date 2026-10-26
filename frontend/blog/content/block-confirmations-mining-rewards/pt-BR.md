---
title: 'Confirmações de blocos e recompensas de mineração imaturas'
description: 'Acompanhe um bloco Blocknet até a maturidade e o pagamento. Entenda a contagem de profundidade do bntpool e por que as confirmações são medidas em blocos.'
tags: ['Recompensas de mineração', 'Confirmações', 'Blocknet']
faq:
  - q: 'Quantas confirmações o bntpool exige?'
    a: 'Na verificação de 26 de outubro de 2026, bntpool.com publica 60 blocos. O controle subtrai a altura encontrada da atual e compara o hash com o nó. Consulte Get Started ou a API pública para o valor atual.'
  - q: '60 blocos significam exatamente cinco horas?'
    a: 'Não. Sessenta intervalos-alvo de cinco minutos dão uma estimativa de 300 minutos, cinco horas. O avanço real depende de novos blocos, cujos intervalos variam. Maturidade, limite de pagamento e transação posterior são etapas diferentes.'
  - q: 'Preciso continuar minerando para uma recompensa amadurecer?'
    a: 'Um bloco encontrado ganha profundidade com os blocos da rede, mesmo com seu minerador desconectado. Sua participação depende das regras contábeis do pool. Parar não acelera nem reinicia a maturidade; acompanhe o registro em My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Acompanhe um bloco Blocknet pela confirmação, maturidade e pagamento à carteira. Este guia explica a profundidade por trás dos cartões do bntpool.com. O [guia PPLNS](/blog/pt-br/how-pplns-payouts-work/) cobre a divisão; o [guia de configuração](/blog/pt-br/mine-blocknet-on-bntpool/) ensina a começar.

## O que são confirmações e recompensas imaturas?

<Answer>
Confirmações descrevem a posição de um bloco abaixo da ponta atual. Uma recompensa imatura vem de um bloco recente que ainda não atingiu a profundidade exigida. O bntpool verifica profundidade e identidade antes de confirmar; elegibilidade do saldo e pagamento seguem separadamente.
</Answer>

*O bloco ganha profundidade, alcança a maturidade e passa pelo saldo elegível até um pagamento separado.*

## Como o bntpool conta a profundidade?

O pool subtrai a altura do bloco encontrado da altura atual. Em 26 de outubro de 2026, a exigência publicada era 60 blocos. Nessa convenção, o bloco começa em zero. Outra página pode contar o próprio bloco como primeira confirmação; compare a regra e o número.

Exemplo: um bloco em 70.000 tem profundidade zero com ponta 70.000, um em 70.001 e 60 em 70.060. São alturas ilustrativas. O pool também compara o hash com seu nó. A [implementação](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) registra essas verificações.

## Por que cinco horas é uma estimativa?

Blocknet tem intervalo-alvo de cinco minutos. Sessenta vezes cinco são 300 minutos, cinco horas. Ajuda a planejar, mas a condição é uma quantidade de blocos. Observe o avanço da cadeia desde a altura encontrada até a profundidade necessária, em vez de um prazo fixo no relógio.

Qualquer bloco da cadeia aceita aumenta a profundidade; não precisa vir do bntpool. A velocidade do seu minerador não cria um relógio próprio. Os intervalos variam e o tempo pode diferir da estimativa. Os guias de [dificuldade](/blog/pt-br/mining-difficulty-explained/) e [sorte](/blog/pt-br/mining-pool-luck-explained/) explicam ajuste da rede e variação das rodadas.

## Separe pendente, confirmado e pago

Um bloco registrado fica pendente enquanto ganha profundidade. Sua parte estimada pode aparecer em [My Stats](/stats) se houver trabalho elegível. A recompensa cria moedas; a regra coinbase da carteira Blocknet exige profundidade de 60 blocos antes de gastá-las. A espera correspondente do pool acompanha esse processo.

Após verificar profundidade e identidade, o pool pode confirmar e distribuir a recompensa aos saldos elegíveis. Isso não envia pagamentos imediatamente a todas as contas. O processo avalia separadamente saldos confirmados e condições de envio. Estado do bloco, saldo e histórico mostram partes diferentes do fluxo.

## Por que verificar o hash do bloco?

Altura identifica uma posição; hash identifica um bloco específico. Nós podem conhecer blocos concorrentes, e a cadeia aceita define qual ocupa a posição. O pool consulta o nó e compara o hash. Só a profundidade não prova que o bloco original continua na cadeia aceita.

Um bloco que sai dessa cadeia pode ser marcado órfão, e sua recompensa provisória deixa de ser elegível como recompensa da cadeia aceita. Esse estado é tratado separadamente. Use o link do hash em [Blocos](/blocks); duas entradas na mesma altura não precisam ser o mesmo evento. A estimativa permanece vinculada ao bloco.

## Como o pagamento difere da recompensa minerada?

A recompensa começa na saída coinbase do bloco. Um pagamento do pool é uma transação posterior para seu endereço. Na data verificada, o mínimo publicado é 0,1 BNT. Maturidade e limite de saldo são condições diferentes; o [guia PPLNS](/blog/pt-br/how-pplns-payouts-work/) detalha distribuição e pagamento.

Depois do envio, acompanhe a transação e o estado de recebimento e disponibilidade na carteira. As confirmações pertencem ao pagamento, não ao bloco original. Não é necessário trocar de endereço nem reconectar o minerador durante a maturação. Compare [histórico de pagamentos](/payouts) e carteira com o mesmo endereço previsto.

## Como acompanhar a recompensa inteira?

Em [Blocos](/blocks), identifique altura, hash e estado. Abra [My Stats](/stats) com o endereço do minerador e veja estimativas e recompensas confirmadas. Quando houver pagamento, siga sua transação no histórico. Cada observação corresponde a bloco, saldo da conta ou pagamento à carteira.

Confira as condições em [Get Started](/start) ou `https://bntpool.com/api/info` e o [estado do pool](/status). Para investigar, registre endereço, identificador de bloco ou transação e horário; nunca compartilhe palavras de recuperação. O [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/) explica o restante. Blocos ganham profundidade mesmo offline; elegibilidade segue trabalho registrado e cadeia.

<CTA target="pool" title="Acompanhe suas recompensas" text="Conecte-se ao bntpool.com e acompanhe estimativas, saldos confirmados e pagamentos em My Stats." />
