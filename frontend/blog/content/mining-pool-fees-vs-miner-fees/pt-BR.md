---
title: 'Taxas do pool e taxas do desenvolvedor do minerador'
description: 'Entenda as taxas do pool e do minerador no Blocknet. Veja onde cada uma se aplica, como combiná-las e como evitar descontar duas vezes a mesma taxa.'
tags: ['Guia de mineração', 'Taxas do pool', 'Recompensas']
faq:
  - q: 'São a mesma cobrança?'
    a: 'Não. A taxa do pool faz parte da distribuição; a do desenvolvedor pertence ao software. seine v0.2.36 dedica 2% do tempo sujeito à taxa ao desenvolvimento após a carência inicial. A taxa separada publicada pelo bntpool.com era 0% em 26 de outubro de 2026.'
  - q: 'Como combinar as taxas?'
    a: 'Multiplique a estimativa bruta por um menos a fração do minerador e depois por um menos a do pool. Use sua versão real e o ajuste atual. Isso estima recompensas retidas, sem garantir pagamento nem calcular lucro em dinheiro.'
  - q: 'Devo descontar a taxa do hashrate informado pelo pool?'
    a: 'Identifique o significado da taxa de hash. Se já estima trabalho creditado ao seu endereço após o tempo do desenvolvedor, outro desconto seria duplicado. O calculador aceita zero para esse ajuste já refletido. Não desconte novamente de saldo confirmado ou pagamento registrado.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Identifique as duas camadas de taxas de uma estimativa Blocknet e onde atuam. O guia usa seine v0.2.36 e ajustes públicos do bntpool conferidos em 26 de outubro de 2026. Ajuda a preencher o [calculador](/blog/pt-br/estimate-blocknet-mining-rewards/#mining-calculator) e entender [My Stats](/stats).

## Qual a diferença entre taxa do pool e do minerador?

<Answer>
A taxa do pool pertence à distribuição da recompensa; a do desenvolvedor pertence à programação do software. Responsáveis e etapas diferem. Confira ambas, identifique entrada bruta ou já ajustada e aplique cada ajuste pertinente uma vez.
</Answer>

*O trabalho passa por um ajuste de tempo do minerador e outro de recompensa do pool antes do valor retido estimado.*

## Sobre o que incide a taxa do pool?

O pool reúne trabalho elegível e divide a recompensa distribuível pelas suas regras. bntpool calcula a taxa configurada sobre a recompensa registrada do bloco antes de distribuir o restante por PPLNS. O [guia PPLNS](/blog/pt-br/how-pplns-payouts-work/) explica participação e créditos.

Em 26 de outubro de 2026, `https://bntpool.com/api/info` publica `pool_fee_pct` como 0,0, ou 0%. Confira o valor atual em [Get Started](/start) ou na API ao atualizar estimativas. Zero descreve esse ajuste do pool; a política do software continua separada.

## Como funciona a taxa de seine?

A v0.2.36 documenta 2% fixos em qualquer pool e em solo. É tempo de mineração para o endereço do desenvolvedor, não retirada posterior da carteira. Os primeiros 10 minutos são livres; o rastreador conta o tempo após a carência. O [guia de configuração](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) descreve a política.

A programação funciona por rodadas, então uma janela curta pode não dar exatamente 2%. É o objetivo de longo prazo após a carência, não promessa por minuto ou bloco. Guarde a versão: guias antigos ou outros programas podem diferir. A taxa do pool não substitui esse dado do software.

## Como combinar porcentagens?

Use frações retidas. Com m para minerador e p para pool, em frações, mantém-se `(1 - m) * (1 - p)` do bruto. É a conta do [estimador](/blog/pt-br/estimate-blocknet-mining-rewards/). Somar porcentagens não é regra geral: o segundo ajuste incide sobre o restante.

Exemplo apenas aritmético: 100 BNT brutos hipotéticos, ajuste do minerador de 2% e pool verificado em 0% dão 100 × 0,98 × 1,00 = 98 BNT. Ilustra o regime estável, não resultado medido, recompensa prometida ou sessão curta com carência inicial.

## Escolha o hashrate correto

Uma taxa local ou de benchmark descreve trabalho na máquina. Antes do fator do desenvolvedor, determine se inclui todo o tempo ou apenas trabalho para seu endereço. Leia origem e janela temporal. O [guia de hashrate](/blog/pt-br/what-is-hashrate/) explica visões local, do pool e da rede.

A taxa do pool para seu endereço pode refletir o tempo creditado a outro. Se já inclui esse ajuste, coloque zero na taxa adicional do minerador. Isso muda a conta, não a política real. A taxa do pool ainda se aplica ao estimar recompensa elegível bruta; confira o que cada número inclui.

## Por que não descontar novamente do saldo?

Saldo confirmado é resultado contábil, não estimativa bruta intacta. O pool já atribuiu créditos. Subtrair a mesma taxa outra vez do saldo ou pagamento muda o número sem outro evento de cobrança. Use o valor mostrado para conciliar e identifique a etapa antes de estimar.

Recompensa pendente, saldo confirmado e pagamento enviado também têm tempos distintos. Maturidade e limite de pagamento explicam transições; não são porcentagens extras. Consulte o [guia de confirmações](/blog/pt-br/block-confirmations-mining-rewards/) e o [histórico](/payouts). A eventual taxa própria de uma transação da carteira é campo separado.

## Guarde as entradas da estimativa

Salve data, versão, taxas, origem do hashrate e janela. O calculador combina dados atuais do pool/rede com suas entradas; atualize-os ao revisitar. Créditos reais seguem shares elegíveis, blocos e PPLNS. Resultado da fórmula e lançamento posterior respondem perguntas diferentes.

Use [My Stats](/stats) e o [guia de acompanhamento](/blog/pt-br/track-your-miner-on-bntpool/) para registros. Compare mesmo endereço, período e etapa antes de alterar taxas. O calculador estima BNT; eletricidade, equipamento e preço em dinheiro ficam fora da conta. Não precisa mudar o endereço.

<CTA target="pool" title="Acompanhe recompensas no bntpool.com" text="Use o calculador para estimativas e My Stats para trabalho registrado, saldos e pagamentos." />
