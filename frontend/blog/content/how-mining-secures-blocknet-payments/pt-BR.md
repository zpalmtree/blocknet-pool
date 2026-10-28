---
sourceHash: 77522359a409
translation: draft
title: Como a mineração protege os pagamentos Blocknet
description: Entenda como Blocknet escolhe o histórico de pagamentos por prova de trabalho, o papel das confirmações e o que os nós verificam antes de aceitar blocos.
tags: [Blocknet, Segurança de pagamentos, Prova de trabalho]
faq:
  - q: Mais poder de mineração pode autorizar um pagamento da minha carteira?
    a: Poder de mineração ajuda a produzir blocos concorrentes. Gastar fundos ainda exige autorização válida, e os nós verificam provas e equilíbrio das transações antes de aceitar um bloco. Uma ramificação com mais trabalho precisa cumprir essas regras. Prova de trabalho não substitui as chaves privadas necessárias para gastar suas saídas.
  - q: Blocknet segue a cadeia com mais blocos?
    a: Core v0.22.0 calcula trabalho acumulado somando a dificuldade de cada bloco. Uma ramificação válida com mais trabalho pode se tornar a cadeia escolhida, sujeita às regras de reorganização. A contagem de blocos não basta, pois suas dificuldades podem ser diferentes. Tanto trabalho quanto validade importam.
  - q: Confirmações de pagamentos e shares do pool são a mesma coisa?
    a: Um share registra trabalho enviado ao pool. Uma confirmação trata da inclusão de uma transação na cadeia escolhida e dos blocos construídos depois. Shares ajudam a distribuir recompensas; blocos da rede avançam o histórico de pagamentos. Aumentar seus shares não adiciona diretamente confirmações a um pagamento específico.
---

Acompanhe um pagamento em BNT desde sua inclusão em um bloco até um histórico apoiado pelo trabalho de mineração posterior. Este guia explica por que a rede precisa escolher entre históricos concorrentes, o que as confirmações indicam e como essa escolha funciona junto da validação de transações privadas. A implementação descrita é core v0.22.0, verificada em 28 de outubro de 2026.

## O que a mineração protege quando você envia BNT?

<Answer>
A mineração ajuda a rede a escolher um histórico de pagamentos compartilhado e ordenado. Cada bloco aceito se conecta ao anterior e contribui com prova de trabalho. Os nós verificam blocos e transações propostos e comparam ramificações válidas pelo trabalho acumulado, conforme suas regras de reorganização. Blocos posteriores acrescentam trabalho atrás de um pagamento incluído, dando significado prático às confirmações.
</Answer>

Um pagamento válido precisa de autorização e de um lugar no registro. Sua carteira fornece a autorização para gastar seus fundos. Mineração e seleção da cadeia ajudam os nós a concordar sobre quais transações válidas pertencem ao histórico atual. Essas funções se complementam: a prova de trabalho de um bloco não torna aceitável um pagamento inválido.

O [guia de aplicação das regras](https://seineminer.com/pt-br/blog/who-controls-blocknet/) explica quem desenvolve, propõe e valida. Aqui o foco é o próprio pagamento: como sua inclusão passa a integrar o histórico seguido pela sua carteira e pelos outros nós.

## Como os blocos vinculam um histórico de pagamentos?

O cabeçalho inclui o hash do bloco anterior e uma raiz de Merkle derivada das transações. A raiz vincula criptograficamente o conteúdo, enquanto o hash anterior conecta o bloco ao histórico precedente. Alterar uma transação muda a raiz e o cabeçalho minerado. Os blocos descendentes precisam então se referir ao histórico alterado por seus próprios vínculos.

Imagine um pagamento incluído em um bloco, seguido por vários outros blocos aceitos. Substituir essa parte do histórico exige uma ramificação concorrente com blocos válidos conectados e trabalho acumulado suficiente, dentro das regras de reorganização do nó. Editar uma transação em um arquivo local não produz essa ramificação. O trabalho pertence a uma sequência específica de cabeçalhos e transações.

O [código de blocos publicado](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) define esses campos, verificações de Merkle e caminhos de seleção da cadeia. O [guia de dificuldade](/blog/pt-br/mining-difficulty-explained/) explica como a rede determina o trabalho exigido para cada novo bloco.

## Por que trabalho acumulado importa mais que quantidade de blocos?

Dois mineradores podem encontrar blocos válidos diferentes que estendem o mesmo pai. Cada proposta tem seu próprio hash, e os nós podem recebê-las em ordens diferentes. Isso pode criar ramificações concorrentes por um breve período. A rede precisa de uma regra que siga o trabalho sustentando o histórico, em vez da ordem em que um participante o anunciou.

Core v0.22.0 calcula trabalho acumulado somando a dificuldade do bloco ao trabalho acumulado do pai. Após a validação, uma ramificação com trabalho estritamente maior pode provocar a troca da cadeia escolhida. Uma alternativa com trabalho igual não provoca essa troca por essa comparação. A dificuldade também precisa ser válida no contexto do pai da ramificação; o minerador não pode simplesmente escrever um número maior no cabeçalho.

Compare duas ramificações com um trecho inicial comum. Esse trecho contribui com o mesmo trabalho para ambos os totais. A diferença decisiva é o trabalho acumulado após a separação, junto das verificações de validade e reorganização. Contar blocos sem considerar a dificuldade pode ocultar essa diferença. É uma comparação conceitual, não um relato de bifurcação ao vivo.

## Como as confirmações ajudam a acompanhar um pagamento?

Quando o pagamento entra na cadeia escolhida, os blocos seguintes são construídos sobre esse histórico. Sua profundidade aumenta conforme chegam. Substituir sua inclusão exigiria um histórico alternativo admissível desde antes do bloco relevante, enquanto a ramificação atual continua acumulando trabalho. Por isso verificar inclusão e profundidade informa mais que apenas observar uma transação transmitida.

A altura identifica uma posição; o hash identifica um bloco específico. Confira o registro da transação e seu bloco e verifique se ele continua na cadeia escolhida. Se o histórico selecionado mudar, inclusão e profundidade também podem mudar. Contadores de confirmação podem incluir o bloco que contém a transação ou contar somente os posteriores: confira a convenção da interface.

Core v0.22.0 também impõe um limite de reorganização: rejeita uma troca cujo ancestral comum esteja abaixo da altura da ponta atual menos 100. É uma regra da implementação em execução, não uma promessa universal ligada a um contador. O [guia de tempo de transferência](https://seineminer.com/pt-br/blog/blocknet-transfer-time/) trata da disponibilidade comum na carteira; as [confirmações de recompensas](/blog/pt-br/block-confirmations-mining-rewards/) explicam maturação coinbase e contabilidade do pool.

## Como verificar gasto repetido em um pagamento privado?

O formato publicado de transações do Blocknet usa uma imagem de chave vinculada à prova de gasto. A validação verifica esse vínculo e rejeita uma imagem já gasta no estado pertinente da cadeia. Assim os nós detectam usos conflitantes de uma saída, enquanto a assinatura em anel esconde qual membro elegível é a entrada real. Valores privados continuam sujeitos a verificações de equilíbrio e intervalo.

Suponha que um proprietário autorize transações conflitantes gastando a mesma saída. Um histórico válido aceito não pode incluir ambos os gastos. A seleção da cadeia importa porque históricos concorrentes podem conter escolhas diferentes; a validação importa porque cada histórico deve obedecer às regras. Mais trabalho não substitui uma assinatura válida nem faz uma transação desequilibrada passar pela verificação contábil.

Essas verificações aparecem no [validador de transações publicado](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). Para os demais mecanismos, consulte o [guia de privacidade do Blocknet](https://seineminer.com/pt-br/blog/blocknet-privacy-features/). Segurança de pagamentos combina gastos válidos, histórico consistente e criptografia que protege o que esse histórico revela.

## Onde a mineração em pool entra na segurança dos pagamentos?

Um pool coordena a busca por um bloco e envia o candidato resolvido ao nó a montante. Um share que satisfaz o alvo mais fácil do pool registra trabalho contribuído; somente uma solução que atinge o alvo da rede pode virar bloco após validação. A [explicação de pools](/blog/pt-br/what-is-a-mining-pool/) separa esses alvos e suas funções.

Seu minerador pode, portanto, contribuir para estender o histórico enquanto o pool contabiliza seu trabalho separadamente. Um saldo privado no pool, uma contagem de shares e um pagamento confirmado na carteira descrevem coisas diferentes. Comece pelo [guia de conexão ao pool](/blog/pt-br/mine-blocknet-on-bntpool/) para participar e use o registro de transações da carteira para acompanhar um pagamento BNT específico.

<CTA target="pool" title="Contribua com trabalho para Blocknet" text="Conecte seine à bntpool.com e acompanhe sua mineração, recompensas e pagamentos pelas ferramentas públicas do pool." />
