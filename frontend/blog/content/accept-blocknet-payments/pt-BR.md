---
title: "Aceite pagamentos em Blocknet por produtos e serviços"
description: "Aceite BNT por pedidos e faturas, identifique os recebimentos na carteira Blocknet, confira confirmações e resolva pagamentos parciais e reembolsos."
tags: [Blocknet, BNT]
faq:
  - q: "Como sei que um cliente pagou em Blocknet?"
    a: "Localize o recebimento na sua carteira sincronizada, confira valor e referência do pedido e verifique as confirmações. Guarde o identificador completo da transação. Uma captura do cliente ou link de pagamento não comprova o recebimento; o explorador também não mostra o valor privado pago ao seu endereço."
  - q: "E se o cliente enviar menos BNT do que foi pedido?"
    a: "Mantenha o pedido parcialmente pago e calcule o restante com base nos recebimentos verificados. Peça a diferença com a mesma referência e registre os dois identificadores. Conte cada recebimento apenas uma vez. Se combinarem um cancelamento, confirme o endereço de reembolso na conversa existente do pedido."
  - q: "Um memo marca o pedido como pago automaticamente?"
    a: "Não. Ele ajuda a relacionar um recebimento ao pedido, mas o pagador pode alterá-lo ou omiti-lo. Confira valor e estado da transação. A carteira cria solicitações e mostra recebimentos; este fluxo manual não adiciona checkout automático nem sistema de gestão de pedidos."
sourceHash: '3309a069e5de'
translation: draft
---

Aceite Blocknet informando um valor claro em BNT, verificando o recebimento na sua carteira e guardando o registro com o pedido. Usamos um pedido fictício de **1.25 BNT**, com referência **order-1042**. O procedimento usa a carteira desktop publicada v0.22.0; as capturas têm dados demonstrativos identificados.

## Como aceitar um pagamento em Blocknet?

<Answer>
Combine valor em BNT e referência, envie uma solicitação para um endereço que você controla e espere o recebimento na carteira sincronizada. Confira valor, referência e confirmações. Depois registre o identificador da transação e marque o pedido como pago conforme a política de confirmação combinada com o cliente.
</Answer>

O [guia da carteira desktop](https://seineminer.com/pt-br/blog/blocknet-desktop-wallet/) ensina a criar ou abrir a carteira. Mantenha o backup de recuperação separado dos pedidos. Receber pagamentos de clientes não exige rodar um minerador nem entrar em um pool.

## Defina o valor e identifique o pedido

Registre referência, valor em BNT, endereço de recebimento e produto ou serviço. Se cotar em outra moeda, combine o valor em BNT e a validade da cotação antes do pagamento. O link contém o valor solicitado; não atualiza câmbio nem impõe prazo.

No exemplo, peça **1.25 BNT** com memo **order-1042**. Um número de pedido basta: endereços de entrega e outros dados ficam no seu sistema privado. Um [memo criptografado](https://seineminer.com/pt-br/blog/blocknet-encrypted-memos/) acompanha o pagamento, mas qualquer pessoa que receber um link compartilhado poderá ler o memo escrito nele.

Em **Receive → Payment Request**, preencha valor e memo e escolha **Generate Payment Link**. Confira destinatário e valor antes de compartilhar. O [guia de links de pagamento](https://seineminer.com/pt-br/blog/blocknet-payment-links/) explica formatos e destinatários por nome. Para uma fatura fixa, guarde o endereço completo com a solicitação.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Carteira Blocknet publicada com solicitação de 1.25 BNT, memo order-1042 e endereço de demonstração." loading="lazy" decoding="async"></a>
<figcaption>Interface real em inglês com dados demonstrativos. Gerar a solicitação não movimenta fundos. Abra a imagem para ampliá-la.</figcaption>
</figure>

## Confira o recebimento na sua carteira

Peça ao cliente que inclua a referência e guarde o identificador da transação. Quando nó e carteira estiverem atualizados, abra **History** e procure o recebimento. Compare valor e memo com o pedido. O pagador pode alterar ambos; a solicitação não comprova o que foi enviado.

Salve o identificador completo e o valor recebido. Não conte a mesma transação duas vezes por aparecer no Dashboard e no History, ou por o cliente reenviar seu identificador. Relacione recebimentos concretos, não a variação do saldo total: ele pode incluir mineração, outros clientes e transferências suas.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History da carteira Blocknet mostra um recebimento de 1.25 BNT com memo order-1042 e dados demonstrativos." loading="lazy" decoding="async"></a>
<figcaption>O recebimento de exemplo corresponde ao valor e à referência pedidos. Verifique o estado antes de entregar o pedido.</figcaption>
</figure>

Se o recebimento não aparecer, compare endereço completo e identificador com o cliente e confira a sincronização. A captura de envio fornece contexto; sua carteira de recebimento determina o valor recebido.

## Quando marcar o pedido como pago?

Diferencie recebimento não confirmado, transação incluída em um bloco e fundos disponíveis para gastar. A carteira publicada espera dez blocos posteriores ao bloco do pagamento comum antes de permitir gastar suas saídas. O [guia de tempo de transferência](https://seineminer.com/pt-br/blog/blocknet-transfer-time/) explica os contadores e a espera variável.

Informe a política de confirmações antes do pagamento. Um fluxo manual simples espera que o recebimento correspondente fique disponível para gasto antes de entregar. Um valor visível não completa essa verificação. Não prometa horário fixo de entrega com base na contagem da carteira: a chegada dos blocos varia. [Como a mineração protege pagamentos](/blog/pt-br/how-mining-secures-blocknet-payments/) explica a profundidade de confirmação.

## Resolva pagamentos parciais, extras ou sem referência

| Recebimento verificado | Próxima ação |
|---|---|
| 1.00 BNT para o pedido de 1.25 BNT | Registre pagamento parcial e peça os 0.25 BNT restantes. |
| Mais 0.25 BNT depois | Guarde ambos os identificadores, conte o total de 1.25 BNT uma vez e confira as confirmações de ambos. |
| Mais de 1.25 BNT | Combine reembolso ou crédito e registre a decisão. |
| Valor correto sem memo | Confirme identificador completo e destino com o cliente antes de associar. |
| Pagamento pendente | Deixe o pedido aguardando confirmação e consulte o estado novamente. |

Para reembolsar, peça confirmação de um endereço na conversa existente do pedido. Não deduza um endereço de devolução da transação pública: as entradas do Blocknet não fornecem um endereço convencional do remetente. Revise valor, destino e taxa antes de aprovar o novo envio e salve seu identificador com o pagamento original.

## Compartilhe somente a prova necessária

O explorador ajuda a verificar inclusão, mas não revela o valor privado recebido pelo seu endereço. Se houver dúvida sobre o recebimento, o remetente pode apresentar uma prova específica pelo [guia de provas de pagamento](https://seineminer.com/pt-br/blog/blocknet-payment-proof/). Verifique correspondência do destinatário, valor e estado na cadeia; a validade da chave sozinha não basta.

Guarde de forma privada pedido, solicitação, identificadores de entrada, valores verificados, decisão sobre confirmações e eventual reembolso. Compartilhe apenas o recibo ou prova relevante. Nunca forneça palavras de recuperação, senhas ou chaves privadas de visualização da carteira inteira como prova.

Se também minera BNT, separe os [registros de pagamentos do pool](/blog/pt-br/track-your-miner-on-bntpool/) dos pedidos mesmo usando a mesma carteira. Assim fica claro qual transferência pagou cada fatura.


<CTA target="pool" title="Receba recompensas de mineração também" text="O guia do pool mostra como conectar um minerador e acompanhar seus pagamentos junto com outros recebimentos de Blocknet." />
