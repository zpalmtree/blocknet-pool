---
title: "Acepta pagos en Blocknet por productos y servicios"
description: "Acepta BNT por pedidos o facturas, identifica los pagos recibidos en tu monedero Blocknet, comprueba confirmaciones y gestiona pagos parciales y reembolsos."
tags: [Blocknet, BNT]
faq:
  - q: "¿Cómo sé que un cliente ha pagado en Blocknet?"
    a: "Busca el ingreso en tu propio monedero sincronizado, compara el importe y la referencia del pedido y comprueba su estado de confirmación. Guarda el identificador completo de la transacción con el pedido. Una captura del cliente o un enlace de pago no demuestra la recepción; el explorador tampoco muestra el importe privado enviado a tu dirección."
  - q: "¿Qué hago si el cliente envía menos BNT de lo solicitado?"
    a: "Mantén el pedido como parcialmente pagado y calcula lo que falta a partir de los ingresos verificados. Pide la diferencia con la misma referencia y guarda ambos identificadores de transacción. Cuenta cada ingreso una sola vez. Si acordáis cancelar, confirma una dirección de reembolso por el canal habitual del pedido."
  - q: "¿Un memo marca automáticamente el pedido como pagado?"
    a: "No. El memo ayuda a relacionar un ingreso con un pedido, pero el pagador puede cambiarlo u omitirlo. Comprueba el importe recibido y el estado de la transacción. El monedero crea solicitudes y muestra ingresos; este procedimiento manual no añade un sistema automático de cobro o gestión de pedidos."
sourceHash: '3309a069e5de'
translation: draft
---

Acepta un pago en Blocknet indicando al cliente un importe claro en BNT, comprobando el ingreso en tu monedero y guardándolo con el pedido. Usaremos un pedido ficticio de **1.25 BNT** con referencia **order-1042**. El recorrido sigue el monedero de escritorio v0.22.0; las capturas contienen datos de demostración identificados.

## ¿Cómo se acepta un pago en Blocknet?

<Answer>
Acuerda el importe en BNT y la referencia, envía una solicitud a una dirección que controles y espera el ingreso en tu monedero sincronizado. Comprueba importe, referencia y confirmaciones. Después guarda el identificador de transacción y marca el pedido como pagado según la política de confirmación acordada con el cliente.
</Answer>

Consulta la [guía del monedero de escritorio](https://seineminer.com/es/blog/blocknet-desktop-wallet/) si necesitas crear o abrir uno. Guarda su copia de recuperación separada de los pedidos. Para recibir pagos de clientes no necesitas ejecutar un minero ni unirte a un pool.

## Fija el importe e identifica el pedido

Anota la referencia, el importe en BNT, la dirección receptora y lo que compra el cliente. Si cotizas en otra moneda, acordad el importe en BNT y la vigencia de la cotización antes del pago. El enlace contiene la cantidad solicitada; no mantiene un tipo de cambio ni impone una fecha límite.

En el ejemplo, pide **1.25 BNT** y usa **order-1042** como memo. Basta un número de pedido: conserva direcciones de entrega y otros datos del cliente en tu sistema privado. El [memo cifrado](https://seineminer.com/es/blog/blocknet-encrypted-memos/) acompaña al pago, pero cualquiera que reciba un enlace compartido puede leer el memo escrito en él.

En **Receive → Payment Request**, introduce importe y memo y pulsa **Generate Payment Link**. Revisa destinatario e importe antes de compartirlo. La [guía de enlaces de pago](https://seineminer.com/es/blog/blocknet-payment-links/) explica los formatos y los destinatarios por nombre. Guarda la dirección completa con cada factura fija.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Monedero Blocknet publicado con una solicitud de 1.25 BNT, memo order-1042 y dirección de demostración." loading="lazy" decoding="async"></a>
<figcaption>Interfaz real en inglés con datos de demostración. Crear la solicitud no mueve fondos. Abre la imagen para ampliarla.</figcaption>
</figure>

## Comprueba el ingreso en tu propio monedero

Pide al cliente que incluya la referencia y conserve el identificador de transacción. Cuando nodo y monedero estén al día, abre **History** y busca el ingreso. Compara importe y memo con el pedido. El pagador puede modificar ambos campos: la solicitud no demuestra qué envió.

Guarda el identificador completo y el importe recibido. No cuentes una transacción dos veces porque aparezca en Dashboard y History o porque el cliente vuelva a enviarte el mismo identificador. Compara ingresos concretos, no el cambio del saldo total, que puede incluir pagos del pool, de otros clientes o transferencias propias.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History del monedero Blocknet muestra un ingreso de 1.25 BNT con memo order-1042 y datos de demostración." loading="lazy" decoding="async"></a>
<figcaption>El ingreso del ejemplo coincide con el importe y la referencia solicitados. Comprueba su estado antes de entregar el pedido.</figcaption>
</figure>

Si falta el ingreso, compara dirección completa e identificador con el cliente y revisa la sincronización. Su captura de envío aporta contexto, pero tu monedero receptor establece el importe recibido.

## ¿Cuándo debes marcar el pedido como pagado?

Distingue un ingreso sin confirmar, una transacción incluida en un bloque y fondos disponibles para gastar. El monedero publicado espera diez bloques posteriores al bloque de un pago ordinario antes de permitir gastar sus salidas. La [guía de tiempos de transferencia](https://seineminer.com/es/blog/blocknet-transfer-time/) explica los contadores y la espera variable.

Indica la política de confirmación antes del pago. Un procedimiento manual sencillo espera a que el ingreso correspondiente sea gastable antes de entregar. Ver un importe entrante no completa esa comprobación. No prometas una hora fija de entrega basándote en el contador del monedero: los bloques llegan a intervalos variables. [Cómo protege la minería los pagos](/blog/es/how-mining-secures-blocknet-payments/) explica la importancia de la profundidad de confirmación.

## Gestiona pagos parciales, excesivos o sin referencia

| Lo que has verificado | Siguiente paso |
|---|---|
| 1.00 BNT para el pedido de 1.25 BNT | Registra un pago parcial y solicita los 0.25 BNT restantes. |
| Otro ingreso posterior de 0.25 BNT | Guarda ambos identificadores, suma 1.25 BNT una sola vez y comprueba las confirmaciones de ambos. |
| Más de 1.25 BNT | Acuerda un reembolso o saldo a favor y registra la decisión. |
| Importe correcto sin memo | Confirma identificador completo y destino con el cliente antes de asignarlo. |
| Pago pendiente | Mantén el pedido pendiente de confirmación y vuelve a comprobar su estado. |

Para reembolsar, pide al cliente que confirme una dirección receptora en la conversación original del pedido. No deduzcas una dirección de devolución de la transacción pública: sus entradas no ofrecen una dirección convencional del remitente. Revisa importe, destino y comisión antes de aprobar el nuevo envío y guarda su identificador junto al pago original.

## Comparte solo las pruebas necesarias

El explorador ayuda a comprobar la inclusión, pero no revela el importe privado recibido por tu dirección. Si se discute la recepción, el remitente puede aportar pruebas específicas siguiendo la [guía de pruebas de pago](https://seineminer.com/es/blog/blocknet-payment-proof/). Verifica coincidencias del destinatario, importe y estado en cadena; la validez de la clave no basta.

Conserva juntos y en privado el pedido, la solicitud, los identificadores de ingresos, los importes verificados, la decisión sobre confirmaciones y cualquier reembolso. Comparte solo el recibo o prueba pertinente. Nunca entregues palabras de recuperación, contraseñas ni claves privadas de visualización de todo el monedero como prueba.

Si también minas BNT, separa los [registros de pagos del pool](/blog/es/track-your-miner-on-bntpool/) de los pedidos aunque usen el mismo monedero. Así sabrás qué transferencia pagó cada factura.


<CTA target="pool" title="Recibe también recompensas de minería" text="La guía del pool explica cómo conectar un minero y seguir sus pagos junto con tus otros ingresos en Blocknet." />
