---
title: '¿El pago del pool no aparece en tu billetera?'
description: 'Sigue un pago de Blocknet desde su transacción y dirección receptora hasta la sincronización de la billetera y reúne los datos para aclarar su ausencia.'
tags: ['Pagos', 'Billetera', 'Solución de problemas']
faq:
  - q: '¿Un pago en cola significa que mi billetera lo recibió?'
    a: 'En cola es un estado de procesamiento del pool. Comprueba primero si el registro tiene identificador de transacción y su estado actual. El registro del pago, la llegada de la transacción a la cadena y el reconocimiento de su salida por la billetera son etapas distintas. Sigue la transacción concreta cuando esté disponible.'
  - q: '¿El explorador puede mostrar el saldo de mi billetera privada?'
    a: 'El explorador permite comprobar el estado público de una transacción en la cadena. Blocknet utiliza salidas privadas, por lo que una búsqueda normal por dirección no muestra el saldo de tu billetera ni demuestra que una salida te pertenezca. Tu billetera escanea la cadena para reconocer sus propias salidas.'
  - q: '¿Qué datos debo aportar sobre un pago que no aparece?'
    a: 'Proporciona la dirección receptora completa usada en el pool, el identificador de transacción, hora aproximada, estado del pool y progreso de sincronización. Explica si aparece en el historial. Comparte datos de cuenta solo en el contexto de soporte adecuado y nunca envíes frase semilla, claves privadas ni contraseña.'
sourceHash: '915075cf4e2b'
translation: draft
---

Sigue un pago de Blocknet desde el registro del pool hasta la billetera receptora. Esta guía comienza cuando puedes identificar una entrada de pago en bntpool.com. Si solo ves recompensas estimadas o confirmadas pendientes de pago, consulta primero la [guía de PPLNS](/blog/es/how-pplns-payouts-work/): es una etapa anterior con sus propias reglas contables y de pago.

## ¿Cómo rastreas un pago ausente de la billetera?

<Answer>
Encuentra el registro para la dirección receptora completa, anota su identificador de transacción y estado actual y comprueba su estado en la cadena. Abre la billetera del mismo destino y deja que se sincronicen el nodo y el escaneo. Compara el historial antes de decidir si debes investigar el pago o la vista local de la billetera.
</Answer>

*Un registro de pago del pool conecta con una transacción en la cadena y después con la billetera que escanea sus propias salidas entrantes.*

## Empieza por un registro de pago concreto

Abre My Stats y busca la dirección completa usada al obtener esas recompensas. Localiza el pago y anota importe, hora, estado e identificador si existe. Usa esa entrada en vez del total pagado histórico, que puede resumir varios pagos. La [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) explica dónde aparecen estos campos.

La página pública Payouts describe lotes de pagos que pueden incluir varios destinatarios y enlaces de transacciones. Conserva el registro de tu dirección junto con la transacción investigada. El total del lote no es el importe asignado a tu billetera. Si hay varios identificadores, expande la lista y guarda los relevantes en vez de copiar solo una etiqueta abreviada.

## ¿Está en cola, sin confirmar o confirmado?

Lee el estado actual antes de asumir que un pago listado completó todos los pasos. Una entrada en cola sigue en procesamiento del pool; una transacción sin confirmar todavía necesita confirmación en la cadena. Actualiza el registro y sigue su enlace cuando exista. La [página de estado](/status) aporta contexto operativo si varias observaciones apuntan a una interrupción.

Distingue la madurez de la recompensa minera de la confirmación de la transacción de pago. El bloque que generó la recompensa y la transacción posterior que paga a tu billetera tienen identificadores y funciones diferentes. La [guía de confirmaciones](/blog/es/block-confirmations-mining-rewards/) lo explica. Tener un identificador permite investigar algo concreto, pero no demuestra que tu billetera local ya haya escaneado y reconocido el pago.

## Comprueba la transacción en la cadena

Abre el enlace al explorador del pago y compara el identificador completo con tus notas. Lee el estado en la cadena y, si está disponible, la información del bloque. Si el explorador aún no la encuentra, compara el registro actualizado y el estado del servicio antes de concluir nada. Un resultado temporal por sí solo no identifica la causa.

La privacidad de Blocknet implica que una búsqueda normal por dirección en el explorador no muestra el saldo público del destinatario. El estado de la transacción y la comprobación de propiedad de tu billetera responden preguntas distintas. La billetera reconoce salidas entrantes al escanear la cadena. La [guía de direcciones sigilosas](https://seineminer.com/es/blog/stealth-addresses-explained/) explica el proceso; no esperes que una lista pública reproduzca el historial privado.

## ¿Estás mirando la billetera receptora correcta?

Compara la dirección de consulta del pool con la dirección receptora de la billetera abierta. Copia los valores completos: prefijos abreviados parecidos no bastan. Si minas con un Blocknet ID, conserva la dirección completa del trabajo original. La consulta actual del nombre puede apuntar a otro destino tras un cambio, mientras un pago anterior sigue vinculado al original.

Comprueba cualquier billetera anterior usada antes de cambiar la configuración minera. Actualizar la dirección del minero o la asociación de un nombre no mueve un pago ya asignado a otro destino. La [guía de cambio de dirección](/blog/es/change-mining-wallet-address/) explica ambos historiales. Conserva la billetera original y su copia de seguridad; distingue abrir una billetera existente de crear otra con una dirección diferente.

## Deja que la billetera se sincronice y escanee

Confirma que el nodo de la billetera progresa hacia la cadena actual y que el escaneo ha alcanzado el bloque de la transacción. Sincronización del nodo y escaneo de la billetera son comprobaciones separadas. Una billetera recién abierta o restaurada puede necesitar tiempo para descubrir sus salidas aunque el explorador ya muestre la transacción en un bloque.

Mantén la billetera conectada y sigue su procedimiento documentado de sincronización. Revisa las transacciones entrantes además del saldo principal, pues gastos posteriores pueden cambiarlo. Si hay una entrada, comprueba confirmación y disponibilidad. Empieza por estas observaciones antes de recuperar o volver a escanear todo y conserva copias antes del mantenimiento. La [guía de billeteras](https://seineminer.com/es/blog/blocknet-wallet-for-mining/) cubre acceso y copias de seguridad.

## ¿Qué datos ayudan a resolver una discrepancia pendiente?

Anota dirección completa del pool, identificador de transacción, hora aproximada y estado actual del pool. Añade versión de la billetera, progreso del nodo y del escaneo, y si la transacción entrante aparece en el historial. Explica qué comprobaste. Estos datos distinguen un problema de estado de transacción de una dirección equivocada o una billetera aún poniéndose al día.

Utiliza el canal oficial de soporte del proyecto y comparte datos de cuenta solo donde corresponda. Investigar una transacción no requiere frase de recuperación, claves privadas ni contraseña. Revisa el contenido antes de publicar archivos de diagnóstico completos. Termina cotejando el mismo registro, transacción y billetera receptora; una tasa de hash reciente u otro reinicio del minero no verifica la recepción de un pago anterior.

<CTA target="pool" title="Sigue tus pagos en bntpool.com" text="Busca el registro con tu dirección receptora completa y compara su transacción con el historial de la billetera sincronizada." />
