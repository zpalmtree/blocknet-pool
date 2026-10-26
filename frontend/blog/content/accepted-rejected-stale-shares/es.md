---
title: 'Shares aceptados, rechazados y obsoletos en minería'
description: 'Entiende los shares aceptados, rechazados y obsoletos en bntpool.com, interpreta sus mensajes y descubre cómo la dificultad influye en tus estadísticas.'
tags: ['Guía de minería', 'Shares', 'Solución de problemas']
faq:
  - q: '¿Un share aceptado significa que encontré un bloque?'
    a: 'Significa que el pool aceptó un resultado según sus reglas de procesamiento de shares. Un bloque debe cumplir un objetivo de red separado y ser aceptado por el daemon. Las recompensas siguen el descubrimiento, la validación y el proceso de pago; la confirmación de un share no es un aviso de pago.'
  - q: '¿Por qué se rechaza un share obsoleto?'
    a: 'La respuesta stale job indica que el envío corresponde a una asignación que ya no es válida, por ejemplo porque caducó o su plantilla superó el periodo de gracia. Comprueba que lleguen trabajos nuevos y vuelvan los shares aceptados. Si se repite, revisa la conexión y la versión del minero.'
  - q: '¿Una dificultad de share mayor reduce mi recompensa?'
    a: 'La dificultad cambia el trabajo que representa cada share elegible y su frecuencia esperada. Compara trabajo ponderado por dificultad durante periodos equivalentes, en lugar de contar shares sin ponderar. La recompensa real también depende de la ventana PPLNS, el trabajo elegible, los bloques encontrados y las reglas de validación.'
sourceHash: 5be830208e6c
translation: draft
---

Usa el resultado de cada share en el registro del minero para decidir si continuar, revisar la conexión o investigar rechazos repetidos. Esta guía explica las respuestas de bntpool.com y la dificultad detrás de sus contadores. Si aún no te conectaste, sigue la [guía de configuración](/blog/es/mine-blocknet-on-bntpool/); si una dirección no muestra actividad, consulta la [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/#zero-hashrate).

## ¿Qué es un share aceptado?

<Answer>
Un share es un resultado de minería enviado contra un objetivo asignado por el pool. Una respuesta aceptada confirma su entrada en el flujo de procesamiento y demuestra que llega trabajo al pool. El descubrimiento de bloques, la verificación y la elegibilidad para pagos son pasos distintos; aceptar un share no equivale a pagar.
</Answer>

El minero prueba muchos hashes entre shares enviados. La mayoría no cumple el objetivo de share y nunca se envía. El contador de aceptados cuenta resultados de envíos, no todos los hashes calculados por CPU o GPU. La [guía de hashrate](/blog/es/what-is-hashrate/) explica la diferencia entre el recuento local y la estimación del pool a partir del trabajo enviado.

## ¿En qué se diferencian los shares de los bloques?

El pool asigna trabajos y una dificultad a tu conexión. Un resultado que cumple el objetivo de share demuestra trabajo a esa dificultad. Un candidato a bloque debe cumplir también el objetivo de red y superar las comprobaciones del daemon. Encontrar un share no implica añadir un bloque a la cadena.

bntpool.com registra shares verificados y provisionales durante la validación. Un envío aceptado puede ser provisional, y la ponderación para pagos aplica reglas de validación y elegibilidad. Usa la aceptación como señal de conexión y consulta [Mis estadísticas](/stats) para ver trabajo y recompensas registrados. La [guía de PPLNS](/blog/es/how-pplns-payouts-work/) explica la secuencia tras encontrar un bloque.

## ¿Qué significan los shares rechazados y obsoletos?

Un envío rechazado no superó una comprobación. El motivo sirve más que el contador por sí solo. Un trabajo obsoleto es una categoría de rechazo: su asignación ya no es válida. Lee el mensaje exacto y compáralo con eventos del mismo momento, como una reconexión o un trabajo nuevo.

| Mensaje | Significado y siguiente comprobación |
|---|---|
| `stale job` | La asignación falta, caducó, fue sustituida más allá de su periodo de gracia o usa una plantilla antigua no disponible. Comprueba que vuelvan los trabajos nuevos y los shares aceptados. |
| `duplicate share` | Ya se envió la misma plantilla con el mismo nonce. Comprueba si hay envíos repetidos; reenviar el mismo trabajo no añade otra contribución. |
| `low difficulty share` | El hash enviado no cumple el objetivo asignado. Si se repite, revisa la versión del minero y el manejo del trabajo y su dificultad. |
| `nonce out of assigned range` | El nonce enviado está fuera del rango asignado al trabajo. Revisa la versión del minero y cualquier proxy o integración personalizada. |

Un share obsoleto y un bloque huérfano pertenecen a etapas distintas. El primero afecta al trabajo de una asignación; el segundo, al lugar de un bloque en la cadena. Distingue el registro de shares de la [lista de bloques](/blocks). El mensaje stale job por sí solo no indica que el pool encontrara un bloque huérfano.

## ¿Por qué cambia la dificultad de los shares?

<Answer>
La dificultad variable, o vardiff, ajusta el objetivo de una conexión según su actividad de shares. Una dificultad mayor suele producir menos shares, con más trabajo por share; una menor, más shares con menos trabajo. Compara el trabajo elegible ponderado por dificultad durante el mismo periodo, no solo el número de shares.
</Answer>

Ejemplo aritmético: diez shares elegibles de dificultad 100 representan la misma dificultad total que uno de dificultad 1000; ambos suman 1000. Ilustra la ponderación, no promete una recompensa ni un calendario exacto. Los shares llegan a intervalos variables y las reglas de validación determinan qué trabajo cuenta.

Importa la dificultad de la asignación cuando se envía el share. No compares un resultado antiguo únicamente con la dificultad más reciente en pantalla. El pool conserva la información de las asignaciones para comprobar envíos. Un ajuste normal de dificultad no es un error, y forzar shares más fáciles no crea trabajo computacional adicional.

## ¿Cómo debes interpretar la tasa de rechazo?

Compara envíos aceptados y rechazados en el mismo periodo y ámbito. Los contadores del minero pueden cubrir una sesión, mientras la página del pool usa otro periodo o agrupa varios equipos. Reiniciar el minero cambia la ventana local, pero no borra el historial de tu dirección en el pool.

Como ejemplo, 99 envíos aceptados y uno rechazado producen un 1% de rechazo por cantidad. Ese porcentaje no equivale automáticamente a recompensas perdidas: las dificultades pueden variar y existen reglas de elegibilidad. Mira los motivos, los momentos y si continúa llegando trabajo aceptado antes de cambiar opciones.

## ¿Qué hacer si los rechazos se repiten?

Empieza por un equipo afectado. Anota la versión, el mensaje exacto, cuándo empezó y si siguen llegando trabajos nuevos. Comprueba [Estado](/status) por si hay una incidencia y revisa posibles interrupciones de la conexión. Actualiza desde las [versiones oficiales de seine](https://github.com/zpalmtree/seine/releases) y compara el comportamiento cambiando una cosa cada vez.

Si utilizas un proxy o una integración propia, verifica que preserve el trabajo, el rango de nonce y la dificultad asignados al reenviar. Si continúa el problema, proporciona un extracto breve del registro y los detalles del equipo mediante el canal de soporte del proyecto. Elimina secretos y datos privados; nunca se necesita la frase de recuperación para diagnosticar shares.

## ¿Cómo confirmas que la minería vuelve a funcionar?

Busca trabajos nuevos seguidos de shares aceptados y abre [Mis estadísticas](/stats) con la misma dirección de recepción del minero. Confirma actividad reciente del equipo afectado. Deja que la estimación de hashrate acumule más trabajo antes de comparar medias. Cuando el trabajo llegue normalmente, sigue recompensas estimadas, saldo y pagos como etapas distintas en la guía de PPLNS.

<CTA target="pool" title="Conéctate a bntpool.com" text="Sigue la guía de configuración, confirma shares aceptados y consulta tu actividad por dirección de recepción." />
