---
title: 'Confirmaciones de bloques y recompensas de minería inmaduras'
description: 'Sigue un bloque de Blocknet desde pendiente hasta recompensa madura y pago. Aprende cómo bntpool cuenta la profundidad y por qué se mide en bloques.'
tags: ['Recompensas de minería', 'Confirmaciones', 'Blocknet']
faq:
  - q: '¿Cuántas confirmaciones exige bntpool?'
    a: 'En la comprobación del 26 de octubre de 2026, bntpool.com publica 60 bloques. Calcula la altura actual menos la del bloque encontrado y verifica su hash con el nodo. Consulta Get Started o la API pública para conocer el valor actual.'
  - q: '¿60 bloques significa esperar exactamente cinco horas?'
    a: 'No. Sesenta intervalos de cinco minutos equivalen a 300 minutos, cinco horas, como estimación. El progreso real depende de nuevos bloques, cuyos tiempos varían. Madurez de la recompensa, umbral de pago y transacción posterior son etapas separadas.'
  - q: '¿Debo seguir minando para que madure una recompensa existente?'
    a: 'Un bloque ya encontrado gana profundidad al crecer la cadena, aunque tu minero esté desconectado. Tu participación depende de las reglas contables del pool. Parar no acelera ni reinicia la madurez; sigue la recompensa registrada en My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Sigue un bloque de Blocknet por la confirmación, madurez y pago posterior a tu cartera. Esta guía explica la profundidad de cadena detrás de las tarjetas de recompensas de bntpool.com. El reparto corresponde a la [guía PPLNS](/blog/es/how-pplns-payouts-work/); para empezar a enviar trabajo, consulta la [guía de conexión](/blog/es/mine-blocknet-on-bntpool/).

## ¿Qué son las confirmaciones y las recompensas inmaduras?

<Answer>
Las confirmaciones describen la posición de un bloque bajo la punta actual. Una recompensa inmadura pertenece a un bloque reciente que aún no alcanza la profundidad exigida. bntpool comprueba profundidad e identidad antes de confirmarla; la elegibilidad del saldo y el pago posterior son pasos separados.
</Answer>

*Un bloque gana profundidad con los siguientes, alcanza la madurez y pasa por la elegibilidad del saldo hacia un pago independiente.*

## ¿Cómo cuenta bntpool la profundidad?

El pool resta la altura del bloque encontrado de la altura actual. El requisito publicado era 60 bloques el 26 de octubre de 2026. Según esta convención, el bloque empieza en cero. Otra página puede contar el bloque que contiene la operación como primera confirmación; compara la regla además del número.

Como ejemplo aritmético, un bloque a altura 70.000 tiene profundidad cero con punta 70.000, uno en 70.001 y 60 en 70.060. Son alturas ilustrativas, no datos en directo. El pool también verifica el hash con su nodo. La [implementación](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) recoge ambos controles.

## ¿Por qué cinco horas es solo una estimación?

Blocknet tiene un intervalo objetivo de cinco minutos. Sesenta intervalos por cinco minutos son 300 minutos, cinco horas. Ayuda a planificar, pero la condición es un número de bloques. Observa el avance de la cadena desde la altura encontrada hasta la profundidad necesaria, no un reloj con vencimiento fijo.

Cualquier bloque de la cadena aceptada aumenta la profundidad; no todos tienen que venir de bntpool. La velocidad de tu minero no crea un reloj propio. Los tiempos de llegada varían y pueden apartarse de la estimación. La [guía de dificultad](/blog/es/mining-difficulty-explained/) explica el ajuste de red y la [guía de suerte](/blog/es/mining-pool-luck-explained/) las variaciones de las rondas.

## Distingue pendiente, confirmado y pagado

Un bloque recién registrado aparece pendiente mientras gana profundidad. Tu porción estimada puede aparecer en [My Stats](/stats) si tienes trabajo elegible. La recompensa crea monedas nuevas; la cartera de Blocknet aplica una madurez coinbase de 60 bloques antes de poder gastarlas. La espera equivalente del pool acompaña ese proceso.

Tras cumplir profundidad e identidad, el pool puede confirmar el bloque y distribuir la recompensa elegible a saldos. No significa que todas las direcciones reciban un pago en ese instante. El proceso de pago evalúa por separado saldos confirmados y condiciones de envío. Estado del bloque, saldo y historial describen etapas distintas del mismo recorrido.

## ¿Por qué verifica el pool el hash?

La altura identifica una posición y el hash un bloque concreto. Los nodos pueden conocer bloques competidores; la cadena aceptada determina cuál ocupa esa posición. Por eso el pool pide al nodo el bloque de esa altura y compara su hash. La profundidad sola no demuestra que el bloque original siga en la cadena aceptada.

Un bloque que sale de esa cadena puede marcarse huérfano y su recompensa tentativa deja de ser elegible como recompensa de la cadena aceptada. Se trata por separado de la confirmación. Usa el enlace del hash en [Bloques](/blocks) para inspeccionarlo; dos entradas a igual altura no necesariamente son el mismo evento. La estimación sigue vinculada a su bloque.

## ¿En qué difiere el pago de la recompensa minada?

La recompensa empieza en la salida coinbase del bloque. El pago del pool es una transacción posterior que envía saldo elegible a tu dirección. En la fecha comprobada, el mínimo publicado es 0,1 BNT. Alcanzar madurez y alcanzar ese saldo son condiciones distintas; la [guía PPLNS](/blog/es/how-pplns-payouts-work/) detalla reparto y pago.

Una vez enviado, sigue el registro de esa transacción y la recepción y disponibilidad en tu cartera. Sus confirmaciones pertenecen al pago, no al bloque original del pool. No hace falta crear otra dirección ni reconectar el minero mientras madura un bloque. Usa la dirección prevista al comparar [historial de pagos](/payouts) y cartera.

## ¿Cómo sigues una recompensa de principio a fin?

Empieza en [Bloques](/blocks) por altura, hash y estado. Abre [My Stats](/stats) con la dirección usada por tu minero y revisa recompensas estimadas y confirmadas. Cuando exista un pago, sigue su transacción desde el historial. Cada observación tiene su sitio: bloque, saldo de cuenta y pago a la cartera.

Confirma las condiciones en [Get Started](/start) o `https://bntpool.com/api/info` y consulta el [estado del pool](/status). Para investigar, anota dirección, identificador de bloque o transacción y hora; nunca compartas palabras de recuperación. La [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) explica el resto. Los bloques existentes ganan profundidad con tu minero desconectado; su elegibilidad depende del trabajo registrado y la cadena.

<CTA target="pool" title="Sigue tus recompensas" text="Conéctate a bntpool.com y usa My Stats para seguir recompensas estimadas, saldos confirmados y pagos." />
