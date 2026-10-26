---
title: Cómo funcionan los pagos PPLNS en bntpool.com
description: bntpool.com paga a los mineros Blocknet con una ventana PPLNS de seis horas. Conoce el reparto, las 60 confirmaciones y el mínimo de 0,1 BNT.
tags: [Pagos, PPLNS, bntpool, Guía de minería]
faq:
  - q: '¿Cuánto dura la ventana PPLNS de bntpool.com?'
    a: 'Seis horas. Cuando encuentra un bloque, el pool revisa las seis horas anteriores y reparte la recompensa entre quienes enviaron shares en ese tiempo, en proporción a su dificultad. El valor actual aparece en Get Started.'
  - q: '¿Cuál es el pago mínimo de bntpool.com?'
    a: 'El mínimo es 0,1 BNT. Cuando tu saldo confirmado alcanza esa cantidad, el pool lo envía automáticamente a tu dirección Blocknet, sin pedir ni reclamar nada. Un saldo menor permanece en tu cuenta y crece con cada bloque confirmado en el que tengas shares hasta superar el umbral.'
  - q: '¿Por qué Estimated Rewards difiere de Confirmed Rewards?'
    a: 'Estimated Rewards es tu parte de los bloques encontrados que aún reúnen sus 60 confirmaciones, así que puede cambiar. Confirmed Rewards procede de bloques con 60 bloques de profundidad y espera el pago. Al madurar cada bloque, su importe pasa de la primera tarjeta a la segunda.'
  - q: '¿Dejo de ganar en cuanto dejo de minar?'
    a: 'No. Cada share permanece seis horas en la ventana PPLNS tras enviarlo, así que sigues recibiendo crédito por los bloques encontrados en ese tiempo. Las recompensas confirmadas permanecen en tu saldo y el pool las paga cuando alcanza el mínimo de 0,1 BNT.'
  - q: '¿Qué significa un esfuerzo superior al 100% en Luck?'
    a: 'El esfuerzo compara el trabajo del pool durante una ronda con la dificultad del bloque, que es el trabajo esperado. Más del 100% significa que tardó más que la media; menos del 100%, que fue más rápida. Ambos son normales y, tras muchas rondas, el esfuerzo medio ronda el 100%.'
sourceHash: 08a99741f46c
translation: draft
---

Esta guía explica cómo bntpool.com convierte los shares de tu minero en pagos Blocknet. Aprenderás cómo reparte la recompensa, por qué espera unas cinco horas, cuándo envía tu saldo y cómo leer las tarjetas de la [página de estadísticas](/stats) y la [página Luck](/luck).

Si eres nuevo en la minería en pool, [¿qué es un pool de minería?](/blog/es/what-is-a-mining-pool/) explica lo básico. Si aún no minas, la [guía de seine](https://seineminer.com/es/blog/how-to-mine-blocknet/) te lleva de la descarga a los primeros shares en minutos.

## ¿Qué es PPLNS?

<Answer>
PPLNS significa "pay per last N shares", pago por los últimos N shares. Cuando bntpool.com encuentra un bloque, reparte la recompensa entre los shares enviados en las seis horas anteriores, en proporción a la dificultad de cada uno. Cuanto más trabajo constante aportes en esa ventana, mayor será tu parte de cada bloque.
</Answer>

En bntpool.com, los "últimos N" son un período de seis horas. [Get Started](/start) muestra sistema de pagos, ventana, mínimo y confirmaciones; los mismos valores están en `https://bntpool.com/api/info`.

| Ajuste | Valor en bntpool.com |
|---|---|
| Sistema de pagos | PPLNS |
| Ventana de shares | 6 horas |
| Confirmaciones antes del pago | 60 bloques |
| Pago mínimo | 0,1 BNT |

Los shares se cuentan por dificultad. El pool asigna y ajusta una dificultad por conexión para que cada minero envíe shares regularmente. Una máquina más rápida recibe shares más difíciles y cada uno pesa más. Tu parte depende del trabajo total, una medida justa tanto para un portátil como para un servidor.

## ¿Cómo reparte bntpool.com una recompensa de bloque?

Cada bloque encontrado sigue los mismos pasos:

1. **Se registra el bloque.** Aparece en [Blocks](/blocks) como pendiente.
2. **Se mide la ventana.** El pool toma las seis horas que terminan en ese bloque y suma la dificultad de cada share aceptado por dirección.
3. **Se divide la recompensa.** Cada dirección recibe la misma fracción de recompensa que su fracción del trabajo total.
4. **Madura el bloque.** Tu parte figura como estimación hasta alcanzar 60 bloques de profundidad y pasa al saldo confirmado.
5. **Se paga el saldo.** Al alcanzar 0,1 BNT confirmados, el pool lo envía a tu dirección.

## Un ejemplo calculado

<Note>
Este ejemplo usa cifras redondas inventadas para explicar el cálculo. Los totales reales dependen de la tasa de hash del pool, la tuya y la [recompensa de bloque](/blog/es/blocknet-block-reward-schedule/) del momento.
</Note>

Supón que en las seis horas anteriores a un bloque todos enviaron shares con una dificultad total de 1.000.000 y los tuyos suman 20.000. Tu fracción es 20.000 ÷ 1.000.000 = 2%. Si el pool reparte 400 BNT, recibes 400 × 0,02 = 8 BNT.

Ahora imagina otro minero con exactamente tu tasa que se conectó 30 minutos antes del bloque. Media hora es una doceava parte de seis horas, por lo que tiene unos 1667 de dificultad en la ventana y recibe unos 0,67 BNT. Su trabajo sigue contando: cada share permanece las siguientes seis horas y gana una parte de cada bloque encontrado en ese tiempo.

| Minero (ejemplo) | Trabajo en la ventana | Fracción | Crédito de un pago de 400 BNT |
|---|---|---|---|
| Tú, minando las seis horas | 20.000 | 2% | 8 BNT |
| Misma tasa, entró 30 minutos antes | 1667 | alrededor del 0,17% | unos 0,67 BNT |
| Minero pequeño, minando las seis horas | 100 | 0,01% | 0,04 BNT |

El minero pequeño de la última fila gana 0,04 BNT por bloque: tras tres bloques así, su saldo confirmado supera 0,1 BNT y se paga.

## ¿Por qué PPLNS recompensa la minería constante?

Cada share que envías cuenta para cada bloque encontrado en las seis horas siguientes. Quien mina continuamente siempre tiene una ventana completa de trabajo, así que cada bloque nuevo le acredita a su ritmo pleno. Sus ingresos siguen de cerca su tasa de hash entre bloques.

PPLNS también tolera las pausas. Reiniciar, actualizar un controlador o perder brevemente la red no restablece tu posición, porque los shares enviados permanecen seis horas. El trabajo que añades después de un bloque cuenta para los siguientes, así que la tasa adicional siempre se aprovecha.

Para resultados estables, deja el minero funcionando y ajustándose. seine cambia hilos y kernel CPU durante la minería sin reconectar al pool, manteniendo el flujo de shares. Puedes [descargar seine](https://seineminer.com/es/#download) para Windows, macOS o Linux.

## ¿Por qué espera una recompensa 60 confirmaciones?

Blocknet exige esperar 60 bloques para gastar las monedas recién minadas. El daemon llama a esta regla madurez de coinbase: unas cinco horas con bloques de cinco minutos (60 × 5 minutos = 300 minutos). bntpool.com espera esos mismos 60 bloques para confirmar una recompensa, por lo que todo pago usa monedas que realmente puede enviar.

Durante la maduración, el pool comprueba que el bloque siga en la cadena principal. Al alcanzar 60 bloques de profundidad, lo marca confirmado y suma la recompensa al saldo confirmado de cada minero. En el caso poco frecuente de que la red elija otro bloque a esa altura, lo marca huérfano y elimina sus recompensas estimadas.

## ¿Cuándo llegan los pagos de bntpool.com?

<Answer>
Tras 60 confirmaciones, unas cinco horas con el tiempo de bloque de Blocknet, tu parte pasa a Confirmed Rewards. Cuando el saldo confirmado alcanza 0,1 BNT, bntpool.com lo envía automáticamente a tu dirección. No hay nada que pedir ni reclamar.
</Answer>

Para un minero nuevo, la primera estimación aparece cuando el pool encuentra un bloque con sus shares dentro de la ventana. El primer pago llega al madurar ese bloque y alcanzar el mínimo. Los pagos enviados aparecen en [Payouts](/payouts) y en tu historial de estadísticas.

## ¿Cómo se muestran las recompensas provisionales?

Busca tu dirección o `@name` en [My Stats](/stats) y verás tres tarjetas:

- **Estimated Rewards:** tu parte de bloques aún no maduros. Es provisional y puede cambiar hasta su confirmación o descarte como huérfanos.
- **Confirmed Rewards:** saldo maduro pendiente de pago.
- **Paid Balance:** todo lo enviado ya a tu dirección.

El pool separa las recompensas provisionales para que el saldo confirmado contenga solo importes definitivos. Debajo, una lista muestra cada bloque sin confirmar con tu parte estimada para seguir su maduración. La [guía para seguir tu minero](/blog/es/track-your-miner-on-bntpool/) explica el resto de la página.

## ¿Qué indican Luck y el esfuerzo?

El esfuerzo mide el trabajo del pool para encontrar un bloque. Es el trabajo aceptado durante la ronda dividido por la dificultad del bloque, como porcentaje. La dificultad Blocknet equivale a los hashes esperados por bloque: 100% significa exactamente el trabajo medio. Menos del 100% indica un hallazgo más rápido y más del 100%, uno más lento.

[Luck](/luck) muestra cada ronda con altura, esfuerzo, duración, estado y fecha. El [panel del pool](/) muestra el esfuerzo acumulado de la ronda actual y su suerte media. Las rondas individuales oscilan por encima y debajo del 100%, algo normal en cualquier pool de prueba de trabajo; a lo largo de muchas rondas, el esfuerzo medio ronda el 100%.

La suerte afecta a la frecuencia de los bloques. Tu fracción depende solo de tu parte del trabajo de la ventana, y minar constantemente la mantiene completa tanto en rondas rápidas como largas.

<CTA target="pool" title="Únete al pool" text="Mina de forma constante en bntpool.com y recibe tu parte de cada bloque automáticamente al alcanzar 0,1 BNT." />
