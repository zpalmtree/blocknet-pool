---
title: ¿Qué es la tasa de hash en minería?
description: La tasa de hash mide hashes por segundo. Aprende sus unidades, las diferencias entre tu tasa, la del pool y la red, y por qué fluctúan con el tiempo.
tags: [Tasa de hash, Conceptos de minería, Blocknet, bntpool]
faq:
  - q: '¿Qué tasa alcanza una computadora minando Blocknet?'
    a: 'Los mejores resultados por generación medidos con seine son 49,4 H/s en AMD EPYC Zen 5, 35,7 H/s en EPYC Zen 4, 30,9 H/s en un portátil Apple M4 Max y 21,5 H/s en un servidor Arm AWS Graviton4. Tu cifra depende principalmente del ancho de banda y de cuántos hilos de 2 GB caben en RAM.'
  - q: '¿Por qué Blocknet usa H/s y no MH/s o TH/s?'
    a: 'Cada hash es un cálculo Argon2id que escribe unos 2 GiB y los vuelve a leer, así que incluso un servidor grande termina solo decenas por segundo. Los algoritmos basados en hashes rápidos como SHA-256 alcanzan cifras mucho mayores y usan unidades mayores. Compara tasas solo dentro del mismo algoritmo.'
  - q: '¿Por qué el pool muestra otra tasa que mi minero?'
    a: 'Tu minero cuenta cada hash terminado. El pool solo ve shares y estima la tasa por su dificultad durante la última hora. Los shares llegan aleatoriamente, así que su cifra oscila más y se estabiliza con el tiempo. Compara el gráfico de 24 horas de My Stats con la media del minero.'
  - q: '¿Más tasa de hash significa más recompensas?'
    a: 'Sí, proporcionalmente. Cada hash tiene igual probabilidad de encontrar un bloque, por lo que tu parte esperada de bloques equivale a tu parte de la tasa de red. Duplicar tu tasa duplica lo esperado; si crece la red, tu parte disminuye. En bntpool.com recibes esa parte de cada bloque encontrado.'
sourceHash: 0bfdcdca348e
translation: draft
---

La tasa de hash es la primera cifra que muestran mineros, pools y exploradores. Esta guía explica qué mide, las unidades desde H/s, cómo se relacionan tu tasa, la del pool y la red, y por qué cambian cada minuto, usando Blocknet y bntpool.com como ejemplo.

## ¿Qué es la tasa de hash?

<Answer>
Es el número de hashes que calcula un minero por segundo. Cada hash es un intento de resolver un bloque, con la misma pequeña probabilidad de éxito. Una tasa mayor significa más intentos por segundo, así que tu parte esperada de bloques crece en proporción a tu fracción de la tasa total.
</Answer>

Una cadena de prueba de trabajo pide una cabecera cuyo hash esté bajo un objetivo. No hay atajo: el minero cambia el nonce, calcula el hash, lo compara y repite. La tasa cuenta cuántos intentos completa cada segundo.

En Blocknet, cada intento es un hash Argon2id. El nonce sirve de contraseña y el resto de la cabecera de salt. La función procesa 2 GiB antes de producir 32 bytes. seine y el pool miden hashes terminados por segundo: 1 H/s es un cálculo Argon2id completo cada segundo.

## ¿Qué significan H/s, kH/s y MH/s?

Las unidades siguen prefijos métricos, cada paso mil veces el anterior:

| Unidad | Hashes por segundo | Uso habitual |
|---|---|---|
| H/s | 1 | Una computadora en Blocknet |
| kH/s (KH/s) | 1000 | Totales del pool y la red Blocknet |
| MH/s | 1.000.000 | Algoritmos de prueba de trabajo más rápidos |
| GH/s | 1.000.000.000 | Hashes rápidos como SHA-256 |
| TH/s y superiores | 1.000.000.000.000 o más | Totales de redes con hashes rápidos |

El panel escribe KH/s para miles y cambia de unidad al superar 1000. Así, 44,44 KH/s son 44.440 H/s. Convierte ambas tasas a H/s antes de dividirlas.

## ¿Por qué son pequeñas las tasas de Blocknet?

Su prueba de trabajo es intensiva en memoria. Cada hash escribe unos 2 GiB y lee aproximadamente otros 2 GiB, mucho más que cualquier caché, así que manda la velocidad de la memoria. Cada hilo necesita sus propios 2 GiB: la RAM determina cuántos puedes ejecutar.

Estos son los mejores resultados de seine por generación:

| Hardware | Hilos | Tasa de hash |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Servidor Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Incluso el Zen 5 de 32 hilos completa unos 1,5 hashes por hilo y segundo. Los hashes rápidos dan cifras mucho mayores: la wiki Bitcoin señala que sus mineros de SHA-256 doble ya se medían en miles de millones de hashes por segundo en 2014. Son distintas cantidades de trabajo por hash; solo tiene sentido comparar el mismo algoritmo. [Argon2id explicado](https://seineminer.com/es/blog/argon2id-proof-of-work-explained/) profundiza en él y la [guía de hardware](https://seineminer.com/es/blog/blocknet-mining-hardware-hashrate/) explica RAM y ancho de banda.

## Tu tasa, la del pool y la de la red

A quien mina en un [pool](/blog/es/what-is-a-mining-pool/) le importan tres tasas, medidas de forma distinta.

### Tu tasa de hash

seine cuenta cada hash y muestra **Current**, la tasa actual, y **Average**, la de toda la sesión. Para una lectura estable, usa la media tras unos minutos de ajuste de hilos y kernel con trabajo real.

El pool no ve los hashes, solo shares que superan un objetivo más fácil por conexión. bntpool.com estima tu tasa con la dificultad total aceptada en la última hora y la muestra en [My Stats](/stats). La [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) explica esa página.

### Tasa del pool

**Pool Hashrate** suma el trabajo de shares de todos los conectados en la misma ventana horaria. Indica cuánto trabajo de red realiza bntpool.com y la frecuencia esperada de hallazgos, mostrada como **Expected Block Time**.

### Tasa de la red

Nadie cuenta todos los hashes de la red: se estima desde los bloques. La dificultad es el número esperado de hashes por bloque, y Blocknet busca uno cada 300 segundos.

## ¿Cómo se calcula la tasa de red?

<Answer>
Se estima mediante dificultad y tiempos de bloque. En Blocknet, la dificultad aproxima los hashes esperados por bloque y el objetivo es 300 segundos: tasa aproximada igual a dificultad entre 300. bntpool.com usa el trabajo y las marcas temporales reales de los diez bloques más recientes.
</Answer>

Veamos valores del 25 de octubre de 2026 a las 02:26 EDT. El [explorador](https://explorer.blocknetcrypto.com) mostraba dificultad siguiente de 16.943.147: unos 16,9 millones de hashes por bloque. Dividir entre 300 da aproximadamente 56.477 H/s.

Al mismo tiempo, bntpool.com mostraba 44,44 KH/s, o 44.440 H/s. Son estimaciones desde ángulos distintos. La dificultad da la tasa que produciría bloques exactamente cada cinco minutos. El panel divide el trabajo real de los últimos diez bloques entre su tiempo real: si tardan más, la cifra baja. Blocknet ajusta la dificultad cada bloque, acercando ambos valores con el tiempo.

## ¿Por qué fluctúa la tasa de hash?

Cada cifra tiene sus motivos:

- **Tu máquina.** Al principio seine compara hilos y kernels y elige el más rápido, por lo que la tasa aumenta al ajustarse. Después, otros usos de memoria o CPU pueden ocupar parte del tiempo de cálculo.
- **La visión del pool.** Los shares son aleatorios. La dificultad busca unos 10 cada cinco minutos, unos 120 por hora; una racha afortunada o tranquila mueve la estimación. Un rig nuevo se promedia sobre los minutos minados hasta completar una hora.
- **La red.** Los tiempos de bloque también son aleatorios y diez bloques son una muestra corta. El 25 de octubre de 2026, el panel marcó unos 50.057 H/s a las 00:26 EDT, 62.329 a las 02:03 y 44.440 a las 02:26. Las entradas y salidas de mineros añaden cambios reales a ese ruido.

Para decidir, usa medias: la de sesión de seine, el gráfico de 24 horas y varias consultas de la red durante el día.

## ¿Qué significa tu tasa para las recompensas?

Como cada hash tiene igual probabilidad, tu fracción de la tasa de red es tu parte esperada de bloques. Blocknet produce 288 diarios: con el 0,1% puedes esperar recompensas equivalentes a unos 0,29 bloques por día a largo plazo. [Estimar recompensas Blocknet](/blog/es/estimate-blocknet-mining-rewards/) lo convierte a BNT con los datos del [panel](/).

En bntpool.com, esa parte llega de cada bloque encontrado, acreditada por shares aceptados, con pequeños incrementos a lo largo del día. Si aún no empezaste, la [guía de instalación](https://seineminer.com/es/blog/how-to-mine-blocknet/) te lleva de descargar a enviar shares en minutos.

<Note>
Estas tasas describen el Argon2id actual. Blocknet planea Faraday alrededor del 16 de febrero de 2027, según [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), con un cambio previsto de prueba de trabajo. Mantén seine actualizado: busca versiones al iniciar y cada 12 horas.
</Note>

<CTA target="seine" title="Consulta tu tasa de hash" text="Descarga seine, deja que se ajuste unos minutos y sigue tu tasa en su panel y en My Stats." />
