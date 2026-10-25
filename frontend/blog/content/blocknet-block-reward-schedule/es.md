---
title: Calendario de recompensas de bloque de Blocknet
description: Recompensa mensual de Blocknet, desde 723,25 BNT al inicio hasta una emisión perpetua de 2,0 BNT en el mes 48, con 288 bloques diarios y 98,6 millones previos.
tags: [Blocknet, Recompensa de bloque, Emisión, Minería]
faq:
  - q: '¿Cuál es la recompensa actual de Blocknet?'
    a: 'Depende del mes de la cadena, porque baja cada 8640 bloques. El 25 de octubre de 2026 estaba en el mes 7 y pagaba 467,67 BNT por bloque. El mes 8 comienza a la altura 69.120 con 439,46 BNT. Consulta la altura actual en el panel de bntpool.com y búscala en la tabla.'
  - q: '¿Cuántos bloques Blocknet se minan al día?'
    a: 'Blocknet busca un bloque cada cinco minutos: 288 al día y 8640 por mes de 30 días. La dificultad se ajusta cada bloque con una media ponderada de los últimos 60 tiempos, manteniendo la media cerca de cinco minutos cuando entran o salen mineros.'
  - q: '¿Cuál es la oferta total de Blocknet?'
    a: 'Se minan unos 98,56 millones de BNT antes de la emisión de cola del mes 48. El código y la web oficial lo redondean a unos 100 millones. Después, cada bloque paga 2,0 BNT para siempre, añadiendo 576 BNT diarios o unos 210.240 BNT anuales, de modo que la oferta sigue creciendo lentamente.'
  - q: '¿Cuándo comienza la emisión de cola de Blocknet?'
    a: 'En el mes 48, a la altura 414.720. Son 48 meses de 30 días, o 1440 días de bloques de cinco minutos desde el lanzamiento: aproximadamente cuatro años. La fecha exacta depende de los tiempos reales. Desde esa altura, cada bloque paga 2,0 BNT.'
  - q: '¿Cuándo se pueden gastar las monedas minadas?'
    a: 'La recompensa se puede gastar tras 60 bloques, unas cinco horas con bloques de cinco minutos. Se llama madurez de coinbase. bntpool.com también espera 60 bloques antes de pagar la recompensa, de modo que solo paga recompensas maduras.'
sourceHash: abea18c2c21b
translation: draft
---

Esta página enumera la recompensa Blocknet de cada mes de emisión, explica cómo se calcula y muestra lo que implica el calendario para los mineros.

## ¿Cuál es la recompensa de bloque de Blocknet?

<Answer>
La recompensa empezó en 723,25 BNT por bloque y baja una vez por mes de 30 días y 8640 bloques, siguiendo una curva de descenso suave. Es igual para todos los bloques del mismo mes. Desde el mes 48, a la altura 414.720, cada bloque paga una emisión de cola permanente de 2,0 BNT.
</Answer>

Blocknet busca un bloque cada cinco minutos (300 segundos): 288 diarios y 8640 por mes de 30 días. La dificultad se ajusta cada bloque con una media ponderada de los últimos 60 tiempos para mantener el ritmo cerca de cinco minutos cuando cambia la tasa de hash.

La recompensa depende solo del mes al que pertenece la altura. El mes es la altura dividida por 8640, redondeada hacia abajo: de 0 a 8639 es el mes 0, de 8640 a 17.279 es el mes 1, y así sucesivamente.

## ¿Cómo se calcula la recompensa?

El daemon calcula la recompensa mensual con una fórmula:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

El primer término es la parte superior a los 2,0 BNT de cola. Se reduce por un factor e^-0,75 cada año, así que tras doce meses queda aproximadamente el 47% del valor inicial. Distribuida por meses, esa parte es alrededor del 93,9% de la del mes anterior: pequeños descensos regulares.

El resultado se redondea hacia abajo a unidades atómicas enteras (1 BNT son 100.000.000 unidades). La tabla muestra cada recompensa con dos decimales.

## Tabla de recompensas mensuales de Blocknet

| Mes | Primer bloque | Recompensa por bloque (BNT) | Total al finalizar el mes (millones de BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| Desde el 48 | 414.720 | 2,00 | +0,017 por mes |

El bloque génesis de altura 0 no tiene recompensa, así que el mes 0 contiene 8639 bloques pagados. Todo BNT procede de estas recompensas, sin preminado, como explica [el lanzamiento justo de Blocknet](/blog/es/blocknet-fair-launch/). El 25 de octubre de 2026, el pool trabajaba en el bloque 69.016, cerca del final del mes 7. El [panel](/) muestra la altura actual para buscar su fila, y [Blocks](/blocks) enumera los bloques encontrados.

## ¿Qué ocurre en el mes 48?

<Answer>
El mes 48 comienza a la altura 414.720. Desde allí, cada bloque paga 2,0 BNT fijos para siempre: la emisión de cola. Mantiene una recompensa mientras funcione la cadena, para que los mineros siempre reciban un pago por asegurarla.
</Answer>

La fórmula de descenso se aplica de los meses 0 al 47 y termina en 40,22 BNT por bloque. Desde el 48, el daemon usa directamente el valor de cola: 2,0 BNT permanentes. Con 288 bloques diarios, añade 576 BNT al día o unos 210.240 al año.

El mes 48 empieza tras 1440 días de bloques de cinco minutos desde el lanzamiento, aproximadamente cuatro años. La fecha depende de los tiempos reales, por lo que la altura es la referencia fiable.

## ¿Cuánto BNT se mina antes de la cola?

Sumar todos los bloques desde la altura 1 hasta la 414.719 da 98.561.905,08 BNT, unos 98,6 millones. El código y la web oficial describen unos 100 millones de monedas en cuatro años más la cola; la suma exacta queda un poco por debajo de esa cifra redonda.

La mayor parte se mina al principio. Agrupada por año del calendario:

| Meses | BNT minados | Parte del total anterior a la cola |
|---|---|---|
| 0-11 (año 1) | 54,48 millones | 55,3% |
| 12-23 (año 2) | 25,84 millones | 26,2% |
| 24-35 (año 3) | 12,32 millones | 12,5% |
| 36-47 (año 4) | 5,93 millones | 6,0% |

## ¿Cuándo puedes gastar el BNT minado?

Una recompensa se puede gastar tras añadir otros 60 bloques encima, unas cinco horas después. Es la madurez de coinbase. bntpool.com espera los mismos 60 bloques para pagar y [Payouts](/payouts) enumera los envíos realizados.

## ¿Qué significa el calendario para los mineros?

Las recompensas son mayores al principio. Un bloque del mes 12 paga 342,69 BNT, alrededor del 47% de los 723,25 BNT del mes 0, y uno del mes 24 paga 162,93 BNT. Cada nuevo mes paga menos por bloque hasta llegar a la cola.

Tu parte sigue tu fracción de la tasa de hash de la red. En solitario, los bloques esperados al día son 288 × (tu tasa ÷ tasa de la red), y cada uno paga lo correspondiente a su mes. En un pool, el mismo cálculo se aplica a su tasa y las recompensas se reparten entre los mineros que aportaron trabajo: ganas de cada bloque del pool en vez de esperar uno entero propio. El [panel](/) muestra las tasas del pool y la red, y [My Stats](/stats) la tuya cuando ya minas.

Para empezar ahora:

1. Descarga seine desde [seineminer.com](https://seineminer.com/es/#download). Funciona en Windows, macOS y Linux y se ajusta a tu hardware.
2. Ejecútalo y pega tu dirección Blocknet o `@name`.
3. Acepta el pool predeterminado, `stratum+tcp://bntpool.com:3333`.

La [guía de bntpool.com](/blog/es/mine-blocknet-on-bntpool/) cubre el lado del pool, [Get Started](/start) enumera sus ajustes y la [guía de seine](https://seineminer.com/es/blog/how-to-mine-blocknet/) explica la instalación paso a paso. Para comparar procesadores, lee [hardware y tasa de hash Blocknet](https://seineminer.com/es/blog/blocknet-mining-hardware-hashrate/).

<Note>
Las cifras proceden del código de recompensas del daemon Blocknet, comprobado el 25 de octubre de 2026. Blocknet planea la actualización Faraday alrededor del 16 de febrero de 2027, según [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). Mantén seine actualizado: busca versiones al iniciarse y cada 12 horas.
</Note>

<CTA target="seine" title="Mina Blocknet con seine" text="seine es gratis para Windows, macOS y Linux y mina en bntpool.com de forma predeterminada." />
