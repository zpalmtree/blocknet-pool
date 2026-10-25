---
title: Cómo estimar tus recompensas de minería Blocknet
description: Calcula cuánto BNT mina tu hardware al día con una fórmula, aprende dónde leer los datos en bntpool.com y sigue un ejemplo con fecha y cifras reales.
tags: [Recompensas de minería, Guía de minería, Tasa de hash, bntpool]
faq:
  - q: '¿Cómo se calculan las recompensas Blocknet?'
    a: 'Divide tu tasa de hash entre la de la red, multiplica por los 288 bloques diarios de Blocknet y por la recompensa actual. El resultado es el BNT bruto esperado al día. Consulta la tasa de la red en bntpool.com y la tuya en seine o My Stats.'
  - q: '¿Dónde veo la tasa de hash de la red Blocknet?'
    a: 'En Network Hashrate del panel de bntpool.com, normalmente en KH/s: multiplica por 1000 para obtener H/s. El campo network_hashrate del API bntpool.com/api/stats da el mismo valor en H/s. Se mide sobre los diez últimos bloques y cambia con cada bloque.'
  - q: '¿Cambia la recompensa de bloque con el tiempo?'
    a: 'Sí. Es fija durante cada mes de 30 días y 8640 bloques, y baja al empezar el siguiente hasta llegar a 2,0 BNT permanentes por bloque en el mes 48. Divide la altura entre 8640, redondea hacia abajo y busca ese mes en el calendario.'
  - q: '¿Por qué mis recompensas diarias difieren de la estimación?'
    a: 'La estimación es una media. En un día el pool puede encontrar más o menos bloques de lo esperado y la tasa de la red cambia al entrar mineros. Tras una semana o más de minería constante, las oscilaciones se promedian. Puedes seguir los créditos en My Stats.'
  - q: '¿Minar en un pool cambia cuánto BNT puedo esperar?'
    a: 'Tu parte esperada de las recompensas de la red permanece igual: depende solo de tu parte de su tasa de hash. El pool cambia el momento de cobro. En vez de esperar días por un bloque entero, recibes parte de cada bloque de bntpool.com, en cantidades pequeñas y regulares.'
sourceHash: 28d46138019e
translation: draft
---

Esta guía enseña a estimar cuánto BNT mina tu hardware al día. Verás la fórmula, dónde leer cada dato en el [panel de bntpool.com](/) y un ejemplo fechado con datos reales del pool y las mejores tasas medidas por seine.

## ¿Cuánto BNT minará tu hardware al día?

<Answer>
El BNT esperado al día es tu tasa de hash dividida entre la de la red, por 288 bloques diarios, por la recompensa actual. Tu tasa procede de seine o My Stats, la de la red del panel de bntpool.com, y la recompensa de la altura y del calendario Blocknet.
</Answer>

La fórmula es:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Cada parte tiene una razón:

- **288 bloques diarios.** Blocknet busca un bloque cada cinco minutos: 86.400 segundos diarios entre 300 por bloque son 288. La dificultad cambia cada bloque para mantener el ritmo al entrar mineros.
- **Tu parte de la tasa es tu parte de los bloques.** La dificultad aproxima los hashes esperados para encontrar un bloque. Todos tienen igual probabilidad, así que una máquina que hace el 1% de los hashes espera el 1% de los bloques con el tiempo.
- **La altura fija la recompensa.** Todos los bloques de un mes de 30 días y 8640 bloques pagan lo mismo, así que para cualquier día de ese mes es una constante conocida.

El resultado es la recompensa bruta esperada, promediada en el tiempo.

## ¿Dónde encuentras cada dato?

### Tu tasa de hash

seine la muestra en su panel de terminal desde que arranca. Dale unos minutos: ajusta hilos y kernel CPU con trabajo real, y la cifra se estabiliza cuando encuentra la mejor configuración.

En el pool, **Hashrate** de [My Stats](/stats) muestra la tasa observada por este, promediada en la última hora. Usa la cifra estable de seine o el gráfico de 24 horas.

### Tasa de hash de la red

**Network Hashrate** muestra toda la red en KH/s: 62,33 KH/s son 62.330 H/s. Convierte antes de dividir o te equivocarás por un factor de mil.

El pool calcula el valor con el trabajo y las marcas temporales de los últimos diez bloques, por lo que cambia entre bloques. [¿Qué es la tasa de hash?](/blog/es/what-is-hashrate/) explica el cálculo y sus oscilaciones. Para una entrada más estable, consulta varias veces al día y usa la media. También puedes obtener H/s del API:

```sh
curl -s https://bntpool.com/api/stats
```

La sección `chain` incluye `network_hashrate` en H/s y `current_job_height`, el bloque que trabaja el pool.

### Recompensa de bloque

**Current Block** muestra la altura en la que trabaja la red. Divide entre 8640 y redondea hacia abajo para obtener el mes; búscalo en el [calendario de recompensas](/blog/es/blocknet-block-reward-schedule/). [Blocks](/blocks) permite contrastarlo: su columna Reward muestra la recompensa completa de cada hallazgo.

## Un ejemplo del 25 de octubre de 2026

<Note>
Este ejemplo usa una captura de https://bntpool.com/api/stats del 25 de octubre de 2026 a las 02:03 EDT. La tasa de la red y la recompensa cambian: introduce los valores actuales antes de usar el resultado.
</Note>

| Dato | Valor en la captura |
|---|---|
| Bloque actual | 69.020 (mes 7, porque 69.020 ÷ 8640 = 7,99) |
| Recompensa | 467,67 BNT |
| Tasa de la red | 62.329 H/s (62,33 KH/s en pantalla) |
| Tasa de bntpool.com | 5240 H/s |

Tomemos el servidor AMD EPYC Zen 5 medido por seine a 49,4 H/s:

1. Parte de la red: 49,4 ÷ 62.329 = 0,0793%.
2. Bloques diarios esperados: 0,000793 × 288 = 0,2283.
3. BNT diarios esperados: 0,2283 × 467,67 = unos 106,8 BNT.

El mes 8 comienza a la altura 69.120, unos 100 bloques después de la captura, y paga 439,46 BNT. Con la misma tasa de red, el mismo servidor pasa a 0,2283 × 439,46 = unos 100,3 BNT diarios. Por eso conviene revisar la recompensa al empezar cada mes.

El mismo cálculo con esa tasa de red y el mejor resultado medido de cada generación:

| Hardware | Hilos | Tasa de hash | Parte de la red | BNT/día a 467,67 | BNT/día a 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Servidor Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

Lo que cuenta es tu máquina. Ejecuta seine un rato y usa su tasa estable en vez de la tabla. La guía de [hardware y tasa de hash](https://seineminer.com/es/blog/blocknet-mining-hardware-hashrate/) explica el papel de RAM y ancho de banda. Los resultados completos están en la [página de rendimiento](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## ¿Cómo cambia la estimación al minar en bntpool.com?

Tu parte esperada es igual. Cambia cómo recibes la recompensa. bntpool.com usa PPLNS: cada bloque se reparte entre los shares de las seis horas anteriores, en proporción a su dificultad. [Cómo funcionan los pagos PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto.

El cálculo desde el lado del pool da lo mismo. Sus 5240 H/s eran el 8,4% de la red, así que esperaba 288 × 5240 ÷ 62.329 = unos 24,2 bloques al día, aproximadamente uno por hora. El panel lo muestra como **Expected Block Time** en Round Progress. Un servidor de 49,4 H/s supone el 0,94% del pool; el 0,94% de 24,2 bloques a 467,67 BNT vuelve a dar unos 106,8 BNT diarios. La tasa del pool se cancela.

La diferencia es el tiempo. Solo, ese servidor esperaría un bloque entero cada 4,4 días; [pool frente a solo](/blog/es/pool-vs-solo-mining-blocknet/) lo compara en detalle. En bntpool.com recibe parte de cada uno de los aproximadamente 24 bloques diarios, así que el saldo crece en pasos pequeños continuamente. La comisión del pool y la de desarrollo de seine se descuentan de la cifra bruta.

Dos detalles afectan al primer día:

- **La ventana se llena en seis horas.** La parte del minero nuevo aumenta al llenar la ventana PPLNS y luego se mantiene plena mientras siga minando.
- **Las recompensas maduran antes de pagarse.** Pasan a Confirmed Rewards tras 60 confirmaciones, unas cinco horas, y se envían automáticamente cuando el saldo llega a 0,1 BNT.

## ¿Cómo comparas la estimación con resultados reales?

La estimación es una media; los días oscilan según la suerte de cada ronda. Tras muchas rondas, el esfuerzo medio ronda el 100%, por lo que una semana constante sirve para comparar. Anota Confirmed Rewards más Paid Balance en [My Stats](/stats) al principio y al final. El aumento es lo acreditado por bloques que maduraron esa semana. Compáralo con tu estimación de esos mismos días, usando sus recompensas y tasas de red.

Repite la fórmula si cambia un dato:

- **Al empezar cada mes**, cuando baja la recompensa. Los límites están cada 8640 bloques, unos 30 días.
- **Cuando cambia la tasa de red**, al entrar mineros.
- **Cuando cambia tu tasa**, por añadir máquinas, reservar huge pages en Linux o incorporar una GPU NVIDIA, que seine usa junto a la CPU.

<Note>
Estas tasas corresponden al Argon2id actual de Blocknet. Faraday está previsto alrededor del 16 de febrero de 2027 según [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) y planea cambiar la prueba de trabajo. Mantén seine actualizado: busca versiones al arrancar y cada 12 horas.
</Note>

<CTA target="seine" title="Mide tu tasa de hash" text="Descarga seine, deja que se ajuste unos minutos y usa su tasa en la fórmula con los datos actuales del panel." />
