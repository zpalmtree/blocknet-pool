---
title: 'Dificultad de minería: red y shares'
description: 'Entiende el objetivo de la dificultad de minería, cómo lo ajusta Blocknet y por qué la dificultad de red y la de las shares afectan a estadísticas distintas.'
tags: ['Conceptos de minería', 'Dificultad', 'Blocknet']
faq:
  - q: '¿Una dificultad mayor reduce mi hashrate?'
    a: 'La dificultad cambia el objetivo que debe cumplir un resultado. No cambia directamente cuántos hashes calcula tu equipo por segundo. Con el mismo hashrate, una dificultad de red mayor alarga el intervalo esperado entre bloques. Comprueba el rendimiento del minero por separado de la frecuencia de bloques o shares.'
  - q: '¿Una share de mayor dificultad paga más?'
    a: 'Representa más trabajo de hash esperado, pero con el mismo hashrate se envían menos shares de ese tipo. Compara el trabajo ponderado por dificultad durante el mismo periodo. En bntpool.com, las recompensas también dependen de los bloques aceptados y del cálculo PPLNS, no del número bruto de shares.'
  - q: '¿Con qué frecuencia ajusta Blocknet la dificultad?'
    a: 'Blocknet calcula la dificultad del bloque siguiente después de cada bloque a partir del historial reciente. La regla actual utiliza una media móvil ponderada linealmente de 60 intervalos y busca 300 segundos por bloque. Los intervalos recientes pesan más. Cada bloque puede llegar antes o después de ese objetivo.'
sourceHash: '5c203e8c7523'
translation: draft
---

La dificultad conecta el trabajo de tu equipo con los bloques que acepta la red. Esta guía explica el objetivo, el ajuste de Blocknet y la dificultad independiente de las shares del pool. Úsala para interpretar sus cambios antes de comparar hashrate, tiempos de bloque o la [calculadora de recompensas](/blog/es/estimate-blocknet-mining-rewards/#mining-calculator).

## ¿Qué es la dificultad de minería?

<Answer>
La dificultad describe el trabajo de hash esperado para encontrar un resultado que cumpla un objetivo. Al aumentar, los resultados válidos son menos frecuentes con el mismo hashrate. En Blocknet, la dificultad de red equivale aproximadamente a los hashes esperados por bloque; la de las shares corresponde a otro objetivo de envío.
</Answer>

Cada hash produce un número. El resultado cumple si ese número satisface el umbral objetivo. Un objetivo menor acepta una fracción menor de los resultados posibles y tiene mayor dificultad. Blocknet obtiene el objetivo de 256 bits dividiendo el mayor valor de 256 bits entre la dificultad. Por eso duplicarla duplica aproximadamente el trabajo esperado sin cambiar la operación de hash.

## ¿Cómo ajusta Blocknet la dificultad de red?

Blocknet calcula la dificultad siguiente después de cada bloque. La regla actual de la red principal utiliza una media móvil ponderada linealmente, LWMA, de 60 intervalos recientes. Los más nuevos pesan más. El objetivo es 300 segundos por bloque. Al combinar dificultad previa y tiempos ponderados, una sucesión de bloques rápidos tiende a elevarla, y una de bloques lentos tiende a reducirla.

El código limita los intervalos usados en el cálculo y aplica una dificultad mínima. La ventana de 60 intervalos necesita 61 marcas de tiempo; no significa que se ajuste cada 60 bloques. Estas reglas buscan mantener el ritmo de la cadena cerca del objetivo cuando cambia la potencia de cálculo. Cinco minutos es una media: un bloque individual puede llegar mucho antes o después.

## Dificultad de red y dificultad de las shares

El pool asigna un objetivo de share para observar el trabajo aportado entre bloques. Generalmente resulta más fácil cumplirlo que el objetivo de la red. Ambos valores describen trabajo esperado, pero responden a preguntas diferentes y los fijan partes distintas del sistema:

| Dificultad | Qué controla |
|---|---|
| Red | El umbral de trabajo de un bloque candidato, derivado de las reglas de consenso. |
| Share | El umbral de envío que el pool asigna a una conexión o trabajo de minería. |

bntpool.com utiliza dificultad variable, o vardiff, para ajustar la frecuencia de envíos. Con el mismo hashrate, una dificultad mayor suele producir menos shares, cada una con más trabajo esperado. Esto no cambia la dificultad de red. Un resultado que también cumple el objetivo de red puede ser candidato a bloque; el daemon aún debe validarlo. La [guía de resultados de shares](/blog/es/accepted-rejected-stale-shares/) explica los mensajes de aceptación y rechazo.

## ¿Cómo afecta al tiempo esperado entre bloques?

En Blocknet, divide la dificultad de red entre el hashrate que realiza el trabajo para estimar el intervalo medio en segundos. Usa H/s y valores del mismo periodo. El ejemplo mantiene constantes ambos valores para mostrar el cálculo. Son cifras ilustrativas, no mediciones actuales de la red ni una previsión para un minero concreto.

**Segundos esperados por bloque ≈ dificultad de red ÷ hashrate en H/s**

| Dificultad de red | Hashrate | Intervalo esperado |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Con dificultad 30.000 y 100 H/s, la media es 300 segundos. Al duplicar la dificultad a 60.000 manteniendo el hashrate, pasa a 600 segundos. Si el hashrate también se duplica a 200 H/s, vuelve a 300 segundos. Son medias de intentos repetidos. La [guía de suerte del pool](/blog/es/mining-pool-luck-explained/) explica por qué una ronda real puede durar más o menos.

## ¿Por qué cambian las recompensas con un hashrate estable?

Manteniendo tu hashrate, una dificultad de red mayor reduce los bloques esperados por unidad de tiempo mientras se mantenga. Si crece el hashrate total y la dificultad lo sigue, tu proporción del trabajo de red disminuye. La recompensa por bloque es otra variable: el [calendario de recompensas](/blog/es/blocknet-block-reward-schedule/) la cambia de forma independiente. La dificultad no fija los BNT de la recompensa programada.

Usa la calculadora con datos de red recientes para explorar cantidades esperadas de BNT. Comprueba si el hashrate introducido procede del minero o del pool para aplicar bien la comisión del minero, y usa la de tu versión. Las estimaciones son medias bajo ciertos supuestos. El pago real depende además de bloques encontrados, confirmaciones y [PPLNS](/blog/es/how-pplns-payouts-work/); la dificultad por sí sola no determina cuándo cobrarás.

## ¿Qué dificultad debes consultar?

Empieza por la pregunta que quieres resolver. Para descubrir bloques, usa la dificultad de red y el hashrate del minero o pool correspondiente. Para la frecuencia de shares, usa la dificultad asignada a esos trabajos. Para saldos o pagos, abre [My Stats](/stats) y el [historial de pagos](/payouts). Al comparar valores, conserva la fecha, el alcance y las unidades de cada lectura.

El [panel del pool](/) y el [historial de bloques](/blocks) aportan contexto actual. El hashrate de red se estima con el trabajo reciente y el tiempo transcurrido, por lo que puede diferir de la dificultad siguiente dividida entre el objetivo de 300 segundos. La [guía de hashrate](/blog/es/what-is-hashrate/) explica la diferencia. Distingue dificultad, hashrate y suerte al investigar un cambio: describen aspectos distintos.

<CTA target="pool" title="Conecta tu minero a bntpool.com" text="Usa la página de configuración y sigue tus shares y actividad en My Stats." />
