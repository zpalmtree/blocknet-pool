---
title: 'Suerte en un pool de minería: esfuerzo y tiempo entre bloques'
description: 'Entiende la suerte del pool, el esfuerzo del 100% y las rondas largas. Consulta el historial de bntpool.com y distingue encontrar bloques de recibir pagos.'
tags: ['Pools de minería', 'Suerte del pool', 'Blocknet']
faq:
  - q: '¿Un esfuerzo del 100% significa que el pool está a punto de encontrar un bloque?'
    a: 'No. Significa que el trabajo registrado mediante participaciones ha alcanzado la cantidad esperada para un bloque con la dificultad utilizada en ese cálculo. Es una comparación con una media, no un porcentaje de finalización. Cada nuevo hash aún debe cumplir el objetivo de la red.'
  - q: '¿Es mejor un porcentaje de suerte más bajo en bntpool.com?'
    a: 'Primero consulta la etiqueta y su definición. La columna Esfuerzo y la tarjeta Suerte media de bntpool.com expresan el trabajo respecto al esperado: valores más bajos significan menos trabajo registrado por bloque. Otros sitios pueden utilizar la convención inversa. Compara métricas y tamaños de muestra equivalentes, no solo porcentajes.'
  - q: '¿Una ronda larga aumentará mi próximo pago?'
    a: 'Una ronda larga no garantiza un pago personal mayor. El hallazgo de bloques determina cuándo hay una recompensa disponible; la contabilidad PPLNS del pool determina tu parte según el trabajo aceptado elegible. Después se aplican las reglas de confirmación y pago. Consulta tus saldos en Mis estadísticas y la secuencia en la guía de pagos.'
sourceHash: dfd8c710a719
translation: draft
---

El panel del pool muestra tanto el trabajo de minería como los bloques encontrados con ese trabajo. Esta guía explica cómo interpretar los porcentajes de esfuerzo, el tiempo esperado entre bloques y el historial de rondas de bntpool.com. Podrás distinguir una media estadística de una cuenta atrás y comprobar tu conexión minera sin esperar al siguiente bloque del pool.

## ¿Qué es la suerte de un pool de minería?

<Answer>
La suerte de un pool describe cómo se compara el hallazgo real de bloques con la media esperada según su trabajo. Los bloques llegan al azar: algunos requieren poco trabajo y otros más. En bntpool.com, el esfuerzo de una ronda compara el trabajo de las participaciones aceptadas con la dificultad del bloque; un esfuerzo menor significa que encontró el bloque con menos trabajo registrado.
</Answer>

Un pool reúne los intentos de muchos mineros, pero sigue necesitando encontrar un hash que cumpla el objetivo de la red. Las participaciones aceptadas miden las contribuciones con un objetivo más fácil y permiten estimar cuánto trabajo de hash hubo entre hallazgos. La [introducción a los pools](/blog/es/what-is-a-mining-pool/) explica esa relación; la suerte describe la variación en los bloques resultantes.

## ¿Cómo calcula el pool el esfuerzo?

Para una ronda completada, bntpool.com divide el trabajo de las participaciones aceptadas entre la dificultad del bloque y multiplica por 100. Ese trabajo se pondera por dificultad: no es simplemente el número de participaciones enviadas. Una participación con mayor dificultad asignada representa más trabajo de hash esperado que otra de menor dificultad.

**Esfuerzo (%) = trabajo de participaciones aceptadas ÷ dificultad del bloque × 100**

Por ejemplo, supongamos que un bloque tiene dificultad 10.000 y el pool registra 5000 unidades de trabajo aceptado durante su ronda. Su esfuerzo es del 50%. Con 20.000 unidades de trabajo y la misma dificultad, sería del 200%. Son ejemplos aritméticos, no mediciones de un bloque concreto en producción.

| Esfuerzo | Cómo interpretarlo |
|---|---|
| 50% | La mitad del trabajo esperado para esa dificultad |
| 100% | La cantidad de trabajo esperada para un bloque |
| 200% | El doble del trabajo esperado para esa dificultad |

El esfuerzo de la ronda actual utiliza la dificultad actual, que puede cambiar conforme avanza la cadena. Las filas históricas utilizan la dificultad de su bloque. Por tanto, la cifra compara trabajo; no es un indicador de progreso que deba detenerse en el 100%. También es distinta de la duración de la ronda: un pool más rápido puede realizar el mismo trabajo en menos tiempo.

## ¿Qué significa el tiempo esperado entre bloques?

Es el intervalo medio indicado por la proporción estimada del hashrate de la red que aporta el pool. El panel de bntpool.com lo calcula multiplicando 300 segundos por el hashrate de la red y dividiendo entre el del pool. Utiliza las mismas unidades para ambos. La [guía de hashrate](/blog/es/what-is-hashrate/) explica por qué estas estimaciones cambian al llegar participaciones aceptadas y nuevos bloques.

**Segundos esperados = 300 × H/s de la red ÷ H/s del pool**

Si se estima la red en 100.000 H/s y el pool en 10.000 H/s, el pool aporta una décima parte del total estimado. Su intervalo esperado es de 3000 segundos, o 50 minutos. Esa media puede corresponder a una combinación de rondas cortas y largas; no programa el próximo bloque para dentro de exactamente 50 minutos.

El panel también compara el tiempo transcurrido con el esperado. Este porcentaje basado en tiempo y el esfuerzo basado en participaciones pueden diferir porque utilizan datos distintos. Comprueba las etiquetas antes de comparar dos cifras. Los cambios del hashrate del pool o de la dificultad de la red pueden modificar la estimación actual aunque siga en curso la misma ronda.

## ¿Tiene que llegar un bloque tras un esfuerzo del 100%?

<Answer>
Al alcanzar el 100% de esfuerzo, el pool no tiene un bloque pendiente por obligación. Los hashes anteriores fallidos no aumentan la probabilidad de que el siguiente cumpla un objetivo de red sin cambios. Con hashrate y dificultad constantes, la espera esperada desde ahora sigue siendo la misma, incluso después de una ronda larga. Más trabajo genera más intentos, no una fecha límite.
</Answer>

Un modelo sencillo de probabilidad ayuda a entenderlo. Con intentos independientes y una tasa constante de hallazgo de bloques, el tiempo de espera es aproximadamente exponencial. La probabilidad de seguir sin bloque tras un intervalo esperado es de alrededor del 36,8%; tras dos, del 13,5%; tras tres, del 5,0%. Estos porcentajes describen muchas rondas hipotéticas en condiciones fijas, no un pronóstico de la ronda actual.

| Intervalos esperados transcurridos | Probabilidad aproximada de seguir sin bloque |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

El cálculo es `exp(-t / T)`, donde `T` es el intervalo medio y `t` el tiempo transcurrido. La [referencia del NIST sobre la distribución exponencial](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) recoge esta función de supervivencia. El hashrate y la dificultad reales varían: interpreta la tabla como una explicación del azar. Una racha de bloques rápidos tampoco cambia la probabilidad del siguiente hash si el objetivo permanece igual.

## ¿Cómo se consulta el historial de suerte de bntpool.com?

Abre [Suerte](/luck) para comparar rondas completadas. Cada fila muestra un bloque, esfuerzo, duración de ronda, estado y hora de hallazgo. Observa varias rondas juntas en lugar de elegir una excepcionalmente corta o larga. El enlace del bloque permite inspeccionar el registro correspondiente en la cadena, y la paginación permite consultar un historial más amplio.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Página real del historial de suerte de bntpool.com con esfuerzo, duración de ronda y estado de bloque" width="1200" height="833" loading="lazy" />

Esta captura se tomó el 26 de octubre de 2026. Sus filas y estados son una instantánea histórica; consulta la página en vivo para ver valores actuales. Un estado pendiente o confirmado describe la confirmación del bloque. No cambia el significado del porcentaje de esfuerzo registrado y es independiente de un pago a tu dirección.

En el panel, Suerte media es la media aritmética de los porcentajes de esfuerzo registrados de bloques no huérfanos hasta la altura actual de la cadena. Pese al nombre de la tarjeta, sigue la convención del esfuerzo: un valor menor significa menos trabajo registrado por bloque. Las medias resumen esa muestra; no prometen que las rondas futuras compensen un resultado anterior concreto.

## ¿Cómo afecta la suerte del pool a tus recompensas?

La suerte del pool cambia cuándo se encuentran los bloques. PPLNS decide cómo repartir la recompensa de un bloque encontrado entre el trabajo aceptado elegible. Son pasos distintos. El límite entre rondas no convierte la ventana PPLNS en una regla de pago exclusiva de cada ronda; consulta [cómo funcionan los pagos PPLNS](/blog/es/how-pplns-payouts-work/) para conocer la ventana, las confirmaciones y el umbral de pago reales.

Por eso tu saldo puede cambiar por etapas aunque tu minero siga enviando participaciones aceptadas. La [calculadora de recompensas](/blog/es/estimate-blocknet-mining-rewards/#mining-calculator) estima la producción a partir del hashrate y los datos de la red. Sirve para estimar resultados a lo largo del tiempo; no predice el bloque ni el momento en que llegará un pago a tu monedero.

## ¿Qué debes comprobar durante una ronda larga?

Comprueba lo que puedes observar directamente: que el minero funcione, que sigan aceptándose participaciones y que la dirección correcta aparezca en [Mis estadísticas](/stats). Consulta [Estado](/status) para ver la salud pública del servicio. Si dejan de aceptarse tus participaciones, sigue la [guía de diagnóstico de estadísticas](/blog/es/track-your-miner-on-bntpool/#zero-hashrate); una ronda larga del pool y un equipo desconectado requieren comprobaciones distintas.

Cuando tu conexión funcione correctamente, interpreta la suerte con un historial significativo y mantén las mismas definiciones. Así cada cifra del panel tiene una función clara: el hashrate estima trabajo por segundo, el esfuerzo compara trabajo realizado con el esperado para un bloque y los saldos muestran la contabilidad de recompensas. Nada de ello requiere adivinar cuándo llegará el siguiente hash ganador.

<CTA target="pool" title="Empieza a minar en bntpool.com" text="Conecta seine, confirma tus participaciones aceptadas y sigue las rondas y recompensas en el panel del pool." />
