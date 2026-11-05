---
title: Minería Blocknet en pool o en solitario
description: Elige pool o minería en solitario para Blocknet. Calcula bloques diarios, compara recompensas compartidas y en solitario en bntpool.com y configura la minería solo.
tags: [Guía de minería, Minería en solitario, bntpool, PPLNS]
faq:
  - q: '¿Cada cuánto encontraría un bloque Blocknet en solitario?'
    a: 'La media es 288 × (tu tasa de hash ÷ tasa de la red) bloques diarios, porque Blocknet busca uno cada cinco minutos. Con 50 H/s en una red de 50.000 H/s son 0,288 bloques al día, uno cada tres días y medio aproximadamente. Consulta la tasa actual en bntpool.com.'
  - q: '¿Necesito un nodo Blocknet para minar en bntpool.com?'
    a: 'No. El pool ejecuta el nodo, crea las plantillas y envía los bloques encontrados. Solo necesitas seine y una dirección Blocknet o nombre Blocknet ID. Ejecuta seine, pega la dirección y acepta el pool predeterminado: tu parte de cada bloque se paga allí.'
  - q: '¿Puede seine minar Blocknet en solitario?'
    a: 'Sí, de dos formas. Pon solo: delante de tu dirección, como en ./seine --address solo:YOUR_ADDRESS, y bntpool.com te paga la recompensa completa de cada bloque que encuentres, sin nodo que ejecutar. O inicia seine con --mode daemon para minar contra tu propio nodo Blocknet sincronizado.'
  - q: '¿Puedo cambiar luego entre pool y solo?'
    a: 'Sí. La misma descarga sirve para todas las opciones. Añade o quita el prefijo solo: de tu dirección, o añade --mode daemon para tu propio nodo, y reinicia seine cuando quieras cambiar. Los shares ya enviados para recompensas compartidas permanecen en la ventana PPLNS de seis horas y siguen ganando mientras estén dentro.'
sourceHash: 'aecc3e8f992f'
translation: draft
---

Esta guía te ayuda a elegir entre las recompensas compartidas del pool y la minería en solitario para Blocknet, ya sea en solitario a través de bntpool.com o contra tu propio nodo. Calcularás cuántos bloques encuentra tu tasa por sí sola, verás cómo el pool los convierte en pagos regulares y aprenderás a configurar cada opción con seine.

## ¿Conviene minar Blocknet en pool o en solitario?

<Answer>
Ambos siguen el mismo cálculo: tu parte de la tasa de hash de la red determina tu parte de sus bloques. En bntpool.com ganas una fracción de cada bloque encontrado, con pagos regulares. En solitario conservas toda la recompensa de tus hallazgos, adecuado para operaciones grandes. bntpool.com ofrece ambas opciones.
</Answer>

Para la mayoría, bntpool.com es un comienzo sencillo. Es el pool predeterminado de seine: solo introduces tu dirección y no tienes que ejecutar ni sincronizar un nodo. La minería solo también está plenamente soportada. Pon `solo:` delante de tu dirección y bntpool.com te paga la recompensa completa de cada bloque que encuentren tus rigs, sin nodo que ejecutar. seine también tiene un modo daemon que mina directamente contra tu propio nodo Blocknet. El resto de la guía ayuda a decidir según tu hardware.

## ¿Cuántos bloques encontrarías en solitario?

<Answer>
Blocknet busca un bloque cada cinco minutos, 288 al día. Solo, puedes esperar 288 × (tu tasa ÷ tasa de la red) bloques diarios. Divide un día entre ese número para obtener el intervalo medio. Consulta la tasa de la red en el panel de bntpool.com.
</Answer>

La fórmula funciona porque Blocknet ajusta la dificultad para que la red encuentre un bloque cada 300 segundos de media. La dificultad aproxima los hashes necesarios antes de acertar. Como cada hash tiene igual probabilidad, tu fracción de los hashes es tu fracción esperada de los bloques.

Necesitas dos números:

- **Tu tasa de hash.** seine la muestra mientras mina; [My Stats](/stats) también tras minar en el pool.
- **La tasa de red.** El [panel](/) la muestra en Network Hashrate, junto a Pool Hashrate. Ambas cambian: revísalas al calcular.

Cada bloque paga la recompensa de su mes. El [calendario](/blog/es/blocknet-block-reward-schedule/) las enumera.

## Un ejemplo calculado

<Note>
Son cifras redondas hipotéticas para explicar el cálculo, no datos actuales. Usa la tasa de red del panel para tu estimación.
</Note>

Supón una red de 50.000 H/s. Un rig de 50 H/s, aproximadamente lo medido por seine en un servidor AMD EPYC Zen 5, tiene el 0,1%. Esto encontrarían ese rig y dos operaciones mayores:

| Tu tasa (ejemplo) | Parte de la red | Bloques diarios esperados | Intervalo medio |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | unas 83 horas (3,5 días) |
| 500 H/s | 1% | 2,88 | unas 8,3 horas |
| 5000 H/s | 10% | 28,8 | unos 50 minutos |

Pon ahora el rig de 50 H/s en un pool de 5000 H/s totales. Este encuentra unos 28,8 bloques diarios, uno cada 50 minutos de media. El rig aporta el 1% del trabajo y recibe el 1% de cada bloque. Sigue siendo el equivalente a 0,288 bloques diarios. Cambia la llegada: una parte pequeña cada unos 50 minutos, frente a un bloque entero cada varios días.

La tabla también explica por qué solo encaja con operaciones grandes. A 5000 H/s, un minero encuentra bloques tan a menudo como un pool de ese tamaño y sus ingresos son regulares por sí mismos.

Para convertir bloques diarios en BNT con un ejemplo fechado de datos reales, consulta [cómo estimar tus recompensas](/blog/es/estimate-blocknet-mining-rewards/).

## ¿Por qué ofrece un pool pagos más regulares?

bntpool.com reúne la tasa de todos y encuentra bloques mucho más a menudo que un solo rig. Reparte cada recompensa entre quienes ayudaron a encontrarla mediante PPLNS: revisa las seis horas anteriores y divide según la dificultad de los shares de cada minero.

Eso tiene varios efectos prácticos:

- **Ingresos de cada bloque.** Mientras tus shares estén en la ventana, ganas una parte de cada hallazgo. Síguelos en [Blocks](/blocks).
- **Pagos automáticos.** Tras 60 bloques de profundidad, tu parte pasa al saldo confirmado y se envía al alcanzar el pago mínimo, 1 BNT a 4 de noviembre de 2026. Los envíos aparecen en [Payouts](/payouts).
- **Continuidad al reiniciar.** Los shares permanecen seis horas: un reinicio o actualización de controlador no restablece tu posición.
- **Solo ejecutas el minero.** El pool gestiona nodo, plantillas y envíos. Puedes usar varios rigs para una dirección o un `@name` Blocknet ID.

La [guía PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto paso a paso con otro ejemplo. [Get Started](/start) muestra los ajustes actuales.

## ¿Por qué minar Blocknet en solitario?

La minería en solitario te paga la recompensa completa de cada bloque que encuentras. Encaja mejor cuando tu tasa encuentra bloques a menudo. En el ejemplo, una operación con el 10% de la red encuentra uno cada 50 minutos, así que los pagos en solitario llegan con la misma regularidad que los de un pool.

Hay dos formas de minar en solitario con seine:

- **En solitario en bntpool.com.** Pon `solo:` delante de tu dirección. El pool ejecuta el nodo, envía tus bloques y paga la recompensa completa de cada uno, menos la comisión del pool, a tu dirección mediante sus pagos normales. Puedes combinar rigs en solitario y compartidos en una misma dirección.
- **En solitario contra tu propio nodo.** El modo daemon de seine mina sobre las plantillas de tu propio nodo, y cada bloque paga directamente a su billetera. Si ya tienes un nodo Blocknet para tu billetera u otros servicios, la minería en solitario solo añade el minero.

seine también está preparado para el modo daemon. Trabaja sobre las plantillas del nodo y, si este olvidó una tras reiniciarse, vuelve a enviar el bloque resuelto completo para que llegue a la cadena.

## ¿Cómo configuras la minería solo con seine?

Para minar en solitario en bntpool.com, instala seine v0.3.1 o posterior e inícialo con el prefijo en tu dirección:

```sh
./seine --address solo:YOUR_BLOCKNET_ADDRESS
```

El panel marcará entonces tu cuenta como **Solo**. [Mina Blocknet en solitario en bntpool.com](/blog/es/solo-mine-blocknet-on-bntpool/) explica los nombres Blocknet ID, los nombres de rig, los pagos y dónde aparecen tus bloques en solitario en la página de estadísticas.

Para minar en solitario contra tu propio nodo:

1. **Instala y sincroniza un nodo.** Con el gestor `blocknet`, ejecuta:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Esto escribe la cookie API en `~/.config/bnt/data/mainnet/api.cookie`. bntpool.com publica `checkpoints.dat` en [Checkpoints](/checkpoints) para acelerar la primera sincronización; la [guía de checkpoints](/blog/es/blocknet-checkpoints-fast-sync/) explica su uso.
2. **Espera a que termine.** Los mensajes `[sync] progress` llegan al 100%. El nodo solo sirve plantillas cuando está al día.
3. **Inicia seine en modo daemon.**

   ```sh
   ./seine --mode daemon
   ```

   Para una configuración personalizada, indica API y cookie:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

La [guía solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) cubre contraseñas y configuraciones personalizadas. La [guía de seine](https://seineminer.com/es/blog/how-to-mine-blocknet/) explica la descarga y los primeros shares. Puedes [descargarlo](https://seineminer.com/es/#download) para Windows, macOS o Linux.

## ¿Cuál deberías elegir?

Aplica la fórmula con tu tasa y la de la red del [panel](/), y observa el intervalo medio.

- **Empieza en bntpool.com** si usas PC, portátil o pocos servidores, o quieres pagos regulares sin nodo. Ejecuta seine, pega tu dirección o `@name` y acepta el pool.
- **Mina en solitario en bntpool.com** si tu operación tiene una parte grande de la red, de modo que encuentra bloques a menudo por sí sola. Añade `solo:` a tu dirección y conserva el nodo, las estadísticas y los pagos del pool.
- **Mina en solitario contra tu propio nodo** si tienes esa tasa de hash y ya ejecutas, o quieres ejecutar, tu propio nodo sincronizado.

Puedes reconsiderarlo fácilmente. Todas las opciones usan la misma descarga: un prefijo o una opción y un reinicio bastan para cambiar al crecer tu hardware. Si tienes un nodo local mientras minas en el pool, seine también muestra el saldo de su billetera junto al del pool.

<Note>
Blocknet planea Faraday alrededor del 16 de febrero de 2027, según [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), con un cambio previsto de prueba de trabajo. seine v0.3.1 ya está preparado: la minería en pool pasa por sí sola a la nueva prueba de trabajo y, con las actualizaciones automáticas activadas, seine instala las nuevas versiones por ti.
</Note>

<CTA target="pool" title="Empieza a minar en bntpool.com" text="Ejecuta seine, pega tu dirección y recibe una parte de cada bloque que encuentre el pool, o añade solo: y conserva cada bloque que encuentres." />
