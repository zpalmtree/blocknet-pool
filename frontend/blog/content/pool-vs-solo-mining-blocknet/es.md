---
title: Minería Blocknet en pool o en solitario
description: Elige pool o minería en solitario para Blocknet. Calcula bloques diarios, conoce los pagos regulares de bntpool.com y configura la minería solo.
tags: [Guía de minería, Minería en solitario, bntpool, PPLNS]
faq:
  - q: '¿Cada cuánto encontraría un bloque Blocknet en solitario?'
    a: 'La media es 288 × (tu tasa de hash ÷ tasa de la red) bloques diarios, porque Blocknet busca uno cada cinco minutos. Con 50 H/s en una red de 50.000 H/s son 0,288 bloques al día, uno cada tres días y medio aproximadamente. Consulta la tasa actual en bntpool.com.'
  - q: '¿Necesito un nodo Blocknet para minar en bntpool.com?'
    a: 'No. El pool ejecuta el nodo, crea las plantillas y envía los bloques encontrados. Solo necesitas seine y una dirección Blocknet o nombre Blocknet ID. Ejecuta seine, pega la dirección y acepta el pool predeterminado: tu parte de cada bloque se paga allí.'
  - q: '¿Puede seine minar Blocknet en solitario?'
    a: 'Sí. Con ./seine --mode daemon mina directamente contra tu nodo sincronizado, usando su cookie API para conectar. Cada bloque que encuentres paga la recompensa completa a la billetera del nodo. La guía solo del repositorio cubre configuraciones personalizadas y contraseñas.'
  - q: '¿Puedo cambiar luego entre pool y solo?'
    a: 'Sí. La misma descarga sirve para ambos. Ejecuta seine normalmente para bntpool.com o añade --mode daemon para tu nodo, y reinícialo al cambiar. Los shares ya enviados permanecen en la ventana PPLNS de seis horas y siguen ganando mientras estén dentro.'
sourceHash: 171d240d6c4b
translation: draft
---

Esta guía te ayuda a elegir entre bntpool.com y minar contra tu propio nodo. Calcularás cuántos bloques encuentra tu tasa por sí sola, verás cómo el pool los convierte en pagos regulares y aprenderás a configurar ambas opciones con seine.

## ¿Conviene minar Blocknet en pool o en solitario?

<Answer>
Ambos siguen el mismo cálculo: tu parte de la tasa de hash de la red determina tu parte de sus bloques. En bntpool.com ganas una fracción de cada bloque encontrado, con pagos regulares. En solitario conservas toda la recompensa de tus hallazgos, adecuado para operaciones grandes con nodo propio.
</Answer>

Para la mayoría, bntpool.com es un comienzo sencillo. Es el pool predeterminado de seine: solo introduces tu dirección y no tienes que ejecutar ni sincronizar un nodo. La minería solo también está plenamente soportada mediante el modo daemon. El resto de la guía ayuda a decidir según tu hardware.

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
- **Pagos automáticos.** Tras 60 bloques de profundidad, tu parte pasa al saldo confirmado y se envía al alcanzar 0,1 BNT. Los envíos aparecen en [Payouts](/payouts).
- **Continuidad al reiniciar.** Los shares permanecen seis horas: un reinicio o actualización de controlador no restablece tu posición.
- **Solo ejecutas el minero.** El pool gestiona nodo, plantillas y envíos. Puedes usar varios rigs para una dirección o un `@name` Blocknet ID.

La [guía PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto paso a paso con otro ejemplo. [Get Started](/start) muestra los ajustes actuales.

## ¿Por qué minar Blocknet en solitario?

Controlas todas las partes del proceso. Tu nodo crea cada plantilla y comprueba todos los bloques, y cada hallazgo paga íntegramente a su billetera.

Encaja mejor cuando tu tasa encuentra bloques a menudo. En el ejemplo, una operación con el 10% de la red encuentra uno cada 50 minutos, con una regularidad similar a la del pool. Si ya tienes un nodo para tu billetera u otros servicios, solo añade el minero.

seine también está preparado para esto. Trabaja sobre las plantillas del nodo y, si este olvidó una tras reiniciarse, vuelve a enviar el bloque resuelto completo para que llegue a la cadena.

## ¿Cómo configuras la minería solo con seine?

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
- **Mina en solitario** si tienes una parte grande de la red, encuentras bloques a menudo y ya ejecutas o quieres ejecutar tu nodo sincronizado.

Puedes reconsiderarlo fácilmente. Ambos modos usan la misma descarga: una opción y un reinicio bastan para cambiar al crecer tu hardware. Si tienes un nodo local mientras minas en el pool, seine también muestra el saldo de su billetera junto al del pool.

<Note>
Blocknet planea Faraday alrededor del 16 de febrero de 2027, según [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), con un cambio previsto de prueba de trabajo. Mantén seine actualizado: busca versiones al arrancar y cada 12 horas.
</Note>

<CTA target="pool" title="Empieza a minar en bntpool.com" text="Ejecuta seine, pega tu dirección y recibe una parte de cada bloque, pagada automáticamente desde 0,1 BNT." />
