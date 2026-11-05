---
title: Mina Blocknet en solitario en bntpool.com
description: 'Mina Blocknet en solitario sin ejecutar un nodo. Pon solo: delante de tu dirección en seine y bntpool.com te paga la recompensa completa de cada bloque.'
tags: [Guía de minería, Minería en solitario, bntpool, seine]
faq:
  - q: '¿Cómo mino Blocknet en solitario en bntpool.com?'
    a: 'Actualiza seine a v0.3.1 o posterior y pon solo: delante de tu dirección de pago, por ejemplo ./seine --address solo:YOUR_ADDRESS. seine se conecta a bntpool.com como siempre, y el pool te paga la recompensa completa de cada bloque que encuentre tu rig mediante sus pagos normales. No necesitas ejecutar un nodo.'
  - q: '¿Necesito mi propio nodo Blocknet para minar en solitario?'
    a: 'No. Con el prefijo solo:, bntpool.com ejecuta el nodo, crea las plantillas de bloque y envía los bloques que encuentras. Tu rig solo ejecuta seine. El modo daemon de seine es la otra forma de minar en solitario, contra tu propio nodo sincronizado, y encaja con mineros que ya tienen uno.'
  - q: '¿Puedo usar un nombre Blocknet ID para minar en solitario?'
    a: 'Sí. Usa solo:@name o solo:$name en lugar de solo:ADDRESS, y ponlo entre comillas en la línea de comandos para que tu shell lo pase sin cambios. El pool resuelve el nombre a su dirección completa, acredita tus bloques en solitario y los paga a esa dirección como cualquier otro pago.'
  - q: '¿Cómo vuelvo a las recompensas del pool?'
    a: 'Quita el prefijo solo: de tu dirección y reinicia seine. Tu rig vuelve a compartir las recompensas PPLNS de cada bloque que encuentra el pool. Cada rig elige su modo al conectarse, así que puedes tener unos rigs en solitario y otros con recompensas compartidas en la misma dirección.'
sourceHash: '623b1e0b356a'
translation: draft
---

Esta guía te muestra cómo minar Blocknet en solitario en bntpool.com: añade un prefijo a tu dirección en seine y el pool te paga la recompensa completa de cada bloque que encuentren tus rigs. No instalas ni sincronizas un nodo. Lo configurarás, verás cómo se pagan los bloques en solitario y sabrás dónde seguirlos en el pool.

## ¿Qué es la minería en solitario en bntpool.com?

<Answer>
La minería en solitario en bntpool.com te paga solo por los bloques que encuentran tus propios rigs, y cada uno paga su recompensa de bloque completa a tu dirección. El pool sigue ejecutando el nodo, creando las plantillas de bloque y enviando tus bloques, así que obtienes recompensas en solitario con la misma configuración que la minería en pool. Se activa poniendo solo: delante de tu dirección.
</Answer>

En la minería normal en pool, bntpool.com reparte cada bloque entre los mineros cuyos shares están en su ventana PPLNS de seis horas, así que ganas una parte de cada bloque que encuentra el pool. Un rig en solitario conserva en cambio la recompensa completa de sus propios bloques y no gana nada de los bloques que encuentran otros. Ambos usan el mismo servidor, la misma dirección y los mismos pagos. Solo cambia el inicio de sesión.

Minar en solitario a través del pool te da recompensas en solitario sin el trabajo de un nodo:

- **Sin nodo que ejecutar.** El nodo del pool sigue la cadena, sirve plantillas nuevas y envía tus bloques resueltos.
- **Pagos automáticos.** Tus bloques se pagan a tu dirección o nombre Blocknet ID como cualquier otro pago de bntpool.com.
- **Estadísticas en vivo.** Tus rigs, tu tasa de hash y tus bloques aparecen en la [página de estadísticas](/stats) del pool.
- **Un prefijo para cambiar.** Quita `solo:` y reinicia seine para volver a las recompensas compartidas.

El modo daemon de seine es la otra forma de minar en solitario, contra tu propio nodo Blocknet sincronizado. [Minería Blocknet en pool o en solitario](/blog/es/pool-vs-solo-mining-blocknet/) compara las tres opciones y muestra cada cuánto encuentra un bloque tu tasa de hash.

## ¿Cómo empiezas a minar en solitario con seine?

<Answer>
Instala seine v0.3.1 o posterior e inícialo con solo: delante de tu dirección Blocknet, por ejemplo ./seine --address solo:YOUR_ADDRESS en macOS o Linux. seine se conecta a bntpool.com, su pool predeterminado, y el panel marca tu cuenta como Solo. También puedes escribir solo: y tu dirección cuando seine te pida una dirección de pago en su primera ejecución.
</Answer>

1. **Actualiza seine.** Los inicios de sesión en solitario necesitan seine v0.3.1 o posterior. [Descarga seine](https://seineminer.com/es/#download) o deja que las actualizaciones automáticas lo instalen.
2. **Inicia seine con el prefijo.** En macOS o Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   En Windows, en PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine mina en bntpool.com por defecto. La página [Get Started](/start) del pool muestra el mismo comando con la dirección del pool escrita.
3. **Revisa el panel.** El panel de saldo marca tu cuenta como **Solo**, y el registro muestra la confirmación del pool de que se te paga por los bloques que encuentra esta dirección.

El prefijo funciona en cualquier lugar donde seine acepta una dirección: la pregunta de la primera ejecución, la configuración guardada, la API de control local y los rigs del proxy de granja. seine lo guarda con tu dirección, así que las ejecuciones posteriores siguen minando en solitario.

## ¿Puedes minar en solitario con un nombre Blocknet ID?

Sí. Un nombre Blocknet ID funciona igual que una dirección completa. Escribe `solo:@name` o `solo:$name` y ponlo entre comillas para que tu shell lo pase sin cambios:

```sh
./seine --address 'solo:@yourname'
```

El pool resuelve el nombre en blocknet.id y paga tus bloques en solitario a la dirección que hay detrás. Para nombrar un rig, añádelo a una dirección completa tras un punto, como en `solo:YOUR_ADDRESS.rig1`, o pasa `--pool-worker rig1` con cualquier dirección o nombre. El prefijo acepta mayúsculas y minúsculas, así que `SOLO:` también funciona.

## ¿Cómo se pagan los bloques en solitario?

Cuando tu rig encuentra un bloque, el pool lo registra como bloque en solitario. Cuando alcanza 60 bloques de profundidad, unas cinco horas, el pool acredita en tu saldo su recompensa de bloque completa, menos la comisión del pool. La comisión es del 0% a 4 de noviembre de 2026. Después, tu saldo se paga a tu dirección mediante los pagos normales del pool.

Los pagos siguen las mismas reglas que la minería en pool. Cada bloque espera 60 confirmaciones para que una reorganización corta de la cadena no pueda deshacer una recompensa ya pagada, y [confirmaciones de bloque y recompensas de minería](/blog/es/block-confirmations-mining-rewards/) explica por qué. El endpoint `/api/info` del pool indicaba un pago mínimo de 1 BNT el 4 de noviembre de 2026, y una recompensa de bloque de Blocknet supera con creces esa cifra, así que un bloque en solitario confirmado se paga en el calendario normal de pagos del pool. La [página de pagos](/payouts) lista cada pago que envía el pool.

Si minas a una dirección de depósito de un exchange con un memo de pago, la minería en solitario funciona igual. El memo y su propio pago mínimo se aplican a tus bloques en solitario igual que a las recompensas compartidas.

El trabajo de tu rig sigue contando para la tasa de hash, el esfuerzo de ronda y la suerte del pool, y un bloque en solitario cierra la ronda del pool como cualquier otro bloque. Los shares en solitario nunca entran en la ventana PPLNS, así que otros mineros no ganan nada de tus bloques y tú no ganas nada de los suyos.

## ¿Cada cuánto encontrará un bloque un rig en solitario?

Blocknet busca un bloque cada cinco minutos, o 288 bloques al día. Un rig en solitario encuentra de media 288 × (tu tasa de hash ÷ tasa de hash de la red) de ellos cada día. Consulta la tasa de hash de la red en la [portada de bntpool.com](/) y la tuya en el panel de seine, y divide un día entre el resultado para obtener el tiempo medio entre tus bloques.

Las recompensas en solitario llegan como bloques enteros, así que encajan con mineros cuya tasa de hash encuentra bloques a menudo: granjas grandes, flotas de servidores y cualquiera con una parte grande de la red. Los rigs más pequeños ganan de media lo mismo con las recompensas PPLNS compartidas, pagadas en partes regulares de cada bloque que encuentra el pool. [Minería Blocknet en pool o en solitario](/blog/es/pool-vs-solo-mining-blocknet/) desarrolla ejemplos con tres tasas de hash.

## ¿Dónde ves tus rigs y bloques en solitario?

Busca tu dirección en la [página de estadísticas](/stats). Puedes pegarla con o sin el prefijo `solo:`. La página marca entonces tu trabajo en solitario:

- **Workers** lista cada rig con una columna Tipo que indica **Solo** para los rigs que enviaron shares en solitario dentro de la ventana de tasa de hash.
- **Estimated rewards** marca tus bloques en solitario mientras esperan sus confirmaciones.
- **Blocks** y **Luck** muestran una columna Tipo, Pool o Solo, antes del estado de cada bloque. La [página de bloques](/blocks) lista todos los bloques que ha encontrado el pool, los más recientes primero.

[Sigue tu minero en bntpool.com](/blog/es/track-your-miner-on-bntpool/) recorre el resto de la página de estadísticas.

## ¿Puedes combinar rigs en solitario y en pool?

Sí. Cada rig elige su modo al conectarse, así que una dirección puede tener rigs en solitario y rigs en pool a la vez. Una granja puede minar en solitario con sus servidores más grandes y mantener los equipos de escritorio con recompensas compartidas, todo pagado a un mismo saldo. El pool mantiene separados los dos tipos de trabajo: un share encontrado en una conexión en solitario solo cuenta para los bloques en solitario de esa dirección.

Para devolver un rig al pool, quita `solo:` de su dirección, en tu comando o en la configuración guardada, y reinicia seine. Los shares que el rig ya envió en modo solitario siguen siendo en solitario, y los nuevos shares entran en la ventana PPLNS de inmediato.

<CTA target="pool" title="Mina en solitario en bntpool.com" text="Añade solo: a tu dirección en seine y conserva la recompensa completa de cada bloque que encuentren tus rigs, sin nodo que ejecutar." />
