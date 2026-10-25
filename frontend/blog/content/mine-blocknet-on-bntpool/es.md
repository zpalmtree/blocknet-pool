---
title: Mina Blocknet en bntpool.com
description: Únete a bntpool.com, el pool predeterminado de seine. Conecta tu dirección o @name a stratum+tcp://bntpool.com:3333, nombra tus rigs y consulta tus datos.
tags: [Guía de minería, bntpool, Blocknet ID, seine]
faq:
  - q: '¿Cuál es la dirección Stratum de bntpool.com?'
    a: 'Es stratum+tcp://bntpool.com:3333, el pool que seine propone al iniciarse por primera vez. Pulsa Enter para aceptarlo o indícalo con --pool-url. Tu dirección Blocknet o nombre Blocknet ID es tu usuario, y el pool paga las recompensas a esa dirección.'
  - q: '¿Puedo minar en bntpool.com con un nombre Blocknet ID?'
    a: 'Sí. Introduce @name o $name donde seine o el pool pidan una dirección. El pool comprueba la correspondencia firmada de blocknet.id y acredita shares y pagos a la dirección completa asociada. Necesitas seine v0.2.16 o posterior y debes entrecomillar el nombre en la línea de comandos para que el shell no lo modifique.'
  - q: '¿Necesito una cuenta para minar en bntpool.com?'
    a: 'No. No hay registro, contraseña ni correo electrónico. Tu dirección Blocknet, o un nombre Blocknet ID que apunte a ella, es tu usuario y destino de pagos. Inicia seine con ella y búscala en My Stats para ver tu tasa de hash, shares, rigs, saldo y pagos.'
  - q: '¿Puedo minar desde varias computadoras hacia una dirección?'
    a: 'Sí. Ejecuta seine en cada computadora con la misma dirección o nombre y asigna un nombre distinto a cada rig. El pool cuenta shares, saldo y pagos por dirección, de modo que todos los rigs suman a un saldo y un pago, y My Stats muestra cada rig en su propia fila.'
sourceHash: f6080e03ffd0
translation: draft
---

Esta guía explica cómo unirte a bntpool.com con seine: la dirección del pool, el acceso con una dirección Blocknet o un `@name` de Blocknet ID, los nombres de los rigs y cómo comprobar que llegan tus shares. Si aún no tienes seine instalado, sigue primero la [guía de instalación](https://seineminer.com/es/blog/how-to-mine-blocknet/) y vuelve aquí.

## ¿Cuál es la dirección del pool bntpool.com?

<Answer>
La dirección Stratum es `stratum+tcp://bntpool.com:3333`. seine la ofrece como pool predeterminado al iniciarse por primera vez, así que basta con pulsar Enter cuando lo pregunta. Accedes con tu dirección Blocknet o tu `@name` de Blocknet ID. No hay que crear una cuenta y las recompensas llegan a esa dirección.
</Answer>

Estos son los ajustes del pool según su endpoint público `/api/info` del 25 de octubre de 2026:

| Ajuste | Valor |
|---|---|
| Dirección Stratum | `stratum+tcp://bntpool.com:3333` |
| Usuario | Dirección Blocknet, `@name` o `$name` |
| Sistema de pagos | PPLNS con una ventana de 6 horas |
| Confirmaciones antes del pago | 60 bloques |
| Pago mínimo | 0,1 BNT |

Puedes consultar esos mismos valores en cualquier momento en [Get Started](/start).

## ¿Por qué bntpool.com es el pool predeterminado de seine?

Cuando ejecutas seine por primera vez, pregunta por el pool y propone `stratum+tcp://bntpool.com:3333`. El pool y el minero encajan de varias formas prácticas:

- **Un protocolo.** bntpool.com usa el protocolo Stratum de seine, versión 2, por lo que solo necesitas configurar tu dirección.
- **Nombres en ambos lados.** seine y el pool aceptan nombres Blocknet ID y los verifican con el mismo registro firmado de blocknet.id.
- **Comprobación de versiones.** Cada rig comunica su versión de seine con sus shares. La página de estadísticas marca las versiones antiguas con una etiqueta "update available" que enlaza a las versiones de seine.
- **Una guía para configurar seine.** Get Started explica cómo descargarlo y ejecutarlo, con un comando para copiar.

## ¿Cómo conectas seine a bntpool.com?

Al iniciar seine por primera vez, aparecen dos preguntas:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Pega tu dirección o escribe tu nombre y pulsa Enter para aceptar bntpool.com. seine guarda ambas respuestas en `seine-data/seine.config.json`, por lo que los siguientes inicios pasan directamente a minar. También elige y guarda un nombre de rig en ese archivo, como veremos más abajo.

En un rig sin interfaz, un servicio o un script, nadie puede responder. Pasa ambos valores como opciones al primer inicio, con el mismo comando de Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Para cambiar después de pool o dirección, edita o elimina `seine-data/seine.config.json`, o sustituye los valores guardados con `--address` y `--pool-url`.

## ¿Puedes minar hacia un @name de Blocknet ID?

<Answer>
Sí. bntpool.com acepta un nombre corto Blocknet ID, escrito `@name` o `$name`, donde acepta una dirección. Comprueba la correspondencia firmada de blocknet.id, valida la dirección asociada y acredita shares, saldo y pagos a esa dirección completa durante toda la conexión.
</Answer>

Los nombres no distinguen mayúsculas: `@Alice` y `@alice` son el mismo usuario. La compatibilidad llegó con seine v0.2.16 y está en cualquier versión actual. Antes de minar, seine muestra la dirección obtenida para que compruebes que coincide con tu billetera. En la línea de comandos, usa comillas simples; sin ellas, el shell intentaría expandir `$name` como variable:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Si después apuntas tu nombre a otra billetera, reinicia seine. El minero resuelve el nombre guardado en cada inicio y mantiene la dirección durante toda la ejecución; el pool hace lo mismo por conexión. El saldo ya ganado permanece en la dirección que lo ganó y se paga allí, por lo que mover un nombre nunca mueve tus monedas.

Puedes registrar un nombre en [blocknet.id](https://blocknet.id). La [guía de Blocknet ID](https://seineminer.com/es/blog/blocknet-id-pay-by-name/) explica los pagos por nombre.

## ¿Cómo nombras tus rigs?

Cada conexión se identifica con un nombre de rig además de tu dirección. En el primer inicio, seine elige `seine-` seguido de cuatro cifras y lo guarda, así que la misma máquina conserva su nombre tras reiniciarse. Para elegir uno reconocible, indícalo en el comando:

```sh
./seine --pool-worker rig-01
```

El pool conserva hasta 64 caracteres. Un nombre estable también ayuda al reiniciar: recuerda la dificultad de shares de cada rig durante una hora, identificada por tu dirección y el nombre, de modo que al reconectarse retoma la dificultad ya ajustada.

## ¿Puedes minar hacia una dirección desde varios rigs?

Sí. Inicia seine en cada máquina con la misma dirección o `@name` y un nombre de rig diferente. El pool cuenta shares, saldos y pagos por dirección, así que todos suman a un saldo y un pago. Cada rig tiene su fila en las estadísticas, lo que facilita comprobar que todas las máquinas minan.

## ¿Cómo compruebas que tu minero funciona?

El panel de seine muestra la tasa de hash y los shares aceptados en la propia máquina. Para ver el lado del pool, abre [My Stats](/stats), introduce tu dirección, `@name` o `$name` y pulsa **Lookup**. Si usas un nombre, la página muestra la dirección resuelta junto al buscador. También recuerda la última consulta en ese navegador. La [guía para seguir tu minero](/blog/es/track-your-miner-on-bntpool/) explica todas las tarjetas.

Busca tres cosas:

- **Hashrate y Shares Accepted.** La tarjeta muestra la tasa que observa el pool y el contador de aceptados sube al enviar trabajo.
- **Una fila por rig.** Cada fila muestra nombre, tasa de hash, shares aceptados y rechazados, y hora del último share. Uno reciente indica que está conectado y trabajando.
- **Recent Shares.** Los últimos shares aparecen con el rig que los envió, su dificultad y estado.

El pool ajusta la dificultad por conexión mientras minas, de modo que incluso una CPU pequeña envía shares regularmente y los primeros aceptados aparecen en minutos. Para saber qué tasa esperar de tu hardware, consulta la [guía de hardware Blocknet](https://seineminer.com/es/blog/blocknet-mining-hardware-hashrate/).

Después llegan las recompensas. Estimated Rewards aparece cuando el pool encuentra un bloque mientras tus shares están en la ventana de seis horas. Tras 60 confirmaciones, tu parte pasa a Confirmed Rewards y se envía automáticamente cuando tienes al menos 0,1 BNT. [Cómo funcionan los pagos PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto. El historial del pool está en [Blocks](/blocks) y [Payouts](/payouts).

## ¿Qué incluye Get Started?

La página [Get Started](/start) reúne todo en una pantalla:

- **Información del pool.** Dirección Stratum (pulsa para copiar), sistema de pagos, mínimo y confirmaciones antes del pago, leídos en vivo.
- **Inicio rápido en tres pasos.** Descarga seine desde [seineminer.com](https://seineminer.com/es/#download), ejecútalo e introduce tu dirección o nombre y la URL del pool; empieza a minar y sigue tasa y saldo en My Stats.
- **Un comando para copiar.** El comando `./seine` completo con la URL del pool. Introduce tu dirección o `'@yourname'` entre comillas.
- **Notas de configuración.** Dónde guarda seine sus ajustes, cómo restablecerlos y qué opciones los sustituyen.
- **Cómo se cuentan las recompensas.** Una explicación breve de PPLNS: los shares enviados antes de encontrar un bloque reciben su recompensa; el trabajo posterior cuenta para los siguientes. También indica que las recompensas de bloques sin confirmar aparecen por separado hasta su confirmación.
- **Capturas** del primer inicio de seine y del panel de minería, para saber qué esperar.

<CTA target="pool" title="Empieza a minar en bntpool.com" text="Conecta seine a bntpool.com con tu dirección o @name y sigue la llegada de shares en My Stats." />
