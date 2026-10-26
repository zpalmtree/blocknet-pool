---
title: ¿Qué es un pool de minería?
description: Un pool reúne la tasa de hash de muchos mineros y reparte las recompensas. Aprende cómo funciona, qué son PPLNS, PPS y solo, y cómo elegir uno.
tags: [Conceptos de minería, Pools de minería, PPLNS, bntpool]
faq:
  - q: '¿Unirme a un pool aumenta lo que mino?'
    a: 'El pool no cambia tu tasa de hash ni tu parte del trabajo de la red. Cambia cómo llegan las recompensas. Solo esperas bloques enteros en intervalos aleatorios; en un pool recibes una parte de cada hallazgo, con pagos frecuentes que siguen tu tasa.'
  - q: '¿Qué diferencia hay entre PPLNS y PPS?'
    a: 'PPLNS paga una parte de cada bloque que el pool encuentra, según el trabajo enviado en una ventana reciente. PPS paga un importe fijo por cada share aceptado según su valor esperado, encuentre o no un bloque. bntpool.com usa PPLNS con seis horas de ventana.'
  - q: '¿Necesito mi propio nodo para minar en un pool?'
    a: 'No. El pool ejecuta el nodo, crea plantillas, comprueba shares y envía bloques. En bntpool.com solo necesitas seine y una dirección Blocknet o nombre Blocknet ID. Ejecuta seine, pega la dirección y acepta el pool predeterminado: tu parte de cada bloque se paga allí.'
  - q: '¿Cómo empiezo en bntpool.com?'
    a: 'Descarga seine para Windows, macOS o Linux desde seineminer.com, ejecútalo y pega tu dirección o @name. Acepta bntpool.com, el pool predeterminado, y empieza a minar. Busca tu dirección en las estadísticas para ver tasa de hash, shares y saldo.'
sourceHash: 'a17a47ab1d73'
translation: draft
---

Esta guía explica qué es un pool, por qué la mayoría de los mineros se unen y cómo calcula lo que gana cada uno. Verás PPLNS, PPS y minería solo, y qué revisar antes de conectar tu minero. El pool Blocknet bntpool.com sirve de ejemplo a lo largo de la guía.

## ¿Qué es un pool de minería?

<Answer>
Es un servidor que combina la [tasa de hash](/blog/es/what-is-hashrate/) de muchos mineros para encontrar bloques juntos. Reparte trabajo sobre el mismo bloque, cuenta lo enviado por cada uno y distribuye la recompensa en proporción. Los mineros reciben pagos pequeños y frecuentes en vez de bloques enteros poco habituales.
</Answer>

En una moneda de prueba de trabajo, los mineros compiten calculando hashes de la cabecera con distintos nonces hasta que uno queda bajo el objetivo de la red. Cada hash es un intento independiente con la misma pequeña probabilidad. Quien acierta recibe la recompensa.

El pool convierte la competencia en trabajo en equipo. Todos los conectados a bntpool.com trabajan en sus bloques y, cuando uno encuentra un hash ganador, se reparte la recompensa entre quienes aportaron trabajo.

## ¿Por qué se unen los mineros a un pool?

<Answer>
Para recibir pagos regulares. Solo, un minero habitual tiene una parte minúscula de la tasa de red y puede esperar días entre bloques, con intervalos aleatorios. Un pool encuentra bloques a menudo y cada minero recibe parte de todos, con ingresos regulares que siguen su tasa de hash.
</Answer>

En Blocknet es fácil comprobarlo: un bloque cada cinco minutos son 288 diarios. Solo puedes esperar 288 × (tu tasa ÷ tasa de red) bloques al día.

Con cifras hipotéticas redondas, un rig de 50 H/s en una red de 50.000 H/s tiene el 0,1% y encuentra 0,288 bloques diarios de media: uno cada tres días y medio. Es solo una media. Los hallazgos son aleatorios y el intervalo real puede ser mucho menor o mayor.

Pon el mismo rig en un pool de 5000 H/s y el pool encontrará unos 28,8 bloques diarios, uno cada 50 minutos de media. El rig hace el 1% del trabajo y recibe el 1% de cada bloque. Aporta lo mismo; la recompensa llega en pequeñas partes muchas veces al día. [Pool frente a solo para Blocknet](/blog/es/pool-vs-solo-mining-blocknet/) compara operaciones mayores.

## ¿Cómo funciona un pool?

Todos siguen el mismo ciclo básico. En bntpool.com:

1. **Crea una plantilla.** Ejecuta su nodo Blocknet y obtiene plantillas de él, así que los mineros no necesitan ejecutar ni sincronizar nodos.
2. **Envía trabajo.** Los mineros conectan por Stratum, el protocolo de trabajo de los pools, a `stratum+tcp://bntpool.com:3333`. Cada conexión recibe un trabajo basado en la plantilla actual.
3. **Los mineros calculan hashes y envían shares.** Un share supera un objetivo más fácil asignado por el pool y demuestra trabajo.
4. **Un share que supera también el objetivo de red es un bloque.** El pool comprueba ambos objetivos y envía el bloque a su nodo cuando cumple el de la red.
5. **Reparte y paga.** Divide la recompensa según su sistema y envía la parte de cada minero cuando está confirmada y supera el mínimo.

## ¿Qué es un share?

Es un hash que supera un umbral menos exigente que un bloque real. Un bloque necesita quedar bajo el objetivo de red; un share, bajo el objetivo mucho más fácil del pool. La mayoría no son bloques, pero muestran regularmente cuánto trabajo hace cada minero.

Se ponderan por dificultad. En Blocknet, dificultad D implica unos D hashes esperados por acierto: un share de dificultad 60 representa unos 60 hashes. bntpool.com ajusta la dificultad por conexión para obtener unos 10 shares cada cinco minutos y empieza las conexiones nuevas en 60. Un servidor rápido recibe shares más difíciles que un portátil; ambos reciben crédito por el trabajo real. Además, el pool recuerda la dificultad del rig una hora para retomarla tras un reinicio breve.

## Sistemas de pago habituales

El sistema de pago convierte shares en ingresos. Estos son los más comunes.

| Sistema | Por qué cobras | Funcionamiento |
|---|---|---|
| PPLNS (pay per last N shares) | Bloques encontrados por el pool | Reparte cada recompensa entre los shares de una ventana reciente, según dificultad |
| PPS (pay per share) | Cada share aceptado | Paga un importe fijo según el valor esperado del share, encuentre o no un bloque |
| Solo | Bloques que encuentras tú | Minas contra tu nodo y conservas toda la recompensa |

**PPLNS** mira hacia atrás desde cada hallazgo. En bntpool.com, los "últimos N shares" son seis horas: suma la dificultad de todos los aceptados en ese período y paga a cada minero su fracción del trabajo. Un share cuenta para cada bloque mientras esté en la ventana. Quien mina constantemente mantiene una ventana completa y un reinicio no la borra. [Cómo funciona PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto paso a paso.

**PPS** paga cada share al llegar por su valor esperado: dificultad del share dividida entre dificultad de red, por recompensa de bloque. El operador asume las oscilaciones de los hallazgos y paga desde su propio saldo entre ellos.

**La minería solo** prescinde del pool. Tu nodo crea plantillas y cada bloque paga la recompensa completa a tu billetera. Encaja con operaciones grandes que encuentran bloques a menudo. seine soporta ambos: bntpool.com por defecto y `./seine --mode daemon` para tu nodo sincronizado.

## ¿Qué debes buscar en un pool?

Estos detalles determinan cómo cobras y cómo puedes comprobarlo:

- **Un sistema claro.** Debe publicar el método y ajustes como la ventana PPLNS. bntpool.com los muestra en [Get Started](/start) y `https://bntpool.com/api/info`.
- **Confirmaciones y mínimo.** Las recompensas esperan suficiente profundidad y se pagan al superar un mínimo. bntpool.com confirma tras 60 bloques, igual que la madurez coinbase, y paga automáticamente desde 0,1 BNT.
- **Cifras públicas verificables.** Debes poder ver todos los bloques y pagos. bntpool.com ofrece [Blocks](/blocks), [Payouts](/payouts), esfuerzo por ronda en [Luck](/luck) y salud en [Status](/status).
- **Tus estadísticas.** Busca tu dirección para tasa, shares y saldo en [My Stats](/stats). La [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) explica sus tarjetas.
- **Configuración sencilla.** Menos ajustes permiten empezar antes. bntpool.com es el valor predeterminado de seine, que ajusta hilos, kernels y memoria. Puedes acceder con un [nombre Blocknet ID](https://seineminer.com/es/blog/blocknet-id-pay-by-name/) como `@name` en lugar de la dirección completa.

## bntpool.com de un vistazo

| Ajuste | bntpool.com |
|---|---|
| Sistema de pagos | PPLNS |
| Ventana de shares | 6 horas |
| Confirmaciones antes del pago | 60 bloques |
| Pago mínimo | 0,1 BNT |
| Dirección Stratum | `stratum+tcp://bntpool.com:3333` |

El [panel](/) muestra mineros conectados, tasas de pool y red, bloques y esfuerzo actual. El 25 de octubre de 2026 a las 02:25 EDT, el pool tenía unos 5200 H/s de una red de unos 44.400 H/s y 246 mineros. Es alrededor del 12%, suficiente para esperar unos 34 bloques diarios, uno cada 42 minutos. Cambia constantemente: consulta los valores actuales y usa la [guía de recompensas](/blog/es/estimate-blocknet-mining-rewards/) para convertirlos a BNT con tu tasa.

Para unirte, [descarga seine](https://seineminer.com/es/#download), ejecútalo, pega tu dirección o `@name` y acepta el pool. La [guía de seine](https://seineminer.com/es/blog/how-to-mine-blocknet/) explica cada paso, y [minar en bntpool.com](/blog/es/mine-blocknet-on-bntpool/) cubre nombres de rigs y primeros shares.

<CTA target="pool" title="Únete a bntpool.com" text="Conecta seine al pool y recibe parte de cada bloque, enviada automáticamente a tu dirección al alcanzar 0,1 BNT." />
