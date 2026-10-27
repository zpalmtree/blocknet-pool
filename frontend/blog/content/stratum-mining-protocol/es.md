---
title: '¿Qué es Stratum? Cómo hablan los mineros con un pool'
description: 'Sigue la conversación Stratum entre seine y bntpool.com: inicio de sesión, trabajos, objetivos, envíos y respuestas, y aprende a leer la actividad de conexión.'
tags: ['Conceptos de minería', 'Stratum', 'Minería en pool']
faq:
  - q: '¿Qué hace Stratum en la minería en pool?'
    a: 'Stratum transporta la conversación entre minero y pool: identifica la sesión, entrega trabajo y lleva envíos de participaciones y respuestas. El minero calcula los hashes localmente. El pool conecta ese trabajo con su nodo y mantiene la contabilidad necesaria para su sistema de pagos.'
  - q: '¿Una dirección Stratum es igual que una dirección web?'
    a: 'Sirven para conexiones distintas. La web bntpool.com usa HTTPS para páginas y datos de API; su punto de minería documentado es stratum+tcp://bntpool.com:3333. Introduce ese punto en la configuración del pool del minero. Abrir la web correctamente no establece por sí solo una conexión de minería.'
  - q: '¿Una participación Stratum aceptada significa que se envió un pago?'
    a: 'No. La respuesta informa del resultado de un envío dentro de la sesión. Verificación, bloque válido para la red, elegibilidad para recompensas y pago completado son etapas distintas. Sigue la actividad aceptada en My Stats y consulta la guía de pagos para entender confirmaciones, PPLNS y el umbral de pago.'
sourceHash: '5cc5086d7b28'
translation: draft
---

Sigue un trabajo de minería desde bntpool.com hasta seine y una participación de vuelta al pool. Entender esa conversación facilita leer conexiones, actualizaciones de trabajo y respuestas. Esta guía describe la implementación de bntpool.com; para configurar el punto de conexión y la dirección de pago, consulta la [guía del pool](/blog/es/mine-blocknet-on-bntpool/).

## ¿Qué es Stratum en minería?

<Answer>
Stratum es la capa de comunicación que permite al minero recibir trabajo del pool y enviar resultados. Con seine y bntpool.com, una conexión TCP persistente transporta mensajes JSON de inicio de sesión, trabajos y participaciones. Tu equipo calcula los hashes localmente; el pool obtiene plantillas de su nodo y procesa el trabajo enviado.
</Answer>

*Un minero envía mensajes de inicio de sesión y participaciones al pool, recibe trabajos y respuestas, y el pool intercambia plantillas y candidatos a bloque con su nodo.*

## El punto de conexión identifica el servicio de minería

El punto documentado de bntpool.com es `stratum+tcp://bntpool.com:3333`. El esquema indica el transporte, el nombre de host identifica el servicio y el puerto selecciona el receptor de minería. La conexión del navegador a `https://bntpool.com` tiene otra función. Copia el punto completo desde [Get Started](/start) a la configuración del pool para abrir la conexión prevista.

La conexión persistente permite que ambas partes envíen mensajes cuando haya trabajo. bntpool usa JSON delimitado por saltos de línea e identificadores que relacionan solicitudes y respuestas. Estos detalles describen el formato de este pool; el minero debe admitirlo, junto con sus capacidades y el algoritmo de Blocknet. El [código del protocolo](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) recoge los campos actuales.

## ¿Qué ocurre al iniciar sesión?

El cliente envía una solicitud `login` con la dirección de pago, nombre del equipo, versión del protocolo y capacidades admitidas. El pool la comprueba y devuelve información del protocolo y capacidades necesarias. Así se establece cómo intercambiará trabajo la sesión. Una conexión TCP correcta es el primer paso; un inicio de sesión aceptado confirma que también empezó la conversación de la aplicación.

El nombre del equipo identifica su actividad bajo una dirección. Usa nombres distintos para varios equipos y conserva la dirección de pago deseada. El inicio necesita la dirección receptora, no la frase de recuperación ni la clave privada de gasto. Cuando se acepta la sesión y hay trabajo disponible, el pool envía un mensaje `job` para empezar el hashing.

## ¿Qué contiene un trabajo de minería?

Un trabajo incluye identificadores de la asignación y la plantilla, datos de cabecera, altura del bloque, objetivo de participación, objetivo de red y un rango de nonces. El nonce es el valor que cambia mientras se calculan hashes. El rango define el espacio de búsqueda de la sesión. El identificador permite relacionar un envío posterior con el trabajo y las condiciones asignadas originalmente.

El objetivo de participación determina qué resultados enviar como prueba de trabajo. El objetivo de red determina cuáles pueden ser candidatos a bloque. La mayoría de participaciones válidas sirven a la contabilidad sin convertirse en bloques. La [guía de dificultad](/blog/es/mining-difficulty-explained/) explica la relación entre tamaño del objetivo y trabajo esperado; el protocolo entrega esos objetivos al minero dentro de la asignación.

## Los trabajos pueden cambiar con la conexión abierta

Un nuevo trabajo no significa por sí solo una desconexión. El pool puede distribuir trabajo actualizado cuando cambia la plantilla de su nodo o enviar una asignación con otra dificultad de participación. La dificultad variable adapta el umbral de envío al flujo observado. El minero sigue la nueva asignación mientras la conexión persistente continúa transportando mensajes en ambas direcciones.

Distingue los identificadores de trabajo y plantilla al leer registros técnicos. Varias asignaciones pueden usar la misma plantilla con detalles de sesión diferentes. La altura no describe todos los cambios. Como operador, comprueba que llegan trabajos, progresa el hashing y los envíos reciben respuesta; la [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) muestra dónde ver esa actividad.

## ¿Qué contiene el envío de una participación?

Cuando seine encuentra un resultado válido, envía una solicitud `submit` con el trabajo, nonce y hash declarado. El pool usa el contexto de la asignación para procesarlo y responde con el identificador de la solicitud. El hash declarado forma parte del mensaje; la verificación y la validación de bloques determinan cómo continúa. Un candidato a bloque se envía al daemon para validación.

La respuesta puede indicar aceptación, estado de verificación e información de dificultad. Son campos distintos: no interpretes cada respuesta correcta como un bloque descubierto. La contabilidad y los pagos ocurren después de este intercambio. La [guía de resultados de participaciones](/blog/es/accepted-rejected-stale-shares/) explica las respuestas; la [guía PPLNS](/blog/es/how-pplns-payouts-work/) relaciona el trabajo elegible con recompensas y pagos.

## ¿Cómo ayuda esta secuencia a comprobar un minero?

Lee las etapas en orden: punto accesible, sesión aceptada, trabajo recibido, hashing activo y envíos respondidos. Si se pierde la conexión, el cliente reconecta y establece una sesión antes de recibir más trabajo. Usa el mensaje exacto y su hora para ubicar la etapa. Una web funcional aporta contexto, pero no demuestra que ese equipo pueda acceder al puerto de minería.

Abre [My Stats](/stats) con la dirección de pago deseada para seguir el equipo y consulta el [estado del pool](/status). Tras un arranque o reconexión, deja que las estimaciones de hashrate acumulen participaciones. Distingue la conversación del protocolo de los plazos de pago: login prepara la sesión, los trabajos entregan tareas, las participaciones informan resultados y la contabilidad determina las etapas posteriores de recompensa.

<CTA target="pool" title="Conecta seine a bntpool.com" text="Usa la página de configuración del pool y sigue los trabajos, participaciones y actividad de tu equipo en My Stats." />
