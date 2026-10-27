---
title: '¿Afecta la velocidad de internet a minar Blocknet?'
description: 'Comprueba cómo afecta internet a la minería de Blocknet: distingue ancho de banda y latencia, compara Wi-Fi y Ethernet y verifica reconexiones y shares.'
tags: ['Guía de minería', 'Conexión', 'Diagnóstico']
faq:
  - q: '¿Un plan de internet más rápido aumenta el hashrate?'
    a: 'El minero calcula hashes en su CPU o GPU: una mayor velocidad de descarga anunciada no acelera directamente ese cálculo. La conexión afecta a la recepción de trabajos y al envío de resultados. Investiga congestión, interrupciones y respuestas del pool antes de atribuir un cambio local al plan.'
  - q: '¿Puedo minar Blocknet por Wi-Fi?'
    a: 'Sí, si permite una comunicación estable con el pool. Observa shares aceptadas y reconexiones durante el uso normal. Si hay cortes, una comparación con Ethernet puede aislar el tramo inalámbrico. Mantén las opciones del minero y el destino mientras comparas.'
  - q: '¿Un ping correcto demuestra que funciona la conexión al pool?'
    a: 'Ping comprueba un intercambio ICMP con el host que responde. La sesión de minería también necesita el endpoint configurado, transporte, inicio de sesión, trabajos actuales y envíos aceptados. Una web o ping pueden funcionar aunque otro servicio no sea accesible; consulta el registro del minero y del pool.'
sourceHash: 'bf51814bc06d'
translation: draft
---

Comprueba la conexión de minería de Blocknet de forma reproducible: separa cálculo local y comunicación, anota interrupciones y cambia una condición de red cada vez. Esta guía trata del minero conectado a bntpool.com. Descargar el programa o sincronizar un nodo completo son cargas distintas que debes medir por separado.

## ¿Una conexión más rápida acelera la minería?

<Answer>
Más ancho de banda de descarga no hace que CPU o GPU calculen más hashes directamente. La conexión transporta trabajos, envíos y respuestas; importan su entrega estable y puntual. Revisa trabajos, reconexiones y shares aceptadas antes de cambiar de plan, distinguiendo hashrate local y estimación del pool.
</Answer>

*Un minero intercambia trabajos y shares con un pool por una ruta de red; un reloj representa el tiempo de entrega.*

## Separa ancho de banda, latencia y hashrate local

El ancho de banda describe capacidad de transferencia; la latencia, demora. Consulta la [referencia de términos de red](https://www.cloudflare.com/learning/performance/glossary/what-is-latency/) si una prueba mezcla ambos. Descarga y latencia responden preguntas diferentes, y el destino de la prueba puede seguir otra ruta que el pool. Conserva el contexto de cada medición.

Los H/s del minero miden el cálculo de sus dispositivos. El pool estima trabajo mediante shares recibidas a lo largo del tiempo, con variación propia. Una diferencia breve entre pantallas no mide directamente pérdidas de red. Consulta unidades y ventanas en la [guía de hashrate](/blog/es/what-is-hashrate/) y reúne resultados de envíos antes de explicar esa diferencia.

## ¿Qué viaja entre el minero y el pool?

El minero inicia sesión, recibe un trabajo, calcula localmente y devuelve shares válidas. No sube cada hash intentado ni su área de memoria en cada intento. La [guía Stratum](/blog/es/stratum-mining-protocol/) sigue los mensajes. Recibir el trabajo, enviar el resultado y recibir respuesta son observaciones diferentes, aunque compartan conexión.

La cantidad de mensajes depende de trabajos, dificultad de shares, envíos, keepalives y reconexiones. Sin medir una sesión concreta no corresponde fijar un consumo mensual universal. Para presupuestar datos, usa contadores por dispositivo del sistema o router durante un periodo representativo. Anota intervalo y otro tráfico; excluye descargas y sincronización del nodo del consumo de minería.

## Compara Wi-Fi y Ethernet sin cambiar el minero

Si los cortes coinciden con el uso inalámbrico, prueba Ethernet en el mismo equipo cuando esté disponible. Conserva dirección del pool, backends, hilos y trabajo en primer plano. Anota inicio y fin y observa el registro de conexión. Cambiar también ajustes de hardware impediría aislar el enlace de red.

Comprueba si las demoras coinciden con subidas grandes, copias en la nube u otras transferencias domésticas. Pausa temporalmente una transferencia que controles, observa el patrón y restáurala. Una prueba de velocidad también genera tráfico: anota cuándo la ejecutas. Mantén la protección habitual y mina solo en redes donde tengas permiso.

## ¿Qué indica un problema de conexión al pool?

Sigue la secuencia: conexión al endpoint, inicio de sesión, trabajo, envío y respuesta. Si se rechaza el acceso, comprueba dirección y mensaje exacto en vez de atribuirlo a Wi-Fi. Ante cortes repetidos, conserva horas y líneas de registro cercanas. La [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) cubre endpoint y hashrate cero; la [página de estado](/status) aporta contexto del servicio.

Tras recuperarse, busca un trabajo nuevo utilizable y shares aceptadas recientes. Que vuelva la web o responda ping no demuestra que la minería haya completado esas etapas. Tampoco un error del navegador demuestra que Stratum se detuvo. Registra qué comprobó cada prueba para separar web, transporte y respuesta del pool.

## ¿Cómo interpretar envíos tardíos o rechazados?

Las demoras pueden importar cuando cambia el trabajo, pero una share rechazada no demuestra por sí sola una conexión lenta. Relaciona su motivo con la cronología de trabajos y reconexiones. Duplicados, dificultad y validez del trabajo tienen causas distintas. La [guía de resultados de shares](/blog/es/accepted-rejected-stale-shares/) explica categorías y datos útiles para errores repetidos.

Un breve periodo sin shares tampoco demuestra pérdida de paquetes. Su descubrimiento varía y la dificultad asignada afecta a la frecuencia. Compara un periodo representativo con la misma configuración y observa respuestas reales. Si baja también el cálculo local, investiga CPU, GPU y memoria por separado; la [guía de minería en el escritorio](https://seineminer.com/es/blog/mine-while-using-pc/) ayuda cuando compartes el equipo.

## Anota los datos y verifica la mejora

Registra versión, tipo de conexión, endpoint, horas aproximadas y mensajes exactos de reconexión o rechazo. Indica si afectó a otros dispositivos y si Ethernet o pausar una transferencia cambió el patrón. Protege datos de cuenta al compartir registros. Esto permite un informe más útil que una cifra de velocidad sin hora ni destino.

Tras un cambio concreto, confirma pool y dirección, llegada de trabajos y shares nuevas aceptadas. Compara después el registro del pool dejando actualizar su ventana. Conserva la configuración que entregue mensajes de forma estable en tu entorno. Busca una conexión cuyo comportamiento puedas explicar y mide el ancho de banda para el tráfico real que necesites presupuestar.

<CTA target="pool" title="Sigue el trabajo aceptado en bntpool.com" text="Compara los mensajes de conexión del minero y los registros del pool al evaluar tu red habitual." />
