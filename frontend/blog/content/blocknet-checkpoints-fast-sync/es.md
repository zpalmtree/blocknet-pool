---
title: Sincroniza Blocknet rápido con checkpoints de bntpool.com
description: Los nodos Blocknet se sincronizan rápido con checkpoints.dat de bntpool.com. Aprende cómo fija la cadena y cómo descargar, verificar y cargar el archivo.
tags: [Nodo Blocknet, Checkpoints, Sincronización rápida, bntpool]
faq:
  - q: '¿De dónde obtiene sus checkpoints un nodo Blocknet?'
    a: 'De bntpool.com. En el primer inicio, si no hay checkpoints.dat en el directorio de datos, el daemon lo descarga de https://bntpool.com/checkpoints.dat para sincronizarse rápido. También puedes descargarlo desde Checkpoints, comprobar su SHA-256 y ponerlo en el directorio antes de iniciar el nodo.'
  - q: '¿Con qué frecuencia actualiza bntpool.com checkpoints.dat?'
    a: 'El daemon del pool reescribe el archivo al crecer la cadena, con una entrada cada 100 bloques. Cada entrada está al menos 100 bloques por debajo de la punta, más allá de la profundidad máxima de reorganización de Blocknet, de modo que solo cubre historial asentado. Checkpoints muestra la última altura y cuándo cambió el archivo.'
  - q: '¿Los checkpoints cambian mi saldo o historial?'
    a: 'No. Un nodo sincronizado con checkpoints guarda todos los bloques y registra cada salida e imagen de clave en sus índices, por lo que el escaneo y los saldos funcionan igual que tras una sincronización completa. Solo permiten omitir la reverificación de prueba de trabajo y firmas del historial avalado por los hashes.'
  - q: '¿Puedo sincronizar sin checkpoints?'
    a: 'Sí. Inicia el daemon con --full-sync o establece full_sync en true en la configuración blocknet. El nodo omite la descarga y vuelve a verificar prueba de trabajo y firmas de todos los bloques desde génesis. Tarda más que con checkpoints, por lo que estos son el valor predeterminado para nodos nuevos.'
sourceHash: 7a3491743f7d
translation: draft
---

Un nodo Blocknet nuevo debe ponerse al día con todos los bloques desde génesis antes de servir a una billetera o un minero en solitario. Los checkpoints aceleran esa tarea. Esta guía explica el archivo de bntpool.com, cómo permite omitir las comprobaciones más lentas manteniendo el nodo en la cadena real y cómo descargarlo, verificarlo y cargarlo tú mismo.

## ¿Qué son los checkpoints de Blocknet?

<Answer>
Son una lista de hashes conocidos, uno cada 100 bloques, que bntpool.com ofrece como checkpoints.dat. El nodo comprueba que los bloques recibidos coincidan con esos hashes y omite reverificar prueba de trabajo y firmas hasta el último checkpoint, acelerando mucho la sincronización inicial.
</Answer>

El archivo es texto plano con un par `height:hash` por línea. Sus primeras líneas el 25 de octubre de 2026 eran:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Cada hash es el ID del bloque: SHA3-256 de su cabecera. El daemon del pool escribe el archivo desde su cadena e incluye solo bloques con al menos 100 bloques de profundidad. Cien bloques es la finalidad estricta de Blocknet, la reorganización más profunda permitida, por lo que cada checkpoint está en historial asentado.

El 25 de octubre de 2026 a las 02:04 EDT, el archivo tenía 689 checkpoints, desde la altura 100 hasta la 68.900, en 48.811 bytes. Crece una línea cada unos 100 bloques, aproximadamente ocho horas con bloques de cinco minutos. Consulta [Checkpoints](/checkpoints) para la altura actual.

## ¿Cómo usa los checkpoints un nodo Blocknet?

Un nodo que los ha cargado toma la última altura como objetivo de sincronización rápida. Hasta alcanzarla, confía en los checkpoints publicados para las dos comprobaciones más costosas y realiza las demás normalmente:

- **La prueba de trabajo se apoya en los checkpoints.** Cada bloque requiere un hash Argon2id sobre 2 GiB. Recalcularlo para todo el historial cuesta, y los hashes ya avalan ese historial.
- **También las firmas RingCT y las pruebas de rango.** Los comentarios del daemon identifican la verificación de pruebas de rango de las salidas como el mayor coste individual de sincronización, porque cada salida tiene una, incluida la coinbase de cada bloque.
- **Todas las demás comprobaciones siguen activas.** Enlaces entre bloques, dificultad LWMA, marcas de tiempo, tamaño y forma, doble gasto mediante imágenes de clave y vinculación entre entradas y firmas. Las salidas e imágenes se indexan normalmente, de modo que el escaneo y los saldos son iguales a los de una sincronización completa.

Solo se omiten comprobaciones para bloques que extienden la punta actual y están por debajo del último checkpoint. Una vez superado, cada bloque nuevo se verifica por completo, incluida la prueba de trabajo.

### Cómo fijan los checkpoints la cadena de confianza

Durante la sincronización, cualquier bloque a una altura de checkpoint debe tener exactamente el hash del archivo. Si difiere, se rechaza por discrepancia. Al haber uno cada 100 bloques, un par no puede conducir al nodo por otro historial: el siguiente checkpoint lo revelaría.

También aceleran los reinicios. Si ya hay bloques en disco, el daemon busca el checkpoint más alto que coincida y revisa solo lo posterior, en vez de recorrer toda la cadena desde génesis.

## ¿Un nodo nuevo descarga checkpoints automáticamente?

<Answer>
Sí. Si al iniciar el daemon no hay checkpoints.dat en el directorio de datos, descarga https://bntpool.com/checkpoints.dat, lo carga y sincroniza rápido hasta el último checkpoint. La descarga es de mejor esfuerzo: si falla, el nodo arranca igualmente y verifica la cadena completa.
</Answer>

Esa dirección está configurada en el propio core, así que cada nodo nuevo usa por defecto el archivo de bntpool.com. Solo lo descarga si no existe, da 15 segundos a la petición y limita el tamaño a 32 MiB.

## ¿Cómo usar los checkpoints de bntpool.com paso a paso?

Si prefieres descargar y comprobar el archivo tú mismo, o renovar una copia antigua antes de una resincronización larga, sigue estos pasos.

### 1. Encuentra tu directorio de datos

El archivo vive junto a la base de datos de la cadena:

- **Gestor `blocknet`:** `~/.config/bnt/data/mainnet/` en macOS y Linux; `C:\Users\<you>\.config\bnt\data\mainnet\` en Windows, salvo que cambies `data_dir` en su configuración.
- **Core ejecutado directamente:** `./blocknet-data-mainnet` en la carpeta de inicio, o la ruta indicada con `--data`.

### 2. Descarga checkpoints.dat

Abre [Checkpoints](/checkpoints) y pulsa **Download checkpoints.dat**, o usa la terminal. Este ejemplo escribe en el directorio predeterminado del core:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Descargar sobre un archivo existente lo reemplaza: así se renueva una copia antigua.

### 3. Comprueba el SHA-256

Checkpoints publica el SHA-256 del archivo actual. Calcula el de tu copia y compáralo:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

En macOS usa `shasum -a 256`. El hash cambia al añadir un checkpoint, así que compáralo con la página justo después de descargar.

### 4. Comprueba un checkpoint en el explorador

Puedes contrastar cualquier línea con el [explorador Blocknet](https://explorer.blocknetcrypto.com). Abre una altura incluida, por ejemplo `explorer.blocknetcrypto.com/block/68900`, y compara su hash con la línea de 68.900. Lo hicimos con el último checkpoint el 25 de octubre de 2026 y coincidían.

### 5. Inicia el nodo

Inicia o reinicia el daemon. Lee `checkpoints.dat` al arrancar y sincroniza rápido hasta el último checkpoint. Si ejecutas el core con su shell interactivo (`--cli`), `load-checkpoints` carga el archivo en un nodo activo e informa de la altura objetivo:

```text
> load-checkpoints
```

Los checkpoints ayudan cuando vas atrasado. Si ya superaste la última altura, el nodo verifica cada bloque por completo y un archivo más reciente no cambia nada.

## ¿Cómo consultar el archivo desde un script?

El pool publica sus detalles como JSON en `https://bntpool.com/api/checkpoints`, sin clave API:

| Campo | Significado |
|---|---|
| `available` | Si el pool tiene un archivo disponible |
| `url` | Dónde descargarlo |
| `entries` | Número de checkpoints |
| `latest_height`, `latest_hash` | Altura máxima y hash de su bloque |
| `bytes`, `sha256` | Tamaño y SHA-256 |
| `updated_at` | Última modificación, como marca Unix |

La descarga incluye cabeceras `X-Checkpoint-Height` y `X-Checkpoint-Sha256`, y un `ETag` basado en el hash. Un script puede enviar `If-None-Match` y recibir rápidamente `304 Not Modified` si no cambió:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Las respuestas pueden almacenarse en caché 60 segundos, suficiente para un archivo que cambia aproximadamente cada ocho horas.

<Note>
blocknetcrypto.com indica que Faraday traerá una prueba de trabajo más ligera para que los nodos nuevos alcancen antes la cadena. Hasta entonces, los checkpoints permiten poner un nodo al día rápidamente.
</Note>

## ¿Qué puedes hacer con un nodo sincronizado?

Un nodo sincronizado sirve a la billetera y al API de Blocknet, y seine se conecta a él para minar en solitario con `--mode daemon`. [Pool frente a minería en solitario](/blog/es/pool-vs-solo-mining-blocknet/) ayuda a decidir si encaja con tu hardware. Si prefieres empezar sin nodo, seine mina en bntpool.com por defecto: la [guía de instalación](https://seineminer.com/es/blog/how-to-mine-blocknet/) te lleva a los primeros shares y [minar en bntpool.com](/blog/es/mine-blocknet-on-bntpool/) explica el lado del pool.

<CTA target="pool" title="Mina en el pool que sirve los checkpoints de Blocknet" text="Únete a bntpool.com en minutos y sigue en vivo tu tasa de hash, shares y pagos." />
