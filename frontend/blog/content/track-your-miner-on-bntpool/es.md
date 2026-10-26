---
title: Sigue tu minero en bntpool.com
description: Busca tu dirección o @name para ver tasa de hash, shares, saldo y pagos; sigue bloques, suerte y disponibilidad, o consulta el API público desde scripts.
tags: [bntpool, Estadísticas de minería, API del pool, Blocknet]
faq:
  - q: '¿Por qué la tasa de bntpool.com difiere de seine?'
    a: 'seine cuenta todos los hashes de tu máquina; el pool estima la tasa desde la dificultad de los shares aceptados durante la última hora. Llegan en momentos aleatorios, así que la cifra del pool oscila más y se estabiliza con el tiempo. Compara el gráfico de 24 horas con la media de seine.'
  - q: '¿Puedo consultar mis estadísticas con un @name?'
    a: 'Sí. Escribe tu nombre Blocknet ID, como @name o $name, en My Stats. La página lo resuelve mediante blocknet.id, muestra tus datos y coloca la dirección completa en el buscador. Usa esa dirección al llamar al API desde un script.'
  - q: '¿El API de bntpool.com necesita una clave?'
    a: 'No. Estadísticas, bloques, pagos, suerte, estado y endpoints de mineros son JSON público sobre HTTPS, accesible mediante curl o un panel doméstico sin registro. Los importes son unidades atómicas: divide entre 100.000.000 para obtener BNT y consulta cada 30 a 60 segundos.'
  - q: '¿Por qué Estimated Rewards muestra cero?'
    a: 'Aparece cuando el pool encuentra un bloque mientras tus shares están en la ventana de pagos. Para un minero nuevo puede requerir algunas rondas. Primero aparecen shares aceptados y tasa de hash; después, recompensas estimadas que maduran a Confirmed Rewards y se pagan automáticamente.'
sourceHash: 'f785e00cbe03'
translation: draft
---

Cuando seine mina en bntpool.com, el pool muestra todo lo que observa: tasa de hash, cada share aceptado, lo ganado y cada pago a tu dirección. Esta guía recorre My Stats, las páginas públicas del pool y el API para scripts y paneles domésticos.

Si aún no minas, la [guía de instalación de seine](https://seineminer.com/es/blog/how-to-mine-blocknet/) te lleva a tus primeros shares en minutos.

## ¿Cómo consultas las estadísticas de tu minero?

<Answer>
Abre My Stats en bntpool.com/stats, pega tu dirección o @name y pulsa Lookup. Verás tasa de hash, shares aceptados y rechazados, saldos estimado, confirmado y pagado, un gráfico, una fila por rig y pagos recientes. La página se actualiza sola mientras esté abierta.
</Answer>

En [My Stats](/stats), usa la misma dirección o [nombre Blocknet ID](https://seineminer.com/es/blog/blocknet-id-pay-by-name/) que diste a seine. Un `@name` o `$name` se resuelve mediante blocknet.id y el buscador pasa a mostrar su dirección completa. La página recuerda la última consulta en ese navegador para tener los datos listos la próxima vez.

## ¿Qué muestra My Stats?

La página agrupa tarjetas y tablas que responden distintas preguntas sobre tu minero.

### Saldo

- **Estimated Rewards:** tu parte de bloques aún en maduración.
- **Confirmed Rewards:** saldo maduro pendiente del próximo pago.
- **Paid Balance:** todo lo enviado a tu dirección.

Si tienes recompensas en proceso, **Estimated From Recent Blocks** muestra altura, crédito estimado, confirmaciones restantes y fecha de cada bloque. Las alturas enlazan al [explorador Blocknet](https://explorer.blocknetcrypto.com) para comprobarlas. [Los pagos PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto y el momento del pago.

### Minería y shares

Las tarjetas **Mining** muestran **Hashrate**, cuántos **Blocks Found** resolvieron tus rigs para el pool y tu fecha de inicio (**Mining Since**). Debajo aparecen **Shares Accepted**, **Shares Rejected**, **Reject Rate** y **Avg Difficulty**. Un rig sano envía shares aceptados regularmente y tiene una tasa de rechazo cercana a cero.

**Hashrate History** grafica la tasa durante 1h, 24h, 7d o 30d. Los períodos largos permiten comprobar si el rig funcionó toda la noche.

### Una fila por rig

Si tienes varias máquinas, nombra cada una y se mostrarán por separado con tasa de hash, shares aceptados y rechazados y hora del último share. seine elige en el primer inicio `seine-` seguido de cuatro cifras; puedes cambiarlo:

```sh
./seine --pool-worker rig-01
```

Una versión antigua recibe la etiqueta **update available**, enlazada a las [versiones de seine](https://github.com/zpalmtree/seine/releases), para detectar máquinas que olvidaste actualizar.

### Pagos y shares recientes

**Recent Payouts** enumera los pagos con importe, enlace a la transacción y estado queued, unconfirmed o confirmed. **Recent Shares** muestra tus últimos 20 shares con trabajo, rig, dificultad y estado: la comprobación más rápida de que una máquina recién iniciada alcanza el pool.

## ¿Por qué difiere la tasa del pool de la de seine?

seine cuenta todos los hashes. El pool solo ve shares, así que estima la tasa con la suma de sus dificultades durante la última hora. Cada share es aleatorio, y bntpool.com asigna una dificultad por conexión para obtener unos 10 cada 5 minutos: una ventana corta tiene ruido.

Dos hábitos ayudan a comparar:

- Usa medias: el gráfico 24h frente a la media de seine del mismo período.
- Dale tiempo a un rig nuevo. El pool promedia los minutos transcurridos desde que empezó, así que la lectura inicial depende de pocos shares. Tras una hora usa la ventana completa.

Recent Shares confirma inmediatamente que llega trabajo, incluso antes de estabilizarse la tasa.

<span id="zero-hashrate"></span>

## ¿Por qué seine está minando pero My Stats muestra una tasa de hash de cero?

Empieza por la dirección de pago, la conexión al pool y los shares aceptados. La tasa local indica que el equipo calcula hashes; la estimación del pool necesita shares aceptados para la dirección consultada.

1. **Comprueba la dirección.** Busca la misma dirección receptora o el `@name` registrado que usa seine. Si cambiaste la dirección de un nombre corto, reinicia seine para resolverlo de nuevo y consulta la dirección resultante en My Stats.
2. **Comprueba el pool.** Verifica que seine use `stratum+tcp://bntpool.com:3333`. Un equipo minando en otro pool o en modo solo no añade shares aquí. La [guía de configuración del pool](/blog/es/mine-blocknet-on-bntpool/) explica la conexión.
3. **Comprueba Recent Shares.** Espera shares aceptados y deja que la página se actualice. Los shares llegan a intervalos variables; una tasa local activa no garantiza una estimación inmediata del pool. Si llegan shares aceptados, compara las medias a lo largo del tiempo.
4. **Lee el mensaje de conexión.** Usa la tabla si el minero todavía no envía shares aceptados. Consulta [Status](/status) para ver incidentes actuales.

| Mensaje en seine | Qué comprobar |
|---|---|
| `failed to connect to pool at` | Verifica el nombre del servidor y el puerto, tu conexión y si se permite TCP saliente al puerto 3333. |
| `login rejected:` | Lee el motivo que sigue; comprueba la dirección receptora o el nombre corto y usa la versión actual de seine. |
| `stale job` | El share corresponde a una tarea antigua. Observa si llegan tareas nuevas y shares aceptados; si continúa, comprueba la conexión y la versión del minero. |
| `submit interrupted during reconnect` | Seine se está reconectando. Espera la aceptación del inicio de sesión y nuevos shares aceptados tras recuperar la conexión. |

Un cero en **Estimated Rewards** es otra cuestión: pueden llegar shares aceptados antes de que un bloque del pool genere recompensas. La [guía de PPLNS](/blog/es/how-pplns-payouts-work/) explica esa secuencia.

## ¿Qué páginas públicas muestran todo el pool?

Estas páginas no requieren buscar una dirección y muestran abiertamente la actividad. El [panel](/) reúne mineros conectados, tasas del pool y la red, bloques encontrados y suerte media.

### Bloques

[Blocks](/blocks) lista todos los hallazgos, nuevos primero: altura enlazada al explorador, recompensa, esfuerzo, duración, estado y fecha. Filtra por Confirmed, Pending u Orphaned.

### Pagos

[Payouts](/payouts) muestra lotes con importe total, número de mineros pagados, enlaces de transacciones y estado. Aparecen como unconfirmed al difundirse y pasan a confirmed poco después de la primera confirmación de bloque: puedes seguir tu pago desde el pool y el explorador.

### Suerte

[Luck](/luck) muestra el esfuerzo de cada ronda: trabajo aceptado dividido por dificultad del bloque. El 100% es exactamente lo esperado; un valor menor indica un hallazgo más rápido. Las rondas individuales oscilan mucho, así que conviene observar **Average Luck**. La tabla **Pool Luck History** enlaza aquí con **View All**.

### Estado

[Status](/status) muestra la salud del pool: si están disponibles pool, HTTP público, API, Stratum, base de datos y daemon; la antigüedad de la plantilla actual; altura; disponibilidad durante 10 minutos, 6 horas, 24 horas y 7 días; e incidentes. Dos sistemas miden disponibilidad: un monitor en el servidor y una sonda externa en Cloudflare.

## ¿Cómo usas el API de bntpool.com?

<Answer>
Es JSON público sobre HTTPS sin clave. Consulta https://bntpool.com/api/miner/ seguido de tu dirección para tasa, rigs, shares y pagos; añade /balance para saldos y usa /api/stats para las tasas de pool y red. Divide los importes atómicos entre 100.000.000 para obtener BNT.
</Answer>

Todas las páginas anteriores leen estos endpoints, así que puedes obtener directamente todo lo que ves:

| Endpoint | Resultado |
|---|---|
| `/api/stats` | Mineros conectados, tasa del pool, bloques, tasa de red y altura |
| `/api/stats/history?range=24h` | Historial de tasa del pool (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Ronda actual, esfuerzo medio y rondas recientes |
| `/api/info` | Puerto Stratum, sistema y ventana de pagos, mínimo |
| `/api/blocks?status=confirmed` | Hallazgos filtrables por estado |
| `/api/payouts/recent` | Lotes de pago con hashes de transacciones |
| `/api/luck` | Esfuerzo y duración de cada ronda |
| `/api/status` | Salud, ventanas de disponibilidad e incidentes |
| `/api/miner/{address}` | Tu tasa, totales, rigs, últimos shares y pagos |
| `/api/miner/{address}/balance` | Saldos estimado, confirmado y pagado |
| `/api/miner/{address}/hashrate?range=24h` | Tu historial de tasa de hash |

Los endpoints de listas aceptan `limit` (25 por defecto, hasta 200) y `offset`, y responden con `items` y `total`. Los de mineros requieren la dirección completa: cópiala del buscador tras consultar un @name.

Para leer tus datos desde una terminal:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Con `jq` puedes extraer valores, como tasa y shares aceptados, o total pagado en BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Muestra tu minero en un panel doméstico

El mismo endpoint sirve como sensor. En Home Assistant, la integración RESTful lo lee con unas líneas de `configuration.yaml`:

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Consultar cada 30 a 60 segundos basta: la tasa es una media horaria y My Stats actualiza detalles cada 30 segundos. El API limita solicitudes por cliente. Si un script recibe HTTP 429, debe reducir el ritmo y esperar lo indicado en `Retry-After` antes de reintentar.

## ¿Y el panel de seine?

El pool muestra lo acreditado. Para ver la máquina en vivo, el panel de terminal de seine muestra hashes y shares al producirse. También puede ofrecer un API local con REST, eventos enviados por el servidor y métricas Prometheus en `/metrics`, vinculado por defecto a `127.0.0.1:9977`. La [guía de control API](https://github.com/zpalmtree/seine/blob/master/docs/API.md) explica opciones y endpoints. Muchos usan ambos: métricas de seine por máquina y API del pool para saldos y pagos.

¿Nuevo en seine? [Descárgalo](https://seineminer.com/es/#download), usa el pool predeterminado y tu dirección aparecerá en My Stats minutos después del primer share aceptado.

<CTA target="pool" title="Empieza a minar y sigue los datos en vivo" text="Únete a bntpool.com en minutos y consulta tasa de hash, shares y saldo en My Stats." />
