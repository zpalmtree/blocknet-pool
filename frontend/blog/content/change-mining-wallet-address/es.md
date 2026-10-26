---
title: 'Cambiar la dirección de tu cartera de minería Blocknet'
description: 'Cambia la dirección receptora de tus mineros Blocknet, verifica las nuevas participaciones en bntpool.com y sigue las recompensas de la dirección anterior.'
tags: ['Guía de minería', 'Cartera', 'Pagos']
faq:
  - q: '¿Cambiar la dirección mueve el saldo anterior del pool?'
    a: 'Cambiar el destino del minero dirige el trabajo futuro a la nueva dirección. Las participaciones, recompensas y pagos existentes siguen asociados a la dirección original. Consulta ambas direcciones por separado en Mis estadísticas y conserva el acceso a la cartera anterior mientras sigues sus recompensas pendientes.'
  - q: '¿Debo cambiar la dirección en cada ordenador?'
    a: 'Sí. Cada proceso de minería usa su propio destino configurado. Actualiza y verifica primero un ordenador y repite después en los demás, incluida su configuración guardada y argumentos de inicio. Nombres distintos permiten identificar los equipos que ya envían trabajo a la nueva dirección.'
  - q: '¿Cambiar mi Blocknet ID actualiza un minero activo?'
    a: 'seine resuelve el Blocknet ID guardado al iniciarse y conserva esa dirección durante la ejecución, incluidas las reconexiones. Tras cambiar el destino del ID, reinicia cada minero y comprueba la dirección completa resuelta. El cambio de nombre no reasigna trabajo ni saldos de la dirección anterior.'
sourceHash: 'fca912d46d55'
translation: draft
---

Cambia el destino del trabajo de minería futuro y conserva un registro claro de tus recompensas anteriores. Esta guía parte de una instalación de seine conectada a bntpool.com. Para la primera conexión consulta la [guía del pool](/blog/es/mine-blocknet-on-bntpool/); para una nueva cartera receptora, la [guía de cartera](https://seineminer.com/es/blog/blocknet-wallet-for-mining/).

## ¿Cómo se cambia la dirección de minería?

<Answer>
Detén un minero, actualiza la dirección receptora en su configuración guardada o comando habitual y reinícialo. Comprueba la dirección completa resuelta y las participaciones aceptadas; busca después esa dirección en bntpool.com. Repite en los demás equipos. El trabajo y las recompensas anteriores siguen vinculados a la dirección con la que se registraron.
</Answer>

*El trabajo nuevo va a una segunda cartera; los registros de recompensas anteriores siguen vinculados a la primera.*

## Anota ambos destinos antes del cambio

Copia la nueva dirección directamente de la cartera que quieras usar. El minero necesita el destino público o un Blocknet ID registrado, sin frase de recuperación ni clave privada. Conserva también la dirección completa original: seguirá siendo la clave para consultar la actividad anterior del pool tras cambiar la configuración.

Abre Mis estadísticas para la dirección anterior y anota las recompensas pendientes, saldo confirmado y pagos recientes. Registra los nombres de los equipos que contribuyen allí. Así tendrás una referencia de la transición; la nueva página no contendrá el historial anterior. Conserva el acceso a la cartera original mientras sigues las recompensas y pagos restantes.

## Actualiza un minero y comprueba su comando

Detén un minero y revisa cómo se inicia normalmente. Con una configuración existente, `--address` tiene prioridad sobre la dirección guardada sin reemplazarla. El ejemplo de PowerShell elige el destino para esta ejecución. Sustituye `@yourname` por tu nuevo ID o dirección completa y conserva los argumentos de pool, datos y hardware que ya has probado.

```powershell
.\seine.exe --address "@yourname"
```

En Linux o macOS, usa la sintaxis siguiente desde tu directorio habitual. Ambos ejemplos inician minería real: elige el correspondiente a tu sistema. La instalación existente aporta los demás ajustes de conexión. Mantén el mismo nombre del equipo para reconocerlo cuando aparezcan sus participaciones bajo la nueva dirección.

```sh
./seine --address "@yourname"
```

## Haz que el siguiente inicio normal use la dirección nueva

Para guardar el cambio, edita `address` en `seine.config.json` con el minero detenido. Conserva los demás campos y JSON válido. Confirma la carpeta de datos: `./seine-data` es relativa al directorio de trabajo y `--data-dir` puede seleccionar otra. Edita el archivo que realmente usa tu lanzador habitual.

Actualiza o elimina cualquier `--address` anterior del acceso directo, script, tarea programada o servicio. Un comando manual correcto no modifica esos otros puntos de inicio. Reinicia con el lanzador habitual y verifica de nuevo. La [guía de configuración de seine](https://seineminer.com/es/blog/configure-seine-miner/) explica la prioridad de los ajustes y cómo otra carpeta de trabajo puede seleccionar otra configuración.

## ¿Qué cambia si usas un Blocknet ID?

En seine v0.2.36, `@name` se resuelve y valida al iniciar. La dirección completa resultante queda fija durante esa ejecución, incluidas las reconexiones al pool. Si cambias el destino del nombre, reinicia cada minero y revisa la dirección resuelta. Reconectar el proceso existente no repite la resolución inicial de seine.

Anota las direcciones completas anterior y nueva aunque el nombre legible siga igual. La contabilidad usa la dirección resuelta, así que cambiar la asociación no redirige las participaciones ni saldos ya registrados. Si el nombre no se resuelve, confirma su asociación o usa deliberadamente la dirección completa que verificaste en tu cartera.

## Verifica todos los ordenadores bajo la nueva dirección

Comprueba el destino y los envíos aceptados en el minero; busca después la nueva dirección completa en Mis estadísticas. Identifica el nombre del equipo y sus participaciones recientes. Una nueva estimación de hashrate necesita trabajo y tiempo: verifica primero que llega trabajo. La [guía de estadísticas](/blog/es/track-your-miner-on-bntpool/) explica los contadores y cómo revisar un equipo que no aparece.

Tras confirmar el primero, repite el cambio en los restantes. Un nombre diferente por ordenador facilita la revisión. Comprueba también el inicio normal de cada máquina para que un lanzador antiguo no vuelva después al destino anterior. La [guía de varios ordenadores](/blog/es/mine-multiple-computers-one-wallet/) explica cómo los nombres organizan el trabajo bajo una dirección común.

## ¿Qué pasa con las recompensas de la dirección anterior?

Los créditos y pagos existentes siguen asignados a la dirección anterior. Sus participaciones válidas pueden seguir contribuyendo en las ventanas PPLNS correspondientes, y las recompensas pendientes siguen las reglas de confirmación. Ver hashrate cero no significa que toda la contabilidad haya terminado. Las guías de [PPLNS](/blog/es/how-pplns-payouts-work/) y [confirmaciones](/blog/es/block-confirmations-mining-rewards/) distinguen trabajo registrado, recompensas en maduración y pagos.

Cambiar el destino no combina saldos ni evita el mínimo de pago. Consulta las reglas actuales del pool y sigue ambas páginas hasta entender las cantidades restantes. Cambiar el nombre del equipo tampoco transfiere recompensas. Termina comprobando que todos los mineros previstos envían trabajo nuevo a la nueva dirección y que conservas la cartera anterior y su historial.

<CTA target="pool" title="Comprueba tu configuración en bntpool.com" text="Verifica el destino y el trabajo aceptado en cada equipo, y sigue por separado las recompensas anteriores y nuevas." />
