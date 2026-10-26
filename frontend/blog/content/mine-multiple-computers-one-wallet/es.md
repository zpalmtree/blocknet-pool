---
title: 'Minar Blocknet desde varios ordenadores a un monedero'
description: 'Añade otro minero de Blocknet al mismo monedero en bntpool.com. Nombra cada equipo, comprueba sus shares y entiende las recompensas y los nombres duplicados.'
tags: ['Guía de minería', 'Equipos', 'Configuración']
faq:
  - q: '¿Necesito un monedero para cada ordenador?'
    a: 'No. Cada ordenador puede usar la misma dirección receptora en bntpool.com y un nombre distinto para identificar cada instalación. El pool contabiliza las recompensas por dirección. En las máquinas de minería solo necesitas esa dirección o tu Blocknet ID, no la frase de recuperación.'
  - q: '¿Qué ocurre si dos mineros usan el mismo nombre?'
    a: 'Con la misma dirección, su historial de shares se agrupa bajo ese nombre. Así resulta más difícil identificar qué máquina produce una lectura. Asigna un nombre diferente a cada instalación y reiníciala con él; los shares ya registrados conservan su etiqueta original.'
  - q: '¿Añadir otro equipo divide mi pago?'
    a: 'Los nombres organizan la actividad mostrada y no crean saldos independientes bajo una misma dirección. El trabajo válido de los ordenadores contribuye a la participación PPLNS de esa dirección, cuyo saldo sigue las reglas normales de confirmación y pago del pool.'
sourceHash: 'a07a17533ad2'
translation: draft
---

Puedes añadir un segundo ordenador a bntpool.com y conservar un único monedero receptor. Pon un nombre propio a cada instalación, compruébala por separado y después supervisa la dirección común. Esta guía explica cómo añadir otra máquina y resolver nombres duplicados. Para tu primer minero, completa antes la [guía de configuración del pool](/blog/es/mine-blocknet-on-bntpool/).

## ¿Pueden varios ordenadores minar al mismo monedero?

<Answer>
Sí. Ejecuta seine en cada ordenador con la misma dirección receptora y un nombre de equipo diferente. bntpool.com registra el trabajo y las recompensas por dirección; los nombres separan la actividad de las estadísticas. Verifica shares aceptados en cada máquina antes de confiar en la vista conjunta.
</Answer>

*Varios ordenadores envían trabajo a través del pool hacia una dirección receptora; cada uno conserva su propio nombre.*

## Elige la dirección receptora y los nombres

Usa la dirección receptora de tu monedero o un Blocknet ID que apunte a ella. Comprueba la dirección resuelta en cada minero antes de dejarlo funcionando. El nombre facilita introducirla, pero la contabilidad sigue la dirección resultante. Si cambias el destino del ID, una ejecución existente puede conservar la resolución anterior hasta reiniciarse; las recompensas previas siguen asociadas a su dirección original.

La [guía del monedero para minería](https://seineminer.com/es/blog/blocknet-wallet-for-mining/) explica cómo obtener la dirección. Las máquinas solo necesitan ese destino público: conserva la frase de recuperación y las claves privadas con el monedero. Elige nombres cortos como `desktop-01` y `server-02`. El pool elimina espacios exteriores y conserva hasta 64 caracteres; coloca la parte que distingue al equipo al principio, no al final de una etiqueta larga.

## Añade el segundo minero con un nombre propio

Instala la versión adecuada de seine en el segundo ordenador y ejecútala desde su carpeta extraída. El primer ejemplo para PowerShell indica explícitamente nombre receptor, pool y equipo. Sustituye `@yourname` por tu ID o dirección completa antes de ejecutarlo. Conserva las comillas del argumento y añade los parámetros de backend o ajuste ya probados que necesite esa máquina.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

En Linux o macOS, el comando correspondiente usa el ejecutable que aparece debajo y otro nombre de equipo. Los sistemas operativos pueden diferir; la dirección receptora debe coincidir. Cada ejemplo inicia minería real. Ejecuta el apropiado en cada ordenador y deja terminar el inicio y ajuste habituales. No necesitas ejecutar ambos ejemplos en una sola máquina.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## ¿Qué ajustes se comparten entre ordenadores?

Comparte la dirección receptora y el servidor del pool. Conserva una carpeta de datos local y un nombre propios para cada instalación. seine guarda la conexión en `seine.config.json` dentro de su directorio de datos; el valor predeterminado `./seine-data` depende de la carpeta de inicio. Reiniciar desde otra carpeta puede encontrar otra configuración. Una carpeta de lanzamiento estable facilita mantener los ajustes.

Si copias la configuración del primer equipo, indica un nombre distinto en el comando del segundo y confírmalo al arrancar. Conserva ese nombre explícito en futuros comandos: la sobrescritura de línea de comandos no modifica un nombre ya guardado. Ajusta el hardware para cada ordenador y deja que se optimice localmente. El argumento de dirección establece el destino del pago y el de equipo la etiqueta; introdúcelos por separado como en los ejemplos.

## ¿Cómo compruebas que ambos aportan trabajo?

Abre My Stats en bntpool.com y busca la dirección común. Comprueba los dos nombres y un share reciente de cada uno. Verifica también envíos aceptados en cada minero. Hazlo equipo por equipo: así relacionas cada proceso local con su fila del pool y encuentras errores con más facilidad que mirando únicamente el hashrate total.

El pool estima el hashrate a partir de los shares enviados, así que un equipo nuevo necesita tiempo y trabajo aceptado para ofrecer una lectura útil. La tasa local y la estimación del pool miden cosas e intervalos distintos. Confirma primero actividad con shares recientes y contadores aceptados; después compara periodos más largos. La [guía de estadísticas](/blog/es/track-your-miner-on-bntpool/) explica tarjetas y filas sin deducir dispositivos a partir del total de conexiones.

## ¿Qué hacer si falta un equipo o comparte nombre?

Si ambos ordenadores usan el mismo nombre bajo la misma dirección, sus shares se agrupan. Cambia uno mediante el parámetro de nombre de equipo y reinicia ese minero. Los nuevos envíos usan la etiqueta nueva; el historial conserva la anterior. La fila antigua puede seguir visible mientras sus shares estén dentro del periodo mostrado. Renombrar un equipo no mueve el saldo ganado por la dirección.

Si falta una máquina, compara su dirección resuelta real, servidor y nombre con la página abierta. Comprueba después que esté conectada y envíe shares aceptados. Una CPU y una GPU dentro del mismo proceso de seine no necesitan nombres separados para este flujo. Ante shares rechazados o caducados, sigue la [guía de estados](/blog/es/accepted-rejected-stale-shares/) en vez de cambiar repetidamente la dirección del monedero.

## ¿Cómo se combinan las recompensas y los pagos?

El trabajo válido de los ordenadores contribuye a la participación de una dirección en la ventana PPLNS. Los nombres ayudan a seguir las aportaciones y no crean saldos separados. Añadir una máquina suma el trabajo útil que realmente envíe, no una recompensa fija por dispositivo o nombre. Hardware y tiempos de actividad distintos pueden aportar cantidades diferentes; añadir nombres por sí solo no aumenta el trabajo ganado.

La [guía PPLNS](/blog/es/how-pplns-payouts-work/) explica el reparto al encontrar un bloque. Las reglas de confirmación y pago siguen aplicándose al saldo conjunto de la dirección. No necesitas fusionar monederos ni transferir saldos entre nombres al añadir un ordenador. Antes de dejarlo funcionando, verifica destino, nombre distinto y shares aceptados en cada máquina. Repite esa breve lista cuando añadas la siguiente.

<CTA target="pool" title="Conecta otro ordenador a bntpool.com" text="Usa una dirección receptora, asigna un nombre distinto a cada instalación y verifica sus shares aceptados." />
