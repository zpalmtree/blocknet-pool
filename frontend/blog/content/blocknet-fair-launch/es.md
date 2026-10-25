---
title: 'Lanzamiento justo de Blocknet: sin preminado'
description: Blocknet nació sin preminado, fondo de desarrollo ni tesorería. Comprueba el génesis vacío y la coinbase de una sola salida, y empieza a minar BNT.
tags: [Blocknet, Lanzamiento justo, Bloque génesis, Minería, bntpool]
faq:
  - q: '¿Tuvo Blocknet preminado?'
    a: 'No. Su bloque génesis, creado el 15 de febrero de 2026, no contiene ninguna transacción, así que no existían monedas antes de la minería. Las primeras procedieron de la recompensa del bloque 1, y cada BNT posterior se pagó a quien minó su bloque de origen.'
  - q: '¿Tiene Blocknet un fondo de desarrollo o tesorería?'
    a: 'No. El consenso permite exactamente una transacción coinbase por bloque con exactamente una salida por el valor de la recompensa. No hay segunda salida para desarrolladores, fundadores o tesorería, y todos los nodos rechazan cualquier bloque que intente añadirla.'
  - q: '¿Cuándo se creó el bloque génesis de Blocknet?'
    a: 'Su marca temporal es el 15 de febrero de 2026 a las 20:58:38 EST, equivalente a las 01:58:38 UTC del 16 de febrero. Lleva el mensaje "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything", y su hash de bloque anterior es el SHA3-256 de ese mensaje.'
  - q: '¿Cómo verifico el lanzamiento justo por mi cuenta?'
    a: 'Abre el bloque 0 en el explorador Blocknet: verás cero transacciones y una raíz de Merkle de ceros. Después consulta el código abierto del daemon, donde génesis tiene una lista vacía de transacciones y la validación coinbase acepta una salida igual a la recompensa.'
  - q: '¿Cómo empiezo a minar Blocknet?'
    a: 'Descarga seine para Windows, macOS o Linux, ejecútalo e introduce tu dirección Blocknet o nombre corto Blocknet ID. Pulsa Enter para aceptar bntpool.com y empezará a minar inmediatamente. Después busca tu dirección en las estadísticas del pool.'
sourceHash: 814839500c3d
translation: draft
---

Blocknet comenzó sin asignar nada a nadie. Esta página muestra el contenido del bloque génesis, las reglas que impiden preminado, fondo de desarrollo o tesorería, cómo comprobarlas en el explorador y el código, y cómo minar BNT en bntpool.com.

## ¿Tuvo Blocknet preminado?

<Answer>
No. El génesis, creado el 15 de febrero de 2026, no tiene transacciones: no existían monedas antes de empezar la minería. Desde el bloque 1, cada bloque paga una sola salida coinbase por el importe exacto previsto, así que todo BNT en circulación lo minó alguien ejecutando la prueba de trabajo.
</Answer>

No hay asignación para fundadores, desarrolladores, inversores o una fundación, ni en génesis ni después. La oferta procede únicamente de recompensas de bloque. Las reglas que lo garantizan forman parte del consenso y todos los nodos las comprueban en cada bloque.

## ¿Qué contiene el génesis de Blocknet?

Está fijado en el daemon, por lo que cada nodo parte del mismo bloque 0. Sus campos son constantes:

| Campo | Valor |
|---|---|
| Altura | 0 |
| Marca temporal | 15 de febrero de 2026, 20:58:38 EST (01:58:38 UTC del 16 de febrero) |
| Mensaje | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Hash anterior | SHA3-256 del mensaje: `a94a7f2b…d909df` |
| Dificultad | 4, el mínimo de la red |
| Nonce | 0 |
| Transacciones | ninguna |
| Raíz de Merkle | todos ceros |

El mensaje es el titular de un artículo de Wired del 15 de febrero de 2026, citado como fuente en el código. Como el hash anterior deriva del mensaje, el titular queda fijado en la identidad del bloque 0 y de todo lo construido encima.

El comentario de la función génesis lo explica: "Génesis no tiene transacciones: ni coinbase ni monedas quemadas. El primer bloque real, a la altura 1, tendrá la primera coinbase". La validación lo respalda. Un nodo rechaza el bloque 0 salvo que tenga exactamente cero transacciones y una raíz de Merkle de ceros. Ni un nodo modificado puede introducir monedas sin que los demás lo rechacen.

## ¿Cómo descarta el consenso un fondo de desarrollo o tesorería?

<Answer>
Cada bloque debe empezar con exactamente una coinbase, sin entradas, con comisión cero y una sola salida cuyo importe sea la recompensa de esa altura. No hay lugar para un segundo pago, así que no puede existir una salida para desarrolladores, fundadores o tesorería en la cadena.
</Answer>

Varias comprobaciones colaboran:

- **Una coinbase por bloque.** La primera transacción debe ser coinbase y ninguna otra puede serlo.
- **Una salida, sin entradas.** Una coinbase con cero o dos salidas, o con una entrada, no es válida.
- **La recompensa exacta.** Blocknet oculta importes con compromisos de Pedersen, pero la coinbase es especial. Su factor de cegado deriva de datos públicos: la clave pública de la transacción y la altura. Cada nodo reconstruye el compromiso de la recompensa prevista y lo compara. Cualquier otro importe causa rechazo.
- **Un memo vacío.** El memo coinbase debe estar vacío: las recompensas tampoco pueden llevar datos extra.

Las transacciones ordinarias no pueden crear monedas. Sus entradas deben igualar salidas más comisión, comprobado sobre los compromisos. Cada salida lleva una prueba de rango Bulletproofs que demuestra un importe válido y no negativo. Así los importes permanecen privados y la emisión sigue exactamente el calendario.

## ¿Cómo compruebas el lanzamiento justo por tu cuenta?

No necesitas fiarte de nadie: la cadena y el código son públicos.

**En el explorador.** Abre el [bloque 0](https://explorer.blocknetcrypto.com/block/0). Enumera 0 transacciones, una raíz de Merkle de 64 ceros, nonce 0 y un hash anterior que comienza por `a94a7f2b`. La línea de recompensa muestra el importe previsto para esa altura, pero sin transacciones no hay salida que lo reciba. Abre después el [bloque 1](https://explorer.blocknetcrypto.com/block/1): su única transacción es una coinbase sin entradas y con una salida, las primeras monedas.

Puedes comprobar que el hash anterior es realmente el del mensaje. Con OpenSSL 1.1.1 o posterior, ejecuta:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

El resultado termina en `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, el mismo valor que muestra el explorador.

**En el código.** El daemon es abierto en [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). En [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go), busca:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` construye el bloque 0 con lista vacía, `validateGenesisBlock` rechaza cualquier génesis con transacciones y `validateCoinbaseConsensus` exige una salida por el valor de recompensa. En `transaction.go`, `validateCoinbase` y `verifyCommitmentBalance` comprueban la forma coinbase y el equilibrio de las transacciones ordinarias.

## ¿Por qué importa un lanzamiento justo a los mineros?

Significa que las recompensas explican por completo de dónde salen las monedas.

- **Todas las monedas nuevas van a mineros.** Sin preminado ni tesorería, el 100% de la emisión paga a quienes encuentran bloques. El bloque 1 pagó 723,25093035 BNT en su única salida coinbase, y cada bloque posterior paga su recompensa completa igual.
- **Todos siguen las mismas reglas.** Desde el bloque 1, obtener BNT nuevos exige ejecutar la prueba de trabajo. El [calendario de recompensas](/blog/es/blocknet-block-reward-schedule/) está en el código y se aplica igual al primer minero y a quien empieza hoy.
- **La prueba de trabajo sirve para computadoras comunes.** Blocknet usa Argon2id con 2 GiB por hash, diseñado para resistir ASIC. La minería sigue abierta a PC, portátiles y servidores existentes. La [explicación de Argon2id](https://seineminer.com/es/blog/argon2id-proof-of-work-explained/) cuenta por qué.
- **La emisión se audita fácilmente.** Cada moneda procede de una salida coinbase comprobada por todos los nodos. El total emitido a cualquier altura se deduce directamente del calendario.

Si acabas de conocer la moneda, [¿qué es Blocknet?](https://seineminer.com/es/blog/what-is-blocknet/) explica privacidad, pagos por nombre y billeteras.

## ¿Cómo minas Blocknet tú mismo?

Participar en el lanzamiento justo significa minar. La vía más rápida es seine con bntpool.com:

1. **Consigue una dirección.** Instala la [billetera de línea de comandos](https://github.com/blocknetprivacy/blocknet/releases/latest) o la [de escritorio](https://github.com/blocknetprivacy/blocknet-gui/releases/latest), copia tu dirección o usa un nombre Blocknet ID como `@name`.
2. **Descarga seine.** Obtén el archivo para Windows, macOS o Linux en [seineminer.com](https://seineminer.com/es/#download) y descomprímelo. No hay que instalar nada.
3. **Ejecútalo y acepta los valores predeterminados.** Pega tu dirección o `@name` y pulsa Enter para aceptar `stratum+tcp://bntpool.com:3333`. seine empieza enseguida y ajusta hilos y kernels mientras trabaja, con CPU y tarjetas NVIDIA en Windows y Linux.
4. **Sigue tu minero.** Busca tu dirección en [My Stats](/stats) para ver tasa de hash, shares y saldo, y sigue los hallazgos en [Blocks](/blocks).

La [guía de bntpool.com](/blog/es/mine-blocknet-on-bntpool/) explica el acceso por dirección o nombre y el seguimiento, [Get Started](/start) muestra los ajustes de pagos y la [guía de minería Blocknet](https://seineminer.com/es/blog/how-to-mine-blocknet/) explica la instalación de seine en cada sistema.

<CTA target="seine" title="Mina tu parte" text="Todo BNT se mina. Descarga seine y empieza en bntpool.com en pocos minutos." />
