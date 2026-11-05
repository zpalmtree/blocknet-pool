---
title: Mina Blocknet directamente a un exchange
description: 'Mina Blocknet directamente a tu dirección de depósito de SafeTrade u otro exchange en bntpool.com. Añade tu memo de depósito en seine y el pool paga a tu cuenta.'
tags: [Guía de minería, bntpool, SafeTrade, Pagos]
faq:
  - q: '¿Puedo minar Blocknet directamente a un exchange?'
    a: 'Sí. Inicia seine v0.3.1 o posterior con tu dirección de depósito del exchange y tu memo de depósito, como en ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com paga tus recompensas a esa dirección con el memo adjunto, así que el exchange las acredita en tu cuenta.'
  - q: '¿Por qué seine pide un memo de pago?'
    a: 'Exchanges como SafeTrade usan una dirección de depósito compartida para todos sus clientes y distinguen los depósitos por el memo. seine conoce estas direcciones compartidas, así que cuando introduces una te pide el memo y lo guarda con la dirección. Copia el memo exactamente de la página de depósito del exchange.'
  - q: '¿Cuál es el pago mínimo a un exchange en bntpool.com?'
    a: 'Los pagos con memo empiezan en 10 BNT, según indicaba el pool el 4 de noviembre de 2026. Tu saldo confirmado se acumula en el pool hasta llegar a esa cifra, y entonces el pool lo envía a la dirección del exchange con tu memo. Los pagos a una billetera personal empiezan en 1 BNT.'
  - q: '¿Cómo consulto mi saldo de minería en un exchange?'
    a: 'En la página de estadísticas de bntpool.com, introduce la dirección de depósito del exchange, elige Añadir memo e introduce tu memo. Cada par de dirección y memo tiene su propio saldo, así que solo ves tu tasa de hash, tus rigs y tus pagos, no los de otros clientes que usan la misma dirección del exchange.'
sourceHash: '8f73aae25c7a'
translation: draft
---

Esta guía te muestra cómo minar Blocknet directamente a tu cuenta de un exchange en bntpool.com. Le das a seine la dirección de depósito del exchange y tu memo de depósito, y el pool paga allí tus recompensas con el memo adjunto, para que el exchange te las acredite. Cubre el comando, el pago mínimo y cómo consultar tu saldo.

## ¿Puedes minar Blocknet directamente a un exchange?

<Answer>
Sí. bntpool.com paga las recompensas de minería a una dirección de depósito de un exchange con tu memo de depósito adjunto. Inicia seine v0.3.1 o posterior con la dirección del exchange y tu memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Los pagos con memo empiezan en 10 BNT, y el exchange acredita cada uno en tu cuenta.
</Answer>

Minar a un exchange te ahorra un paso cuando piensas vender lo que minas. Las recompensas llegan directamente a tu cuenta del exchange, listas para vender en el [mercado BNT/USDT](/blog/es/sell-blocknet-bnt-safetrade/), sin transferencia desde una billetera de por medio.

Exchanges como SafeTrade dan a todos sus clientes la misma dirección de depósito y distinguen los depósitos por un memo, un código corto único para tu cuenta. Por eso el memo importa: es lo que vincula un pago contigo. El pool mantiene un saldo separado para cada par de dirección y memo, así que tus recompensas nunca se mezclan con las de otros mineros que usan el mismo exchange.

## ¿Cómo configuras la minería a un exchange en seine?

<Answer>
Copia la dirección de depósito de BNT y tu memo de depósito de la página de depósito del exchange. Inicia seine con ambos: --address para la dirección y --payout-memo para el memo. Si introduces una dirección de exchange conocida en la pregunta de la primera ejecución, seine te pide el memo por sí solo y lo guarda. Comprueba en el registro la confirmación del pool y mina como siempre.
</Answer>

1. **Consigue tus datos de depósito.** En el exchange, abre la página de depósito de BNT y copia la dirección de depósito y el memo. Copia el memo exactamente: las mayúsculas y los ceros iniciales importan.
2. **Inicia seine con ambos.** En macOS o Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   En Windows, en PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Deja que seine lo guarde.** seine guarda el memo con la dirección, así que las ejecuciones posteriores siguen pagando a tu cuenta del exchange. Si escribes una dirección de exchange conocida en la pregunta de la primera ejecución, seine te pide el memo y guarda ambos.

La página [Get Started](/start) del pool tiene una sección «¿Minas hacia un exchange?» que crea este comando por ti: pega la dirección y el memo y copia el resultado. seine necesita un memo para las direcciones compartidas de exchanges conocidos, como las de SafeTrade. Los servicios, scripts y otros mineros desatendidos deben pasar `--payout-memo` en la línea de comandos, porque no hay ninguna pregunta que responder.

seine comprueba el memo con el pool al iniciar sesión. bntpool.com lo confirma y entonces empieza la minería. Para nombrar tus rigs, pasa `--pool-worker rig-01` igual que con una dirección personal.

## ¿Cómo se pagan los pagos a un exchange?

Las recompensas siguen el mismo camino que cualquier pago de bntpool.com. Tus shares ganan una parte de cada bloque que encuentra el pool dentro de su ventana PPLNS de seis horas, y la recompensa de cada bloque se confirma tras 60 bloques. Después, tu saldo confirmado se acumula en el pool.

El endpoint `/api/info` del pool indicaba estos pagos mínimos el 4 de noviembre de 2026:

| Destino | Pago mínimo |
|---|---|
| Dirección de exchange con memo | 10 BNT |
| Billetera personal | 1 BNT |

Cuando tu saldo confirmado llega a 10 BNT, el pool lo envía a la dirección del exchange con tu memo. El exchange acredita entonces el depósito tras su propio número de confirmaciones; su página de depósito indica cuántas necesita. [Cómo funcionan los pagos PPLNS](/blog/es/how-pplns-payouts-work/) explica en detalle la parte del pool.

La minería en solitario también funciona con memo. Pon `solo:` delante de la dirección del exchange y mantén `--payout-memo`, y el pool paga a tu cuenta del exchange la recompensa completa de cada bloque que encuentres. [Minería en solitario en bntpool.com](/blog/es/solo-mine-blocknet-on-bntpool/) explica las recompensas en solitario.

## ¿Cómo consultas tu saldo?

Abre la [página de estadísticas](/stats), pega la dirección de depósito del exchange, elige **Añadir memo** e introduce tu memo. La página muestra entonces tu tasa de hash, tus rigs, tu saldo y tus pagos para ese par de dirección y memo. Otros clientes del mismo exchange tienen sus propios pares y sus propios saldos.

Cada pago de **Recent Payouts** enlaza a su transacción en el explorador. Compáralo con el historial de depósitos del exchange para seguir las monedas hasta tu cuenta. [Sigue tu minero en bntpool.com](/blog/es/track-your-miner-on-bntpool/) explica el resto de la página de estadísticas.

## ¿Cómo vuelves a una billetera personal?

Inicia seine una vez con tu nueva dirección y `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine guarda la nueva dirección sin memo, y los nuevos shares cuentan para tu billetera personal. Un saldo ya acreditado al par del exchange se queda con ese par y se le paga cuando alcanza el mínimo; puedes seguirlo en la página de estadísticas. [Cambia tu dirección de minería](/blog/es/change-mining-wallet-address/) explica cómo acredita el pool el trabajo tras un cambio de dirección.

## ¿Qué configuraciones admiten memos?

Los memos necesitan la conexión directa de seine con el pool, que es la predeterminada. El modo daemon de seine mina contra tu propio nodo y paga a la billetera del nodo, así que no tiene memo. El proxy de granja no transmite memos, así que conecta los rigs del exchange directamente al pool. seine solo mina en un pool que confirma tu memo, así que tu trabajo siempre se paga a la cuenta que elegiste.

<CTA target="pool" title="Mina a tu cuenta del exchange" text="Inicia seine con tu dirección de depósito y tu memo, y bntpool.com paga tus recompensas directamente a tu cuenta del exchange desde 10 BNT." />
