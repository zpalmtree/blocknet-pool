---
title: 'Comisiones del pool y del desarrollador del minero'
description: 'Entiende las comisiones del pool y del minero en Blocknet. Aprende dónde se aplican, cómo combinarlas y cómo evitar descontar dos veces el mismo concepto.'
tags: ['Guía de minería', 'Comisiones', 'Recompensas']
faq:
  - q: '¿Son el mismo cargo?'
    a: 'No. La comisión del pool pertenece al reparto de recompensas; la del desarrollador, al software. seine v0.2.36 dedica un 2% del tiempo sujeto a comisión al desarrollo tras la gracia inicial. El pool bntpool.com publicaba un 0% al comprobarlo el 26 de octubre de 2026.'
  - q: '¿Cómo se combinan?'
    a: 'Para una estimación bruta, multiplica por uno menos la fracción del minero y después por uno menos la del pool. Usa la versión real y la configuración actual. Esto estima recompensas retenidas, no garantiza un pago ni calcula beneficios monetarios.'
  - q: '¿Descuento la comisión del minero del hashrate del pool?'
    a: 'Primero identifica qué representa. Si ya estima trabajo acreditado a tu dirección después del tiempo del desarrollador, descontarlo de nuevo lo duplicaría. El calculador permite poner cero para ese ajuste ya reflejado. No vuelvas a descontar ninguna comisión de un saldo confirmado o pago registrado.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Identifica las dos capas de comisiones de una estimación Blocknet y dónde actúan. La guía usa seine v0.2.36 y ajustes públicos de bntpool comprobados el 26 de octubre de 2026. Ayuda a elegir entradas del [calculador](/blog/es/estimate-blocknet-mining-rewards/#mining-calculator) y entender [My Stats](/stats).

## ¿Qué diferencia hay entre la comisión del pool y del minero?

<Answer>
La del pool forma parte de repartir recompensas; la del desarrollador forma parte de la programación del minero. Tienen responsables y etapas distintos. Comprueba ambas, identifica si la entrada es bruta o ya ajustada y aplica cada ajuste pertinente una sola vez.
</Answer>

*El trabajo pasa por un ajuste de tiempo del minero y otro de recompensa del pool antes de obtener la cantidad retenida estimada.*

## ¿Sobre qué se aplica la comisión del pool?

El pool reúne trabajo elegible y reparte la recompensa distribuible según sus reglas. bntpool calcula la comisión configurada sobre la recompensa del bloque antes de distribuir el resto mediante PPLNS. Es una etapa de reparto. La [guía PPLNS](/blog/es/how-pplns-payouts-work/) explica participación y créditos.

El 26 de octubre de 2026, `https://bntpool.com/api/info` publica `pool_fee_pct` como 0,0, es decir, 0%. Consulta el valor actual en [Get Started](/start) o la API al actualizar una estimación. Cero describe ese ajuste del pool; la política del software sigue siendo otra entrada.

## ¿Cómo funciona la comisión de seine?

La versión pública v0.2.36 documenta un 2% uniforme en cualquier pool y en solitario. Se programa como tiempo de minería para la dirección del desarrollador, no como retirada posterior de tu cartera. Los primeros 10 minutos no tienen comisión; se cuenta el tiempo posterior. La [guía de configuración](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) describe la política.

La programación funciona por rondas, por lo que una ventana corta puede no dar exactamente 2%. Es el objetivo a largo plazo tras la gracia, no una promesa por minuto o recompensa. Guarda la versión: otra guía histórica o programa puede tener otra política. La comisión del pool no sustituye ese dato del software.

## ¿Cómo combinas los porcentajes?

Usa fracciones retenidas. Con m para el minero y p para el pool, como fracciones, se conserva `(1 - m) * (1 - p)` del bruto. Lo aplica el [estimador](/blog/es/estimate-blocknet-mining-rewards/). Sumar porcentajes no es la regla general: el segundo ajuste actúa sobre lo que queda.

Ejemplo puramente aritmético: parte de 100 BNT brutos hipotéticos, aplica 2% al minero y el 0% del pool comprobado. Da 100 × 0,98 × 1,00 = 98 BNT. Ilustra el ajuste estable; no es medición de hardware, recompensa prometida ni simulación de una sesión corta con gracia.

## Elige la entrada de hashrate correcta

Una tasa local o benchmark describe trabajo de la máquina. Antes de aplicar el factor del desarrollador, averigua si incluye todo el tiempo o solo trabajo para tu dirección. Lee su origen y ventana temporal. La [guía de hashrate](/blog/es/what-is-hashrate/) explica las observaciones locales, del pool y de la red.

La tasa del pool para tu dirección puede reflejar ya el tiempo acreditado a otra. Si la entrada incluye ese ajuste, escribe cero para la comisión adicional del minero. Cambia la aritmética, no la política real. La comisión del pool sigue aplicándose al estimar recompensas elegibles brutas; comprueba qué incluye cada cifra.

## ¿Por qué no descontar otra vez del saldo?

Un saldo confirmado es un resultado contable, no una estimación bruta intacta. El pool ya asignó créditos. Restar de nuevo esa comisión al saldo o al pago registrado altera la cifra sin otro evento de comisión. Usa el importe mostrado al conciliar y determina su etapa antes de estimar.

Recompensa pendiente, saldo confirmado y pago enviado también difieren en tiempo. Madurez y umbral de pago explican esos pasos; no son porcentajes adicionales. Consulta la [guía de confirmaciones](/blog/es/block-confirmations-mining-rewards/) y el [historial de pagos](/payouts). La comisión propia de una transacción de cartera, si procede, es un campo separado.

## Conserva el origen de la estimación

Guarda fecha, versión, comisiones, origen del hashrate y ventana. El calculador combina datos actuales del pool y red con tus entradas. Actualiza los datos al volver al resultado. Las recompensas reales también dependen de shares elegibles, bloques y PPLNS; fórmula y asiento contable responden preguntas diferentes.

Usa [My Stats](/stats) y la [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/) para ver trabajo y recompensas. Antes de cambiar comisiones, compara dirección, periodo y etapa iguales. El calculador estima BNT; electricidad, equipo y precio monetario quedan fuera de este cálculo. No necesitas cambiar de dirección.

<CTA target="pool" title="Sigue las recompensas en bntpool.com" text="Usa el calculador para estimar y My Stats para trabajo registrado, saldos y pagos." />
