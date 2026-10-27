---
title: '¿Se puede minar Blocknet a tiempo parcial?'
description: 'Mina Blocknet en sesiones cortas: sigue las shares en la ventana PPLNS de seis horas, vuelve con la misma dirección y consulta los saldos pendientes.'
tags: ['Guía de minería', 'PPLNS', 'Horarios']
faq:
  - q: '¿Debo minar seis horas antes de que cuenten mis shares?'
    a: 'No. Las shares aptas pueden participar en la ventana de recompensa de un bloque sin esperar seis horas. Tu parte depende del trabajo ponderado por dificultad frente al total del pool, y debe encontrarse un bloque para asignar su recompensa.'
  - q: '¿Debe seguir conectado el minero para recibir un pago existente?'
    a: 'Una conexión activa no es un requisito de pago. Los saldos confirmados se consideran según las reglas del pool, incluido el mínimo actual. Desconectarte no borra el saldo ni solicita por sí mismo un pago inmediato.'
  - q: '¿Reconectar reinicia la antigüedad de mis shares?'
    a: 'No. Las shares existentes conservan sus marcas de tiempo. El trabajo nuevo entra en las ventanas correspondientes al volver; reconectar no renueva shares antiguas ni añade trabajo por las horas desconectadas. Comprueba nuevas shares aceptadas tras reiniciar.'
sourceHash: '0a11c393062e'
translation: draft
---

Puedes ejecutar un minero de Blocknet en horas elegidas y volver después. Distingue la conexión actual, las shares enviadas y las recompensas asignadas. Esta guía sigue una sesión corta para que puedas parar, reiniciar y comprobar tu cuenta de bntpool.com sabiendo qué observar.

## ¿Puede una sesión corta generar recompensas?

<Answer>
Sí. El trabajo apto de una sesión corta puede participar en bloques cuyas ventanas PPLNS incluyan esas shares. No necesitas completar seis horas. Las recompensas dependen de que el pool encuentre bloques y de tu proporción de trabajo ponderado por dificultad. Al parar no añades trabajo; las shares envejecen normalmente y los saldos siguen las reglas de pago.
</Answer>

*Una línea temporal muestra minería, una pausa y el regreso, con marcas separadas para elegibilidad de shares y saldo existente.*

## Distingue los distintos plazos

bntpool publica actualmente una ventana PPLNS de seis horas, 60 bloques antes de que la recompensa pueda pagarse y un mínimo de 0,1 BNT en [Primeros pasos](/start). Son etapas distintas: trabajo incluido, maduración y umbral de saldo. La ventana de seis horas no es un temporizador que prometa un pago.

La [guía PPLNS](/blog/es/how-pplns-payouts-work/) explica la fórmula. Las shares aptas se ponderan por dificultad frente al resto del trabajo apto de la ventana. Los minutos conectado no fijan un porcentaje de la próxima recompensa. Dispositivos, participación del pool y resultados de shares pueden cambiar los pesos aunque dos sesiones duren igual.

## Sigue una share durante una pausa

Horario local ilustrativo: minas de 18:00 a 20:00 y una share apta queda registrada a las 19:00. Un bloque cuya ventana termine a las 22:00 mira seis horas atrás, hasta las 16:00. La share entra aunque estés desconectado. Esto ilustra elegibilidad, no predice que el pool encuentre un bloque a esa hora.

Una ventana que termine a las 02:00 del día siguiente empieza a las 20:00, por lo que la share de las 19:00 queda fuera. Volver a las 08:00 no cambia su fecha. El trabajo nuevo contribuye a ventanas posteriores. La [guía de suerte](/blog/es/mining-pool-luck-explained/) explica la variación de bloques: no planifiques suponiendo que ya toca uno.

## Para conservando una referencia

Antes de una pausa prevista, anota dirección receptora, nombre del equipo, hora aproximada y últimos mensajes de shares aceptadas. Comprueba la cuenta con la [guía de seguimiento](/blog/es/track-your-miner-on-bntpool/). Esa referencia permite distinguir un equipo detenido de otra dirección o una pantalla que aún no se ha actualizado.

Sal normalmente de seine con `q` en el panel o `Ctrl+C` en la terminal. Conserva el comando y la configuración. La [guía de uso del PC](https://seineminer.com/es/blog/mine-while-using-pc/) explica cómo compaginar tareas; parar correctamente no exige borrar ajustes, cambiar dirección ni pedir al pool que cierre la cuenta.

## ¿Qué ocurre con el saldo pendiente mientras estás desconectado?

Que una share salga de la ventana de un bloque posterior no borra una recompensa ya asignada a otro anterior. Las estimadas siguen el estado y la maduración del bloque; las confirmadas sin pagar permanecen hasta que las procesa el sistema. La [guía de confirmaciones](/blog/es/block-confirmations-mining-rewards/) distingue crédito provisional y saldo maduro.

Un saldo confirmado inferior a 0,1 BNT sigue por debajo del umbral automático actual hasta que nuevo crédito alcance el mínimo. Reconectar no lo incrementa ni fuerza un pago. Un saldo apto tampoco necesita que el minero siga conectado durante el procesamiento. El [historial de pagos](/payouts) distingue saldo pendiente y transacción enviada.

## Vuelve a la misma cuenta y verifica trabajo nuevo

Reinicia el comando guardado y comprueba pool, dirección y nombre del equipo antes de comparar estadísticas. La dirección identifica la cuenta cuyo saldo seguías. Si cambias deliberadamente de destino, consulta la [guía de cambio de dirección](/blog/es/change-mining-wallet-address/): el saldo anterior no se transfiere silenciosamente a la cuenta nueva.

Busca un trabajo actual y nuevas shares aceptadas; deja que se actualice la ventana de medición del pool. El promedio de una sesión recién iniciada puede diferir de la estimación móvil del pool. La [guía de hashrate](/blog/es/what-is-hashrate/) explica esos intervalos. Comprueba trabajo real y cuenta correcta antes de interpretar una diferencia breve como recompensas ausentes.

## Organiza las sesiones según tu rutina

Elige horas según el uso del ordenador y tu presupuesto eléctrico. La [guía de electricidad](https://seineminer.com/es/blog/mining-electricity-use/) calcula consumo para el horario real, incluidos intervalos con tarifas distintas. Una sesión más corta reduce tiempo de funcionamiento, pero la energía exacta sigue dependiendo del equipo y la carga medidos.

Registra inicios, paradas, trabajo aceptado y saldos durante varias sesiones. La [calculadora de recompensas](/blog/es/estimate-blocknet-mining-rewards/) ofrece una expectativa bajo sus supuestos, no una promesa para una noche concreta. Evalúa un periodo representativo y distingue conexión actual, elegibilidad de shares, maduración y pagos enviados.

<CTA target="pool" title="Sigue tus sesiones en bntpool.com" text="Vuelve con la misma dirección, verifica shares nuevas y sigue el saldo por sus etapas habituales de pago." />
