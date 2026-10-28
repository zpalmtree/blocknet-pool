---
sourceHash: 77522359a409
translation: draft
title: Cómo protege la minería los pagos de Blocknet
description: Aprende cómo Blocknet elige el historial de pagos mediante prueba de trabajo, qué aportan las confirmaciones y qué verifican los nodos al aceptar bloques.
tags: [Blocknet, Seguridad de pagos, Prueba de trabajo]
faq:
  - q: ¿Puede más potencia minera autorizar un pago desde mi monedero?
    a: La potencia minera ayuda a producir bloques competidores. Gastar fondos sigue requiriendo autorización válida, y los nodos comprueban las pruebas y el balance de las transacciones antes de aceptar un bloque. Una rama con más trabajo debe respetar esas reglas. La prueba de trabajo no sustituye las claves privadas necesarias para gastar tus salidas.
  - q: ¿Sigue Blocknet la cadena con más bloques?
    a: Core v0.22.0 calcula el trabajo acumulado sumando la dificultad de cada bloque. Una rama válida con más trabajo puede convertirse en la cadena elegida, sujeta a las reglas de reorganización. El número de bloques no basta porque pueden tener distintas dificultades. Importan tanto el trabajo como la validez.
  - q: ¿Son lo mismo las confirmaciones de un pago y los shares del pool?
    a: Un share registra trabajo minero enviado al pool. Una confirmación se refiere a la inclusión de una transacción en la cadena elegida y a los bloques construidos después. Los shares ayudan a repartir recompensas, mientras los bloques de red avanzan el historial de pagos. Más shares no añaden directamente confirmaciones a un pago concreto.
---

Sigue un pago en BNT desde su inclusión en un bloque hasta un historial respaldado por trabajo minero posterior. Esta guía explica por qué la red necesita elegir entre historiales competidores, qué indican las confirmaciones y cómo convive esa elección con la validación de transacciones privadas. La implementación descrita es core v0.22.0, comprobada el 28 de octubre de 2026.

## ¿Qué protege la minería cuando envías BNT?

<Answer>
La minería ayuda a elegir un historial de pagos compartido y ordenado. Cada bloque aceptado enlaza con su predecesor y aporta prueba de trabajo. Los nodos comprueban bloques y transacciones propuestos, y comparan ramas válidas por trabajo acumulado según sus reglas de reorganización. Los bloques posteriores añaden trabajo detrás de un pago incluido y dan significado práctico a sus confirmaciones.
</Answer>

Un pago válido necesita autorización y un lugar en el registro. Tu monedero proporciona la autorización para gastar tus fondos. La minería y la selección de cadena ayudan a los nodos a acordar qué transacciones válidas pertenecen al historial actual. Ambas tareas se complementan: la prueba de trabajo de un bloque no vuelve aceptable un pago inválido.

La [guía de aplicación de reglas](https://seineminer.com/es/blog/who-controls-blocknet/) explica quién desarrolla, propone y valida. Aquí nos centramos en el pago: cómo su inclusión pasa a formar parte del historial que siguen tu monedero y los demás nodos.

## ¿Cómo vinculan los bloques un historial de pagos?

La cabecera de un bloque contiene el hash del bloque anterior y una raíz de Merkle derivada de sus transacciones. La raíz compromete criptográficamente el contenido del bloque, mientras el hash anterior lo enlaza con el historial previo. Cambiar una transacción modifica la raíz y la cabecera que se mina. Los bloques descendientes deben entonces referirse a ese historial modificado mediante sus propios enlaces.

Imagina un pago incluido en un bloque, seguido de varios bloques aceptados. Reemplazar esa parte del historial requiere una rama competidora con bloques válidos enlazados y suficiente trabajo acumulado, dentro de las reglas de reorganización del nodo. Editar una transacción en un archivo local no produce esa rama. El trabajo corresponde a una secuencia concreta de cabeceras y transacciones.

El [código de bloques publicado](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) define esos campos, las comprobaciones de Merkle y los procesos de selección de cadena. La [guía de dificultad](/blog/es/mining-difficulty-explained/) explica cómo determina la red el trabajo exigido para cada nuevo bloque.

## ¿Por qué importa más el trabajo acumulado que el número de bloques?

Dos mineros pueden encontrar bloques válidos distintos que extiendan el mismo padre. Cada propuesta tiene su propio hash y los nodos pueden recibirlas en órdenes diferentes. Esto puede producir ramas competidoras brevemente. La red necesita una regla que siga el trabajo que respalda un historial, en vez del orden en que lo anunció un participante.

Core v0.22.0 calcula el trabajo acumulado sumando la dificultad del bloque al trabajo acumulado de su padre. Tras la validación, una rama con trabajo estrictamente mayor puede provocar un cambio de cadena elegida. Una alternativa con el mismo trabajo no provoca ese cambio mediante esta comparación. La dificultad debe ser válida para el contexto del padre de esa rama; un minero no puede escribir simplemente un número mayor en la cabecera.

Compara, por ejemplo, dos ramas con un tramo inicial común. Ese tramo aporta el mismo trabajo a ambos totales. La diferencia decisiva es el trabajo acumulado después de separarse, junto con las comprobaciones de validez y reorganización. Contar bloques sin considerar su dificultad puede ocultar esa diferencia. Es una comparación conceptual, no el informe de una bifurcación en curso.

## ¿Cómo ayudan las confirmaciones a seguir un pago?

Una vez incluido el pago en la cadena elegida, los bloques posteriores se construyen sobre ese historial. Su profundidad aumenta conforme llegan. Reemplazar su inclusión exigiría un historial alternativo admisible desde antes del bloque relevante, mientras la rama actual sigue acumulando trabajo. Por eso comprobar inclusión y profundidad posterior informa más que observar una transacción difundida.

La altura identifica una posición; el hash identifica un bloque concreto. Para seguir un pago, consulta su registro y el bloque que lo contiene, y verifica que este sigue en la cadena elegida. Si cambia el historial seleccionado, también pueden cambiar la inclusión y la profundidad. Algunos contadores incluyen el bloque contenedor y otros cuentan solo los posteriores: comprueba la convención de la interfaz.

Core v0.22.0 también aplica un límite de reorganización: rechaza un cambio cuyo ancestro común esté por debajo de la altura de la punta actual menos 100. Es una regla de la implementación en ejecución, no una promesa universal asociada a un contador. La [guía de tiempos de transferencia](https://seineminer.com/es/blog/blocknet-transfer-time/) trata la disponibilidad para gastar en el monedero; las [confirmaciones de recompensas](/blog/es/block-confirmations-mining-rewards/), la maduración coinbase y la contabilidad del pool.

## ¿Cómo se comprueba que un pago privado no gaste dos veces?

El formato de transacciones publicado de Blocknet usa una imagen de clave vinculada a la prueba de gasto. La validación comprueba ese vínculo y rechaza una imagen ya gastada en el estado pertinente de la cadena. Así los nodos detectan usos incompatibles de una salida, mientras la firma de anillo oculta qué miembro admisible es la entrada real. Los importes privados siguen pasando comprobaciones de balance y rango.

Supón que un propietario autoriza transacciones incompatibles que gastan la misma salida. Un historial válido aceptado no puede contener ambos gastos. La selección de cadena importa porque los historiales competidores pueden contener elecciones diferentes; la validación importa porque todos deben obedecer las reglas de gasto. Más trabajo no sustituye una firma válida ni permite que una transacción desequilibrada supere la comprobación contable.

Estas comprobaciones aparecen en el [validador de transacciones publicado](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). La [guía de privacidad de Blocknet](https://seineminer.com/es/blog/blocknet-privacy-features/) explica los mecanismos de privacidad más amplios. La seguridad de pagos combina gasto válido, historial coherente y criptografía que protege lo que ese historial revela.

## ¿Qué papel tiene la minería en pool en la seguridad de pagos?

Un pool coordina la búsqueda de un bloque y envía el candidato resuelto a su nodo de conexión. Un share que cumple el objetivo más fácil del pool registra trabajo aportado; solo una solución que cumpla el objetivo de red puede convertirse en bloque tras validarse. La [explicación de pools](/blog/es/what-is-a-mining-pool/) distingue ambos objetivos y sus funciones.

Tu minero puede contribuir al proceso que extiende el historial de pagos mientras el pool contabiliza tu trabajo por separado. Un saldo privado del pool, un número de shares y un pago confirmado en el monedero describen cosas diferentes. Empieza por la [guía de conexión al pool](/blog/es/mine-blocknet-on-bntpool/) para participar y usa el registro de tu monedero para seguir un pago en BNT concreto.

<CTA target="pool" title="Aporta trabajo a Blocknet" text="Conecta seine a bntpool.com y sigue tu trabajo minero, recompensas y pagos con las herramientas públicas del pool." />
