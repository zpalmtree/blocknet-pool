---
title: "Blocknet-Mining-Rechner"
description: "Schätze BNT pro Tag und Woche anhand deiner Hashrate, aktueller Netzwerkdaten und des Belohnungsplans. Gebühren werden separat ausgewiesen."
tags: [Mining-Belohnungen, Mining-Anleitung, Hashrate, bntpool]
sourceHash: c8369c1db7c0
translation: draft
faq:
  - q: 'Wie berechnet man Blocknet-Mining-Belohnungen?'
    a: 'Teile deine Hashrate durch die Netzwerk-Hashrate, multipliziere mit 288, der täglichen Blockzahl von Blocknet, und mit der aktuellen Blockbelohnung. Das ergibt die erwarteten Brutto-BNT pro Tag. Die Netzwerk-Hashrate steht im Dashboard von bntpool.com, deine eigene in seine oder auf My Stats.'
  - q: 'Wo sehe ich die Blocknet-Netzwerk-Hashrate?'
    a: 'Das Dashboard von bntpool.com zeigt sie auf der Karte Network Hashrate, meist in KH/s. Multipliziere mit 1.000 für H/s. Denselben Wert in H/s liefert das Feld network_hashrate der Pool-API unter bntpool.com/api/stats. Er wird über die letzten zehn Blöcke gemessen und mit jedem Block aktualisiert.'
  - q: 'Ändert sich die Blocknet-Blockbelohnung mit der Zeit?'
    a: 'Ja. Innerhalb jedes 30-Tage-Monats aus 8.640 Blöcken bleibt sie gleich und sinkt zu Beginn des nächsten Monats, bis sie ab Monat 48 dauerhaft 2,0 BNT pro Block beträgt. Teile die aktuelle Blockhöhe durch 8.640, runde ab und suche diesen Monat im Belohnungsplan.'
  - q: 'Warum weichen meine täglichen Pool-Belohnungen von der Schätzung ab?'
    a: 'Die Schätzung ist ein Durchschnitt. An einzelnen Tagen kann der Pool mehr oder weniger Blöcke finden als erwartet, und neue Miner verändern die Netzwerk-Hashrate. Bei kontinuierlichem Mining über eine Woche oder länger gleichen sich tägliche Schwankungen aus. Deine Gutschriften verfolgst du auf My Stats.'
  - q: 'Ändert Pool-Mining meine erwartete BNT-Menge?'
    a: 'Dein erwarteter Anteil an den Netzwerk-Belohnungen bleibt gleich, denn er hängt nur von deinem Anteil an der Netzwerk-Hashrate ab. Der Pool verändert den Zeitpunkt: Statt tagelang auf einen ganzen Block zu warten, erhältst du einen Anteil an jedem Block von bntpool.com und damit kleine, regelmäßige Beträge.'
---

<RewardsCalculator />

Diese Anleitung zeigt dir, wie du die täglich von deiner Hardware geschürfte BNT-Menge schätzt. Du lernst die Formel, findest ihre Eingabewerte im [Dashboard von bntpool.com](/) und rechnest ein datiertes Beispiel mit Live-Pool-Daten und den besten von seine gemessenen Hashraten durch.

## Wie viel BNT minet deine Hardware pro Tag?

<Answer>
Die erwarteten BNT pro Tag ergeben sich aus deiner Hashrate geteilt durch die Netzwerk-Hashrate, multipliziert mit 288 Blöcken täglich und der aktuellen Blockbelohnung. Deine Hashrate kommt aus seine oder My Stats, die Netzwerk-Hashrate aus dem Dashboard von bntpool.com und die Belohnung aus aktueller Blockhöhe und Blocknets Belohnungsplan.
</Answer>

Als Formel:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Jeder Bestandteil hat einen einfachen Grund:

- **288 Blöcke täglich.** Blocknet strebt einen Block alle fünf Minuten an. 86.400 Sekunden pro Tag geteilt durch 300 Sekunden pro Block ergibt 288. Die Difficulty passt sich nach jedem Block an, sodass die Blockchain auch bei neuen Minern ungefähr dieses Tempo hält.
- **Dein Hashrate-Anteil ist dein Blockanteil.** Blocknets Difficulty entspricht ungefähr den erwarteten Hashes bis zu einem Block. Jeder Hash hat dieselbe Lösungschance. Ein Rechner mit 1 % der Netzwerk-Hashes kann daher langfristig 1 % der Blöcke erwarten.
- **Die Blockbelohnung hängt von der Höhe ab.** Alle Blöcke desselben 30-Tage-Monats aus 8.640 Blöcken zahlen gleich viel. Innerhalb eines Monats ist die Belohnung daher für jeden Tag eine bekannte Konstante.

Das Ergebnis ist deine zeitlich gemittelte, erwartete Bruttobelohnung.

## Wo findest du die Eingabewerte?

### Deine Hashrate

Der Miner seine zeigt die Hashrate sofort nach dem Start im Terminal-Dashboard. Gib ihm einige Minuten: Er stimmt Thread-Anzahl und CPU-Kernel mit echter Mining-Arbeit ab. Sobald er die besten Einstellungen für deinen Rechner gefunden hat, stabilisiert sich der Wert.

Wenn du im Pool minest, zeigt die Karte **Hashrate** auf [My Stats](/stats) die Sicht des Pools, gemittelt über die letzte Stunde. Verwende für die Schätzung den stabilen Wert von seine oder das 24-Stunden-Diagramm des Pools.

### Netzwerk-Hashrate

Die Karte **Network Hashrate** zeigt die Hashrate des gesamten Netzwerks in KH/s. 62,33 KH/s entsprechen 62.330 H/s. Rechne vor der Division in H/s um, sonst liegt dein Ergebnis um den Faktor tausend daneben.

Der Pool misst diesen Wert aus Arbeit und Zeitstempeln der letzten zehn Blöcke. Er ändert sich daher von Block zu Block. [Was ist die Hashrate?](/blog/de/what-is-hashrate/) erklärt Berechnung und Schwankungen. Prüfe den Wert für eine stabilere Eingabe mehrmals am Tag und verwende den Durchschnitt. Die API liefert ihn ebenfalls in H/s:

```sh
curl -s https://bntpool.com/api/stats
```

Der Abschnitt `chain` der Antwort enthält `network_hashrate` in H/s und `current_job_height`, den Block, an dem der Pool arbeitet.

### Blockbelohnung

Die Karte **Current Block** zeigt die aktuell bearbeitete Netzwerkhöhe. Teile sie durch 8.640, runde ab und suche den Monat im [Blocknet-Blockbelohnungsplan](/blog/de/blocknet-block-reward-schedule/). Ein schneller Gegencheck ist die [Blockseite](/blocks): Ihre Spalte Reward zeigt die vollständige Belohnung jedes gefundenen Pool-Blocks.

## Ein Rechenbeispiel vom 25. Oktober 2026

<Note>
Dieses Beispiel verwendet eine Momentaufnahme von https://bntpool.com/api/stats vom 25. Oktober 2026 um 02:03 Uhr EDT. Netzwerk-Hashrate und Blockbelohnung ändern sich. Setze aktuelle Dashboard-Werte ein, bevor du dich auf das Ergebnis stützt.
</Note>

| Eingabewert | Wert der Momentaufnahme |
|---|---|
| Aktueller Block | 69.020 (Monat 7, denn 69.020 ÷ 8.640 = 7,99) |
| Blockbelohnung | 467,67 BNT |
| Netzwerk-Hashrate | 62.329 H/s (angezeigt als 62,33 KH/s) |
| Hashrate von bntpool.com | 5.240 H/s |

Nimm den AMD-EPYC-Zen-5-Server aus den seine-Benchmarks mit 49,4 H/s:

1. Netzwerkanteil: 49,4 ÷ 62.329 = 0,0793 %.
2. Erwartete Blöcke täglich: 0,000793 × 288 = 0,2283.
3. Erwartete BNT täglich: 0,2283 × 467,67 = etwa 106,8 BNT.

Monat 8 beginnt auf Höhe 69.120, etwa 100 Blöcke nach der Momentaufnahme, mit 439,46 BNT pro Block. Bei derselben Netzwerk-Hashrate ergibt derselbe Server dann 0,2283 × 439,46 = etwa 100,3 BNT täglich. Deshalb lohnt es sich, die Belohnung zu jedem Monatsbeginn zu prüfen.

Hier dieselbe Beispielrechnung mit der Netzwerk-Hashrate der Momentaufnahme für das beste von seine gemessene Ergebnis jeder Hardwaregeneration:

| Hardware | Threads | Hashrate | Netzwerkanteil | BNT/Tag bei 467,67 | BNT/Tag bei 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793 % | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573 % | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | 0,0496 % | 66,8 | 62,7 |
| Arm-Server (AWS Graviton4) | 16 | 21,5 H/s | 0,0345 % | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210 % | 28,3 | 26,6 |

Entscheidend ist der Wert deines eigenen Rechners. Lass seine eine Weile laufen und verwende dessen stabile Hashrate anstelle der Tabelle. Die Anleitung [Blocknet-Mining-Hardware und Hashrate](https://seineminer.com/de/blog/blocknet-mining-hardware-hashrate/) erklärt den Einfluss von Speicherbandbreite und RAM. Vollständige Ergebnisse stehen auf der [Performance-Seite von seine](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## Wie verändert Mining auf bntpool.com die Schätzung?

Dein erwarteter Anteil bleibt im Pool gleich. Anders ist der Eingang der Belohnung. bntpool.com zahlt per PPLNS: Jede Blockbelohnung wird nach Difficulty auf die Shares verteilt, die in den sechs Stunden davor eingereicht wurden. [So funktionieren PPLNS-Auszahlungen](/blog/de/how-pplns-payouts-work/) erklärt diese Aufteilung.

Du kannst dieselbe Schätzung aus Sicht des Pools durchführen und erhältst dasselbe Ergebnis. Die 5.240 H/s des Pools waren zum Aufnahmezeitpunkt 8,4 % des Netzwerks. Er erwartete daher 288 × 5.240 ÷ 62.329 = etwa 24,2 Blöcke täglich, ungefähr einen pro Stunde. Im Bereich Round Progress zeigt das Dashboard dies als **Expected Block Time**. Ein 49,4-H/s-Server hat 0,94 % der Pool-Hashrate. 0,94 % von 24,2 Blöcken zu 467,67 BNT sind erneut etwa 106,8 BNT täglich. Die Pool-Hashrate kürzt sich heraus.

Der Unterschied liegt im Zeitpunkt. Allein würde dieser Zen-5-Server ungefähr alle 4,4 Tage einen ganzen Block erwarten. [Pool- oder Solo-Mining für Blocknet](/blog/de/pool-vs-solo-mining-blocknet/) vergleicht beide genauer. Auf bntpool.com erhält er einen Anteil an jedem der rund 24 täglichen Pool-Blöcke; sein Guthaben wächst rund um die Uhr in kleinen Schritten. Pool-Gebühr und Entwicklergebühr von seine werden vom Bruttowert der Formel abgezogen.

Zwei Pool-Eigenschaften prägen deinen ersten Tag:

- **Das Fenster füllt sich über sechs Stunden.** Der Anteil eines neuen Miners an jedem Block wächst, während seine Shares das PPLNS-Fenster füllen. Danach bleibt er bei voller Leistung, solange der Miner weiterarbeitet.
- **Belohnungen reifen vor der Auszahlung.** Nach 60 Bestätigungen, etwa fünf Stunden, wandert die Blockbelohnung zu Confirmed Rewards. Der Pool sendet dein Guthaben automatisch ab 0,1 BNT.

## Wie vergleichst du die Schätzung mit echten Ergebnissen?

Die Schätzung ist ein Durchschnitt. Einzelne Tage schwanken darum, weil das Pool-Glück von Runde zu Runde variiert. Über viele Runden nähert sich der Effort-Durchschnitt 100 %. Eine Woche kontinuierlichen Minings ist daher ein guter Vergleichszeitraum. Notiere zu Wochenbeginn und -ende die Summe von Confirmed Rewards und Paid Balance auf [My Stats](/stats). Die Zunahme entspricht den Gutschriften aus Blöcken, die in dieser Woche reiften. Vergleiche sie mit der Schätzung für dieselben Tage anhand der jeweiligen Belohnungen und Netzwerk-Hashraten.

Berechne die Formel neu, sobald sich ein Eingabewert ändert:

- **Zu jedem neuen Monat**, wenn die Belohnung sinkt. Monatsgrenzen liegen alle 8.640 Blöcke, ungefähr alle 30 Tage.
- **Bei veränderter Netzwerk-Hashrate**, etwa durch neue Miner.
- **Bei veränderter eigener Hashrate**, nach einem zusätzlichen Rechner, reservierten Huge Pages unter Linux oder einer NVIDIA-GPU, auf der seine parallel zur CPU minet.

<Note>
Diese Hashraten gelten für Blocknets aktuellen Argon2id-Proof-of-Work. Laut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) ist um den 16. Februar 2027 das Netzwerk-Upgrade Faraday geplant, das den Proof of Work ändern soll. Aktualisiere den Miner seine regelmäßig: Er prüft beim Start und alle 12 Stunden auf neue Versionen.
</Note>

<CTA target="seine" title="Miss deine eigene Hashrate" text="Lade seine herunter, lass ihn sich einige Minuten abstimmen und setze seine Hashrate zusammen mit aktuellen Dashboard-Werten in die Formel ein." />
