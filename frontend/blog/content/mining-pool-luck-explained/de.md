---
title: 'Pool-Glück erklärt: Aufwand und Zeit zwischen Blöcken'
description: 'Verstehe Pool-Glück, 100 % Aufwand und lange Runden. Lies den Rundenverlauf von bntpool.com und unterscheide erwartete Blockzeiten von deinen Auszahlungen.'
tags: ['Mining-Pools', 'Pool-Glück', 'Blocknet']
faq:
  - q: 'Bedeutet 100 % Aufwand, dass der Pool gleich einen Block findet?'
    a: 'Nein. Die erfasste Share-Arbeit hat damit die für einen Block erwartete Menge bei der im Rechenweg verwendeten Schwierigkeit erreicht. Das ist ein Vergleich mit einem Durchschnitt, kein Fertigstellungsgrad. Jeder neue Hash muss weiterhin das Ziel des Netzwerks erfüllen.'
  - q: 'Ist ein niedrigerer Glückswert auf bntpool.com besser?'
    a: 'Lies zuerst die Bezeichnung und Definition. Die Spalte Aufwand und die Karte Durchschnittliches Glück auf bntpool.com setzen geleistete zu erwarteter Arbeit ins Verhältnis. Niedrigere Werte bedeuten weniger erfasste Arbeit pro Block. Andere Seiten verwenden eventuell den Kehrwert. Vergleiche passende Kennzahlen und Stichprobengrößen statt nur Prozentwerte.'
  - q: 'Erhöht eine lange Runde meine nächste Auszahlung?'
    a: 'Eine lange Runde garantiert keine höhere persönliche Auszahlung. Der Blockfund bestimmt, wann eine Belohnung verfügbar wird; die PPLNS-Abrechnung bestimmt deinen Anteil anhand berechtigter akzeptierter Arbeit. Danach gelten Bestätigungs- und Auszahlungsregeln. Prüfe deine Guthaben unter Meine Statistiken und den Ablauf im Auszahlungsleitfaden.'
sourceHash: 'f7051fa67591'
translation: draft
---

Das Pool-Dashboard zeigt sowohl die Mining-Arbeit als auch die damit gefundenen Blöcke. Dieser Leitfaden erklärt Aufwandsprozente, erwartete Blockzeiten und den Rundenverlauf auf bntpool.com. So kannst du einen statistischen Durchschnitt von einem Countdown unterscheiden und deine Mining-Verbindung prüfen, ohne auf den nächsten Block des Pools zu warten.

## Was ist Pool-Glück beim Mining?

<Answer>
Pool-Glück beschreibt, wie tatsächliche Blockfunde vom aufgrund der Arbeit erwarteten Durchschnitt abweichen. Blöcke werden zufällig gefunden: Manche benötigen wenig Arbeit, andere mehr. Auf bntpool.com vergleicht der Rundenaufwand akzeptierte Share-Arbeit mit der Blockschwierigkeit. Ein niedrigerer Aufwand bedeutet, dass die Runde ihren Block mit weniger erfasster Arbeit gefunden hat.
</Answer>

Ein Pool bündelt die Versuche vieler Miner, muss aber weiterhin einen Hash finden, der das Netzwerkziel erfüllt. Akzeptierte Shares messen Beiträge anhand eines leichteren Ziels. Damit schätzt der Pool die Hash-Arbeit zwischen Blockfunden. Die [Einführung in Mining-Pools](/blog/de/what-is-a-mining-pool/) erklärt diesen Zusammenhang; Glück beschreibt die Schwankung der daraus entstehenden Blockfunde.

## Wie berechnet der Pool den Aufwand?

Bei einer abgeschlossenen Runde teilt bntpool.com die akzeptierte Share-Arbeit durch die Schwierigkeit des Blocks und multipliziert mit 100. Share-Arbeit wird nach Schwierigkeit gewichtet und ist nicht einfach die Anzahl eingereichter Shares. Ein Share mit höherer zugewiesener Schwierigkeit steht für mehr erwartete Hash-Arbeit als einer mit niedrigerer Schwierigkeit.

**Aufwand (%) = akzeptierte Share-Arbeit ÷ Blockschwierigkeit × 100**

Angenommen, ein Block hat die Schwierigkeit 10.000 und der Pool erfasst während der Runde 5.000 Einheiten akzeptierter Arbeit. Der Aufwand beträgt dann 50 %. Bei 20.000 Einheiten und gleicher Blockschwierigkeit wären es 200 %. Das sind Rechenbeispiele, keine Messungen eines bestimmten produktiven Blocks.

| Aufwand | Bedeutung |
|---|---|
| 50% | Die Hälfte der für diese Schwierigkeit erwarteten Arbeit |
| 100% | Die für einen Block erwartete Arbeitsmenge |
| 200% | Das Doppelte der für diese Schwierigkeit erwarteten Arbeit |

Der Aufwand der aktuellen Runde verwendet die aktuelle Schwierigkeit, die sich mit dem Fortschritt der Chain ändern kann. Historische Zeilen verwenden die Schwierigkeit ihres Blocks. Die Zahl vergleicht Arbeit und ist keine Fortschrittsanzeige, die bei 100 % enden muss. Sie unterscheidet sich auch von der Rundendauer: Ein schnellerer Pool kann dieselbe Arbeit in kürzerer Zeit leisten.

## Was bedeutet die erwartete Blockzeit?

Sie ist das durchschnittliche Intervall, das sich aus dem geschätzten Anteil des Pools an der Netzwerk-Hashrate ergibt. Das Dashboard von bntpool.com multipliziert dafür 300 Sekunden mit der Netzwerk-Hashrate und teilt durch die Pool-Hashrate. Verwende für beide dieselben Einheiten. Der [Hashrate-Leitfaden](/blog/de/what-is-hashrate/) erklärt, warum sich diese Schätzungen mit akzeptierten Shares und neuen Blöcken ändern.

**Erwartete Sekunden = 300 × Netzwerk-H/s ÷ Pool-H/s**

Wird das Netzwerk auf 100.000 H/s und der Pool auf 10.000 H/s geschätzt, stellt der Pool ein Zehntel der geschätzten Gesamtleistung. Sein erwartetes Intervall beträgt 3.000 Sekunden, also 50 Minuten. Dieser Durchschnitt kann aus kurzen und langen Runden entstehen; er legt den nächsten Block nicht auf genau 50 Minuten ab jetzt fest.

Das Dashboard vergleicht auch verstrichene und erwartete Zeit. Dieser zeitbasierte Prozentsatz kann vom Share-basierten Aufwand abweichen, weil beide unterschiedliche Eingaben verwenden. Prüfe die Bezeichnung, bevor du zwei Zahlen vergleichst. Änderungen der Pool-Hashrate oder Netzwerkschwierigkeit können die Schätzung verschieben, während dieselbe Runde noch läuft.

## Ist nach 100 % Aufwand ein Block fällig?

<Answer>
Bei 100 % Aufwand ist dem Pool kein Block geschuldet. Vorherige erfolglose Hashes erhöhen nicht die Chance, dass der nächste ein unverändertes Netzwerkziel erfüllt. Bei konstanter Hashrate und Schwierigkeit bleibt die erwartete Wartezeit ab jetzt gleich, auch nach einer langen Runde. Mehr Mining-Arbeit schafft mehr Versuche, keinen festen Termin.
</Answer>

Ein einfaches Wahrscheinlichkeitsmodell verdeutlicht das. Bei unabhängigen Versuchen und konstanter Blockfundrate ist die Wartezeit näherungsweise exponentialverteilt. Die Wahrscheinlichkeit, nach einem erwarteten Intervall noch keinen Block zu haben, beträgt etwa 36,8 %; nach zwei etwa 13,5 %; nach drei etwa 5,0 %. Diese Werte beschreiben viele hypothetische Runden unter festen Bedingungen, keine Vorhersage für die aktuelle Pool-Runde.

| Verstrichene erwartete Intervalle | Ungefähre Wahrscheinlichkeit, noch keinen Block zu haben |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Die Berechnung lautet `exp(-t / T)`, wobei `T` das mittlere Intervall und `t` die verstrichene Zeit ist. Die [NIST-Referenz zur Exponentialverteilung](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) beschreibt diese Überlebensfunktion. Reale Hashrate und Schwierigkeit schwanken; die Tabelle erklärt daher den Zufall. Auch eine Serie schneller Blöcke ändert bei gleichem Ziel nicht die Wahrscheinlichkeit des nächsten Hashes.

## Wie liest du den Glücksverlauf von bntpool.com?

Öffne [Glück](/luck), um abgeschlossene Runden zu vergleichen. Jede Zeile zeigt einen Block, Aufwand, Rundendauer, Status und Fundzeit. Betrachte mehrere Runden gemeinsam, statt eine ungewöhnlich kurze oder lange auszuwählen. Über den Blocklink kannst du den zugehörigen Chain-Eintrag prüfen; die Seitennavigation erschließt einen längeren Verlauf.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Echte Glücksverlaufsseite von bntpool.com mit Aufwand, Rundendauer und Blockstatus" width="1200" height="833" loading="lazy" />

Dieser Screenshot wurde am 26. Oktober 2026 aufgenommen. Zeilen und Status sind eine historische Momentaufnahme; aktuelle Werte findest du auf der Live-Seite. Ausstehend oder bestätigt beschreibt den Bestätigungszustand eines Blocks. Das ändert nicht die Bedeutung des erfassten Aufwands und ist von einer Zahlung an deine Adresse getrennt.

Auf dem Dashboard ist Durchschnittliches Glück das arithmetische Mittel der erfassten Aufwandsprozente aller nicht verwaisten Blöcke bis zur aktuellen Chain-Höhe. Trotz des Kartennamens gilt die Aufwandskonvention: Niedriger bedeutet weniger erfasste Arbeit pro Block. Mittelwerte fassen diese Stichprobe zusammen. Sie versprechen nicht, dass zukünftige Runden ein bestimmtes früheres Ergebnis ausgleichen.

## Wie beeinflusst Pool-Glück deine Belohnungen?

Pool-Glück verändert den Zeitpunkt von Blockfunden. PPLNS entscheidet, wie die Belohnung eines gefundenen Blocks auf berechtigte akzeptierte Arbeit verteilt wird. Das sind verschiedene Schritte. Eine Rundengrenze macht das PPLNS-Fenster nicht zu einer ausschließlich rundenbezogenen Auszahlungsregel. Lies [wie PPLNS-Auszahlungen funktionieren](/blog/de/how-pplns-payouts-work/) für das tatsächliche Fenster, Bestätigungen und die Auszahlungsschwelle.

Dein Guthaben kann sich daher schrittweise ändern, während dein Miner weiter akzeptierte Shares einreicht. Der [Belohnungsrechner](/blog/de/estimate-blocknet-mining-rewards/#mining-calculator) schätzt Erträge anhand von Hashrate und Netzwerkdaten. Er hilft bei Erwartungen über längere Zeiträume; er sagt weder den Block noch den Zeitpunkt einer Zahlung an deine Wallet voraus.

## Was solltest du während einer langen Runde prüfen?

Prüfe direkt beobachtbare Dinge: Dein Miner läuft, Shares werden weiter akzeptiert und unter [Meine Statistiken](/stats) erscheint die richtige Adresse. [Status](/status) zeigt den öffentlichen Dienstzustand. Wenn deine akzeptierten Shares ausbleiben, nutze die [Fehlersuche für Miner-Statistiken](/blog/de/track-your-miner-on-bntpool/#zero-hashrate). Eine lange Pool-Runde und ein getrenntes Rig erfordern unterschiedliche Prüfungen.

Ist deine Verbindung in Ordnung, beurteile Glück anhand eines aussagekräftigen Verlaufs und derselben Definitionen. So hat jede Dashboard-Zahl eine klare Rolle: Hashrate schätzt Arbeit pro Sekunde, Aufwand vergleicht Arbeit mit der für einen Block erwarteten Arbeit und Guthaben zeigen die Belohnungsabrechnung. Dafür musst du nicht erraten, wann der nächste erfolgreiche Hash kommt.

<CTA target="pool" title="Starte Mining auf bntpool.com" text="Verbinde seine, prüfe deine akzeptierten Shares und verfolge Runden und Belohnungen im Pool-Dashboard." />
