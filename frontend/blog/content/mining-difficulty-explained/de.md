---
title: 'Mining-Schwierigkeit: Netzwerk und Shares'
description: 'Verstehe das Mining-Ziel, Blocknets Schwierigkeitsanpassung und warum Netzwerk- und Share-Schwierigkeit unterschiedliche Kennzahlen beim Mining beeinflussen.'
tags: ['Mining-Grundlagen', 'Schwierigkeit', 'Blocknet']
faq:
  - q: 'Senkt höhere Schwierigkeit meine Hashrate?'
    a: 'Die Schwierigkeit verändert das Ziel eines Ergebnisses, nicht direkt die pro Sekunde berechneten Hashes. Bei gleicher Hashrate verlängert höhere Netzwerkschwierigkeit den erwarteten Abstand zwischen Blockfunden. Prüfe die Leistung des Miners getrennt von Änderungen der Block- oder Share-Häufigkeit.'
  - q: 'Bringt eine schwierigere Share mehr Ertrag?'
    a: 'Sie repräsentiert mehr erwartete Hasharbeit, wird bei gleicher Hashrate aber seltener eingereicht. Vergleiche schwierigkeitsgewichtete Arbeit über denselben Zeitraum. Auf bntpool.com hängen tatsächliche Belohnungen auch von akzeptierten Blöcken und PPLNS ab, nicht allein von der Anzahl eingereichter Shares.'
  - q: 'Wie oft passt Blocknet die Schwierigkeit an?'
    a: 'Blocknet berechnet nach jedem Block die Schwierigkeit des nächsten aus der jüngsten Kettenhistorie. Die aktuelle Regel nutzt einen linear gewichteten gleitenden Durchschnitt über 60 Zeitintervalle und strebt 300 Sekunden pro Block an. Neuere Intervalle wiegen stärker; einzelne Blöcke können früher oder später eintreffen.'
sourceHash: '5c203e8c7523'
translation: draft
---

Mining-Schwierigkeit verbindet die Rechenarbeit deiner Hardware mit den vom Netzwerk akzeptierten Blöcken. Diese Anleitung erklärt Zielwert, Blocknets Anpassung und die separate Share-Schwierigkeit. Verstehe Änderungen dieses Werts, bevor du Hashrate, erwartete Blockzeit oder den [Ertragsrechner](/blog/de/estimate-blocknet-mining-rewards/#mining-calculator) vergleichst.

## Was bedeutet Mining-Schwierigkeit?

<Answer>
Schwierigkeit beschreibt die erwartete Hasharbeit für ein Ergebnis, das einen Zielwert erfüllt. Höhere Schwierigkeit macht solche Ergebnisse bei gleicher Hashrate seltener. Bei Blocknet entspricht die Netzwerkschwierigkeit ungefähr den erwarteten Hashes pro Block; die Share-Schwierigkeit beschreibt ein separates Ziel für Einreichungen beim Pool.
</Answer>

Jeder Hash erzeugt eine Zahl, die den Zielschwellenwert erfüllen muss. Ein kleineres Ziel akzeptiert einen kleineren Anteil möglicher Ergebnisse und hat höhere Schwierigkeit. Blocknet teilt den größten 256-Bit-Wert durch die Schwierigkeit, um das 256-Bit-Ziel zu erhalten. Eine Verdoppelung verdoppelt daher ungefähr die erwartete Arbeit, ohne die Hashoperation zu verändern.

## Wie passt Blocknet die Netzwerkschwierigkeit an?

Nach jedem Block wird die nächste Schwierigkeit berechnet. Die aktuelle Mainnet-Regel nutzt einen linear gewichteten gleitenden Durchschnitt, LWMA, über 60 jüngste Zeitintervalle. Neuere Intervalle erhalten mehr Gewicht. Das Ziel beträgt 300 Sekunden pro Block. Aus vergangenen Schwierigkeiten und gewichteten Zeiten folgt: Anhaltend schnelle Blockfolgen erhöhen die Schwierigkeit tendenziell, langsame senken sie.

Die Implementierung begrenzt die verwendeten Zeitintervalle und setzt eine Mindestschwierigkeit. Für 60 Intervalle sind 61 Zeitstempel nötig; die Anpassung erfolgt also nicht erst alle 60 Blöcke. Die Regeln halten die Kette bei wechselnder Rechenleistung nahe am Zieltempo. Fünf Minuten sind ein Durchschnitt; einzelne Blöcke können deutlich früher oder später eintreffen.

## Netzwerkschwierigkeit und Share-Schwierigkeit

Ein Pool vergibt ein Share-Ziel, um die beigetragene Arbeit zwischen Blockfunden zu erfassen. Dieses ist gewöhnlich leichter zu erfüllen als das Netzwerkziel. Beide Zahlen beschreiben erwartete Arbeit, beantworten aber unterschiedliche Fragen und werden von verschiedenen Teilen des Systems festgelegt:

| Schwierigkeit | Was sie steuert |
|---|---|
| Netzwerk | Die Arbeitsschwelle für einen Blockkandidaten gemäß den Konsensregeln. |
| Share | Die vom Pool einer Verbindung oder Mining-Aufgabe zugewiesene Einreichungsschwelle. |

bntpool.com passt mit variabler Schwierigkeit, vardiff, die Einreichungshäufigkeit an. Bei gleicher Hashrate entstehen mit höherer Share-Schwierigkeit meist weniger Shares, die jeweils mehr erwartete Arbeit darstellen. Das Netzwerkziel ändert sich dadurch nicht. Erfüllt ein Ergebnis auch dieses Ziel, kann es ein Blockkandidat werden; der Daemon validiert den Block weiterhin. Die [Share-Anleitung](/blog/de/accepted-rejected-stale-shares/) erklärt Annahmen und Ablehnungen.

## Wie beeinflusst Schwierigkeit die erwartete Blockzeit?

Teile bei Blocknet die Netzwerkschwierigkeit durch die arbeitende Hashrate, um den mittleren Abstand in Sekunden zu schätzen. Nutze H/s und Werte desselben Zeitraums. Das folgende Rechenbeispiel hält Schwierigkeit und Hashrate konstant. Es zeigt illustrative Zahlen, keine aktuellen Netzwerkmessungen oder Vorhersage für einen bestimmten Miner.

**Erwartete Sekunden pro Block ≈ Netzwerkschwierigkeit ÷ Hashrate in H/s**

| Netzwerkschwierigkeit | Hashrate | Erwarteter Abstand |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Bei Schwierigkeit 30.000 und 100 H/s beträgt der Mittelwert 300 Sekunden. Bei 60.000 und gleicher Hashrate sind es 600 Sekunden. Verdoppelt sich auch die Hashrate auf 200 H/s, sind es wieder 300 Sekunden. Das sind Mittelwerte wiederholter Versuche. Die [Pool-Luck-Anleitung](/blog/de/mining-pool-luck-explained/) erklärt die Abweichungen tatsächlicher Runden.

## Warum ändern sich Erträge bei gleichbleibender Hashrate?

Bei unveränderter eigener Hashrate verringert höhere Netzwerkschwierigkeit die erwarteten Blockfunde pro Zeiteinheit, solange sie gilt. Steigt die gesamte Hashrate und folgt die Schwierigkeit, wird dein Anteil an der Netzwerkarbeit kleiner. Die Blockbelohnung ist eine weitere Größe: Der [Emissionsplan](/blog/de/blocknet-block-reward-schedule/) ändert sie unabhängig. Schwierigkeit bestimmt nicht die BNT-Menge der vorgesehenen Blockbelohnung.

Nutze aktuelle Netzwerkdaten im Rechner für erwartete BNT-Mengen. Prüfe, ob die eingegebene Hashrate vom Miner oder Pool stammt, damit die Minergebühr richtig berücksichtigt wird; verwende den Satz deiner Version. Schätzungen beschreiben Mittelwerte unter bestimmten Annahmen. Zahlungen hängen außerdem von gefundenen Blöcken, Bestätigungen und [PPLNS](/blog/de/how-pplns-payouts-work/) ab. Die Schwierigkeit allein sagt den Auszahlungstermin nicht voraus.

## Welche Schwierigkeit solltest du prüfen?

Beginne mit deiner Frage. Für Blockfunde nutze Netzwerkschwierigkeit und Hashrate des betrachteten Miners oder Pools. Für die Share-Häufigkeit nutze die Schwierigkeit der jeweiligen Aufgaben. Für Guthaben oder Zahlungen öffne [My Stats](/stats) und die [Auszahlungshistorie](/payouts). Halte Zeitstempel, Bezugsbereich und Einheiten neben jedem Vergleichswert fest.

Das [Pool-Dashboard](/) und die [Blockhistorie](/blocks) liefern aktuellen Kontext. Die Netzwerkhashrate dort wird aus jüngster Arbeit und vergangener Blockzeit geschätzt und muss daher nicht der nächsten Schwierigkeit geteilt durch 300 Sekunden entsprechen. Die [Hashrate-Anleitung](/blog/de/what-is-hashrate/) erklärt den Unterschied. Trenne Schwierigkeit, Hashrate und Glück bei der Diagnose: Sie beschreiben verschiedene Aspekte.

<CTA target="pool" title="Miner mit bntpool.com verbinden" text="Nutze die Einrichtungsseite und verfolge danach Shares und Mining-Aktivität in My Stats." />
