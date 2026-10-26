---
title: 'Blockbestätigungen und unreife Mining-Belohnungen erklärt'
description: 'Verfolge einen Blocknet-Poolblock bis zur reifen Belohnung und Auszahlung. Erfahre, wie bntpool die Blocktiefe zählt und weshalb Blockzahlen entscheidend sind.'
tags: ['Mining-Belohnungen', 'Bestätigungen', 'Blocknet']
faq:
  - q: 'Wie viele Bestätigungen verlangt bntpool?'
    a: 'Bei der Prüfung am 26. Oktober 2026 veröffentlicht bntpool.com eine Anforderung von 60 Blöcken. Die Prüfung zieht die Fundhöhe von der aktuellen Höhe ab und vergleicht den Blockhash mit dem Node. Den aktuellen Wert findest du unter Get Started oder in der öffentlichen Pool-Info-API.'
  - q: 'Bedeuten 60 Blöcke genau fünf Stunden Wartezeit?'
    a: 'Nein. Sechzig Intervalle mit fünf Minuten Zielzeit ergeben geschätzte 300 Minuten oder fünf Stunden. Tatsächlich zählen neue Blöcke, deren Ankunftszeiten schwanken. Reife, Auszahlungsschwelle und spätere Zahlungstransaktion sind getrennte Schritte.'
  - q: 'Muss ich weiter minen, damit eine bestehende Belohnung reift?'
    a: 'Ein gefundener Block gewinnt Tiefe durch neue Netzwerkblöcke, unabhängig von deiner Verbindung. Dein Anspruch folgt den Abrechnungsregeln des Pools. Ein Stopp beschleunigt oder startet die Reifung nicht neu; verfolge den erfassten Betrag unter My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Verfolge einen gefundenen Blocknet-Block durch Bestätigung, Reifung und spätere Wallet-Zahlung. Diese Anleitung erklärt die Kettentiefe hinter den Belohnungskarten von bntpool.com. Die Aufteilung behandelt der [PPLNS-Leitfaden](/blog/de/how-pplns-payouts-work/); zum Einstieg dient die [Pool-Einrichtung](/blog/de/mine-blocknet-on-bntpool/).

## Was sind Bestätigungen und unreife Belohnungen?

<Answer>
Bestätigungen beschreiben die Position eines Blocks unter der aktuellen Kettenspitze. Eine unreife Belohnung gehört zu einem jungen Block, der die erforderliche Tiefe noch nicht erreicht hat. bntpool prüft Tiefe und Identität vor der Bestätigung; Guthabenberechtigung und spätere Zahlung folgen getrennt.
</Answer>

*Ein gefundener Block gewinnt durch weitere Blöcke Tiefe, erreicht die Reifegrenze und führt über ein berechtigtes Guthaben zu einer separaten Wallet-Zahlung.*

## Wie zählt bntpool die Blocktiefe?

Der Pool zieht die Höhe des gefundenen Blocks von der aktuellen Kettenhöhe ab. Am 26. Oktober 2026 betrug die veröffentlichte Anforderung 60 Blöcke. Nach dieser Konvention beginnt der Fundblock bei null. Andere Seiten zählen eventuell den enthaltenen Block als erste Bestätigung; vergleiche deshalb auch die Zählregel.

Als Rechenbeispiel hat ein Fund auf Höhe 70.000 bei Spitze 70.000 Tiefe null, bei 70.001 Tiefe eins und bei 70.060 Tiefe 60. Das sind Beispielhöhen, keine Live-Meldung. Zusätzlich vergleicht der Pool den Hash mit seinem Node. Die [Implementierung](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) dokumentiert beide Prüfungen.

## Warum sind fünf Stunden nur eine Schätzung?

Blocknet zielt auf fünf Minuten zwischen Netzwerkblöcken. Sechzig mal fünf ergibt 300 Minuten oder fünf Stunden. Das hilft bei der Planung, aber die Reifebedingung ist eine Blockzahl. Entscheidend ist der Kettenfortschritt ab der Fundhöhe bis zur nötigen Tiefe, kein fester Ablaufzeitpunkt.

Jeder neue Block der akzeptierten Kette erhöht die Tiefe; er muss nicht von bntpool stammen. Die Geschwindigkeit deines Miners erzeugt keine eigene Bestätigungsuhr. Ankunftszeiten schwanken, daher weicht die Dauer vom Zielwert ab. Der [Schwierigkeitsleitfaden](/blog/de/mining-difficulty-explained/) erklärt die Netzwerkanpassung, der [Glücksleitfaden](/blog/de/mining-pool-luck-explained/) die Rundenvariation.

## Unterscheide ausstehend, bestätigt und bezahlt

Ein neu erfasster Poolblock bleibt ausstehend, während seine Tiefe wächst. Bei berechtigter Arbeit kann dein geschätzter Anteil unter [My Stats](/stats) erscheinen. Die Blockbelohnung erzeugt neue Coins; die Coinbase-Reiferegel der Blocknet-Wallet verlangt 60 Blöcke Tiefe vor dem Ausgeben. Die entsprechende Pool-Wartezeit folgt diesem Prozess.

Nach erfolgreicher Tiefen- und Identitätsprüfung kann der Pool den Block bestätigen und berechtigte Belohnungen auf Guthaben verteilen. Nicht jede Adresse erhält im selben Moment eine Zahlung. Der Auszahlungsvorgang prüft bestätigte Guthaben und Sendebedingungen getrennt. Blockstatus, Guthaben und Zahlungshistorie beschreiben verschiedene Teile desselben Ablaufs.

## Warum prüft der Pool den Blockhash?

Eine Höhe bezeichnet eine Position, ein Hash einen bestimmten Block. Nodes können konkurrierende Blöcke kennen; die akzeptierte Kette legt die Belegung fest. Deshalb fragt der Pool den Node nach dem Block dieser Höhe und vergleicht den Hash. Tiefe allein beweist nicht, dass der ursprünglich erfasste Block weiterhin dazugehört.

Ein ausgeschiedener Block kann als verwaist markiert werden; seine vorläufige Belohnung zählt dann nicht als Belohnung der akzeptierten Kette. Der Pool behandelt diesen Status getrennt. Über den Hash-Link auf [Blocks](/blocks) prüfst du den konkreten Block. Zwei Einträge gleicher Höhe müssen nicht dasselbe Ereignis sein. Eine Schätzung bleibt an ihren Block gebunden.

## Wie unterscheidet sich die Auszahlung von der Blockbelohnung?

Die geminte Belohnung liegt zunächst im Coinbase-Output des Blocks. Eine Poolzahlung ist eine spätere Transaktion an deine Empfangsadresse. Zum Prüfdatum veröffentlicht bntpool mindestens 0,1 BNT für Auszahlungen. Blockreife und Guthabenschwelle sind unterschiedliche Bedingungen; Details zur Aufteilung und Auszahlung stehen im [PPLNS-Leitfaden](/blog/de/how-pplns-payouts-work/).

Nach dem Senden verfolgst du die eigene Transaktion sowie Eingang und Ausgebbarkeit in deiner Wallet. Ihre Bestätigungen gehören zur Zahlung, nicht zum ursprünglichen Poolblock. Während ein Block reift, musst du weder eine neue Adresse erstellen noch den Miner neu verbinden. Vergleiche die [Auszahlungshistorie](/payouts) und Wallet unter derselben vorgesehenen Adresse.

## Wie verfolgst du eine Belohnung vollständig?

Beginne unter [Blocks](/blocks) mit Höhe, Hash und Status. Öffne [My Stats](/stats) mit der Mining-Adresse und prüfe geschätzte und bestätigte Belohnungen. Sobald eine Zahlung existiert, folge ihrem Transaktionslink. So hat jede Beobachtung ihren Platz: Block, Kontoguthaben und Wallet-Zahlung.

Prüfe aktuelle Werte unter [Get Started](/start) oder `https://bntpool.com/api/info` und den [Poolstatus](/status). Notiere für Untersuchungen Adresse, Block- oder Transaktionskennung und Beobachtungszeit; teile niemals Wiederherstellungswörter. Der [Tracking-Leitfaden](/blog/de/track-your-miner-on-bntpool/) erklärt weitere Statistiken. Bestehende Blöcke gewinnen auch bei ausgeschaltetem Miner Tiefe; Berechtigung folgt erfasster Arbeit und Kettenzustand.

<CTA target="pool" title="Verfolge deine Mining-Belohnungen" text="Verbinde dich mit bntpool.com und beobachte geschätzte Belohnungen, bestätigte Guthaben und Zahlungen unter My Stats." />
