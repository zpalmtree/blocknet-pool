---
title: 'Pool-Gebühren und Miner-Entwicklergebühren'
description: 'Verstehe Pool- und Miner-Gebühren beim Blocknet-Mining: wo sie anfallen, wie du sie kombinierst und wie du denselben Abzug nicht doppelt berechnest.'
tags: ['Mining-Anleitung', 'Pool-Gebühren', 'Mining-Erträge']
faq:
  - q: 'Sind beide Gebühren dieselbe Belastung?'
    a: 'Nein. Die Pool-Gebühr gehört zur Ertragsverteilung, die Entwicklergebühr zur Mining-Software. seine v0.2.36 verwendet nach der Startfreiphase 2% der gebührenpflichtigen Mining-Zeit für Entwicklung. Die separat veröffentlichte Gebühr von bntpool.com lag am 26. Oktober 2026 bei 0%.'
  - q: 'Wie kombiniert man beide Gebühren?'
    a: 'Multipliziere eine Bruttoschätzung mit eins minus Miner-Gebührenanteil und danach mit eins minus Pool-Gebührenanteil. Nutze deine tatsächliche Version und die aktuelle Pool-Einstellung. Das schätzt verbleibende Erträge, garantiert aber weder eine Auszahlung noch einen Geldgewinn.'
  - q: 'Miner-Gebühr von der Pool-Hashrate abziehen?'
    a: 'Prüfe zuerst, was die Rate bedeutet. Schätzt sie bereits deiner Adresse gutgeschriebene Arbeit nach Entwicklerzeit, wäre ein weiterer Abzug doppelt. Im Rechner kannst du dafür null eingeben. Von einem bestätigten Guthaben oder aufgezeichneten Zahlungsvorgang wird keine der Gebühren erneut abgezogen.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Ordne die beiden Gebühren einer Blocknet-Schätzung ihren jeweiligen Stufen zu. Diese Anleitung nutzt seine v0.2.36 und öffentliche bntpool-Einstellungen vom 26. Oktober 2026. Sie hilft bei Eingaben im [Mining-Rechner](/blog/de/estimate-blocknet-mining-rewards/#mining-calculator) und beim Lesen von [My Stats](/stats).

## Was unterscheidet Pool- und Miner-Gebühren?

<Answer>
Die Pool-Gebühr gehört zur Ertragsverteilung, die Entwicklergebühr zum Zeitplan der Software. Zuständigkeit und Stufe unterscheiden sich. Prüfe beide Einstellungen, bestimme ob die Eingabe brutto oder schon angepasst ist und wende jede relevante Anpassung einmal an.
</Answer>

*Auf die Mining-Arbeit folgen eine Miner-Zeitanpassung und eine getrennte Pool-Ertragsanpassung vor dem geschätzten verbleibenden Kontobetrag.*

## Worauf wird die Pool-Gebühr angewandt?

Der Pool sammelt berechtigte Arbeit und verteilt den verfügbaren Ertrag nach seinen Regeln. bntpool berechnet die konfigurierte Gebühr aus dem aufgezeichneten Blockertrag und verteilt den Rest per PPLNS. Es ist ein Verteilungsschritt. Die [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/) erklärt Teilnahme und Gutschriften.

Am 26. Oktober 2026 veröffentlicht `https://bntpool.com/api/info` für `pool_fee_pct` den Wert 0,0, also 0%. Lies den aktuellen Wert unter [Get Started](/start) oder der API bei jeder neuen Schätzung. Null beschreibt diese Pool-Einstellung; die Softwarepolitik bleibt eine eigene Eingabe.

## Wie funktioniert seines Entwicklergebühr?

v0.2.36 dokumentiert pauschal 2% auf jedem Pool und im Solomodus. Die Software plant Mining-Zeit für die Entwickleradresse ein, statt später Geld aus deiner Wallet abzuziehen. Die ersten 10 Minuten sind gebührenfrei; der Tracker zählt danach. Die [Konfigurationsanleitung](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) beschreibt dies.

Die Planung erfolgt in Runden, daher muss ein kurzer Ausschnitt nicht genau 2% ergeben. Der Wert ist das langfristige Ziel nach der Freiphase, keine Zusage pro Minute oder Block. Notiere die Version: ältere Anleitungen oder andere Programme können anders arbeiten. Die Pool-Gebühr ersetzt diese Softwareinformation nicht.

## Wie kombiniert man Prozentsätze?

Rechne mit verbleibenden Anteilen. Sind m und p Miner- und Pool-Gebühr als Bruchteile, bleiben `(1 - m) * (1 - p)` des Bruttos. So arbeitet der [Ertragsrechner](/blog/de/estimate-blocknet-mining-rewards/). Einfaches Addieren ist keine allgemeine Regel, da die zweite Anpassung auf den Rest wirkt.

Reines Rechenbeispiel: hypothetisch 100 BNT brutto, 2% Miner-Anpassung und die geprüften 0% Pool-Gebühr ergeben 100 × 0,98 × 1,00 = 98 BNT. Das zeigt nur die langfristige Anpassung, kein Hardwareergebnis, versprochenen Ertrag oder kurze Sitzung samt Startfreiphase.

## Die richtige Hashrate wählen

Eine lokale Miner- oder Benchmarkrate beschreibt Arbeit auf dem Rechner. Kläre vor dem Entwicklerfaktor, ob sie gesamte Mining-Zeit oder bereits nur Arbeit für deine Adresse abbildet. Prüfe Herkunft und Zeitfenster. Die [Hashrate-Anleitung](/blog/de/what-is-hashrate/) erklärt lokale, Pool- und Netzwerkbeobachtungen.

Die Pool-Rate deiner Adresse kann Zeit mit anderen Gutschriften bereits berücksichtigen. Ist diese Anpassung enthalten, gib null als zusätzliche Miner-Gebühr ein. Das ändert die Rechnung, nicht die Softwarepolitik. Die Pool-Gebühr gilt weiterhin bei der Schätzung berechtigter Bruttoerträge; prüfe den Inhalt jeder Anzeige.

## Warum Gebühren nicht erneut vom Guthaben abziehen?

Bestätigtes Guthaben ist ein Buchungsergebnis, keine unberührte Bruttoschätzung. Der Pool hat Gutschriften bereits zugeteilt. Dieselbe Gebühr erneut von Guthaben oder Zahlung abzuziehen verändert nur die Zahl ohne weiteres Gebührenereignis. Verwende den angezeigten Betrag zum Abgleich und kläre vor neuer Schätzung die Stufe.

Ausstehender Ertrag, bestätigtes Guthaben und gesendete Zahlung unterscheiden sich auch zeitlich. Blockreife und Auszahlungsschwelle erklären den Übergang; sie sind keine zusätzlichen Prozentgebühren. Nutze die [Bestätigungsanleitung](/blog/de/block-confirmations-mining-rewards/) und den [Zahlungsverlauf](/payouts). Eine gegebenenfalls anfallende Wallet-Transaktionsgebühr ist ein eigenes Feld.

## Eingaben nachvollziehbar festhalten

Speichere Datum, Version, Gebühren, Herkunft der Hashrate und Messfenster. Der Rechner kombiniert aktuelle Pool-/Netzdaten mit deinen Eingaben. Aktualisiere die Daten beim erneuten Besuch. Echte Gutschriften folgen auch Shares, gefundenen Blöcken und PPLNS; Formel und spätere Buchung beantworten verschiedene Fragen.

Nutze [My Stats](/stats) und die [Tracking-Anleitung](/blog/de/track-your-miner-on-bntpool/) für Arbeit und Erträge. Vergleiche zuerst dieselbe Adresse, Zeit und Stufe. Der Rechner schätzt BNT; Strom, Geräte und Geldpreis liegen außerhalb dieser Rechnung. Zum Verstehen der Prozentsätze brauchst du keine neue Wallet-Adresse.

<CTA target="pool" title="Erträge auf bntpool.com verfolgen" text="Nutze den Rechner für Schätzungen und My Stats für Arbeit, Guthaben und Zahlungen." />
