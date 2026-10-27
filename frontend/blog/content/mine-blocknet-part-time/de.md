---
title: 'Kann man Blocknet nur zeitweise minen?'
description: 'Blocknet in kurzen Sitzungen minen: Shares im sechsstündigen PPLNS-Fenster verfolgen, mit derselben Adresse zurückkehren und offene Guthaben prüfen.'
tags: ['Mining-Anleitung', 'PPLNS', 'Zeitplanung']
faq:
  - q: 'Muss ich sechs Stunden minen, bevor Shares zählen?'
    a: 'Nein. Geeignete Shares können ohne sechsstündige Wartezeit am Belohnungsfenster eines Blocks teilnehmen. Dein Anteil ergibt sich aus der nach Schwierigkeit gewichteten Arbeit relativ zur gesamten Pool-Arbeit im Fenster. Eine Blockbelohnung setzt einen gefundenen Block voraus.'
  - q: 'Muss der Miner für eine bestehende Auszahlung verbunden bleiben?'
    a: 'Eine aktive Mining-Verbindung ist keine Auszahlungsvoraussetzung. Bestätigte Guthaben werden nach den Pool-Regeln einschließlich der aktuellen Mindestgrenze berücksichtigt. Eine Trennung löscht das Guthaben nicht und fordert auch nicht automatisch eine sofortige Zahlung an.'
  - q: 'Erneuert eine Verbindung das Alter bisheriger Shares?'
    a: 'Nein. Bestehende Shares behalten ihre Zeitstempel. Neue Arbeit fließt nach deiner Rückkehr in die passenden Fenster ein; erneutes Verbinden verlängert alte Shares nicht und erzeugt keine Arbeit für Offline-Stunden. Prüfe nach dem Start neue akzeptierte Shares.'
sourceHash: '0a11c393062e'
translation: draft
---

Du kannst einen Blocknet-Miner zu ausgewählten Zeiten betreiben und später zurückkehren. Unterscheide aktuelle Verbindung, bereits eingereichte Shares und zugeteilte Belohnungen. Diese Anleitung folgt einer kurzen Sitzung, damit du planbar stoppst, neu startest und dein bntpool.com-Konto prüfst.

## Kann eine kurze Sitzung Pool-Belohnungen erhalten?

<Answer>
Ja. Geeignete Arbeit einer kurzen Sitzung kann an Blöcken teilnehmen, deren PPLNS-Fenster diese Shares enthalten. Sechs Stunden musst du nicht erst absolvieren. Belohnungen hängen von gefundenen Blöcken und deinem gewichteten Fensteranteil ab. Nach dem Stoppen kommt keine neue Arbeit hinzu; Shares altern normal und Guthaben folgen den Auszahlungsregeln.
</Answer>

*Eine Zeitleiste zeigt Mining, Pause und Rückkehr mit getrennten Markierungen für Share-Berechtigung und bestehendes Guthaben.*

## Die verschiedenen Zeitabläufe unterscheiden

bntpool veröffentlicht derzeit sechs Stunden PPLNS-Fenster, 60 Blöcke bis zur Auszahlungsreife und 0,1 BNT Mindestauszahlung auf der [Startseite für Miner](/start). Das sind berücksichtigte Arbeit, Blockreife und Guthabenschwelle. Das sechsstündige Fenster ist kein Timer, der eine Zahlung nach sechs Stunden verspricht.

Die [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/) beschreibt die Aufteilung. Geeignete Shares werden nach Schwierigkeit gegen alle geeignete Arbeit im Fenster gewichtet. Online-Minuten allein bestimmen keinen festen Prozentsatz der nächsten Belohnung. Geräte, Pool-Beteiligung und Share-Ergebnisse können bei gleicher Sitzungsdauer unterschiedliche Gewichte erzeugen.

## Einen Share durch die Pause verfolgen

Beispiel in Ortszeit: Du minest von 18:00 bis 20:00; ein geeigneter Share wird um 19:00 erfasst. Ein Block mit Fensterende 22:00 schaut sechs Stunden bis 16:00 zurück. Der Share liegt darin, obwohl du bereits offline bist. Das erklärt die Berechtigung, sagt aber keinen Blockfund zu dieser Uhrzeit voraus.

Ein Fenster bis 02:00 am Folgetag beginnt um 20:00; der Share von 19:00 liegt außerhalb. Erneutes Verbinden um 08:00 ändert seinen Zeitstempel nicht. Frische Arbeit zählt anschließend für spätere Fenster. Die [Glück-Anleitung](/blog/de/mining-pool-luck-explained/) erklärt wechselnde Blockabstände; plane nicht mit einem vermeintlich fälligen Block.

## Mit einem nachvollziehbaren Stand stoppen

Notiere vor der Pause Empfangsadresse, Rig-Namen, ungefähre Stoppzeit und letzte Meldungen akzeptierter Shares. Prüfe das Konto anhand der [Tracking-Anleitung](/blog/de/track-your-miner-on-bntpool/). Damit kannst du eine gestoppte Maschine von einer anderen Adresse oder noch nicht aktualisierten Anzeige unterscheiden.

Beende seine regulär mit `q` im Dashboard oder `Ctrl+C` im Terminal. Bewahre Startbefehl und Konfiguration auf. Die [Desktop-Anleitung](https://seineminer.com/de/blog/mine-while-using-pc/) erklärt die Abstimmung mit anderen Aufgaben. Zum sauberen Stoppen musst du weder Einstellungen löschen noch Adresse wechseln oder ein Pool-Konto schließen lassen.

## Was passiert offline mit unbezahltem Guthaben?

Verlässt ein Share das Fenster eines späteren Blocks, löscht das keine bereits einem früheren Block zugeteilte Belohnung. Geschätzte Belohnungen folgen weiter Blockstatus und Reife; bestätigte unbezahlte Beträge bleiben bis zur Verarbeitung im Konto. Die [Bestätigungsanleitung](/blog/de/block-confirmations-mining-rewards/) trennt vorläufige Gutschrift und reifes Guthaben.

Ein bestätigtes Guthaben unter 0,1 BNT bleibt unter der aktuellen automatischen Schwelle, bis weitere Gutschriften das Minimum erreichen. Verbinden allein erhöht es nicht und erzwingt keine Zahlung. Umgekehrt braucht ein berechtigtes Guthaben keinen online bleibenden Miner während der Verarbeitung. Die [Auszahlungshistorie](/payouts) trennt offene Beträge von gesendeten Transaktionen.

## Zum selben Konto zurückkehren und neue Arbeit prüfen

Starte den gespeicherten Befehl und prüfe Pool, Empfangsadresse und Rig-Namen vor dem Statistikvergleich. Die Adresse identifiziert das beobachtete Konto. Bei bewusstem Zielwechsel hilft die [Adresswechsel-Anleitung](/blog/de/change-mining-wallet-address/); das Guthaben des alten Kontos wird nicht unbemerkt ins neue übertragen.

Achte auf einen aktuellen Job und neue akzeptierte Shares, dann auf die Aktualisierung des Pool-Messfensters. Eine neue Sitzungsdurchschnittsrate kann von der rückblickenden Pool-Schätzung abweichen. Die [Hashrate-Anleitung](/blog/de/what-is-hashrate/) erklärt die Zeitfenster. Prüfe tatsächliche Arbeit und richtiges Konto, bevor du eine kurze Differenz als fehlende Belohnung deutest.

## Sitzungen nach deinem Alltag planen

Wähle verfügbare Stunden nach Computernutzung und Strombudget. Die [Stromanleitung](https://seineminer.com/de/blog/mining-electricity-use/) berechnet Energie für deinen tatsächlichen Zeitplan, bei Bedarf mit mehreren Tarifabschnitten. Kürzere Sitzungen reduzieren Laufzeit; der genaue Energiebedarf hängt weiterhin von gemessener Hardware und Last ab.

Protokolliere Starts, Stopps, akzeptierte Arbeit und Guthaben über mehrere Sitzungen. Der [Ertragsrechner](/blog/de/estimate-blocknet-mining-rewards/) liefert einen Erwartungswert unter seinen Annahmen, keine Zusage für einen Abend. Betrachte einen repräsentativen Zeitraum und trenne Verbindung, Share-Fenster, Blockreife und gesendete Zahlungen.

<CTA target="pool" title="Sitzungen auf bntpool.com verfolgen" text="Kehre mit derselben Empfangsadresse zurück, prüfe neue Shares und verfolge die regulären Auszahlungsschritte." />
