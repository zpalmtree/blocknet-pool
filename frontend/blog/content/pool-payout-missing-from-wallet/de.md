---
title: 'Pool-Auszahlung gesendet, aber nicht in der Wallet?'
description: 'Verfolge eine Blocknet-Auszahlung über Transaktion und Empfangsadresse bis zum Wallet-Scan und sammle die Angaben zur Klärung eines fehlenden Eintrags.'
tags: ['Auszahlungen', 'Wallet', 'Fehlerbehebung']
faq:
  - q: 'Bedeutet eine vorgemerkte Auszahlung bereits Wallet-Empfang?'
    a: 'Eine Auszahlung in der Warteschlange ist ein Verarbeitungszustand des Pools. Prüfe zunächst, ob der Datensatz eine Transaktionskennung enthält und welchen Status er aktuell hat. Auszahlungseintrag, Aufnahme der Transaktion in die Chain und Erkennung ihres Outputs durch die Wallet sind getrennte Schritte. Folge der konkreten Transaktion, sobald sie verfügbar ist.'
  - q: 'Kann der Explorer meinen privaten Wallet-Saldo anzeigen?'
    a: 'Der Explorer zeigt den öffentlichen Chain-Status einer Transaktion. Blocknet verwendet private Outputs. Eine gewöhnliche Adresssuche zeigt deshalb weder deinen Wallet-Saldo noch beweist sie, dass ein bestimmter Output dir gehört. Deine Wallet scannt die Chain, um ihre eigenen Outputs zu erkennen.'
  - q: 'Welche Angaben helfen bei einer fehlenden Auszahlung?'
    a: 'Nenne vollständige Empfangsadresse der Pool-Abfrage, Transaktionskennung, ungefähre Zeit, Pool-Status und Synchronisierungsfortschritt. Beschreibe, ob die Transaktion in der Wallet-Historie steht. Teile Kontodaten nur im passenden Support-Kontext und niemals Seed-Phrase, private Schlüssel oder Wallet-Passwort.'
sourceHash: '915075cf4e2b'
translation: draft
---

Verfolge eine Blocknet-Auszahlung vom Pool-Eintrag bis zur vorgesehenen Wallet. Diese Anleitung beginnt mit einem erkennbaren Auszahlungseintrag auf bntpool.com. Siehst du nur geschätzte oder bestätigte Mining-Belohnungen vor der Auszahlung, beginne mit der [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/). Das ist eine frühere Phase mit eigenen Abrechnungs- und Auszahlungsregeln.

## Wie verfolgst du eine in der Wallet fehlende Auszahlung?

<Answer>
Suche den Auszahlungseintrag zur vollständigen Empfangsadresse, notiere Transaktionskennung und aktuellen Status und prüfe die Transaktion auf der Chain. Öffne die Wallet für dasselbe Ziel und lass Synchronisierung und Scan aufholen. Vergleiche die Historie, bevor du entscheidest, ob die Zahlung oder die lokale Wallet-Ansicht weitere Prüfung braucht.
</Answer>

*Ein Pool-Auszahlungseintrag führt zu einer Chain-Transaktion und dann zur Wallet, die ihre eingehenden Outputs scannt.*

## Beginne mit einem konkreten Auszahlungseintrag

Öffne My Stats mit der vollständigen Adresse, unter der die Belohnungen erarbeitet wurden. Notiere beim passenden Eintrag Betrag, Zeit, Status und vorhandene Transaktionskennung. Untersuche diesen Eintrag statt der lebenslangen Auszahlungssumme, die mehrere Zahlungen enthalten kann. Die [Tracking-Anleitung](/blog/de/track-your-miner-on-bntpool/) zeigt die Felder.

Die öffentliche Payouts-Seite beschreibt Auszahlungsbatches mit möglicherweise mehreren Empfängern und Transaktionslinks. Halte den Eintrag deiner Adresse neben der untersuchten Transaktion bereit. Der Batch-Gesamtbetrag ist nicht der deiner Wallet zugewiesene Betrag. Klappe mehrere Transaktionskennungen aus und bewahre die relevanten vollständigen Kennungen statt nur eines gekürzten Labels.

## Ist die Auszahlung vorgemerkt, unbestätigt oder bestätigt?

Lies den aktuellen Status, bevor du alle Schritte als abgeschlossen betrachtest. Ein Warteschlangeneintrag ist noch ein Pool-Verarbeitungszustand; eine unbestätigte Transaktion braucht Chain-Bestätigung. Aktualisiere den Eintrag und öffne den Transaktionslink, sobald vorhanden. Die [Statusseite](/status) liefert Betriebskontext, wenn mehrere Beobachtungen auf eine Unterbrechung hindeuten.

Trenne Reife der Mining-Belohnung und Bestätigung der Auszahlungstransaktion. Der belohnende Block und die spätere Zahlung an deine Wallet haben unterschiedliche Kennungen und Aufgaben. Die [Bestätigungsanleitung](/blog/de/block-confirmations-mining-rewards/) erklärt dies. Eine Transaktionskennung ist ein konkreter Prüfpunkt, beweist aber nicht, dass deine lokale Wallet die Zahlung bereits gescannt und erkannt hat.

## Prüfe die Transaktion auf der Chain

Öffne den Explorer-Link der Auszahlung und vergleiche die vollständige Kennung mit deinen Notizen. Lies den Chain-Status und verfügbare Blockinformationen. Findet der Explorer sie noch nicht, vergleiche aktualisierten Pool-Eintrag und Dienststatus, bevor du urteilst. Ein vorübergehendes Suchergebnis allein benennt keine Ursache.

Durch Blocknets Privatsphäre zeigt eine normale Explorer-Adresssuche keinen öffentlichen Empfängersaldo. Transaktionsstatus und Eigentumsprüfung der Wallet beantworten verschiedene Fragen. Die Wallet erkennt eingehende Outputs beim Chain-Scan. Die [Stealth-Adressen-Anleitung](https://seineminer.com/de/blog/stealth-addresses-explained/) erläutert das Verfahren; eine gewöhnliche öffentliche Adressliste bildet die private Wallet-Historie nicht nach.

## Ist die richtige Empfangs-Wallet geöffnet?

Vergleiche Pool-Abfrageadresse und Empfangsadresse der geöffneten Wallet vollständig. Ähnliche gekürzte Präfixe reichen nicht. Nutzt du eine Blocknet ID, bewahre die vollständige Adresse der ursprünglichen Arbeit. Eine aktuelle Namensabfrage kann nach einer Zuordnungsänderung woandershin zeigen, während eine frühere Auszahlung ihrem ursprünglichen Ziel zugeordnet bleibt.

Prüfe eine ältere Wallet, die du vor einer Änderung der Mining-Einstellungen genutzt hast. Änderungen an Miner-Adresse oder Namenszuordnung verschieben keine bereits zugewiesene Zahlung. Die [Adresswechsel-Anleitung](/blog/de/change-mining-wallet-address/) erklärt beide Historien. Bewahre ursprüngliche Wallet und Backup und unterscheide das Öffnen einer vorhandenen Wallet vom Erstellen einer neuen mit anderer Adresse.

## Lass Wallet-Synchronisierung und Scan aufholen

Prüfe, ob der Wallet-Node zur aktuellen Chain aufholt und der Wallet-Scan den Block der Auszahlungstransaktion erreicht hat. Node-Synchronisierung und Wallet-Scan sind getrennte Fortschritte. Eine gerade geöffnete oder wiederhergestellte Wallet braucht unter Umständen Zeit zur Output-Erkennung, obwohl der Explorer die Transaktion bereits in einem Block zeigt.

Lass die Wallet verbunden und folge ihrem dokumentierten Synchronisierungsablauf. Vergleiche eingehende Historie und Gesamtsaldo, da spätere Ausgaben den Saldo verändern können. Bei vorhandenem Eintrag prüfe Bestätigung oder Verfügbarkeit. Beginne damit vor Wiederherstellung oder vollständigem Rescan und sichere Backups vor Wartung. Die [Wallet-Anleitung](https://seineminer.com/de/blog/blocknet-wallet-for-mining/) behandelt Zugriff und Sicherungen.

## Welche Angaben klären eine verbleibende Abweichung?

Notiere vollständige Pool-Adresse, Transaktionskennung, ungefähre Zeit und aktuellen Pool-Status. Ergänze Wallet-Version, Node- und Scan-Fortschritt und ob die eingehende Transaktion in der Historie steht. Beschreibe durchgeführte Prüfungen. So lassen sich Transaktionsstatus, falsche Adresse und eine noch aufholende Wallet unterscheiden.

Nutze den offiziellen Supportweg des Projekts und teile Kontodaten nur im passenden Kontext. Die Untersuchung benötigt weder Wiederherstellungsphrase noch private Schlüssel oder Passwort. Prüfe Diagnose-Dateien vor Veröffentlichung. Ordne abschließend denselben Auszahlungseintrag, dieselbe Transaktion und Empfangs-Wallet zu; eine aktuelle Hashrate oder ein weiterer Miner-Neustart bestätigt keine frühere Zahlung.

<CTA target="pool" title="Auszahlungen auf bntpool.com verfolgen" text="Finde den Eintrag mit deiner vollständigen Empfangsadresse und vergleiche seine Transaktion mit der synchronisierten Wallet-Historie." />
