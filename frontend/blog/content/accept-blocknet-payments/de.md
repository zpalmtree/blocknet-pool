---
title: "Blocknet-Zahlungen für Waren und Dienste annehmen"
description: "Nimm BNT für Bestellungen an, ordne Zahlungseingänge in deiner Blocknet-Wallet zu und prüfe Bestätigungen, Teilzahlungen sowie Rückerstattungen Schritt für Schritt."
tags: [Blocknet, BNT]
faq:
  - q: "Woran erkenne ich, dass ein Kunde mit Blocknet bezahlt hat?"
    a: "Suche den Eingang in deiner synchronisierten Wallet, gleiche Betrag und Bestellreferenz ab und prüfe den Bestätigungsstand. Speichere die vollständige Transaktions-ID zur Bestellung. Ein Kunden-Screenshot oder Zahlungslink allein belegt keinen Eingang. Auch der Explorer zeigt nicht den privaten Betrag, der an deine Adresse gezahlt wurde."
  - q: "Was mache ich, wenn ein Kunde zu wenig BNT schickt?"
    a: "Lass die Bestellung teilweise bezahlt und berechne den Rest aus tatsächlich geprüften Eingängen. Bitte um die Differenz mit derselben Referenz und speichere beide Transaktions-IDs. Zähle jeden Eingang nur einmal. Bei vereinbarter Stornierung bestätigst du die Rückzahlungsadresse mit dem Kunden im bisherigen Bestellgespräch."
  - q: "Markiert ein Memo eine Bestellung automatisch als bezahlt?"
    a: "Nein. Das Memo hilft beim Zuordnen, kann aber vom Zahler geändert oder weggelassen werden. Prüfe Betrag und Transaktionsstatus selbst. Die Wallet erstellt Anforderungen und zeigt Eingänge; dieser manuelle Ablauf ergänzt weder eine automatische Kasse noch eine Bestellverwaltung."
sourceHash: '3309a069e5de'
translation: draft
---

Nimm Blocknet an, indem du einen klaren BNT-Betrag nennst, den Eingang in deiner Wallet prüfst und ihn zur Bestellung speicherst. Unser fiktiver Auftrag über **1.25 BNT** hat die Referenz **order-1042**. Gezeigt wird die veröffentlichte Desktop-Wallet v0.22.0 mit gekennzeichneten Demodaten.

## Wie nimmst du eine Blocknet-Zahlung an?

<Answer>
Vereinbare BNT-Betrag und Bestellreferenz, sende eine Anforderung für eine von dir kontrollierte Adresse und warte auf den Eingang in deiner synchronisierten Wallet. Gleiche Betrag und Referenz ab, prüfe Bestätigungen, speichere die Transaktions-ID und markiere die Bestellung gemäß der mit dem Kunden vereinbarten Bestätigungsregel als bezahlt.
</Answer>

Zum Erstellen oder Öffnen dient die [Desktop-Wallet-Anleitung](https://seineminer.com/de/blog/blocknet-desktop-wallet/). Bewahre die Wiederherstellungssicherung getrennt von Bestellungen auf. Für Kundenzahlungen brauchst du weder einen laufenden Miner noch einen Pool.

## Betrag und Bestellung festhalten

Notiere Referenz, BNT-Betrag, Empfangsadresse und Kaufgegenstand. Bei einem Preis in anderer Währung vereinbarst du vor der Zahlung den BNT-Betrag und die Gültigkeit des Angebots. Ein Zahlungslink enthält den angeforderten Betrag; er verwaltet keinen Wechselkurs und erzwingt keine Frist.

Fordere im Beispiel **1.25 BNT** mit dem Memo **order-1042** an. Eine Bestellnummer genügt; Lieferadresse und Kundendaten bleiben in deiner privaten Verwaltung. Ein [verschlüsseltes Memo](https://seineminer.com/de/blog/blocknet-encrypted-memos/) begleitet die Zahlung. Der Memotext in einem geteilten Link ist jedoch für dessen Empfänger lesbar.

Trage unter **Receive → Payment Request** Betrag und Memo ein und wähle **Generate Payment Link**. Prüfe Empfänger und Betrag vor dem Teilen. Die [Zahlungslink-Anleitung](https://seineminer.com/de/blog/blocknet-payment-links/) erklärt Formate und Namensempfänger. Speichere bei einer festen Rechnung die vollständige Empfangsadresse mit der Anforderung.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Veröffentlichte Blocknet-Wallet mit einer Anforderung über 1.25 BNT, Memo order-1042 und einer Demo-Adresse." loading="lazy" decoding="async"></a>
<figcaption>Echte englische Oberfläche mit Demodaten. Die Anforderung bewegt noch kein Geld. Öffne das Bild zum Vergrößern.</figcaption>
</figure>

## Eingang in deiner eigenen Wallet prüfen

Bitte den Kunden, die Referenz einzutragen und die Transaktions-ID aufzubewahren. Sobald Node und Wallet aufgeholt haben, suche den Eingang unter **History**. Vergleiche Betrag und Memo mit der Bestellung. Beide Angaben lassen sich beim Bezahlen ändern; die Anforderung belegt daher nicht den gesendeten Betrag.

Speichere vollständige ID und erhaltenen Betrag. Zähle dieselbe Transaktion nicht doppelt, wenn sie in Dashboard und History erscheint oder der Kunde die ID erneut schickt. Ordne konkrete Eingänge zu, statt die Veränderung des Gesamtguthabens zu verwenden: Darin können Mining-Auszahlungen, andere Kunden und eigene Transfers stecken.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History der Blocknet-Wallet zeigt einen Demo-Eingang von 1.25 BNT mit dem Memo order-1042." loading="lazy" decoding="async"></a>
<figcaption>Der Beispieleingang passt zu Betrag und Referenz. Prüfe seinen Status vor der Ausführung der Bestellung.</figcaption>
</figure>

Fehlt ein Eingang, vergleiche vollständige Zieladresse und ID mit dem Kunden und prüfe die Synchronisierung. Sein Sendebildschirm gibt Kontext; deine empfangende Wallet stellt den erhaltenen Betrag fest.

## Wann ist die Bestellung bezahlt?

Unterscheide unbestätigte Eingänge, in einen Block aufgenommene Transaktionen und ausgebbares Guthaben. Die veröffentlichte Wallet wartet bei gewöhnlichen Zahlungen zehn weitere Blöcke nach dem enthaltenden Block, bevor deren Outputs ausgebbar sind. Die [Anleitung zur Transferdauer](https://seineminer.com/de/blog/blocknet-transfer-time/) erklärt Zähler und variable Wartezeiten.

Teile deine Bestätigungsregel vor dem Bezahlen mit. Ein einfacher manueller Ablauf wartet bis zur Ausgebbarkeit des passenden Eingangs, bevor er die Bestellung erfüllt. Ein sichtbarer Eingang allein genügt dafür nicht. Versprich anhand des Wallet-Countdowns keine feste Lieferzeit: Blockabstände schwanken. [Wie Mining Zahlungen absichert](/blog/de/how-mining-secures-blocknet-payments/) erklärt die Bestätigungstiefe.

## Teilzahlungen, Mehrzahlungen und unklare Eingänge

| Geprüfter Eingang | Nächster Schritt |
|---|---|
| 1.00 BNT für den Auftrag über 1.25 BNT | Teilzahlung buchen und weitere 0.25 BNT anfordern. |
| Später weitere 0.25 BNT | Beide IDs speichern, insgesamt 1.25 BNT einmal zählen und beide Bestätigungsstände prüfen. |
| Mehr als 1.25 BNT | Rückzahlung oder Gutschrift vereinbaren und festhalten. |
| Richtiger Betrag ohne Memo | Vollständige ID und Ziel mit dem Kunden abgleichen, bevor du zuordnest. |
| Ausstehende Zahlung | Bestellung auf Bestätigung warten lassen und Status erneut prüfen. |

Lass eine Rückzahlungsadresse im bestehenden Bestellgespräch bestätigen. Leite sie nicht aus der öffentlichen Transaktion ab: Blocknet-Inputs liefern keine herkömmliche Absenderadresse. Prüfe Rückzahlungsbetrag, Ziel und Gebühr vor der neuen Sendefreigabe und speichere deren ID mit der ursprünglichen Zahlung.

## Nur benötigte Zahlungsnachweise teilen

Der Explorer hilft beim Prüfen der Blockaufnahme, zeigt aber nicht den privaten Betrag an deine Adresse. Bei strittigem Eingang kann der Absender anhand der [Zahlungsnachweis-Anleitung](https://seineminer.com/de/blog/blocknet-payment-proof/) transaktionsbezogene Belege liefern. Prüfe Empfängerzuordnung, Betrag und Chain-Status; ein gültiger Transaktionsschlüssel allein genügt nicht als Zahlungsbeleg.

Bewahre Bestellung, Anforderung, Eingangs-IDs, geprüfte Beträge, Bestätigungsentscheidung und Rückzahlungen gemeinsam privat auf. Teile nur den betreffenden Beleg. Gib niemals Wiederherstellungswörter, Wallet-Passwörter oder private View-Keys für die gesamte Wallet als Zahlungsnachweis weiter.

Beim zusätzlichen Mining hältst du [Pool-Auszahlungen](/blog/de/track-your-miner-on-bntpool/) auch bei gemeinsamer Wallet getrennt von Kundenbestellungen. So bleibt jede Rechnung ihrem Transfer zugeordnet.


<CTA target="pool" title="Auch Mining-Erträge in der Wallet empfangen" text="Die Pool-Anleitung zeigt, wie du einen Miner verbindest und seine Auszahlungen neben anderen Blocknet-Eingängen verfolgst." />
