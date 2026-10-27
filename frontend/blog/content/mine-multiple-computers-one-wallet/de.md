---
title: 'Mit mehreren Computern Blocknet für eine Wallet minen'
description: 'Füge auf bntpool.com einen zweiten Miner für dieselbe Wallet hinzu. Benenne Rechner, prüfe Shares und verstehe gemeinsame Erträge sowie doppelte Gerätenamen.'
tags: ['Mining-Anleitung', 'Rechner', 'Einrichtung']
faq:
  - q: 'Brauche ich für jeden Rechner eine eigene Wallet?'
    a: 'Nein. Jeder Rechner kann dieselbe Empfangsadresse auf bntpool.com verwenden und erhält einen eigenen Gerätenamen. Der Pool rechnet Erträge je Adresse ab. Auf Mining-Rechnern brauchst du nur die Empfangsadresse oder deine Blocknet ID, nicht die Wiederherstellungsphrase der Wallet.'
  - q: 'Was passiert bei zwei Minern mit demselben Namen?'
    a: 'Unter derselben Adresse wird ihre Share-Historie unter diesem Namen zusammengefasst. Dadurch lässt sich eine Anzeige schwerer einem Rechner zuordnen. Gib jeder Installation einen eigenen Namen und starte sie damit neu. Bereits gespeicherte Shares behalten ihre ursprüngliche Bezeichnung.'
  - q: 'Teilt ein zusätzlicher Rechner meine Auszahlung auf?'
    a: 'Gerätenamen ordnen die angezeigte Mining-Aktivität und erzeugen keine getrennten Guthaben unter derselben Adresse. Gültige Arbeit deiner Rechner trägt zur PPLNS-Beteiligung dieser Adresse bei. Für ihr Guthaben gelten weiterhin die üblichen Bestätigungs- und Auszahlungsregeln des Pools.'
sourceHash: '1cfcb79502c0'
translation: draft
---

Füge bntpool.com einen zweiten Rechner hinzu und behalte eine Empfangs-Wallet. Benenne jede Installation, prüfe sie einzeln und beobachte dann die gemeinsame Adresse. Diese Anleitung behandelt den zweiten Rechner und doppelte Namen. Richte deinen ersten Miner zunächst mit der [Pool-Anleitung](/blog/de/mine-blocknet-on-bntpool/) ein.

## Können mehrere Rechner für dieselbe Wallet minen?

<Answer>
Ja. Starte seine auf jedem Rechner mit derselben Empfangsadresse und einem eigenen Gerätenamen. bntpool.com erfasst Arbeit und Erträge je Adresse; die Namen trennen die Aktivität in der Statistik. Prüfe angenommene Shares jedes Rechners, bevor du dich auf die gemeinsame Adressansicht verlässt.
</Answer>

*Mehrere Rechner senden Arbeit über den Pool an eine Empfangsadresse; jeder Rechner behält einen eigenen Namen.*

## Wähle Empfangsadresse und Namensschema

Verwende deine Wallet-Empfangsadresse oder eine darauf verweisende Blocknet ID. Prüfe auf jedem Miner die aufgelöste Adresse. Ein Name vereinfacht die Eingabe, die Abrechnung folgt jedoch der resultierenden Adresse. Änderst du später das Ziel einer ID, kann ein laufender Miner bis zum Neustart die alte Auflösung behalten. Bereits verdiente Erträge bleiben bei ihrer ursprünglichen Adresse.

Die [Mining-Wallet-Anleitung](https://seineminer.com/de/blog/blocknet-wallet-for-mining/) erklärt die Empfangsadresse. Mining-Rechner brauchen nur dieses öffentliche Ziel; Wiederherstellungsphrase und private Schlüssel bleiben bei der Wallet. Wähle kurze Namen wie `desktop-01` und `server-02`. Der Pool entfernt äußere Leerzeichen und behält bis zu 64 Zeichen. Setze den unterscheidenden Teil deshalb an den Anfang eines langen Namens.

## Füge den zweiten Miner mit eigenem Namen hinzu

Installiere die passende seine-Version auf dem zweiten Rechner und starte sie aus dem entpackten Ordner. Das erste PowerShell-Beispiel setzt Empfangsname, Pool und Gerätename ausdrücklich. Ersetze `@yourname` vorher durch deine ID oder vollständige Empfangsadresse und behalte die Anführungszeichen. Ergänze bereits getestete Backend- oder Tuning-Optionen dieses Rechners.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Unter Linux oder macOS nutzt du den folgenden Programmaufruf mit einem anderen Gerätenamen. Die Betriebssysteme dürfen unterschiedlich sein, die Empfangsadresse soll übereinstimmen. Jedes Beispiel startet echtes Mining. Führe das passende Beispiel auf dem jeweiligen Rechner aus und warte Start und Tuning ab. Beide Beispiele auf demselben Rechner sind nicht erforderlich.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Welche Einstellungen werden gemeinsam verwendet?

Teile Empfangsadresse und Pool-Endpunkt. Jede Installation behält einen eigenen lokalen Datenordner und Gerätenamen. seine speichert Verbindungseinstellungen in `seine.config.json` im Datenverzeichnis; der Standard `./seine-data` hängt vom Startordner ab. Ein Start aus einem anderen Ordner kann eine andere Konfiguration finden. Ein fester Startordner erleichtert die Pflege.

Kopierst du eine Konfiguration vom ersten Rechner, setze im Startbefehl des zweiten einen eigenen Namen und prüfe ihn beim Start. Behalte diese ausdrückliche Angabe in künftigen Befehlen: Eine Überschreibung ändert keinen bereits gespeicherten Namen. Passe Hardware-Optionen an den Rechner an und lasse ihn lokal tunen. Adressargument und Gerätename bestimmen Auszahlungsziel und Bezeichnung getrennt; gib sie wie oben separat an.

## Wie prüfst du den Beitrag beider Rechner?

Öffne My Stats auf bntpool.com für die gemeinsame Empfangsadresse. Suche beide Namen und einen aktuellen Share jedes Geräts. Prüfe auch lokal angenommene Einreichungen. Gehe Rechner für Rechner vor: So ordnest du einen Prozess einer Pool-Zeile zu und erkennst Fehler leichter als allein anhand der Gesamt-Hashrate.

Der Pool schätzt Hashrate anhand eingereichter Shares. Ein neuer Rechner braucht Zeit und angenommene Arbeit für eine aussagekräftige Anzeige. Lokale Rate und Pool-Schätzung betreffen unterschiedliche Messungen und Zeitfenster. Prüfe erst aktuelle Shares und Annahmezähler, dann längere Zeiträume. Die [Statistik-Anleitung](/blog/de/track-your-miner-on-bntpool/) erklärt Karten und Zeilen, ohne aus Verbindungszahlen auf Geräte schließen zu müssen.

## Was tun bei fehlenden oder gleich benannten Geräten?

Gleiche Namen unter derselben Adresse fassen die aufgezeichneten Shares zusammen. Benenne einen Miner mit seiner Namensoption um und starte ihn neu. Neue Shares tragen den neuen Namen, alte behalten ihren bisherigen. Die alte Zeile kann während des Berichtszeitraums weiter erscheinen. Ein neuer Gerätename verschiebt kein bereits verdientes Adressguthaben.

Vergleiche bei einem fehlenden Rechner seine tatsächliche aufgelöste Adresse, den Endpunkt und Namen mit der geöffneten Seite. Prüfe Verbindung und angenommene Einreichungen. CPU und GPU innerhalb eines seine-Prozesses brauchen hierfür keine getrennten Namen. Bei abgelehnten oder veralteten Shares hilft die [Share-Status-Anleitung](/blog/de/accepted-rejected-stale-shares/), statt wiederholt die Wallet-Adresse zu ändern.

## Wie funktionieren gemeinsame Erträge und Auszahlungen?

Gültige Arbeit aller Rechner trägt zur Beteiligung derselben Adresse im PPLNS-Fenster bei. Namen erleichtern die Zuordnung und schaffen keine getrennten Guthaben. Ein zusätzlicher Rechner bringt die tatsächlich eingereichte nutzbare Arbeit, keinen festen Ertrag pro Gerät oder Name. Hardware und Laufzeit können unterschiedliche Beiträge ergeben; mehr Namen allein erzeugen keine zusätzliche Arbeit.

Die [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/) erklärt die Verteilung beim Blockfund. Bestätigungs- und Auszahlungsregeln gelten weiterhin für das gemeinsame Adressguthaben. Du musst weder Wallets zusammenführen noch Guthaben zwischen Namen übertragen. Prüfe vor dem Dauerbetrieb auf jedem Rechner Zieladresse, eigenen Namen und angenommene Shares. Nutze diese kurze Liste auch beim nächsten Gerät.

<CTA target="pool" title="Verbinde einen weiteren Rechner mit bntpool.com" text="Nutze eine Empfangsadresse, benenne jede Installation eindeutig und prüfe ihre angenommenen Shares." />
