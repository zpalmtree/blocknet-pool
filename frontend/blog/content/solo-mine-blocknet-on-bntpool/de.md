---
title: Blocknet solo auf bntpool.com minen
description: 'Mine Blocknet solo, ohne einen Node zu betreiben. Setze in seine solo: vor deine Adresse, und bntpool.com zahlt dir die volle Belohnung jedes gefundenen Blocks.'
tags: [Mining-Anleitung, Solo-Mining, bntpool, seine]
faq:
  - q: 'Wie mine ich Blocknet solo auf bntpool.com?'
    a: 'Aktualisiere seine auf v0.3.1 oder neuer und setze solo: vor deine Auszahlungsadresse, zum Beispiel ./seine --address solo:YOUR_ADDRESS. seine verbindet sich wie gewohnt mit bntpool.com, und der Pool zahlt dir über seine normalen Auszahlungen die volle Belohnung jedes Blocks, den dein Rig findet. Du musst keinen Node betreiben.'
  - q: 'Brauche ich für Solo-Mining einen eigenen Blocknet-Node?'
    a: 'Nein. Mit dem Präfix solo: betreibt bntpool.com den Node, erstellt die Blockvorlagen und reicht die Blöcke ein, die du findest. Auf deinem Rig läuft nur seine. Der Daemon-Modus von seine ist der andere Weg zum Solo-Mining, mit deinem eigenen synchronisierten Node, und passt zu Minern, die bereits einen betreiben.'
  - q: 'Kann ich für Solo-Mining einen Blocknet-ID-Namen nutzen?'
    a: 'Ja. Verwende solo:@name oder solo:$name statt solo:ADDRESS und setze es auf der Kommandozeile in Anführungszeichen, damit deine Shell es unverändert weitergibt. Der Pool löst den Namen in die vollständige Adresse auf, schreibt deine Solo-Blöcke gut und zahlt sie wie jede andere Auszahlung an diese Adresse.'
  - q: 'Wie wechsle ich zurück zu Pool-Belohnungen?'
    a: 'Entferne das Präfix solo: von deiner Adresse und starte seine neu. Dein Rig teilt dann wieder die PPLNS-Belohnungen jedes Blocks, den der Pool findet. Jedes Rig wählt seinen Modus beim Verbinden, daher kannst du mit derselben Adresse einige Rigs solo und andere mit geteilten Belohnungen betreiben.'
sourceHash: '623b1e0b356a'
translation: draft
---

Diese Anleitung zeigt dir, wie du Blocknet solo auf bntpool.com minest: Füge in seine ein Präfix zu deiner Adresse hinzu, und der Pool zahlt dir die volle Belohnung jedes Blocks, den deine Rigs finden. Du installierst und synchronisierst keinen Node. Du richtest es ein, erfährst, wie Solo-Blöcke ausgezahlt werden, und wo du sie auf dem Pool verfolgst.

## Was ist Solo-Mining auf bntpool.com?

<Answer>
Solo-Mining auf bntpool.com bezahlt dich nur für die Blöcke, die deine eigenen Rigs finden, und jeder davon zahlt die volle Blockbelohnung an deine Adresse. Der Pool betreibt weiterhin den Node, erstellt die Blockvorlagen und reicht deine Blöcke ein, sodass du Solo-Belohnungen mit derselben Einrichtung wie beim Pool-Mining erhältst. Du aktivierst es, indem du solo: vor deine Adresse setzt.
</Answer>

Beim normalen Pool-Mining teilt bntpool.com jeden Block unter den Minern auf, deren Shares im sechsstündigen PPLNS-Fenster liegen, sodass du an jedem gefundenen Pool-Block mitverdienst. Ein Solo-Rig behält dagegen die volle Belohnung seiner eigenen Blöcke und verdient nichts an Blöcken, die andere finden. Beide nutzen denselben Server, dieselbe Adresse und dieselben Auszahlungen. Nur der Login ändert sich.

Solo-Mining über den Pool bringt dir Solo-Belohnungen ohne den Aufwand eines Nodes:

- **Kein Node nötig.** Der Node des Pools folgt der Chain, liefert frische Vorlagen und reicht deine gelösten Blöcke ein.
- **Automatische Auszahlungen.** Deine Blöcke werden wie jede andere bntpool.com-Auszahlung an deine Adresse oder deinen Blocknet-ID-Namen gezahlt.
- **Live-Statistiken.** Deine Rigs, deine Hashrate und deine Blöcke erscheinen auf der [Statistikseite](/stats) des Pools.
- **Ein Präfix zum Wechseln.** Entferne `solo:` und starte seine neu, um zu geteilten Belohnungen zurückzukehren.

Der Daemon-Modus von seine ist der andere Weg zum Solo-Mining, mit deinem eigenen synchronisierten Blocknet-Node. [Pool- oder Solo-Mining für Blocknet](/blog/de/pool-vs-solo-mining-blocknet/) vergleicht alle drei Varianten und zeigt, wie oft deine Hashrate einen Block findet.

## Wie startest du Solo-Mining mit seine?

<Answer>
Installiere seine v0.3.1 oder neuer und starte es mit solo: vor deiner Blocknet-Adresse, zum Beispiel ./seine --address solo:YOUR_ADDRESS unter macOS oder Linux. seine verbindet sich mit bntpool.com, seinem Standardpool, und das Dashboard kennzeichnet dein Konto als Solo. Du kannst solo: und deine Adresse auch eingeben, wenn seine beim ersten Start nach einer Auszahlungsadresse fragt.
</Answer>

1. **Aktualisiere seine.** Solo-Logins brauchen seine v0.3.1 oder neuer. [Lade seine herunter](https://seineminer.com/de/#download) oder lass die automatischen Updates es installieren.
2. **Starte seine mit dem Präfix.** Unter macOS oder Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Unter Windows in PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine minet standardmäßig auf bntpool.com. Die Seite [Get Started](/start) des Pools zeigt denselben Befehl mit ausgeschriebener Pooladresse.
3. **Prüfe das Dashboard.** Das Guthabenfeld kennzeichnet dein Konto als **Solo**, und das Log zeigt die Bestätigung des Pools, dass du für die Blöcke bezahlt wirst, die diese Adresse findet.

Das Präfix funktioniert überall, wo seine eine Adresse annimmt: bei der Abfrage beim ersten Start, in der gespeicherten Konfiguration, in der lokalen Steuer-API und bei Rigs hinter dem Farm-Proxy. seine speichert es mit deiner Adresse, sodass spätere Starts weiter solo minen.

## Kannst du mit einem Blocknet-ID-Namen solo minen?

Ja. Ein Blocknet-ID-Name funktioniert genauso wie eine vollständige Adresse. Schreibe `solo:@name` oder `solo:$name` und setze es in Anführungszeichen, damit deine Shell es unverändert weitergibt:

```sh
./seine --address 'solo:@yourname'
```

Der Pool löst den Namen über blocknet.id auf und zahlt deine Solo-Blöcke an die dahinterliegende Adresse. Um einem Rig einen Namen zu geben, hänge ihn mit einem Punkt an eine vollständige Adresse an, etwa `solo:YOUR_ADDRESS.rig1`, oder übergib `--pool-worker rig1` mit einer beliebigen Adresse oder einem Namen. Das Präfix akzeptiert jede Groß- und Kleinschreibung, daher funktioniert auch `SOLO:`.

## Wie werden Solo-Blöcke ausgezahlt?

Findet dein Rig einen Block, verbucht der Pool ihn als Solo-Block. Sobald er 60 Blöcke tief liegt, nach etwa fünf Stunden, schreibt der Pool deinem Guthaben die volle Blockbelohnung abzüglich der Poolgebühr gut. Die Gebühr beträgt am 4. November 2026 0 %. Dein Guthaben wird dann über die normalen Auszahlungen des Pools an deine Adresse gezahlt.

Für Auszahlungen gelten dieselben Regeln wie beim Pool-Mining. Jeder Block wartet 60 Bestätigungen ab, damit eine kurze Reorganisation der Chain keine bereits gezahlte Belohnung rückgängig machen kann; [Blockbestätigungen und Mining-Belohnungen](/blog/de/block-confirmations-mining-rewards/) erklärt, warum. Der Endpunkt `/api/info` des Pools meldete am 4. November 2026 eine Mindestauszahlung von 1 BNT, und eine Blocknet-Blockbelohnung liegt weit darüber. Ein bestätigter Solo-Block wird daher im normalen Auszahlungsrhythmus des Pools gezahlt. Die [Auszahlungsseite](/payouts) listet jede Zahlung, die der Pool sendet.

Wenn du mit einem Auszahlungs-Memo an eine Exchange-Einzahlungsadresse minest, funktioniert Solo-Mining genauso. Das Memo und seine eigene Mindestauszahlung gelten für deine Solo-Blöcke ebenso wie für geteilte Belohnungen.

Die Arbeit deines Rigs zählt weiterhin zur Hashrate, zum Runden-Effort und zum Glück des Pools, und ein Solo-Block beendet die Runde des Pools wie jeder andere Block. Solo-Shares kommen nie ins PPLNS-Fenster. Andere Miner verdienen also nichts an deinen Blöcken, und du nichts an ihren.

## Wie oft findet ein Solo-Rig einen Block?

Blocknet strebt einen Block alle fünf Minuten an, also 288 Blöcke am Tag. Ein Solo-Rig findet davon im Mittel 288 × (deine Hashrate ÷ Netzwerk-Hashrate) pro Tag. Lies die Netzwerk-Hashrate auf der [Startseite von bntpool.com](/) und deine eigene im Dashboard von seine ab und teile dann einen Tag durch das Ergebnis, um die mittlere Zeit zwischen deinen Blöcken zu erhalten.

Solo-Belohnungen kommen als ganze Blöcke. Sie passen daher zu Minern, deren Hashrate häufig Blöcke findet: große Farmen, Serverflotten und alle mit einem großen Netzwerkanteil. Kleinere Rigs verdienen mit geteilten PPLNS-Belohnungen im Mittel genauso viel, ausgezahlt in gleichmäßigen Anteilen an jedem Block, den der Pool findet. [Pool- oder Solo-Mining für Blocknet](/blog/de/pool-vs-solo-mining-blocknet/) rechnet Beispiele mit drei Hashrates durch.

## Wo siehst du deine Solo-Rigs und -Blöcke?

Suche deine Adresse auf der [Statistikseite](/stats). Du kannst sie mit oder ohne das Präfix `solo:` einfügen. Die Seite kennzeichnet dann deine Solo-Arbeit:

- **Workers** listet jedes Rig mit einer Spalte Typ, die **Solo** für Rigs anzeigt, die im Hashrate-Fenster Solo-Shares gesendet haben.
- **Estimated rewards** kennzeichnet deine Solo-Blöcke, solange sie auf ihre Bestätigungen warten.
- **Blocks** und **Luck** zeigen vor dem Status jedes Blocks eine Spalte Typ, Pool oder Solo. Die [Blockseite](/blocks) listet jeden Block, den der Pool gefunden hat, die neuesten zuerst.

[Deinen Miner auf bntpool.com überwachen](/blog/de/track-your-miner-on-bntpool/) erklärt den Rest der Statistikseite.

## Kannst du Solo- und Pool-Rigs mischen?

Ja. Jedes Rig wählt seinen Modus beim Verbinden, daher kann eine Adresse gleichzeitig Solo-Rigs und Pool-Rigs betreiben. Eine Farm kann auf ihren größten Servern solo minen und die Desktops bei geteilten Belohnungen lassen, alles auf ein Guthaben ausgezahlt. Der Pool hält beide Arten von Arbeit getrennt: Ein Share aus einer Solo-Verbindung zählt nur für die Solo-Blöcke dieser Adresse.

Um ein Rig zurückzuholen, entferne `solo:` aus seiner Adresse, im Befehl oder in der gespeicherten Konfiguration, und starte seine neu. Shares, die das Rig bereits im Solo-Modus gesendet hat, bleiben solo, und neue Shares kommen sofort ins PPLNS-Fenster.

<CTA target="pool" title="Solo minen auf bntpool.com" text="Setze in seine solo: vor deine Adresse und behalte die volle Belohnung jedes Blocks, den deine Rigs finden, ganz ohne eigenen Node." />
