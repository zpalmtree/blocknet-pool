---
title: Pool- oder Solo-Mining für Blocknet
description: Entscheide dich zwischen Pool- und Solo-Mining für Blocknet. Berechne erwartete Blöcke pro Tag, erfahre, wie bntpool.com sie in regelmäßige Auszahlungen umsetzt, und richte Solo-Mining ein.
tags: [Mining-Anleitung, Solo-Mining, bntpool, PPLNS]
sourceHash: 171d240d6c4b
translation: draft
faq:
  - q: 'Wie oft würde ich beim Solo-Mining einen Blocknet-Block finden?'
    a: 'Im Mittel findest du täglich 288 × (deine Hashrate ÷ Netzwerk-Hashrate) Blöcke, denn Blocknet strebt einen Block alle fünf Minuten an. Bei 50 H/s in einem Netzwerk mit 50.000 H/s sind das 0,288 Blöcke täglich, also etwa ein Block alle dreieinhalb Tage. Die aktuelle Netzwerk-Hashrate steht im Dashboard von bntpool.com.'
  - q: 'Brauche ich einen eigenen Blocknet-Node für das Mining auf bntpool.com?'
    a: 'Nein. Der Pool betreibt einen eigenen Blocknet-Node, erstellt Blockvorlagen und reicht gefundene Blöcke ein. Du brauchst nur seine und eine Blocknet-Adresse oder einen Blocknet-ID-Namen. Starte seine, füge deine Adresse ein und akzeptiere den Standardpool. Der Pool zahlt deinen Anteil an jedem Block an diese Adresse.'
  - q: 'Kann seine Blocknet solo minen?'
    a: 'Ja. Starte seine mit ./seine --mode daemon. Der Miner arbeitet dann direkt mit deinem synchronisierten Blocknet-Node und verbindet sich über dessen API-Cookie. Jeder gefundene Block zahlt seine vollständige Belohnung an die Node-Wallet. Die Solo-Mining-Anleitung im seine-Repository erklärt eigene Node-Konfigurationen und Wallet-Passwörter.'
  - q: 'Kann ich später zwischen Pool- und Solo-Mining wechseln?'
    a: 'Ja. Für beide Modi nutzt seine denselben Download. Starte ihn normal für bntpool.com oder mit --mode daemon für deinen eigenen Node und starte ihn bei einem Wechsel neu. Bereits gesendete Pool-Shares bleiben im sechsstündigen PPLNS-Fenster und verdienen bis zu dessen Ablauf weiter.'
---

Diese Anleitung hilft dir bei der Entscheidung zwischen Blocknet-Mining auf bntpool.com und Solo-Mining mit deinem eigenen Node. Du berechnest, wie viele Blöcke deine Hashrate allein findet, erfährst, wie der Pool daraus regelmäßige Auszahlungen macht, und richtest beide Varianten mit seine ein.

## Solltest du Blocknet im Pool oder solo minen?

<Answer>
Für beide gilt dieselbe Rechnung: Dein Anteil an der Netzwerk-Hashrate bestimmt deinen Anteil an dessen Blöcken. Auf bntpool.com verdienst du einen Anteil an jedem gefundenen Pool-Block; entsprechend regelmäßig kommen Auszahlungen. Solo behältst du die volle Belohnung jedes selbst gefundenen Blocks. Das passt zu großen Betrieben mit eigenem Node.
</Answer>

Für die meisten Miner ist bntpool.com der einfache Einstieg. Es ist der Standardpool von seine; außer deiner Adresse brauchst du nichts einzurichten und keinen Node zu betreiben oder zu synchronisieren. Solo-Mining wird ebenfalls vollständig unterstützt: Der Daemon-Modus von seine arbeitet direkt mit deinem Blocknet-Node. Im Folgenden erfährst du, welche Variante zu deiner Hardware passt.

## Wie viele Blöcke würdest du solo finden?

<Answer>
Blocknet strebt einen Block alle fünf Minuten an, also 288 Blöcke täglich. Allein kannst du täglich 288 × (deine Hashrate ÷ Netzwerk-Hashrate) davon erwarten. Teile einen Tag durch diese Zahl, um den mittleren Abstand zwischen deinen Blöcken zu erhalten. Die aktuelle Netzwerk-Hashrate findest du im Dashboard von bntpool.com.
</Answer>

Diese Formel funktioniert, weil Blocknet die Difficulty so anpasst, dass das gesamte Netzwerk im Mittel alle 300 Sekunden einen Block findet. Die Difficulty entspricht ungefähr der Anzahl Hash-Versuche, die das Netzwerk bis zu einem Erfolg erwarten muss. Jeder Hash hat dieselbe Chance. Der Anteil der Netzwerk-Hashes von deinem Rechner ist damit auch dein erwarteter Blockanteil.

Du brauchst zwei Zahlen:

- **Deine Hashrate.** Während des Minings zeigt seine sie im Dashboard. Die [Statistikseite](/stats) des Pools zeigt sie ebenfalls, sobald du dort geminet hast.
- **Die Netzwerk-Hashrate.** Im [Dashboard von bntpool.com](/) steht sie auf der Karte Network Hashrate neben Pool Hashrate. Beide ändern sich; prüfe sie zum Zeitpunkt deiner Berechnung.

Jeder gefundene Block zahlt die Blockbelohnung seines Monats. Der [Blockbelohnungsplan](/blog/de/blocknet-block-reward-schedule/) listet sie nach Monaten auf.

## Ein Rechenbeispiel

<Note>
Die folgenden Zahlen sind hypothetische, runde Beispielwerte, keine aktuellen Messwerte. Verwende für deine eigene Schätzung die aktuelle Netzwerk-Hashrate aus dem Dashboard.
</Note>

Angenommen, das Netzwerk läuft mit 50.000 H/s. Ein Rig mit 50 H/s, ungefähr dem von seine auf einem AMD-EPYC-Zen-5-Server gemessenen Wert, besitzt 0,1 % des Netzwerks. Das würden dieses Rig und zwei größere Betriebe allein finden:

| Deine Hashrate (Beispiel) | Netzwerkanteil | Erwartete Blöcke pro Tag | Mittlerer Blockabstand |
|---|---|---|---|
| 50 H/s | 0,1 % | 0,288 | etwa 83 Stunden (3,5 Tage) |
| 500 H/s | 1 % | 2,88 | etwa 8,3 Stunden |
| 5.000 H/s | 10 % | 28,8 | etwa 50 Minuten |

Nun verbindet sich das 50-H/s-Rig mit einem Pool, der insgesamt ebenfalls 5.000 H/s erreicht. Der Pool findet etwa 28,8 Blöcke täglich, im Mittel einen alle 50 Minuten. Der Anteil des Rigs von 1 % an der Pool-Arbeit bringt ihm 1 % jedes dieser Blöcke. Angerechnet wird dieselbe Arbeit von 0,288 Blöcken täglich. Anders ist ihr Eingang: ungefähr alle 50 Minuten ein kleiner Anteil, solo dagegen alle paar Tage ein ganzer Block.

Die Tabelle zeigt auch, warum Solo-Mining zu großen Betrieben passt. Mit 5.000 H/s findet ein Solo-Miner genauso häufig Blöcke wie ein gleich großer Pool. Seine Einnahmen sind bereits für sich regelmäßig.

Wie du Blöcke pro Tag für deine Hardware in BNT umrechnest, zeigt [Blocknet-Mining-Belohnungen schätzen](/blog/de/estimate-blocknet-mining-rewards/) mit einem datierten Beispiel aus echten Pool-Daten.

## Warum zahlt ein Pool regelmäßiger aus?

bntpool.com bündelt die Hashrate aller Teilnehmer und findet dadurch viel häufiger Blöcke als ein einzelnes Rig. Die Belohnung wird per PPLNS unter den Minern aufgeteilt, deren Arbeit zum Fund beitrug: Der Pool betrachtet die sechs Stunden vor dem Block und verteilt entsprechend der Difficulty ihrer Shares.

Das hat praktische Vorteile:

- **Einnahmen aus jedem Block.** Solange deine Shares im sechsstündigen Fenster liegen, verdienst du an jedem gefundenen Pool-Block mit. Auf der [Blockseite](/blocks) kannst du die Funde verfolgen.
- **Automatische Auszahlungen.** Liegt ein Block 60 Blöcke tief, wandert dein Anteil ins bestätigte Guthaben. Ab 0,1 BNT sendet der Pool es an deine Adresse. Gesendete Zahlungen stehen auf der [Auszahlungsseite](/payouts).
- **Kontinuität bei Neustarts.** Bereits eingereichte Shares bleiben sechs Stunden im Fenster. Ein Neustart oder Treiberupdate setzt deine Position nicht zurück.
- **Nur der Miner muss laufen.** Der Pool betreibt den Node, erstellt Blockvorlagen und reicht Blöcke ein. Mehrere Rigs können auf dieselbe Adresse oder einen Blocknet-ID-`@name` minen.

Die [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/) erklärt die Aufteilung Schritt für Schritt mit einem eigenen Rechenbeispiel. Die Seite [Get Started](/start) nennt die aktuellen Auszahlungseinstellungen.

## Warum Blocknet solo minen?

Beim Solo-Mining kontrollierst du den gesamten Ablauf. Dein eigener Node erstellt jede Blockvorlage und prüft jeden Block selbst. Jeder gefundene Block zahlt die vollständige Belohnung direkt an die Wallet deines Nodes.

Das passt am besten, wenn deine Hashrate häufig Blöcke findet. Im Beispiel oben findet ein Betrieb mit 10 % des Netzwerks ungefähr alle 50 Minuten einen Block. Solo-Auszahlungen kommen damit so regelmäßig wie bei einem Pool. Wenn du bereits für deine Wallet oder andere Dienste einen Blocknet-Node betreibst, ergänzt Solo-Mining diesen um den Miner.

Auch für diesen Modus ist seine gebaut. Der Miner arbeitet mit den Blockvorlagen des Nodes. Hat der Node eine Vorlage etwa nach einem Neustart vergessen, reicht seine den vollständigen gelösten Block erneut ein, damit ein gefundener Block die Blockchain trotzdem erreicht.

## Wie richtest du Solo-Mining mit seine ein?

1. **Installiere und synchronisiere einen Blocknet-Node.** Mit dem Wrapper `blocknet` führst du Folgendes aus:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Das schreibt das API-Cookie des Nodes nach `~/.config/bnt/data/mainnet/api.cookie`. Für eine schnellere erste Synchronisierung stellt bntpool.com auf seiner [Checkpoint-Seite](/checkpoints) eine Datei `checkpoints.dat` bereit. [Einen Blocknet-Node mit Checkpoints synchronisieren](/blog/de/blocknet-checkpoints-fast-sync/) erklärt ihre Nutzung.
2. **Warte auf die vollständige Synchronisierung.** Die Meldungen `[sync] progress` erreichen 100 %. Erst wenn der Node die Blockchain eingeholt hat, liefert er Blockvorlagen.
3. **Starte seine im Daemon-Modus.**

   ```sh
   ./seine --mode daemon
   ```

   Bei einem eigenen Node-Aufbau gibst du API und Cookie selbst an:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

Die [Solo-Mining-Anleitung](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) behandelt Wallet-Passwörter und eigene Konfigurationen. Die [Mining-Anleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) erklärt Download und erste Shares. Du kannst [seine für Windows, macOS oder Linux herunterladen](https://seineminer.com/de/#download).

## Welche Variante solltest du wählen?

Setze deine Hashrate und die aktuelle Netzwerk-Hashrate aus dem [Dashboard](/) in die Formel ein und betrachte den mittleren Blockabstand.

- **Starte auf bntpool.com**, wenn du auf einem Desktop, Laptop oder wenigen Servern minest oder regelmäßige Auszahlungen ohne eigenen Node möchtest. Starte seine, füge Adresse oder `@name` ein und akzeptiere den Standardpool.
- **Mine solo**, wenn dein Betrieb einen großen Netzwerkanteil hält und dadurch selbst häufig Blöcke findet, und du bereits einen synchronisierten Node betreibst oder betreiben möchtest.

Mit seine lässt sich die Entscheidung leicht ändern. Pool- und Daemon-Modus verwenden denselben Download. Wächst deine Hardware, wechselst du mit einer Option und einem Neustart. Wenn neben dem Pool-Mining ein lokaler Node läuft, zeigt seine dessen Wallet-Guthaben neben deinem Pool-Guthaben an.

<Note>
Laut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) plant Blocknet um den 16. Februar 2027 ein Netzwerk-Upgrade namens Faraday, das den Proof of Work ändern soll. Aktualisiere den Miner seine regelmäßig: Er prüft beim Start und alle 12 Stunden auf neue Versionen.
</Note>

<CTA target="pool" title="Starte das Mining auf bntpool.com" text="Starte seine, füge deine Adresse ein und verdiene an jedem Pool-Block mit. Die Auszahlung erfolgt automatisch ab 0,1 BNT." />
