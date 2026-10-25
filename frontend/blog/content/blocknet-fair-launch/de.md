---
title: 'Blocknets Fair Launch: kein Premine, jeder Coin geschürft'
description: Blocknet startete ohne Premine, Entwicklerfonds oder Treasury. Prüfe den leeren Genesis-Block und die Coinbase-Regel mit genau einer Ausgabe selbst und mine BNT.
tags: [Blocknet, Fair Launch, Genesis-Block, Mining, bntpool]
sourceHash: 814839500c3d
translation: draft
faq:
  - q: 'Gab es bei Blocknet einen Premine?'
    a: 'Nein. Blocknets Genesis-Block vom 15. Februar 2026 enthält keinerlei Transaktionen. Vor Beginn des Minings existierten somit keine Coins. Die ersten Coins entstanden mit der Belohnung von Block 1. Jeder BNT seither wurde an den Miner des jeweiligen Blocks ausgezahlt.'
  - q: 'Hat Blocknet einen Entwicklerfonds oder eine Treasury?'
    a: 'Nein. Der Konsens erlaubt pro Block genau eine Coinbase-Transaktion mit genau einer Ausgabe im Wert der exakten Blockbelohnung. Es gibt keine zweite Ausgabe für Entwickler, Gründer oder eine Treasury. Jeder Netzwerk-Node lehnt einen Block ab, der eine solche hinzufügt.'
  - q: 'Wann wurde Blocknets Genesis-Block erstellt?'
    a: 'Sein Zeitstempel ist der 15. Februar 2026 um 20:58:38 Uhr EST, entsprechend dem 16. Februar um 01:58:38 Uhr UTC. Er trägt die Nachricht "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything". Der Hash des Vorgängerblocks ist der SHA3-256-Hash dieser Nachricht.'
  - q: 'Wie kann ich Blocknets Fair Launch selbst prüfen?'
    a: 'Öffne Block 0 im Blocknet-Explorer. Er enthält null Transaktionen und eine Merkle-Wurzel nur aus Nullen. Im offenen Daemon-Quellcode ist der Genesis-Block mit leerer Transaktionsliste festgelegt. Die Coinbase-Prüfung akzeptiert genau eine Ausgabe in Höhe der Blockbelohnung.'
  - q: 'Wie starte ich das Blocknet-Mining?'
    a: 'Lade den Miner seine für Windows, macOS oder Linux herunter, starte ihn und füge bei der Aufforderung deine Blocknet-Adresse oder deinen Blocknet-ID-Kurznamen ein. Akzeptiere mit Enter den Standardpool bntpool.com. Der Miner beginnt sofort; anschließend findest du deine Adresse auf der Statistikseite von bntpool.com.'
---

Blocknet begann ohne Zuteilung an irgendjemanden. Diese Seite zeigt den Inhalt des Genesis-Blocks, die Konsensregeln, die Premine, Entwicklerfonds und Treasury ausschließen, ihre Überprüfung im Explorer und im Quellcode sowie den Einstieg ins eigene BNT-Mining auf bntpool.com.

## Gab es bei Blocknet einen Premine?

<Answer>
Nein. Blocknets Genesis-Block vom 15. Februar 2026 enthält keine Transaktionen. Vor Beginn des Minings existierten keine Coins. Seit Block 1 zahlt jeder Block seine Belohnung über eine einzige Coinbase-Ausgabe in exakt der vorgesehenen Höhe. Jeder umlaufende BNT wurde daher von jemandem geschürft, der den Proof of Work ausführte.
</Answer>

Weder im Genesis-Block noch in späteren Blöcken gibt es Zuteilungen an Gründer, Entwickler, Investoren oder eine Stiftung. Das Coin-Angebot stammt ausschließlich aus Blockbelohnungen. Die dafür zuständigen Regeln gehören zum Konsens und werden von jedem Netzwerk-Node bei jedem Block geprüft.

## Was enthält Blocknets Genesis-Block?

Der Genesis-Block ist fest im Daemon hinterlegt. Jeder Node beginnt daher mit demselben Block 0. Seine Felder sind festgelegt:

| Feld | Wert |
|---|---|
| Höhe | 0 |
| Zeitstempel | 15. Februar 2026, 20:58:38 Uhr EST (16. Februar, 01:58:38 Uhr UTC) |
| Nachricht | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Vorgänger-Hash | SHA3-256 der Nachricht: `a94a7f2b…d909df` |
| Difficulty | 4, das Netzwerkminimum |
| Nonce | 0 |
| Transaktionen | keine |
| Merkle-Wurzel | nur Nullen |

Die Nachricht ist die Überschrift eines Wired-Artikels vom 15. Februar 2026, den der Code als Quelle nennt. Weil sich der Vorgänger-Hash aus der Nachricht ableitet, ist die Überschrift in der Identität von Block 0 und allem darauf Aufgebauten verankert.

Der Kommentar über der Genesis-Funktion im Daemon formuliert es klar: „Genesis enthält keine Transaktionen – keine Coinbase, keine verbrannten Coins. Der erste echte Block auf Höhe 1 enthält die erste Coinbase.“ Die Validierung setzt das durch. Ein Node lehnt Block 0 ab, wenn er nicht exakt null Transaktionen und eine ausschließlich aus Nullen bestehende Merkle-Wurzel enthält. Selbst ein modifizierter Node könnte deshalb keine Coins in den Genesis-Block einschleusen, ohne von allen anderen abgelehnt zu werden.

## Wie schließt der Konsens Entwicklerfonds und Treasury aus?

<Answer>
Jeder Blocknet-Block muss mit genau einer Coinbase-Transaktion beginnen. Diese darf keine Eingaben haben, muss eine Gebühr von null und genau eine Ausgabe in Höhe der Blockbelohnung auf dieser Höhe enthalten. Für eine zweite Zahlung gibt es keinen Platz. Eine Ausgabe an Entwicklerfonds, Gründer oder Treasury kann daher auf der Blockchain nicht existieren.
</Answer>

Mehrere Daemon-Prüfungen greifen dabei ineinander:

- **Eine Coinbase pro Block.** Die erste Transaktion muss eine Coinbase sein; keine weitere im Block darf eine sein.
- **Eine Ausgabe, keine Eingaben.** Eine Coinbase mit null oder zwei Ausgaben oder mit einer Eingabe ist ungültig.
- **Die exakte Belohnung.** Blocknet verbirgt Beträge mit Pedersen-Commitments, doch die Coinbase ist ein Sonderfall. Ihr Blinding-Faktor wird aus öffentlichen Daten abgeleitet: dem öffentlichen Transaktionsschlüssel und der Blockhöhe. Jeder Node kann deshalb das Commitment für die vorgesehene Belohnung rekonstruieren und vergleichen. Bindet die Ausgabe einen anderen Betrag, wird der Block abgelehnt.
- **Ein leeres Memo.** Das Coinbase-Memo muss leer sein. Blockbelohnungen können daher auch keine zusätzlichen Daten transportieren.

Normale Transaktionen können keine Coins erzeugen. Jede muss ausgeglichen sein: Eingaben entsprechen Ausgaben plus Gebühr, direkt anhand der Commitments geprüft. Jede Ausgabe enthält außerdem einen Bulletproofs-Range-Proof, der einen gültigen, nicht negativen Betrag nachweist. So bleiben Beträge privat, während die Ausgabe neuer Coins exakt dem Plan folgt.

## Wie kannst du Blocknets Fair Launch selbst überprüfen?

Du musst niemandem einfach glauben. Sowohl die Blockchain als auch der Code sind öffentlich.

**Im Explorer.** Öffne [Block 0 im Blocknet-Explorer](https://explorer.blocknetcrypto.com/block/0). Er zeigt 0 Transaktionen, eine Merkle-Wurzel aus 64 Nullen, Nonce 0 und einen Vorgänger-Hash mit dem Anfang `a94a7f2b`. Die Zeile zur Blockbelohnung zeigt den vorgesehenen Betrag dieser Höhe. Ohne Transaktionen gibt es jedoch keine Ausgabe, die ihn erhält. Öffne danach [Block 1](https://explorer.blocknetcrypto.com/block/1): Seine einzige Transaktion ist eine Coinbase ohne Eingaben und mit einer Ausgabe, den ersten Coins der Blockchain.

Du kannst bestätigen, dass der Vorgänger-Hash tatsächlich der Hash der Genesis-Nachricht ist. Mit OpenSSL ab Version 1.1.1 führst du aus:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Die Ausgabe endet mit `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, demselben Wert wie im Explorer für Block 0.

**Im Code.** Der Daemon ist unter [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core) quelloffen verfügbar. Suche in [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go) nach diesen Funktionen:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` erstellt Block 0 mit einer leeren Transaktionsliste. `validateGenesisBlock` lehnt jeden Block 0 mit Transaktionen ab. `validateCoinbaseConsensus` erzwingt die einzelne Ausgabe in Höhe der Blockbelohnung. In `transaction.go` prüfen `validateCoinbase` und `verifyCommitmentBalance` die Coinbase-Struktur sowie die Bilanz normaler Transaktionen.

## Warum ist ein Fair Launch für Miner wichtig?

Für Miner bedeutet ein Fair Launch, dass die Herkunft sämtlicher Coins allein durch Blockbelohnungen erklärt wird.

- **Alle neuen Coins gehen an Miner.** Ohne Premine und Treasury-Ausgabe gehen 100 % der Neuemission an diejenigen, die Blöcke finden. Block 1 zahlte 723,25093035 BNT in seiner einzigen Coinbase-Ausgabe. Jeder spätere Block zahlte ebenso die volle Belohnung.
- **Für alle gelten dieselben Regeln.** Seit Block 1 lässt sich neuer BNT nur durch Proof of Work verdienen. Der [Belohnungsplan](/blog/de/blocknet-block-reward-schedule/) ist im Code festgelegt und gilt gleichermaßen für den ersten Miner und jemanden, der heute beginnt.
- **Der Proof of Work eignet sich für normale Rechner.** Blocknet nutzt Argon2id mit 2 GiB Speicher pro Hash, ausgelegt auf ASIC-Resistenz. So bleibt Mining mit vorhandenen Desktops, Laptops und Servern möglich. Die [Argon2id-Erklärung auf seineminer.com](https://seineminer.com/de/blog/argon2id-proof-of-work-explained/) beschreibt die Gründe.
- **Die Neuemission lässt sich leicht prüfen.** Jeder Coin geht auf eine Coinbase-Ausgabe zurück, die jeder Node geprüft hat. Die bis zu einer bestimmten Höhe ausgegebene Gesamtmenge folgt daher unmittelbar aus dem Belohnungsplan.

Wenn du die Coin insgesamt noch nicht kennst, erklärt [Was ist Blocknet?](https://seineminer.com/de/blog/what-is-blocknet/) Privatsphäre, Zahlungen per Namen und Wallets.

## Wie minest du Blocknet selbst?

An einem Fair Launch nimmst du durch Mining teil. Am schnellsten geht das mit dem Miner seine auf bntpool.com:

1. **Besorge dir eine Adresse.** Installiere die offizielle Blocknet-[Kommandozeilen-Wallet](https://github.com/blocknetprivacy/blocknet/releases/latest) oder [Desktop-Wallet](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) und kopiere deine Adresse. Alternativ nutzt du einen Blocknet-ID-Kurznamen wie `@name`.
2. **Lade seine herunter.** Hole das Archiv für Windows, macOS oder Linux von [seineminer.com](https://seineminer.com/de/#download) und entpacke es. Eine Installation ist nicht nötig.
3. **Starte ihn und akzeptiere die Vorgaben.** Füge Adresse oder `@name` ein und bestätige mit Enter den Standardpool `stratum+tcp://bntpool.com:3333`. Der Miner beginnt sofort zu hashen und stimmt Threads und Kernel während des Betriebs ab: auf deiner CPU sowie unter Windows und Linux auf NVIDIA-Grafikkarten.
4. **Beobachte deinen Miner.** Suche deine Adresse auf der [Statistikseite](/stats), um Hashrate, Shares und Guthaben zu sehen. Verfolge die Pool-Funde auf der [Blockseite](/blocks).

Die [Anleitung zum Mining auf bntpool.com](/blog/de/mine-blocknet-on-bntpool/) erklärt Anmeldung per Adresse oder Namen und Kontrolle des Miners. [Get Started](/start) listet die Auszahlungseinstellungen auf. Die [Blocknet-Mining-Anleitung](https://seineminer.com/de/blog/how-to-mine-blocknet/) führt auf jedem System Schritt für Schritt durch die Einrichtung von seine.

<CTA target="seine" title="Mine deinen Anteil" text="Jeder BNT wird geschürft. Lade seine herunter und beginne in wenigen Minuten mit dem Mining auf bntpool.com." />
