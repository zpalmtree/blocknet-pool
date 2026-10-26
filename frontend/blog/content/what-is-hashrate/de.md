---
title: Was bedeutet Hashrate beim Krypto-Mining?
description: Die Hashrate ist die Anzahl der Hashes, die ein Miner pro Sekunde berechnet. Lerne die Einheiten, die Unterschiede zwischen eigener, Pool- und Netzwerk-Hashrate und ihre Schwankungen kennen.
tags: [Hashrate, Mining-Grundlagen, Blocknet, bntpool]
sourceHash: '5af3a8b3f18b'
translation: draft
faq:
  - q: 'Welche Hashrate erreicht ein Rechner beim Blocknet-Mining?'
    a: 'In den Messungen von seine liegen die besten Ergebnisse pro Hardwaregeneration bei 49,4 H/s für einen AMD-EPYC-Zen-5-Server, 35,7 H/s für AMD EPYC Zen 4, 30,9 H/s für ein Apple-M4-Max-Notebook und 21,5 H/s für einen AWS-Graviton4-Arm-Server. Dein Wert hängt vor allem von Speicherbandbreite und der Anzahl der 2-GB-Threads ab, die in deinen RAM passen.'
  - q: 'Warum wird Blocknets Hashrate in H/s statt MH/s oder TH/s gemessen?'
    a: 'Jeder Blocknet-Hash ist eine Argon2id-Berechnung, die etwa 2 GiB Speicher schreibt und wieder liest. Selbst große Server schaffen deshalb nur einige Dutzend Hashes pro Sekunde. Proof-of-Work-Verfahren mit schnellen Hashes wie SHA-256 erreichen viel höhere Zahlen und verwenden größere Einheiten. Vergleiche Hashraten nur innerhalb desselben Algorithmus.'
  - q: 'Warum zeigt der Pool eine andere Hashrate als mein Miner?'
    a: 'Dein Miner zählt jeden fertigen Hash. Der Pool sieht nur eingereichte Shares und schätzt deine Hashrate aus deren Difficulty während der letzten Stunde. Shares treffen zufällig ein; der Pool-Wert schwankt deshalb stärker und stabilisiert sich mit der Zeit. Vergleiche das 24-Stunden-Diagramm auf My Stats mit dem Miner-Durchschnitt.'
  - q: 'Bedeutet eine höhere Hashrate mehr Mining-Belohnungen?'
    a: 'Ja, proportional. Jeder Hash hat dieselbe Chance auf einen Block. Dein erwarteter Anteil an den Netzwerk-Blöcken entspricht daher deinem Anteil an der Netzwerk-Hashrate. Doppelte Hashrate verdoppelt die erwartete Belohnung; eine steigende Netzwerk-Hashrate verkleinert deinen Anteil. Auf bntpool.com erhältst du ihn als Anteil an jedem Pool-Block.'
---

Die Hashrate ist die Zahl, die jeder Miner, Pool und Blockexplorer zuerst zeigt. Diese Anleitung erklärt, was sie misst, wie du die Einheiten ab H/s liest, wie deine Hashrate mit der des Pools und des Netzwerks zusammenhängt und warum alle Werte von Minute zu Minute schwanken. Als Beispiel dienen Blocknet und das Dashboard von bntpool.com.

## Was ist die Hashrate?

<Answer>
Die Hashrate gibt an, wie viele Hashes ein Miner pro Sekunde berechnet. Beim Proof-of-Work-Mining ist jeder Hash ein Versuch, einen Block zu lösen, mit derselben kleinen Erfolgschance. Eine höhere Hashrate bedeutet mehr Versuche pro Sekunde. Dein erwarteter Blockanteil wächst daher proportional zu deinem Anteil an der gesamten Hashrate.
</Answer>

Eine Proof-of-Work-Blockchain verlangt einen Blockheader, dessen Hash unter einem Zielwert liegt. Es gibt keine Abkürzung: Der Miner verändert eine Zahl im Header, die Nonce, hasht das Ergebnis, prüft es gegen den Zielwert und wiederholt den Vorgang. Die Hashrate zählt diese abgeschlossenen Versuche pro Sekunde.

Bei Blocknet ist jeder Versuch ein Argon2id-Hash. Die Nonce wird als Passwort eingegeben, der übrige Blockheader als Salt. Die Funktion verarbeitet 2 GiB Speicher, bevor sie ihr 32 Byte langes Ergebnis liefert. Sowohl seine als auch der Pool melden fertige Hashes pro Sekunde. 1 H/s bedeutet damit eine vollständige Argon2id-Berechnung pro Sekunde.

## Was bedeuten H/s, kH/s und MH/s?

Die Hashrate-Einheiten folgen den üblichen metrischen Präfixen. Jede Stufe ist tausendmal so groß wie die vorherige:

| Einheit | Hashes pro Sekunde | Typischer Einsatz |
|---|---|---|
| H/s | 1 | Einzelner Blocknet-Rechner |
| kH/s (KH/s) | 1.000 | Blocknet-Pool- und Netzwerk-Summen |
| MH/s | 1.000.000 | Schnellere Proof-of-Work-Algorithmen |
| GH/s | 1.000.000.000 | Schnelle Hashes wie SHA-256 |
| TH/s und höher | 1.000.000.000.000 und mehr | Netzwerksummen schneller Hashes |

Das Dashboard von bntpool.com schreibt die Tausendereinheit als KH/s und wechselt ab 1.000 zur nächsthöheren Einheit. 44,44 KH/s entsprechen also 44.440 H/s. Bevor du eine Hashrate durch eine andere teilst, rechne beide in H/s um.

## Warum sind Blocknet-Hashraten so niedrig?

Blocknets Proof of Work ist speicherintensiv. Jeder Hash schreibt etwa 2 GiB und liest etwa 2 GiB zurück, weit mehr, als in einen Prozessorcache passt. Deshalb bestimmt das Speichersystem das Tempo. Jeder Mining-Thread benötigt eigene 2 GiB; der RAM entscheidet somit, wie viele Threads ein Rechner ausführen kann.

Dies sind die besten von seine gemessenen Ergebnisse jeder Hardwaregeneration:

| Hardware | Threads | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Arm-Server (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Selbst der Zen-5-Server mit 32 Threads schafft etwa 1,5 Hashes pro Thread und Sekunde. Algorithmen auf Basis schneller Hashes liefern viel größere Zahlen: Laut Bitcoin-Wiki wurden Bitcoin-Miner mit doppeltem SHA-256 schon 2014 in Milliarden Hashes pro Sekunde eingestuft. Die Zahlen stehen für unterschiedliche Arbeit pro Hash. Eine Hashrate ist deshalb nur im Vergleich mit demselben Algorithmus aussagekräftig. [Argon2id Proof of Work erklärt](https://seineminer.com/de/blog/argon2id-proof-of-work-explained/) beschreibt den Algorithmus ausführlich. Die [Blocknet-Hardware-Anleitung](https://seineminer.com/de/blog/blocknet-mining-hardware-hashrate/) zeigt, wie Speicherbandbreite und RAM die Ergebnisse bestimmen.

## Deine Hashrate, die des Pools und die des Netzwerks

Für das Mining in einem [Pool](/blog/de/what-is-a-mining-pool/) sind drei Hashraten wichtig, die unterschiedlich gemessen werden.

### Deine Hashrate

Der Miner seine zählt jeden abgeschlossenen Hash und zeigt im Dashboard zwei Werte: **Current**, die aktuelle Rate, und **Average**, den Durchschnitt der Sitzung. Für einen stabilen Leistungswert nutze den Durchschnitt, nachdem seine einige Minuten lang Thread-Anzahl und CPU-Kernel mit echter Mining-Arbeit abgestimmt hat.

Der Pool sieht deine Hashes nicht. Er sieht die Shares: Hashes, die einen leichteren Zielwert für deine Verbindung erfüllen. bntpool.com schätzt deine Hashrate aus der gesamten Difficulty deiner akzeptierten Shares in der letzten Stunde und zeigt sie unter [My Stats](/stats). [Deinen Miner auf bntpool.com überwachen](/blog/de/track-your-miner-on-bntpool/) erklärt diese Seite.

### Pool-Hashrate

Die Karte **Pool Hashrate** summiert die Share-Arbeit aller verbundenen Miner über dasselbe einstündige Fenster. Sie zeigt, welchen Teil der Netzwerkarbeit bntpool.com leistet und wie oft der Pool damit einen Block erwarten kann. Diesen Zeitraum zeigt das Dashboard als **Expected Block Time**.

### Netzwerk-Hashrate

Niemand kann jeden Hash im Netzwerk zählen. Deshalb wird die Netzwerk-Hashrate aus Blöcken berechnet. Die Difficulty ist die erwartete Anzahl Hashes bis zu einem Block, und Blocknet strebt einen Block alle 300 Sekunden an.

## Wie wird die Netzwerk-Hashrate berechnet?

<Answer>
Die Netzwerk-Hashrate wird aus Difficulty und Blockzeiten geschätzt. Bei Blocknet entspricht die Difficulty ungefähr den erwarteten Hashes pro Block. Mit 300 Sekunden Zielzeit ergibt sich näherungsweise Difficulty geteilt durch 300. Das Dashboard von bntpool.com berechnet den Wert aus tatsächlicher Arbeit und Zeitstempeln der letzten zehn Blöcke.
</Answer>

Hier die Rechnung mit Live-Werten vom 25. Oktober 2026 um 02:26 Uhr EDT. Der [Blocknet-Explorer](https://explorer.blocknetcrypto.com) zeigte für den nächsten Block eine Difficulty von 16.943.147. Das Netzwerk war damit auf etwa 16,9 Millionen Hashes pro Block eingestellt. Geteilt durch 300 Sekunden ergeben sich etwa 56.477 H/s.

Im selben Moment zeigte bntpool.com 44,44 KH/s beziehungsweise 44.440 H/s Netzwerk-Hashrate. Beide Werte schätzen dieselbe Größe aus unterschiedlichen Blickwinkeln. Der Difficulty-Wert beschreibt die Rate für exakt fünfminütige Blöcke. Das Dashboard teilt die tatsächliche Arbeit der letzten zehn Blöcke durch ihre tatsächliche Dauer. Dauerten die jüngsten Blöcke länger als fünf Minuten, fällt dieser Wert niedriger aus. Blocknet passt die Difficulty nach jedem Block an und hält beide Werte dadurch langfristig nahe beieinander.

## Warum schwankt die Hashrate?

Jeder der drei Werte verändert sich aus eigenen Gründen.

- **Dein Rechner.** In den ersten Minuten vergleicht seine beim Mining Thread-Anzahlen und Kernel und wählt die schnellsten. Die Rate steigt während der Abstimmung. Danach können andere Programme, die Speicher oder Prozessorkerne beanspruchen, Rechenzeit vom Hashing abziehen.
- **Die Sicht des Pools auf dich.** Shares sind Zufallsereignisse. bntpool.com zielt mit der Share-Difficulty jeder Verbindung auf etwa 10 Shares alle fünf Minuten. Eine Stunde enthält daher ungefähr 120 Shares; besonders ergiebige oder ruhige Phasen verändern die Schätzung. Ein neu verbundenes Rig wird über die bisherigen Mining-Minuten gemittelt, bis eine volle Stunde erreicht ist.
- **Das Netzwerk.** Auch Blockzeiten sind zufällig, und zehn Blöcke sind eine kleine Stichprobe. Am 25. Oktober 2026 zeigte das Dashboard etwa 50.057 H/s um 00:26 Uhr EDT, 62.329 H/s um 02:03 Uhr und 44.440 H/s um 02:26 Uhr. Hinzukommende und ausscheidende Miner verursachen neben diesem Rauschen echte Änderungen.

Nutze für Entscheidungen Durchschnittswerte: den Sitzungsdurchschnitt von seine, das 24-Stunden-Diagramm auf My Stats und die mehrmals am Tag geprüfte Netzwerk-Hashrate.

## Was bedeutet deine Hashrate für Belohnungen?

Da jeder Hash dieselbe Chance hat, entspricht dein Anteil an der Netzwerk-Hashrate deinem erwarteten Blockanteil. Blocknet erzeugt 288 Blöcke pro Tag. Ein Miner mit 0,1 % des Netzwerks erwartet langfristig täglich Belohnungen im Wert von etwa 0,29 Blöcken. [Blocknet-Mining-Belohnungen schätzen](/blog/de/estimate-blocknet-mining-rewards/) rechnet dies mit Werten aus dem [Dashboard](/) in BNT pro Tag um.

Auf bntpool.com erhältst du diesen erwarteten Anteil über jeden gefundenen Pool-Block, gutgeschrieben nach deinen akzeptierten Shares. So treffen rund um die Uhr kleine Belohnungsschritte ein. Falls du noch nicht minest, führt dich die [Installationsanleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) in wenigen Minuten vom Download zu deinen ersten Shares.

<Note>
Die Hashraten dieser Anleitung gelten für Blocknets aktuellen Argon2id-Proof-of-Work. Laut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) ist um den 16. Februar 2027 das Netzwerk-Upgrade Faraday geplant, das den Proof of Work ändern soll. Aktualisiere den Miner seine regelmäßig: Er prüft beim Start und alle 12 Stunden auf neue Versionen.
</Note>

<CTA target="seine" title="Sieh dir deine eigene Hashrate an" text="Lade seine herunter, lass ihn sich einige Minuten abstimmen und verfolge deine Hashrate im Miner-Dashboard und auf My Stats." />
