---
title: Blocknets Blockbelohnungsplan
description: Blocknets monatliche Blockbelohnung von 723,25 BNT zum Start bis zur dauerhaften Emission von 2,0 BNT ab Monat 48, mit 288 Blöcken täglich und zuvor 98,6 Millionen BNT.
tags: [Blocknet, Blockbelohnung, Emission, Mining]
sourceHash: abea18c2c21b
translation: draft
faq:
  - q: 'Wie hoch ist die aktuelle Blocknet-Blockbelohnung?'
    a: 'Sie hängt vom Monat der Blockchain ab, denn sie sinkt alle 8.640 Blöcke. Am 25. Oktober 2026 war die Blockchain in Monat 7 mit 467,67 BNT pro Block. Monat 8 beginnt auf Höhe 69.120 mit 439,46 BNT. Suche die aktuelle Blockhöhe im Dashboard von bntpool.com und dann in der Tabelle.'
  - q: 'Wie viele Blocknet-Blöcke werden täglich geschürft?'
    a: 'Blocknet strebt einen Block alle fünf Minuten an, also 288 täglich und 8.640 je 30-Tage-Monat. Die Difficulty passt sich nach jedem Block anhand eines gewichteten Mittels der letzten 60 Blockzeiten an. So bleibt der Durchschnitt nahe fünf Minuten, wenn Miner hinzukommen oder gehen.'
  - q: 'Wie groß ist Blocknets Gesamtangebot?'
    a: 'Vor Beginn der dauerhaften Restemission in Monat 48 werden etwa 98,56 Millionen BNT geschürft. Code und offizielle Website runden dies auf etwa 100 Millionen. Danach zahlt jeder Block dauerhaft 2,0 BNT, also weitere 576 BNT täglich oder ungefähr 210.240 BNT jährlich. Das Angebot wächst langsam weiter.'
  - q: 'Wann beginnt Blocknets dauerhafte Restemission?'
    a: 'Sie beginnt in Monat 48 auf Blockhöhe 414.720. Das entspricht 48 Monaten zu 30 Tagen beziehungsweise 1.440 Tagen mit fünfminütigen Blöcken nach dem Start, ungefähr vier Jahren. Das genaue Datum hängt von tatsächlichen Blockzeiten ab. Ab dieser Höhe zahlt jeder Block 2,0 BNT.'
  - q: 'Wann lassen sich geschürfte Blocknet-Coins ausgeben?'
    a: 'Eine Blockbelohnung wird nach 60 Blöcken ausgebbar, bei fünfminütigen Blöcken etwa nach fünf Stunden. Diese Regel heißt Coinbase-Reife. Auch bntpool.com wartet 60 Blöcke vor der Auszahlung einer Blockbelohnung und zahlt daher nur aus gereiften Blöcken.'
---

Diese Seite listet die Blocknet-Blockbelohnung für jeden Monat des Emissionsplans auf, erklärt ihre Berechnung und zeigt, was der Plan für dich als Miner bedeutet.

## Was ist die Blocknet-Blockbelohnung?

<Answer>
Blocknets Blockbelohnung begann mit 723,25 BNT pro Block und sinkt einmal je 30-Tage-Monat aus 8.640 Blöcken entlang einer gleichmäßigen Abklingkurve. Innerhalb eines Monats bleibt sie für jeden Block gleich. Ab Monat 48 auf Höhe 414.720 zahlt jeder Block eine dauerhafte Restemission von 2,0 BNT.
</Answer>

Blocknet strebt einen Block alle fünf Minuten (300 Sekunden) an. Das ergibt 288 Blöcke täglich und 8.640 je 30-Tage-Monat. Die Difficulty passt sich nach jedem Block anhand eines gewichteten Mittels der letzten 60 Blockzeiten an. So bleibt das Tempo auch bei veränderter Netzwerk-Hashrate nahe fünf Minuten.

Die Belohnung hängt ausschließlich davon ab, in welchen Monat die Blockhöhe fällt. Der Monat ist die abgerundete Blockhöhe geteilt durch 8.640: Höhen 0 bis 8.639 gehören zu Monat 0, Höhen 8.640 bis 17.279 zu Monat 1 und so weiter.

## Wie wird die Belohnung berechnet?

Der Daemon berechnet jede Monatsbelohnung mit einer Formel:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

Der erste Term ist der Teil oberhalb der dauerhaften 2,0 BNT. Er schrumpft jährlich um den Faktor e^-0,75 und beträgt nach zwölf Monaten noch etwa 47 % seines Anfangswerts. Monatlich betrachtet beträgt dieser Teil rund 93,9 % des Vormonats. Die Belohnung sinkt dadurch in kleinen, regelmäßigen Schritten.

Das Ergebnis wird auf ganze atomare Einheiten abgerundet (1 BNT entspricht 100.000.000 atomaren Einheiten). Die Tabelle zeigt jede Belohnung auf zwei Dezimalstellen gerundet.

## Blocknets Belohnungstabelle nach Monaten

| Monat | Erster Block | Belohnung pro Block (BNT) | Insgesamt bis Monatsende geschürft (Millionen BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| ab 48 | 414.720 | 2,00 | +0,017 pro Monat |

Der Genesis-Block auf Höhe 0 trägt keine Belohnung. Monat 0 enthält daher 8.639 bezahlte Blöcke. Jeder BNT stammt ohne Premine aus diesen Blockbelohnungen, wie die Seite zu [Blocknets Fair Launch](/blog/de/blocknet-fair-launch/) erklärt. Am 25. Oktober 2026 arbeitete der Pool an Block 69.016 nahe dem Ende von Monat 7. Das [Dashboard von bntpool.com](/) zeigt die aktuelle Höhe zur Zuordnung in der Tabelle; die [Blockseite](/blocks) listet die gefundenen Pool-Blöcke auf.

## Was geschieht in Monat 48?

<Answer>
Monat 48 beginnt auf Blockhöhe 414.720. Ab dort zahlt jeder Blocknet-Block dauerhaft feste 2,0 BNT. Diese Restemission, die sogenannte Tail Emission, erhält eine Belohnung für jeden Block, solange die Blockchain läuft. Miner werden damit stets für die Absicherung des Netzwerks bezahlt.
</Answer>

Die Abklingformel gilt für Monate 0 bis 47 und endet in Monat 47 bei 40,22 BNT pro Block. Ab Monat 48 verwendet der Daemon direkt den Wert der Restemission. Die Belohnung wird zu 2,0 BNT und bleibt dort. Bei 288 Blöcken täglich kommen dadurch 576 BNT pro Tag oder etwa 210.240 BNT pro Jahr hinzu.

Monat 48 beginnt nach 1.440 Tagen fünfminütiger Blöcke seit dem Start, also ungefähr vier Jahren. Das Kalenderdatum hängt von echten Blockzeiten ab. Die Blockhöhe ist deshalb der verlässliche Bezugspunkt.

## Wie viel BNT wird vor der Restemission geschürft?

Die Summe aller Blöcke von Höhe 1 bis 414.719 beträgt 98.561.905,08 BNT, etwa 98,6 Millionen. Code und offizielle Website beschreiben den Plan als rund 100 Millionen Coins über vier Jahre zuzüglich Restemission. Die exakte Summe liegt knapp unter diesem gerundeten Wert.

Der größte Teil entsteht früh. Nach Planjahren gruppiert:

| Monate | Geschürfte BNT | Anteil an der Gesamtmenge vor der Restemission |
|---|---|---|
| 0-11 (Jahr 1) | 54,48 Millionen | 55,3 % |
| 12-23 (Jahr 2) | 25,84 Millionen | 26,2 % |
| 24-35 (Jahr 3) | 12,32 Millionen | 12,5 % |
| 36-47 (Jahr 4) | 5,93 Millionen | 6,0 % |

## Wann kannst du geschürfte BNT ausgeben?

Eine Blockbelohnung lässt sich ausgeben, sobald 60 weitere Blöcke darüber hinzugefügt wurden, ungefähr fünf Stunden später. Diese Regel heißt Coinbase-Reife. bntpool.com wartet dieselben 60 Blöcke vor der Auszahlung. Die [Auszahlungsseite](/payouts) listet die gesendeten Beträge auf.

## Was bedeutet der Plan für Miner?

Frühere Monate haben größere Belohnungen. Ein Block in Monat 12 zahlt 342,69 BNT, etwa 47 % der 723,25 BNT aus Monat 0. Ein Block in Monat 24 zahlt 162,93 BNT. Bis zur Restemission zahlt jeder neue Monat weniger pro Block als sein Vorgänger.

Dein Belohnungsanteil folgt deinem Anteil an der Netzwerk-Hashrate. Beim Solo-Mining sind täglich 288 × (deine Hashrate ÷ Netzwerk-Hashrate) Blöcke zu erwarten, jeder mit der Belohnung seines Monats. Im Pool gilt dieselbe Rechnung für dessen Hashrate. Jede Blockbelohnung wird auf die Miner verteilt, die Arbeit beigetragen haben. Du verdienst so an jedem Pool-Block, statt auf einen eigenen ganzen Block zu warten. Das [Dashboard von bntpool.com](/) zeigt aktuelle Netzwerk- und Pool-Hashrate; die [Statistikseite](/stats) zeigt die deines Miners, sobald du minest.

So startest du jetzt:

1. Lade seine von [seineminer.com](https://seineminer.com/de/#download) herunter. Der Miner läuft auf Windows, macOS und Linux und stimmt sich selbst auf deine Hardware ab.
2. Starte ihn und füge deine Blocknet-Adresse oder deinen `@name` ein.
3. Akzeptiere den Standardpool `stratum+tcp://bntpool.com:3333`.

Die [Anleitung zum Mining auf bntpool.com](/blog/de/mine-blocknet-on-bntpool/) erklärt die Pool-Schritte, [Get Started](/start) listet die Pool-Einstellungen und die [Mining-Anleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) führt durch die Einrichtung. Ergebnisse verschiedener Prozessoren findest du unter [Blocknet-Mining-Hardware und Hashrate](https://seineminer.com/de/blog/blocknet-mining-hardware-hashrate/).

<Note>
Die Zahlen stammen aus dem Belohnungscode des Blocknet-Daemons, geprüft am 25. Oktober 2026. Laut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) plant Blocknet um den 16. Februar 2027 ein Netzwerk-Upgrade namens Faraday. Aktualisiere den Miner seine regelmäßig: Er prüft beim Start und alle 12 Stunden auf neue Versionen.
</Note>

<CTA target="seine" title="Mine Blocknet mit seine" text="seine ist kostenlos für Windows, macOS und Linux verfügbar und minet standardmäßig auf bntpool.com." />
