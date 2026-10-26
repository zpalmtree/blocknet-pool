---
title: So funktionieren PPLNS-Auszahlungen auf bntpool.com
description: bntpool.com vergütet Blocknet-Miner mit PPLNS über ein sechsstündiges Share-Fenster. Erfahre, wie Belohnungen aufgeteilt werden, warum 60 Bestätigungen nötig sind und wann 0,1 BNT ausgezahlt werden.
tags: [Auszahlungen, PPLNS, bntpool, Mining-Anleitung]
sourceHash: 'ff0605be8a08'
translation: draft
faq:
  - q: 'Wie lang ist das PPLNS-Fenster auf bntpool.com?'
    a: 'Das Fenster beträgt sechs Stunden. Wenn der Pool einen Block findet, betrachtet er die sechs Stunden davor und verteilt die Belohnung unter allen, die in dieser Zeit Shares eingereicht haben, gewichtet nach deren Difficulty. Den aktuellen Wert findest du auf der Seite Get Started des Pools.'
  - q: 'Wie hoch ist die Mindestauszahlung auf bntpool.com?'
    a: 'Die Mindestauszahlung beträgt 0,1 BNT. Sobald dein bestätigtes Guthaben diesen Betrag erreicht, sendet der Pool es automatisch an deine Blocknet-Adresse. Du musst nichts beantragen oder abrufen. Kleinere Guthaben bleiben erhalten und wachsen mit jedem bestätigten Block, an dem deine Shares beteiligt sind, bis zur Schwelle.'
  - q: 'Warum unterscheiden sich Estimated Rewards und Confirmed Rewards?'
    a: 'Estimated Rewards sind dein Anteil an gefundenen Pool-Blöcken, die ihre 60 Bestätigungen noch sammeln; der Betrag kann sich daher ändern. Confirmed Rewards stammen aus Blöcken, die 60 Blöcke tief liegen, und warten auf die Auszahlung. Sobald ein Block reif ist, wandert sein Betrag von der ersten Karte zur zweiten.'
  - q: 'Enden meine Belohnungen, sobald ich das Mining stoppe?'
    a: 'Nein. Jeder Share bleibt nach der Übermittlung sechs Stunden im PPLNS-Fenster. Blöcke, die der Pool in dieser Zeit findet, werden dir weiter anteilig gutgeschrieben. Bereits bestätigte Belohnungen bleiben im Guthaben und werden ausgezahlt, sobald es mindestens 0,1 BNT erreicht.'
  - q: 'Was bedeutet ein Effort über 100 % auf der Luck-Seite?'
    a: 'Effort vergleicht die Arbeit des Pools in einer Runde mit der Difficulty des Blocks, also dem erwarteten Arbeitsaufwand. Über 100 % dauerte die Runde länger als im Mittel, unter 100 % war sie schneller. Beides ist normal; über viele Runden nähert sich der Durchschnitt 100 %.'
---

Diese Anleitung erklärt, wie bntpool.com die Shares deines Miners in Blocknet-Auszahlungen umsetzt. Danach weißt du, wie die Blockbelohnung aufgeteilt wird, warum sie etwa fünf Stunden auf ihre Auszahlbarkeit wartet, wann dein Guthaben versendet wird und wie du die Guthabenkarten auf der [Statistikseite](/stats) und die [Luck-Seite](/luck) liest.

Wenn Pool-Mining neu für dich ist, erklärt [Was ist ein Mining-Pool?](/blog/de/what-is-a-mining-pool/) die Grundlagen. Wenn du noch nicht minest, führt dich die [Mining-Anleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) in wenigen Minuten vom Download zu den ersten Shares.

## Was ist PPLNS?

<Answer>
PPLNS steht für „pay per last N shares“, die Vergütung der letzten N Shares. Findet bntpool.com einen Block, verteilt der Pool die Belohnung auf die Shares, die Miner in den sechs Stunden davor eingereicht haben, gewichtet nach der Difficulty jedes Shares. Je mehr kontinuierliche Arbeit du in diesem Fenster beiträgst, desto größer ist dein Anteil an jedem Block.
</Answer>

Auf bntpool.com bezeichnet „last N“ eine Zeitspanne: sechs Stunden. Die Seite [Get Started](/start) zeigt Auszahlungssystem, Fenster, Mindestbetrag und Bestätigungen. Dieselben Werte werden unter `https://bntpool.com/api/info` veröffentlicht.

| Einstellung | Wert auf bntpool.com |
|---|---|
| Auszahlungssystem | PPLNS |
| Share-Fenster | 6 Stunden |
| Bestätigungen vor Auszahlung | 60 Blöcke |
| Mindestauszahlung | 0,1 BNT |

Shares werden nach Difficulty gewichtet. Jede Miner-Verbindung bekommt ihre eigene Share-Difficulty, die der Pool für einen gleichmäßigen Share-Strom anpasst. Schnellere Rechner erhalten schwierigere Shares; jeder ihrer Shares hat deshalb mehr Gewicht. Entscheidend ist die insgesamt geleistete Arbeit, ein gleichermaßen fairer Maßstab für Laptop und Server.

## Wie verteilt bntpool.com eine Blockbelohnung?

Findet der Pool einen Block, durchläuft die Belohnung immer dieselben Schritte:

1. **Der Block wird erfasst.** Er erscheint auf der [Blockseite](/blocks) mit ausstehendem Status.
2. **Das Fenster wird ausgewertet.** Der Pool summiert für jede Adresse die Difficulty aller akzeptierten Shares in den sechs Stunden bis zum Block.
3. **Die Belohnung wird aufgeteilt.** Jede Adresse erhält denselben Anteil an der Belohnung wie ihr Anteil an der gesamten Arbeit im Fenster.
4. **Der Block reift.** Dein Anteil erscheint zunächst als Schätzung. Liegt der Block 60 Blöcke tief, wandert er in dein bestätigtes Guthaben.
5. **Das Guthaben wird ausgezahlt.** Ab 0,1 BNT bestätigtem Guthaben sendet der Pool es an deine Adresse.

## Ein Rechenbeispiel

<Note>
Dieses Beispiel verwendet erfundene, runde Zahlen zur Veranschaulichung. Echte Beträge hängen von der Hashrate des Pools, deiner Hashrate und der jeweiligen [Blockbelohnung](/blog/de/blocknet-block-reward-schedule/) ab.
</Note>

Angenommen, alle Miner im Pool reichen in den sechs Stunden vor einem Block Shares mit einer gesamten Difficulty von 1.000.000 ein. Deine Shares ergeben 20.000. Dein Fensteranteil beträgt 20.000 ÷ 1.000.000 = 2 %. Verteilt der Pool 400 BNT für diesen Block, erhältst du 400 × 0,02 = 8 BNT.

Ein zweiter Miner mit genau deiner Hashrate verbindet sich erst 30 Minuten vor dem Block. Das ist ein Zwölftel von sechs Stunden, also liegen etwa 1.667 Difficulty von ihm im Fenster; er erhält etwa 0,67 BNT aus diesem Block. Seine Arbeit zählt aber weiter: Jeder dieser Shares bleibt die folgenden sechs Stunden im Fenster und verdient einen Anteil an jedem Block, den der Pool dann findet.

| Miner (Beispiel) | Arbeit im Fenster | Anteil | Gutschrift aus 400 BNT |
|---|---|---|---|
| Du, während aller sechs Stunden aktiv | 20.000 | 2 % | 8 BNT |
| Gleiche Hashrate, 30 Minuten vor dem Block beigetreten | 1.667 | etwa 0,17 % | etwa 0,67 BNT |
| Kleiner Miner, während aller sechs Stunden aktiv | 100 | 0,01 % | 0,04 BNT |

Der kleine Miner der letzten Zeile verdient 0,04 BNT pro Block. Nach drei solchen Blöcken überschreitet sein bestätigtes Guthaben die Schwelle von 0,1 BNT und wird ausgezahlt.

## Warum belohnt PPLNS kontinuierliches Mining?

Jeder eingereichte Share zählt für jeden Block, den der Pool in den folgenden sechs Stunden findet. Wer rund um die Uhr minet, hat immer ein vollständiges Arbeitsfenster hinter sich und erhält bei jedem neuen Block den vollen Anteil entsprechend seiner Leistung. Die Einnahmen folgen der Hashrate von Block zu Block eng.

Dadurch verzeiht PPLNS auch Unterbrechungen. Ein Neustart, Treiberupdate oder kurzer Netzwerkausfall setzt deine Position nicht zurück: Bereits gesendete Shares bleiben sechs Stunden im Fenster. Arbeit nach einem gefundenen Block zählt für spätere Blöcke, sodass zusätzliche Hashrate immer genutzt wird.

Für möglichst gleichmäßige Ergebnisse lässt du deinen Miner laufen und sich selbst abstimmen. Der Miner seine passt Thread-Anzahl und CPU-Kernel während des Minings weiter an, ohne die Pool-Verbindung neu aufzubauen. Dein Share-Strom bleibt kontinuierlich. Du kannst [seine für Windows, macOS oder Linux herunterladen](https://seineminer.com/de/#download).

## Warum wartet eine Blockbelohnung auf 60 Bestätigungen?

Bei Blocknet müssen neu geschürfte Coins 60 Blöcke warten, bevor sie ausgegeben werden dürfen. Die Daemon-Regeln nennen das Coinbase-Reife. Bei fünf Minuten Blockzeit sind das etwa fünf Stunden (60 × 5 Minuten = 300 Minuten). bntpool.com wartet ebenfalls 60 Blöcke, bevor eine Belohnung als bestätigt gilt. Jede Auszahlung stammt damit aus Coins, die der Pool tatsächlich senden kann.

Während ein Block reift, prüft der Pool, ob er weiterhin zur Hauptkette gehört. Liegt er 60 Blöcke tief, wird er als bestätigt markiert und seine Belohnung dem bestätigten Guthaben jedes beteiligten Miners hinzugefügt. In dem seltenen Fall, dass sich das Netzwerk auf einen anderen Block dieser Höhe einigt, gilt der Block als verwaist und seine geschätzten Belohnungen werden entfernt.

## Wann kommen Auszahlungen von bntpool.com an?

<Answer>
Nach 60 Bestätigungen, bei Blocknets fünfminütiger Blockzeit etwa fünf Stunden, wandert dein Anteil auf der Statistikseite zu Confirmed Rewards. Sobald dein bestätigtes Guthaben 0,1 BNT erreicht, sendet bntpool.com es automatisch an deine Blocknet-Adresse. Du musst nichts beantragen oder abrufen.
</Answer>

Bei einem neuen Miner erscheint die erste Schätzung, sobald der Pool einen Block findet, während seine Shares im Fenster liegen. Die erste Zahlung folgt, wenn der Block gereift ist und das Guthaben die Mindesthöhe erreicht. Gesendete Zahlungen stehen auf der [Auszahlungsseite](/payouts) und in deinem eigenen Verlauf auf der Statistikseite.

## Wie werden vorläufige Belohnungen angezeigt?

Suche auf der [Statistikseite](/stats) nach deiner Adresse oder deinem `@name`. Du siehst drei Guthabenkarten:

- **Estimated Rewards:** dein Anteil an noch nicht gereiften Blöcken. Diese Beträge sind vorläufig und können sich bis zur Bestätigung oder Verwaisung jedes Blocks ändern.
- **Confirmed Rewards:** dein gereiftes Guthaben, das auf die Auszahlung wartet.
- **Paid Balance:** alles, was bereits an deine Adresse gesendet wurde.

Der Pool trennt vorläufige Belohnungen ab, damit das bestätigte Guthaben nur endgültige Beträge enthält. Unter den Karten steht jeder unbestätigte Block mit deinem geschätzten Anteil. So kannst du seine Bestätigung verfolgen. Die [Anleitung zur Überwachung deines Miners](/blog/de/track-your-miner-on-bntpool/) erklärt den Rest der Statistikseite.

## Was verraten Luck und Effort?

Effort misst die Arbeit, die der Pool zum Finden eines Blocks geleistet hat: die akzeptierte Arbeit einer Runde geteilt durch die Difficulty des Blocks, als Prozentwert. Blocknets Difficulty entspricht der erwarteten Hash-Anzahl pro Block. 100 % Effort bedeutet deshalb genau den durchschnittlichen Arbeitsaufwand. Unter 100 % wurde der Block schneller als im Mittel gefunden, über 100 % dauerte es länger.

Die [Luck-Seite](/luck) listet jede Runde mit Blockhöhe, Effort, Rundendauer, Status und Fundzeit auf. Das [Pool-Dashboard](/) zeigt den wachsenden Effort der aktuellen Runde und das durchschnittliche Pool-Glück. Einzelne Runden liegen über oder unter 100 %; das ist bei jedem Proof-of-Work-Pool normal. Über viele Runden nähert sich der Effort-Durchschnitt 100 %.

Glück beeinflusst, wie oft Blöcke eintreffen. Dein Anteil an jedem Block hängt nur von deinem Arbeitsanteil im Fenster ab. Kontinuierliches Mining hält diesen Anteil vollständig, egal ob eine Runde kurz oder lang dauert.

<CTA target="pool" title="Tritt dem Pool bei" text="Mine kontinuierlich auf bntpool.com. Dein Anteil an jedem Block wird automatisch ausgezahlt, sobald du 0,1 BNT erreichst." />
