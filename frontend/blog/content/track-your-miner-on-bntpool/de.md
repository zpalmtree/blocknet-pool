---
title: Deinen Miner auf bntpool.com überwachen
description: Suche auf bntpool.com nach Adresse oder @name für Hashrate, Shares, Guthaben und Auszahlungen. Verfolge Blöcke, Glück und Verfügbarkeit oder nutze die öffentliche API.
tags: [bntpool, Mining-Statistik, Pool-API, Blocknet]
sourceHash: '40197b2fa166'
translation: draft
faq:
  - q: 'Warum unterscheidet sich meine Hashrate auf bntpool.com von der in seine?'
    a: 'Der Miner seine zählt jeden berechneten Hash. Der Pool schätzt deine Hashrate aus der Difficulty akzeptierter Shares, gemittelt über die letzte Stunde. Shares treffen zufällig ein, weshalb der Pool-Wert stärker schwankt und sich erst mit der Zeit stabilisiert. Vergleiche das 24-Stunden-Diagramm auf My Stats mit dem Durchschnitt von seine.'
  - q: 'Kann ich meine bntpool.com-Statistik mit einem @name suchen?'
    a: 'Ja. Gib deinen Blocknet-ID-Kurznamen wie @name oder $name ins Suchfeld von My Stats ein. Die Seite löst ihn über blocknet.id auf, zeigt deine Statistik und setzt die vollständige aufgelöste Adresse ins Suchfeld. Verwende diese vollständige Adresse für API-Aufrufe aus Skripten.'
  - q: 'Braucht die API von bntpool.com einen API-Schlüssel?'
    a: 'Nein. Pool-Statistik, Blöcke, Auszahlungen, Glück, Status und Miner-Endpunkte liefern öffentliches JSON über HTTPS. curl und ein Heim-Dashboard können sie ohne Anmeldung lesen. Beträge sind atomare Einheiten; teile durch 100.000.000 für BNT und frage alle 30 bis 60 Sekunden ab.'
  - q: 'Warum zeigt My Stats null Estimated Rewards?'
    a: 'Estimated Rewards erscheinen, sobald der Pool einen Block findet, während deine Shares im Vergütungsfenster liegen. Bei neuen Minern kann das einige Runden dauern. Zuerst erscheinen akzeptierte Shares und Hashrate, dann geschätzte Belohnungen. Diese reifen zu Confirmed Rewards und werden automatisch ausgezahlt.'
---

Öffne nach deinem ersten akzeptierten Share My Stats und prüfe, ob deine Arbeit beim Pool ankommt. Suche deinen Rechner, vergleiche die Hashrate über längere Zeit und verfolge geschätzte Erträge bis zur Bestätigung und Auszahlung. Skripte und die öffentliche API folgen am Ende.

Falls du noch nicht minest, führt dich die [Installationsanleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) in wenigen Minuten zu deinen ersten Shares.

## Wie prüfst du die Statistik deines Miners auf bntpool.com?

<Answer>
Öffne My Stats unter bntpool.com/stats, füge deine Blocknet-Adresse oder deinen @name ein und drücke Lookup. Die Seite zeigt Hashrate, akzeptierte und abgelehnte Shares, geschätztes, bestätigtes und ausgezahltes Guthaben, ein Hashrate-Diagramm, eine Zeile je Rig sowie letzte Auszahlungen. Solange der Tab offen ist, aktualisiert sie sich automatisch.
</Answer>

Gib auf [My Stats](/stats) dieselbe Adresse oder denselben [Blocknet-ID-Kurznamen](https://seineminer.com/de/blog/blocknet-id-pay-by-name/) wie in seine ein. Ein Name wie `@name` oder `$name` wird über blocknet.id aufgelöst; danach zeigt das Suchfeld die vollständige Zieladresse. Die Seite merkt sich die letzte Suche in diesem Browser. Beim nächsten Besuch sind deine Zahlen schon da.

1. Prüfe **Shares Accepted** und **Recent Shares**. Akzeptierte Shares zeigen, dass deine Arbeit den Pool erreicht, auch wenn alle Ertragsanzeigen noch null sind.
2. Suche deinen Rechner unter **Workers**. Vergleiche seinen Namen mit dem in Seine und prüfe, ob sich die Zeit des letzten Shares aktualisiert.
3. Null bei **Blocks Found** ist normal: Gezählt werden Blöcke, die deine eigenen Rechner gelöst haben, nicht deine Beteiligung an den Pool-Erträgen.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-first-mobile.webp" width="352" height="379"><img src="/blog/tracking/tracking-first-desktop.webp" width="1180" height="900" alt="Prüfe zuerst die akzeptierten Shares. Echte englische Oberfläche mit Beispieldaten, keine Live-Erträge." loading="lazy" decoding="async"></picture>
<figcaption>Prüfe zuerst die akzeptierten Shares. Echte englische Oberfläche mit Beispieldaten, keine Live-Erträge.</figcaption>
</figure>

## Was zeigt My Stats?

Karten und Tabellen sind in Gruppen aufgeteilt, die jeweils eine Frage zu deinem Miner beantworten.

### Mining und Shares

Die Karten unter **Mining** zeigen deine aktuelle **Hashrate**, unter **Blocks Found** die von deinen Rigs für den Pool gelösten Blöcke und unter **Mining Since** dein Startdatum. Darunter stehen **Shares Accepted**, **Shares Rejected**, **Reject Rate** und **Avg Difficulty**. Ein gesundes Rig liefert regelmäßig akzeptierte Shares und hat eine Ablehnungsquote nahe null.

**Hashrate History** zeigt deine Hashrate über 1h, 24h, 7d oder 30d. An längeren Zeiträumen erkennst du am besten, ob ein Rig die ganze Nacht lief.

### Eine Zeile je Rig

Gib bei mehreren Rechnern jedem einen eigenen Namen. Der Pool listet sie dann getrennt mit Hashrate, akzeptierten und abgelehnten Shares sowie Zeit des letzten Shares. Beim ersten Start wählt seine einen Namen aus `seine-` und vier Ziffern. Einen eigenen setzt du über die Kommandozeile:

```sh
./seine --pool-worker rig-01
```

Die Tabelle **Workers** zeigt auch die Miner-Version jedes Rechners. Bei alten oder unbekannten Versionen führt ein Hinweis oberhalb der Statistiken zur [seine-Release-Seite](https://github.com/zpalmtree/seine/releases).

## Warum unterscheidet sich die Pool-Hashrate von der in seine?

Der Miner seine zählt jeden berechneten Hash. Der Pool sieht nur eingereichte Shares und schätzt die Hashrate daher aus der gesamten Difficulty deiner akzeptierten Shares in der letzten Stunde. Jeder Share ist zufällig. bntpool.com setzt je Verbindung eine Difficulty für ungefähr 10 Shares alle 5 Minuten; kurze Fenster enthalten deshalb Rauschen.

Zwei Gewohnheiten helfen beim Vergleich:

- Vergleiche Durchschnitte: das 24h-Diagramm von My Stats mit dem Durchschnitt von seine über denselben Zeitraum.
- Gib einem neuen Rig Zeit. Anfangs mittelt der Pool nur über die bisherigen Mining-Minuten; die erste Anzeige beruht auf wenigen Shares. Nach einer Stunde gilt das volle einstündige Fenster.

Recent Shares zeigt sofort, dass Shares eintreffen, noch bevor sich die Hashrate stabilisiert.

## Wann kannst du mit deiner ersten Auszahlung rechnen?

Zwischen dem ersten Share und der ersten Auszahlung gibt es keine feste Wartezeit. Der Pool muss einen Block finden, während deine Shares im Auszahlungsfenster liegen. Dieser Block braucht anschließend 60 Bestätigungen: durchschnittlich etwa fünf Stunden **ab dem Blockfund**, wobei die tatsächliche Zeit schwankt. Erreicht dein bestätigtes Guthaben die aktuelle Mindestgrenze von 0,1 BNT, kann es beim automatischen Auszahlungslauf ausgezahlt werden. Aktuelle Einstellungen stehen unter [Get Started](/start).

Status und Transaktionslink findest du unter **Recent Payouts**. Eine eingereihte Zahlung wurde noch nicht gesendet. Fehlt eine gesendete Zahlung in deiner Wallet, hilft der [Leitfaden zu fehlenden Auszahlungen](/blog/de/pool-payout-missing-from-wallet/).

### Guthaben

- **Estimated Rewards:** dein Anteil an gefundenen Pool-Blöcken, die noch reifen.
- **Confirmed Rewards:** gereiftes Guthaben, das auf die nächste Auszahlung wartet.
- **Paid Balance:** alles, was bereits an deine Adresse gesendet wurde.

Bei noch ausstehenden Belohnungen listet **Estimated From Recent Blocks** jeden Block mit Höhe, geschätzter Gutschrift, verbleibenden Bestätigungen und Fundzeit auf. Jede Höhe führt zum Block im [Blocknet-Explorer](https://explorer.blocknetcrypto.com), damit du ihn selbst prüfen kannst. [So funktionieren PPLNS-Auszahlungen](/blog/de/how-pplns-payouts-work/) erklärt Aufteilung und Auszahlung der Belohnung.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-rewards-mobile.webp" width="352" height="524"><img src="/blog/tracking/tracking-rewards-desktop.webp" width="1081" height="217" alt="Geschätztes, bestätigtes und ausgezahltes Guthaben sind getrennte Stufen. Echte englische Oberfläche mit Beispielbeträgen, keine Ertragsprognose." loading="lazy" decoding="async"></picture>
<figcaption>Geschätztes, bestätigtes und ausgezahltes Guthaben sind getrennte Stufen. Echte englische Oberfläche mit Beispielbeträgen, keine Ertragsprognose.</figcaption>
</figure>

### Auszahlungen und letzte Shares

**Recent Payouts** listet jede Zahlung an deine Adresse mit Betrag, Transaktionslink zum Explorer und Status queued, unconfirmed oder confirmed. **Recent Shares** zeigt die letzten 20 Shares mit Job, Rig, Difficulty und Status. Damit bestätigst du am schnellsten, dass ein gerade gestarteter Rechner den Pool erreicht.

<span id="zero-hashrate"></span>

## Warum arbeitet seine, während My Stats eine Hashrate von null zeigt?

Prüfe zuerst die Auszahlungsadresse, die Poolverbindung und die akzeptierten Shares. Die lokale Hashrate zeigt, dass der Rechner Hashes berechnet; die Poolschätzung benötigt akzeptierte Shares für die abgefragte Adresse.

1. **Adresse abgleichen.** Suche nach derselben Empfangsadresse oder dem registrierten `@name`, den seine verwendet. Hast du die Adresse eines Kurznamens geändert, starte seine neu, damit der Name erneut aufgelöst wird. Prüfe anschließend die aufgelöste Adresse in My Stats.
2. **Pool abgleichen.** Prüfe, ob seine `stratum+tcp://bntpool.com:3333` verwendet. Ein Rechner, der anderswo oder im Solo-Modus mint, liefert hier keine Shares. Die [Pool-Einrichtungsanleitung](/blog/de/mine-blocknet-on-bntpool/) erklärt die Verbindung.
3. **Recent Shares prüfen.** Warte auf akzeptierte Shares und die nächste Seitenaktualisierung. Shares treffen in wechselnden Abständen ein; eine lokale Hashrate garantiert keine sofortige Poolschätzung. Treffen akzeptierte Shares ein, vergleiche Durchschnittswerte über längere Zeit.
4. **Verbindungsmeldung lesen.** Nutze die Tabelle, solange der Miner noch keine akzeptierten Shares liefert. Aktuelle Störungen findest du unter [Status](/status).

| Meldung in seine | Nächster Prüfschritt |
|---|---|
| `failed to connect to pool at` | Prüfe Hostname und Port, deine Verbindung und ob ausgehendes TCP zu Port 3333 erlaubt ist. |
| `login rejected:` | Lies die anschließende Begründung; prüfe Empfangsadresse oder Kurznamen und verwende die aktuelle seine-Version. |
| `stale job` | Der Share gehört zu einem alten Auftrag. Achte auf neue Aufträge und akzeptierte Shares; bleibt das Problem bestehen, prüfe Verbindung und Miner-Version. |
| `submit interrupted during reconnect` | Seine verbindet sich erneut. Achte nach Wiederherstellung der Verbindung auf die bestätigte Anmeldung und neue akzeptierte Shares. |

Null bei **Estimated Rewards** ist eine andere Frage: Shares können akzeptiert werden, bevor ein Poolblock Belohnungen gutschreibt. Der [PPLNS-Leitfaden](/blog/de/how-pplns-payouts-work/) erklärt den Ablauf.

## Welche öffentlichen Seiten zeigen den ganzen Pool?

Diese Seiten brauchen keine Adresssuche und zeigen alle Aktivitäten des Pools öffentlich. Das [Dashboard](/) bündelt die wichtigsten Zahlen: verbundene Miner, Pool- und Netzwerk-Hashrate, gefundene Blöcke und durchschnittliches Glück.

### Blöcke

[Blocks](/blocks) listet jeden gefundenen Block von bntpool.com, die neuesten zuerst: Höhe mit Explorer-Link, Belohnung, Effort, Rundendauer, Status und Fundzeit. Filtere nach Confirmed, Pending oder Orphaned, um den Stand jedes Blocks zu sehen.

### Auszahlungen

[Payouts](/payouts) listet jeden Auszahlungslauf mit Gesamtbetrag, Zahl bezahlter Miner, Transaktionslinks und Status. Nach dem Senden erscheint er als unconfirmed und kurz nach der ersten Blockbestätigung als confirmed. Du kannst deine Zahlung somit sowohl auf Pool-Seite als auch im Explorer verfolgen.

### Glück

[Luck](/luck) zeigt den Effort jeder Runde: akzeptierte Share-Arbeit geteilt durch die Block-Difficulty. 100 % bedeutet exakt den erwarteten Aufwand, ein niedrigerer Wert einen schnelleren Fund. Einzelne Runden schwanken stark in beide Richtungen. Beobachte deshalb **Average Luck** im Dashboard. Dessen Tabelle **Pool Luck History** verlinkt die Seite über **View All**.

### Status

[Status](/status) ist die Zustandsseite des Pools. Sie zeigt, ob Pool, öffentliches HTTP, API, Stratum, Datenbank und Daemon online sind, wie aktuell die Jobvorlage ist, die Blockchain-Höhe, die Verfügbarkeit über die letzten 10 Minuten, 6 Stunden, 24 Stunden und 7 Tage sowie den Störungsverlauf. Zwei Systeme messen die Verfügbarkeit: ein Monitor auf dem Pool-Server und eine externe Prüfung auf Cloudflare.

## Was ist mit dem eigenen Dashboard von seine?

Der Pool zeigt seine Gutschriften an dich. Für den Live-Blick auf den Rechner zeigt das Terminal-Dashboard von seine Hashrate und Shares direkt bei ihrer Entstehung. Zusätzlich kann seine eine lokale Steuerungs-API mit REST, Server-Sent Events und Prometheus-Metriken unter `/metrics` bereitstellen, standardmäßig an `127.0.0.1:9977` gebunden. Die [Steuerungs-API-Anleitung](https://github.com/zpalmtree/seine/blob/master/docs/API.md) erklärt Optionen und Endpunkte. Viele Miner nutzen beides: die Metriken von seine je Rechner und die Pool-API für Guthaben und Auszahlungen.

## Wie nutzt du die API von bntpool.com?

<Answer>
Die API von bntpool.com liefert öffentliches JSON über HTTPS ohne Schlüssel. Rufe https://bntpool.com/api/miner/ mit deiner angehängten Adresse für Hashrate, Rigs, Shares und Auszahlungen auf. Ergänze /balance für Guthaben und nutze /api/stats für Pool- und Netzwerk-Hashrate. Beträge sind atomare Einheiten: Für BNT durch 100.000.000 teilen.
</Answer>

Alle genannten Seiten lesen aus diesen Endpunkten. Was du auf der Website siehst, kannst du daher selbst abrufen:

| Endpunkt | Rückgabewerte |
|---|---|
| `/api/stats` | Verbundene Miner, Pool-Hashrate, gefundene Blöcke, Netzwerk-Hashrate, aktuelle Höhe |
| `/api/stats/history?range=24h` | Verlauf der Pool-Hashrate (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Aktuelle Runde, durchschnittlicher Effort, letzte Runden |
| `/api/info` | Pool-Einstellungen: Stratum-Port, Vergütungsmodell und Fenster, Mindestauszahlung |
| `/api/blocks?status=confirmed` | Gefundene Blöcke, nach Status filterbar |
| `/api/payouts/recent` | Auszahlungsläufe mit Transaktions-Hashes |
| `/api/luck` | Effort und Dauer jeder Runde |
| `/api/status` | Dienstzustand, Verfügbarkeitszeiträume, Störungen |
| `/api/miner/{address}` | Deine Hashrate, Summen, Rigs, letzten Shares und Auszahlungen |
| `/api/miner/{address}/balance` | Geschätztes, bestätigtes und ausgezahltes Guthaben |
| `/api/miner/{address}/hashrate?range=24h` | Dein Hashrate-Verlauf |

Listenendpunkte akzeptieren `limit` (standardmäßig 25, höchstens 200) und `offset` und antworten mit `items` und `total`. Miner-Endpunkte erwarten eine vollständige Adresse. Kopiere sie nach einer @name-Suche auf My Stats aus dem Suchfeld.

So liest du deinen Miner im Terminal aus:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Mit `jq` extrahierst du einzelne Werte, etwa Hashrate und akzeptierte Shares oder die ausgezahlte BNT-Summe:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

<CTA target="pool" title="Starte das Mining und verfolge es live" text="Tritt bntpool.com in wenigen Minuten bei und verfolge Hashrate, Shares und Guthaben auf My Stats." />
