---
title: Blocknet auf bntpool.com minen
description: Starte auf bntpool.com, dem Standardpool von seine. Verbinde deine Adresse oder @name mit stratum+tcp://bntpool.com:3333, benenne Rigs und prüfe Statistiken.
tags: [Mining-Anleitung, bntpool, Blocknet ID, seine]
faq:
  - q: 'Wie lautet die Stratum-Adresse von bntpool.com?'
    a: 'Sie lautet stratum+tcp://bntpool.com:3333 und wird von seine beim ersten Start vorgeschlagen. Drücke bei der Pool-Abfrage Enter oder übergib sie mit --pool-url. Deine Blocknet-Adresse oder dein Blocknet-ID-Name ist der Login. Der Pool zahlt deine Belohnungen an diese Adresse.'
  - q: 'Kann ich mit einem Blocknet-ID-Namen auf bntpool.com minen?'
    a: 'Ja. Gib @name oder $name überall dort ein, wo seine oder der Pool eine Adresse erwartet. Der Pool prüft die signierte Zuordnung bei blocknet.id und schreibt Shares und Auszahlungen der vollständigen Adresse dahinter gut. Du brauchst seine v0.2.16 oder neuer. Setze den Namen auf der Kommandozeile in Anführungszeichen, damit die Shell ihn unverändert lässt.'
  - q: 'Brauche ich ein Konto für bntpool.com?'
    a: 'Nein. Es gibt keine Registrierung, kein Passwort und keine E-Mail. Deine Blocknet-Adresse oder ein darauf verweisender Blocknet-ID-Name ist Login und Auszahlungsziel. Starte seine damit und suche danach auf My Stats, um Hashrate, Shares, Rigs, Guthaben und Auszahlungen zu sehen.'
  - q: 'Können mehrere Computer an dieselbe Adresse minen?'
    a: 'Ja. Starte seine auf jedem Computer mit derselben Adresse oder demselben Namen und gib jedem Rig einen eigenen Namen. Der Pool zählt Shares, Guthaben und Auszahlungen je Adresse. Alle Rigs tragen so zu einem Guthaben und einer Auszahlung bei, während My Stats jedes Rig einzeln auflistet.'
sourceHash: '3126e8e7e396'
translation: draft
---

Diese Anleitung zeigt den Einstieg mit seine auf bntpool.com: Pool-Adresse, Anmeldung mit Blocknet-Adresse oder Blocknet-ID-`@name`, Rig-Namen und die Prüfung eingehender Shares. Falls seine noch nicht installiert ist, folge zuerst der [Installationsanleitung](https://seineminer.com/de/blog/how-to-mine-blocknet/) und kehre danach zurück.

## Wie lautet die Pool-Adresse von bntpool.com?

<Answer>
Die Stratum-Adresse lautet `stratum+tcp://bntpool.com:3333`. Der Miner seine schlägt sie beim ersten Start vor; Enter bei der Pool-Abfrage stellt die Verbindung her. Du meldest dich mit deiner Blocknet-Adresse oder einem Blocknet-ID-`@name` an. Ein Konto ist nicht nötig, und die Belohnungen gehen an diese Adresse.
</Answer>

Diese Einstellungen meldete der öffentliche Endpunkt `/api/info` am 25. Oktober 2026:

| Einstellung | Wert |
|---|---|
| Stratum-Adresse | `stratum+tcp://bntpool.com:3333` |
| Login | Blocknet-Adresse, `@name` oder `$name` |
| Auszahlungssystem | PPLNS mit einem 6-Stunden-Fenster |
| Bestätigungen vor Auszahlung | 60 Blöcke |
| Mindestauszahlung | 0,1 BNT |

Dieselben Werte stehen jederzeit auf [Get Started](/start).

## Warum ist bntpool.com der Standardpool von seine?

Beim ersten Start fragt seine nach einem Pool und trägt `stratum+tcp://bntpool.com:3333` vor. Pool und Miner passen praktisch zusammen:

- **Ein Protokoll.** bntpool.com nutzt das Stratum-Protokoll von seine, Version 2. Außer deiner Adresse musst du nichts konfigurieren.
- **Namen auf beiden Seiten.** Miner und Pool akzeptieren Blocknet-ID-Namen und prüfen denselben signierten Datensatz bei blocknet.id.
- **Versionsprüfung.** Jedes Rig meldet mit den Shares die Version von seine. Die Statistikseite markiert ältere Versionen mit „update available“ und verlinkt die Release-Seite.
- **Eine Einrichtungsanleitung.** Get Started erklärt Download und Start von seine mit einem kopierbaren Befehl.

## Wie verbindest du seine mit bntpool.com?

Beim ersten Start stellt seine zwei Fragen:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Füge deine Adresse ein oder tippe deinen Namen und bestätige bntpool.com mit Enter. Beide Antworten landen in `seine-data/seine.config.json`, sodass spätere Starts direkt minen. Auch ein gewählter Rig-Name wird dort gespeichert; dazu unten mehr.

Auf einem Headless-Rig, in einem Dienst oder Skript kann niemand antworten. Übergib beide Werte beim ersten Start als Optionen, wie auf Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Um Pool oder Adresse später zu ändern, bearbeite oder lösche `seine-data/seine.config.json` oder überschreibe die gespeicherten Werte mit `--address` und `--pool-url`.

## Kannst du an einen Blocknet-ID-@name minen?

<Answer>
Ja. bntpool.com akzeptiert einen Kurznamen als `@name` oder `$name` überall, wo eine Adresse möglich ist. Der Pool prüft die signierte Zuordnung von blocknet.id, validiert die Zieladresse und schreibt während der gesamten Verbindung Shares, Guthaben und Auszahlungen dieser vollständigen Adresse gut.
</Answer>

Groß- und Kleinschreibung spielt keine Rolle: `@Alice` und `@alice` sind derselbe Login. Namen werden seit seine v0.2.16 unterstützt, also von jeder aktuellen Version. Vor Mining-Beginn zeigt seine die aufgelöste Adresse, damit du sie mit deiner Wallet vergleichen kannst. Setze Namen auf der Kommandozeile in einfache Anführungszeichen; sonst würde die Shell `$name` als Variable ersetzen:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Wenn der Name später auf eine andere Wallet verweist, starte seine neu. Ein gespeicherter Name wird bei jedem Start aufgelöst und die Adresse für den gesamten Lauf beibehalten. Der Pool macht das je Verbindung. Bereits verdientes Guthaben bleibt bei der ursprünglichen Adresse und wird dorthin ausgezahlt: Das Umstellen eines Namens verschiebt keine Coins.

Registriere einen Namen bei [blocknet.id](https://blocknet.id). Die [Blocknet-ID-Anleitung](https://seineminer.com/de/blog/blocknet-id-pay-by-name/) erklärt Zahlungen per Name.

## Wie benennst du deine Rigs?

Jede Verbindung meldet neben der Adresse einen Rig-Namen. Beim ersten Start wählt seine `seine-` plus vier Ziffern und speichert den Namen. Derselbe Computer behält ihn nach Neustarts. Einen erkennbaren Namen setzt du so:

```sh
./seine --pool-worker rig-01
```

Der Pool speichert bis zu 64 Zeichen. Ein stabiler Name hilft nach Neustarts: Die Share-Difficulty wird eine Stunde lang anhand von Adresse und Rig-Name gespeichert. Beim Wiederverbinden geht es mit der bereits eingependelten Difficulty weiter.

## Können mehrere Rigs an eine Adresse minen?

Ja. Nutze auf jeder Maschine dieselbe Adresse oder denselben `@name` und unterschiedliche Rig-Namen. Der Pool zählt Shares, Guthaben und Auszahlungen je Adresse, sodass alles zu einem Guthaben und einer Auszahlung zusammenläuft. Jedes Rig hat eine eigene Statistikzeile; so erkennst du, ob alle Maschinen minen.

## Wie prüfst du, ob dein Miner arbeitet?

Das Terminal-Dashboard von seine zeigt Hashrate und akzeptierte Shares direkt auf der Maschine. Für die Pool-Sicht öffne [My Stats](/stats), gib Adresse, `@name` oder `$name` ein und drücke **Lookup**. Bei Namen erscheint die aufgelöste Adresse neben dem Suchfeld. Die Seite merkt sich die letzte Suche in diesem Browser. Die [Anleitung zur Miner-Überwachung](/blog/de/track-your-miner-on-bntpool/) erklärt jede Karte.

Achte auf drei Dinge:

- **Hashrate und Shares Accepted.** Die Hashrate zeigt, was der Pool von dir sieht; akzeptierte Shares steigen mit der eingereichten Arbeit.
- **Eine Zeile pro Rig.** Dort stehen Name, Hashrate, akzeptierte und abgelehnte Shares sowie der letzte Share-Zeitpunkt. Ein gerade eingegangener Share bestätigt Verbindung und Arbeit.
- **Recent Shares.** Die letzten Shares erscheinen mit Rig, Difficulty und Status.

Der Pool passt die Share-Difficulty je Verbindung während des Minings an. Auch eine kleine CPU liefert deshalb regelmäßig Shares; die ersten akzeptierten erscheinen binnen Minuten. Erwartbare Hardwarewerte stehen in der [Blocknet-Hardware-Anleitung](https://seineminer.com/de/blog/blocknet-mining-hardware-hashrate/).

Danach folgen Belohnungen. Estimated Rewards erscheinen, wenn der Pool einen Block findet, während deine Shares im Sechs-Stunden-Fenster liegen. Nach 60 Bestätigungen wandert dein Anteil zu Confirmed Rewards und wird ab mindestens 0,1 BNT automatisch ausgezahlt. [PPLNS-Auszahlungen](/blog/de/how-pplns-payouts-work/) erklärt die Aufteilung. Den gesamten Verlauf findest du unter [Blocks](/blocks) und [Payouts](/payouts).

## Was bietet Get Started?

[Get Started](/start) fasst alles auf einer Seite zusammen:

- **Pool-Informationen.** Stratum-Adresse zum Anklicken und Kopieren, Auszahlungssystem, Mindestbetrag und Bestätigungen, live aus dem Pool gelesen.
- **Schnellstart in drei Schritten.** Lade seine von [seineminer.com](https://seineminer.com/de/#download), starte einmal, gib Adresse oder Namen und Pool-URL ein und verfolge danach Hashrate und Guthaben auf My Stats.
- **Ein kopierbarer Befehl.** Der vollständige `./seine`-Befehl mit Pool-URL. Setze deine Adresse oder `'@yourname'` in Anführungszeichen ein.
- **Einrichtungshinweise.** Speicherort und Zurücksetzen der Konfiguration sowie Optionen zum Überschreiben.
- **Berechnung der Belohnungen.** Eine kurze PPLNS-Erklärung: Shares vor einem Block verdienen dessen Belohnung, spätere Arbeit zählt für folgende Blöcke. Belohnungen unbestätigter Blöcke werden bis zur Bestätigung getrennt angezeigt.
- **Screenshots** der Ersteinrichtung und des Mining-Dashboards, damit du vor dem Start weißt, was dich erwartet.

<CTA target="pool" title="Starte mit dem Mining auf bntpool.com" text="Verbinde seine mit deiner Adresse oder @name mit bntpool.com und verfolge eingehende Shares auf My Stats." />
