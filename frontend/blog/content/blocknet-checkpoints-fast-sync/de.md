---
title: Blocknet-Nodes schnell mit bntpool.com-Checkpoints synchronisieren
description: Blocknet-Nodes synchronisieren mit checkpoints.dat von bntpool.com schneller. Erfahre, wie Checkpoints die Blockchain festlegen, und lade, prüfe und importiere die Datei.
tags: [Blocknet-Node, Checkpoints, Schnelle Synchronisierung, bntpool]
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Woher bekommt ein Blocknet-Node seine Checkpoints?'
    a: 'Von bntpool.com. Fehlt beim ersten Start checkpoints.dat im Datenverzeichnis, lädt der Blocknet-Daemon sie von https://bntpool.com/checkpoints.dat herunter und nutzt sie für die schnelle Synchronisierung. Du kannst sie auch auf der Checkpoint-Seite herunterladen, ihren SHA-256 prüfen und sie vor dem Node-Start ins Datenverzeichnis legen.'
  - q: 'Wie oft aktualisiert bntpool.com checkpoints.dat?'
    a: 'Der Pool-Daemon schreibt die Datei mit wachsender Blockchain neu, mit einem Eintrag für jeden 100. Block. Jeder Eintrag liegt mindestens 100 Blöcke unter der Spitze und damit jenseits der maximalen Reorganisationstiefe von Blocknet. Checkpoints decken nur endgültige Historie ab. Die Checkpoint-Seite zeigt letzte Höhe und Änderungszeit.'
  - q: 'Verändern Checkpoints mein Wallet-Guthaben oder meinen Verlauf?'
    a: 'Nein. Auch mit Checkpoints speichert der Node jeden Block und erfasst jede Ausgabe und jedes Key Image in seinen Indizes. Wallet-Scan und Guthaben funktionieren genau wie nach vollständiger Synchronisierung. Checkpoints erlauben nur, Proof of Work und Signaturen bereits durch Checkpoint-Hashes belegter Historie nicht erneut zu prüfen.'
  - q: 'Kann ich einen Blocknet-Node ohne Checkpoints synchronisieren?'
    a: 'Ja. Starte den Daemon mit --full-sync oder setze full_sync in der Blocknet-Konfiguration auf true. Dann überspringt der Node den Checkpoint-Download und prüft Proof of Work und Signaturen jedes Blocks ab Genesis erneut. Das dauert länger als mit Checkpoints, weshalb neue Nodes diese standardmäßig verwenden.'
---

Ein neuer Blocknet-Node muss alle Blöcke seit Genesis aufholen, bevor er eine Wallet oder einen Solo-Miner bedienen kann. Checkpoints beschleunigen diesen Vorgang. Diese Anleitung erklärt den Inhalt der Checkpoint-Datei von bntpool.com, wie der Daemon damit die langsamsten Prüfungen überspringt und trotzdem an die echte Blockchain gebunden bleibt, und wie du die Datei selbst herunterlädst, prüfst und lädst.

## Was sind Blocknet-Checkpoints?

<Answer>
Blocknet-Checkpoints sind bekannte Block-Hashes, einer für jeden 100. Block. bntpool.com stellt die Liste als checkpoints.dat bereit. Ein synchronisierender Node prüft die empfangenen Blöcke gegen diese Hashes und überspringt bis zum letzten Checkpoint die erneute Prüfung von Proof of Work und Signaturen. Die erste Synchronisierung wird dadurch wesentlich schneller.
</Answer>

Die Textdatei enthält pro Zeile ein Paar `height:hash`. Am 25. Oktober 2026 begannen ihre Zeilen so:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Jeder Hash ist die Block-ID, der SHA3-256-Hash seines Headers. Der eigene Daemon des Pools erzeugt die Datei aus seiner Blockchain und nimmt nur mindestens 100 Blöcke tiefe Blöcke auf. Hundert Blöcke sind Blocknets harte Finalitätsgrenze, die maximale erlaubte Reorganisationstiefe. Jeder Checkpoint liegt daher in endgültiger Historie.

Am 25. Oktober 2026 um 02:04 Uhr EDT enthielt die Datei 689 Checkpoints von Höhe 100 bis 68.900 und war 48.811 Byte groß. Sie wächst ungefähr alle 100 Blöcke um eine Zeile, bei fünfminütigen Blöcken etwa alle acht Stunden. Die [Checkpoint-Seite](/checkpoints) zeigt die aktuelle Höhe.

## Wie verwendet ein Blocknet-Node Checkpoints?

Nach dem Laden setzt der Node die letzte Checkpoint-Höhe als Ziel der schnellen Synchronisierung. Bis seine Blockchain diese Höhe erreicht, stützt er die zwei teuersten Prüfungen auf veröffentlichte Checkpoints und führt alle übrigen normal aus:

- **Der Proof of Work beruht auf den Checkpoints.** Jeder Blocknet-Block hat einen Argon2id-Proof-of-Work über 2 GiB Speicher. Ihn für jeden historischen Block neu zu berechnen ist teuer, und die Checkpoint-Hashes belegen diese Historie bereits.
- **Auch RingCT-Signaturen und Range Proofs beruhen darauf.** Die Kommentare im Daemon nennen die Prüfung von Ausgabe-Range-Proofs den größten einzelnen Aufwand der Synchronisierung: Jede Ausgabe enthält einen, auch die Coinbase jedes Blocks.
- **Alle anderen Prüfungen bleiben aktiv.** Blockverknüpfung, LWMA-Difficulty-Regel, Zeitstempelregeln, Blockgröße und -struktur, Double-Spend-Prüfungen über Key Images sowie die Bindung zwischen Eingaben und Signaturen werden normal geprüft. Ausgaben und Key Images gelangen wie üblich in die Indizes. Wallet-Scans und Guthaben entsprechen daher einer vollständigen Synchronisierung.

Übersprungen werden Prüfungen nur bei Blöcken, die die aktuelle Node-Spitze erweitern, und nur unterhalb des letzten Checkpoints. Danach prüft der Node jeden neuen Block vollständig, einschließlich Proof of Work.

### Wie Checkpoints die vertrauenswürdige Blockchain festlegen

Während der Synchronisierung zum letzten Checkpoint muss jeder Block auf einer Checkpoint-Höhe exakt den Hash der Datei haben. Ein anderer Hash wird mit einem Checkpoint-Mismatch-Fehler abgelehnt. Weil alle 100 Blöcke ein Checkpoint vorliegt, kann ein Peer den schnell synchronisierenden Node nicht auf eine andere Historie führen: Die nächste Checkpoint-Höhe würde die Abweichung aufdecken.

Checkpoints beschleunigen auch jeden Neustart. Sind bereits Blöcke gespeichert, findet der Daemon beim Start den höchsten passenden Checkpoint und prüft nur spätere Blöcke erneut, statt die ganze Blockchain ab Genesis durchzugehen.

## Lädt ein neuer Node Checkpoints automatisch herunter?

<Answer>
Ja. Fehlt beim Start des Blocknet-Daemons checkpoints.dat im Datenverzeichnis, lädt er die Datei von https://bntpool.com/checkpoints.dat, liest sie ein und synchronisiert schnell bis zum letzten Checkpoint. Der Download ist ein Best-Effort-Versuch: Scheitert er, startet der Node trotzdem und prüft die Blockchain vollständig.
</Answer>

Diese Adresse ist direkt im Blocknet-Core als Vorgabe hinterlegt. Neue Nodes synchronisieren daher ohne zusätzliche Einrichtung mit der Datei von bntpool.com. Der Daemon lädt sie nur, wenn noch keine existiert, begrenzt die Anfrage auf 15 Sekunden und den Download auf 32 MiB.

## Wie nutzt du bntpool.com-Checkpoints Schritt für Schritt?

Wenn du die Datei selbst abrufen und prüfen oder vor einer langen erneuten Synchronisierung eine alte Kopie aktualisieren möchtest, erledigst du das mit diesen Schritten.

### 1. Finde dein Datenverzeichnis

Die Checkpoint-Datei liegt im Datenverzeichnis des Nodes neben der Blockchain-Datenbank:

- **Manager `blocknet`:** `~/.config/bnt/data/mainnet/` unter macOS und Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` unter Windows, sofern du nicht `data_dir` in seiner Konfiguration setzt.
- **Direkt gestarteter Core:** `./blocknet-data-mainnet` im Startverzeichnis oder der mit `--data` angegebene Pfad.

### 2. Lade checkpoints.dat herunter

Öffne [Checkpoints](/checkpoints) und klicke auf **Download checkpoints.dat**, oder nutze ein Terminal. Dieses Beispiel schreibt ins Standard-Datenverzeichnis des Cores:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Ein Download auf eine bestehende Datei ersetzt sie. So aktualisierst du eine alte Kopie.

### 3. Prüfe den SHA-256

Die Checkpoint-Seite veröffentlicht den SHA-256 der aktuellen Datei. Berechne den Hash deiner Kopie und vergleiche:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

Unter macOS verwendest du stattdessen `shasum -a 256`. Der Hash ändert sich mit jedem neuen Checkpoint. Vergleiche deshalb direkt nach dem Download mit der Seite.

### 4. Prüfe einen Checkpoint stichprobenartig im Explorer

Jede Zeile lässt sich im öffentlichen [Blocknet-Explorer](https://explorer.blocknetcrypto.com) bestätigen. Öffne die Blockseite einer enthaltenen Höhe, etwa `explorer.blocknetcrypto.com/block/68900`, und vergleiche den Hash mit der Zeile für 68.900. Wir haben das am 25. Oktober 2026 für den letzten Checkpoint getan; die Hashes stimmten überein.

### 5. Starte den Node

Starte den Daemon oder starte ihn neu. Er liest beim Start `checkpoints.dat` und synchronisiert schnell bis zum letzten Checkpoint. Nutzt du die interaktive Core-Shell (`--cli`), lädt `load-checkpoints` die Datei in einen laufenden Node und meldet die Höhe der schnellen Synchronisierung:

```text
> load-checkpoints
```

Checkpoints helfen, solange ein Node zurückliegt. Hat deine Blockchain die letzte Checkpoint-Höhe bereits überschritten, prüft der Node ohnehin jeden Block vollständig. Eine neuere Datei ändert dann nichts.

## Wie prüfst du die Checkpoint-Datei aus einem Skript?

Der Pool veröffentlicht die Dateidaten ohne API-Schlüssel als JSON unter `https://bntpool.com/api/checkpoints`:

| Feld | Bedeutung |
|---|---|
| `available` | Ob der Pool eine Datei bereitstellen kann |
| `url` | Download-Adresse |
| `entries` | Anzahl der Checkpoints in der Datei |
| `latest_height`, `latest_hash` | Höchster Checkpoint und dessen Block-Hash |
| `bytes`, `sha256` | Dateigröße und SHA-256 |
| `updated_at` | Letzte Dateiänderung als Unix-Zeitstempel |

Der Download enthält die Header `X-Checkpoint-Height` und `X-Checkpoint-Sha256` sowie einen aus dem Hash gebildeten `ETag`. Ein Skript kann dadurch `If-None-Match` senden und bei unveränderter Datei schnell `304 Not Modified` erhalten:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Antworten können 60 Sekunden im Cache liegen. Für eine Datei, die sich etwa alle acht Stunden ändert, ist das ausreichend.

<Note>
Laut blocknetcrypto.com soll das geplante Faraday-Upgrade einen leichteren Proof of Work bringen, damit neue Nodes die Blockchain schneller aufholen. Bis dahin sind Checkpoints der schnelle Weg zu einem aktuellen Node.
</Note>

## Was kannst du mit einem synchronisierten Node tun?

Ein synchronisierter Node betreibt die Blocknet-Wallet und die Node-API. Mit ihm kommuniziert seine beim Solo-Mining über `--mode daemon`. [Pool- oder Solo-Mining für Blocknet](/blog/de/pool-vs-solo-mining-blocknet/) hilft zu beurteilen, ob Solo-Mining zu deiner Hardware passt. Möchtest du ohne eigenen Node starten, minet seine standardmäßig auf bntpool.com: Die [Installationsanleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) führt zu den ersten Shares; [Mining auf bntpool.com](/blog/de/mine-blocknet-on-bntpool/) erklärt die Pool-Seite.

<CTA target="pool" title="Mine im Pool, der Blocknet-Checkpoints bereitstellt" text="Tritt bntpool.com in wenigen Minuten bei und verfolge Hashrate, Shares und Auszahlungen live." />
