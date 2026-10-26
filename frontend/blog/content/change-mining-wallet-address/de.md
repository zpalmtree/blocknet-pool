---
title: 'Die Wallet-Adresse für Blocknet-Mining ändern'
description: 'Ändere die Empfangsadresse deiner Blocknet-Miner, prüfe neue Shares auf bntpool.com und verfolge bisherige Belohnungen weiterhin unter der alten Wallet-Adresse.'
tags: ['Mining-Anleitung', 'Wallet', 'Auszahlungen']
faq:
  - q: 'Wird das alte Pool-Guthaben beim Adresswechsel übertragen?'
    a: 'Das neue Mining-Ziel ordnet künftig eingereichte Arbeit der neuen Adresse zu. Vorhandene Shares, Gutschriften und Auszahlungen bleiben mit der ursprünglichen Adresse verbunden. Prüfe beide Adressen getrennt in Meine Statistiken und behalte Zugriff auf die alte Wallet, während du verbleibende Belohnungen verfolgst.'
  - q: 'Muss ich jeden Computer umstellen?'
    a: 'Ja. Jeder Mining-Prozess verwendet sein eigenes konfiguriertes Ziel. Stelle zuerst einen Computer um und prüfe ihn, anschließend die anderen einschließlich gespeicherter Konfiguration und Startargumenten. Unterschiedliche Rig-Namen zeigen, welche Geräte bereits Arbeit unter der neuen Adresse einreichen.'
  - q: 'Aktualisiert eine geänderte Blocknet ID einen laufenden Miner?'
    a: 'seine löst die gespeicherte ID beim Start auf und behält diese Adresse während des Laufs einschließlich Neuverbindungen. Starte nach einer geänderten Zuordnung jeden Miner neu und prüfe die vollständige Adresse. Frühere Arbeit und Guthaben werden dadurch nicht neu zugeordnet.'
sourceHash: '645b2eff8a61'
translation: draft
---

Ändere das Ziel künftiger Mining-Arbeit und behalte frühere Belohnungen im Blick. Diese Anleitung gilt für eine bestehende seine-Verbindung zu bntpool.com. Für den ersten Anschluss nutze die [Pool-Anleitung](/blog/de/mine-blocknet-on-bntpool/), für eine neue Empfangswallet die [Wallet-Anleitung](https://seineminer.com/de/blog/blocknet-wallet-for-mining/).

## Wie ändere ich die Mining-Adresse?

<Answer>
Stoppe einen Miner, ändere die Empfangsadresse in seiner Konfiguration oder seinem regulären Startbefehl und starte ihn neu. Prüfe vollständige Zieladresse und akzeptierte Shares, dann die Adresse auf bntpool.com. Wiederhole dies für weitere Rigs. Frühere Shares und Belohnungen bleiben bei der damals aufgezeichneten Adresse.
</Answer>

*Neue Mining-Arbeit erreicht die zweite Wallet; frühere Belohnungsdaten bleiben mit der ersten verbunden.*

## Beide Ziele vor dem Wechsel festhalten

Kopiere die neue Adresse direkt aus der vorgesehenen Wallet. Der Miner benötigt die öffentliche Adresse oder registrierte Blocknet ID, keine Wiederherstellungsphrase oder privaten Schlüssel. Bewahre die ursprüngliche vollständige Adresse auf: Sie bleibt der Suchschlüssel für frühere Pool-Aktivität.

Öffne Meine Statistiken für die alte Adresse und notiere ausstehende Belohnungen, bestätigtes Guthaben, letzte Auszahlungen und aktive Rig-Namen. Das dokumentiert den Übergang; die neue Adressseite enthält nicht den bisherigen Verlauf. Behalte Zugriff auf die ursprüngliche Wallet, während du restliche Belohnungen und Zahlungen verfolgst.

## Einen Miner und seinen Startbefehl ändern

Stoppe einen Miner und prüfe seinen normalen Startweg. Bei vorhandener Konfiguration übersteuert `--address` den gespeicherten Wert, ohne ihn zu ersetzen. Das PowerShell-Beispiel setzt das Ziel für diesen Lauf. Ersetze `@yourname` durch deine neue ID oder vollständige Adresse und behalte bewährte Pool-, Datenpfad- und Hardwareargumente.

```powershell
.\seine.exe --address "@yourname"
```

Unter Linux oder macOS nutze die folgende Syntax aus deinem normalen Arbeitsverzeichnis. Beide Beispiele starten echtes Mining; wähle das passende Betriebssystem. Die bestehende Einrichtung liefert andere Verbindungswerte. Behalte den Rig-Namen, damit du den Rechner anhand seiner Shares unter der neuen Adresse erkennst.

```sh
./seine --address "@yourname"
```

## Die neue Adresse beim normalen Neustart verwenden

Bearbeite bei gestopptem Miner das Feld `address` in `seine.config.json`. Erhalte andere Felder und gültiges JSON. Prüfe das Datenverzeichnis: `./seine-data` ist relativ zum Arbeitsverzeichnis; `--data-dir` kann einen anderen Pfad wählen. Bearbeite die tatsächlich vom normalen Startweg verwendete Datei.

Aktualisiere oder entferne altes `--address` aus Verknüpfung, Skript, Aufgabe oder Dienst. Ein erfolgreicher manueller Befehl ändert diese Startwege nicht. Starte regulär und prüfe erneut. Die [seine-Konfigurationsanleitung](https://seineminer.com/de/blog/configure-seine-miner/) erklärt Prioritäten und warum ein anderes Arbeitsverzeichnis eine andere Datei auswählen kann.

## Was gilt bei einer Blocknet ID?

seine v0.2.37 löst `@name` beim Start auf und validiert es. Die vollständige Adresse bleibt während dieses Laufs einschließlich Pool-Neuverbindungen fest. Nach geänderter Namenszuordnung starte jeden Miner neu und prüfe das Ziel. Eine Neuverbindung des laufenden Prozesses wiederholt die Startauflösung nicht.

Notiere alte und neue vollständige Adresse, auch wenn der lesbare Name gleich bleibt. Die Buchhaltung folgt der aufgelösten Adresse; eine neue Zuordnung leitet alte Shares oder Guthaben nicht um. Schlägt die Auflösung fehl, prüfe die Zuordnung oder verwende bewusst die in deiner Wallet geprüfte vollständige Adresse.

## Alle Computer unter der neuen Adresse prüfen

Prüfe Ziel und akzeptierte Einreichungen, dann suche die neue vollständige Adresse in Meine Statistiken. Gleiche Rig-Namen und aktuelle Shares ab. Neue Hashrate-Schätzungen benötigen Arbeit und Zeit; kontrolliere zuerst deren Eingang. Die [Statistik-Anleitung](/blog/de/track-your-miner-on-bntpool/) erklärt Zähler und fehlende Rigs.

Nach erfolgreicher Prüfung des ersten Computers folgen die übrigen. Eindeutige Rig-Namen erleichtern die Kontrolle. Teste überall den regulären Neustart, damit ein alter Startbefehl später nicht wieder das vorherige Ziel nutzt. Die [Anleitung für mehrere Computer](/blog/de/mine-multiple-computers-one-wallet/) erklärt die Gruppierung unter einer Empfangsadresse.

## Was passiert mit alten Belohnungen?

Vorhandene Gutschriften und Zahlungen bleiben der alten Adresse zugeordnet. Frühere berechtigte Shares können noch in passenden PPLNS-Fenstern zählen; ausstehende Blockbelohnungen folgen den Bestätigungsregeln. Hashrate null bedeutet nicht abgeschlossene Buchhaltung. [PPLNS](/blog/de/how-pplns-payouts-work/) und [Bestätigungen](/blog/de/block-confirmations-mining-rewards/) erklären aufgezeichnete Arbeit, reifende Belohnungen und Zahlungen.

Ein Zielwechsel vereint keine Guthaben und umgeht keine Auszahlungsschwelle. Prüfe aktuelle Pool-Regeln und beide Adressseiten, bis die verbleibenden Beträge klar sind. Auch Rig-Umbenennung überträgt keine Belohnungen. Bestätige abschließend, dass alle vorgesehenen Miner neue Arbeit zur neuen Adresse senden und alte Wallet samt Verlauf erhalten bleiben.

<CTA target="pool" title="Mining-Einrichtung auf bntpool.com prüfen" text="Kontrolliere Ziel und akzeptierte Arbeit jedes Rigs und verfolge alte sowie neue Belohnungen getrennt." />
