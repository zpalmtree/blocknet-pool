---
title: 'Was ist Stratum? Wie Miner mit einem Pool sprechen'
description: 'Verfolge Stratum zwischen seine und bntpool.com: Anmeldung, Mining-Jobs, Share-Ziele, Einreichungen und Antworten sowie die Bedeutung der Verbindungsaktivität.'
tags: ['Mining-Grundlagen', 'Stratum', 'Pool-Mining']
faq:
  - q: 'Was macht Stratum beim Pool-Mining?'
    a: 'Stratum trägt die Kommunikation zwischen Miner und Pool: Es identifiziert die Sitzung, liefert Arbeit und überträgt Shares sowie Antworten. Der Miner berechnet Hashes lokal. Der Pool verbindet diese Arbeit mit seinem vorgeschalteten Node und führt die Abrechnung für sein Auszahlungssystem.'
  - q: 'Ist eine Stratum-Adresse eine Webadresse?'
    a: 'Die Verbindungen haben verschiedene Aufgaben. bntpool.com verwendet HTTPS für Webseiten und API-Daten; der dokumentierte Mining-Endpunkt ist stratum+tcp://bntpool.com:3333. Trage ihn in der Pool-Einstellung des Miners ein. Das erfolgreiche Öffnen der Webseite stellt noch keine Mining-Verbindung her.'
  - q: 'Bedeutet ein akzeptierter Stratum-Share eine Auszahlung?'
    a: 'Nein. Eine Share-Antwort meldet das Ergebnis einer Einreichung innerhalb der Sitzung. Verifizierung, netzwerkgültiger Block, Belohnungsberechtigung und abgeschlossene Auszahlung sind getrennte Schritte. Verfolge akzeptierte Arbeit unter My Stats; die Auszahlungsanleitung erklärt Bestätigungen, PPLNS und die Auszahlungsschwelle.'
sourceHash: '5cc5086d7b28'
translation: draft
---

Verfolge einen Mining-Job von bntpool.com zu seine und einen Share zurück zum Pool. Diese Kommunikation erklärt Verbindungsmeldungen, Job-Updates und Antworten. Die Anleitung beschreibt die Implementierung von bntpool.com; Endpunkt und Auszahlungsadresse richtest du mit der [Pool-Anleitung](/blog/de/mine-blocknet-on-bntpool/) ein.

## Was ist Stratum beim Mining?

<Answer>
Stratum ist die Kommunikationsschicht, über die ein Miner Pool-Arbeit empfängt und Ergebnisse einreicht. Bei seine und bntpool.com überträgt eine dauerhafte TCP-Verbindung JSON-Nachrichten für Anmeldung, Jobs und Shares. Dein Rechner berechnet Hashes lokal; der Pool holt Blockvorlagen von seinem Node und verarbeitet eingereichte Arbeit.
</Answer>

*Ein Miner sendet Anmeldung und Shares an einen Pool, erhält Jobs und Antworten, und der Pool tauscht Vorlagen und Blockkandidaten mit seinem Node aus.*

## Der Endpunkt bezeichnet die Mining-Verbindung

Der dokumentierte Endpunkt lautet `stratum+tcp://bntpool.com:3333`. Das Schema bestimmt den Transport, der Hostname den Dienst und der Port dessen Mining-Listener. Die Browser-Verbindung zu `https://bntpool.com` hat eine andere Aufgabe. Kopiere den vollständigen Mining-Endpunkt von [Get Started](/start) in die Pool-Einstellung, damit der Client die vorgesehene Verbindung öffnet.

Über die dauerhafte Verbindung können beide Seiten Nachrichten senden, sobald Arbeit vorliegt. bntpool verwendet zeilengetrenntes JSON; Anfrage-IDs ordnen Antworten den Anfragen zu. Dies beschreibt das Format dieses Pools: Ein Miner muss es, seine Fähigkeiten und Blocknets Hashalgorithmus unterstützen. Der [Protokollquelltext](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) dokumentiert die aktuellen Felder.

## Was passiert bei der Anmeldung?

Der Client sendet `login` mit Auszahlungsadresse, Rig-Name, Protokollversion und unterstützten Fähigkeiten. Der Pool prüft die Anmeldung und antwortet mit Protokolldaten und erforderlichen Fähigkeiten. Damit steht fest, wie die Sitzung Arbeit austauscht. Eine TCP-Verbindung ist der erste Schritt; eine akzeptierte Anmeldung bestätigt auch den Beginn der Anwendungskommunikation.

Der Rig-Name kennzeichnet einen Rechner unter einer Adresse. Verwende für mehrere Rigs unterschiedliche Namen und dieselbe gewünschte Auszahlungsadresse. Benötigt wird die Empfangsadresse, nicht die Wiederherstellungsphrase oder der private Ausgabeschlüssel. Nach akzeptierter Sitzung sendet der Pool bei verfügbarer Arbeit eine `job`-Nachricht, damit das Hashing beginnen kann.

## Welche Informationen enthält ein Mining-Job?

Ein Job enthält IDs für Zuweisung und Vorlage, Headerdaten, Blockhöhe, Share-Ziel, Netzwerkziel und einen Nonce-Bereich. Die Nonce ist der veränderte Wert beim Hashen. Der Bereich definiert den Suchraum der Sitzung. Die Job-ID ordnet eine spätere Einreichung der ursprünglichen Arbeit und deren Bedingungen zu.

Das Share-Ziel legt fest, welche Ergebnisse als Arbeitsnachweis eingereicht werden. Das Netzwerkziel bestimmt mögliche Blockkandidaten. Die meisten gültigen Shares dienen der Abrechnung, ohne zu Blöcken zu werden. Die [Schwierigkeitsanleitung](/blog/de/mining-difficulty-explained/) erklärt Zielgröße und erwartete Arbeit; das Protokoll liefert diese Ziele als Teil der Zuweisung.

## Jobs können bei offener Verbindung wechseln

Ein neuer Job bedeutet für sich genommen keinen Verbindungsabbruch. Der Pool kann bei geänderter Node-Vorlage aktualisierte Arbeit oder eine Zuweisung mit neuer Share-Schwierigkeit senden. Variable Schwierigkeit passt die Einreichungsschwelle an den beobachteten Arbeitsfluss an. Der Miner folgt der neuen Zuweisung; die Verbindung transportiert weiter Nachrichten in beide Richtungen.

Unterscheide Job- und Vorlagen-IDs in technischen Logs. Mehrere Zuweisungen können dieselbe Vorlage mit anderen Sitzungsdetails nutzen. Die Höhe allein beschreibt nicht jede Arbeitsänderung. Als Betreiber prüfst du, ob Jobs ankommen, Hashing voranschreitet und Einreichungen Antworten erhalten. Die [Tracking-Anleitung](/blog/de/track-your-miner-on-bntpool/) zeigt die zugehörige Aktivität.

## Was enthält eine Share-Einreichung?

Findet seine ein passendes Ergebnis, sendet es `submit` mit Job-ID, Nonce und behauptetem Hash. Der Pool verarbeitet es im Zuweisungskontext und antwortet mit der Anfrage-ID. Der behauptete Hash ist Teil der Nachricht; Verifizierung und Blockvalidierung bestimmen den weiteren Weg. Ein Blockkandidat wird zur Validierung an den Daemon weitergegeben.

Die Antwort kann Akzeptanz, Verifizierungsstatus und Schwierigkeit enthalten. Lies diese Felder getrennt, statt jede erfolgreiche Antwort als Blockfund zu betrachten. Pool-Abrechnung und Auszahlung folgen außerhalb dieses Austauschs. Die [Share-Anleitung](/blog/de/accepted-rejected-stale-shares/) erklärt Einreichungsergebnisse; die [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/) verbindet berechtigte Arbeit mit Belohnungen und Zahlungen.

## Wie hilft die Abfolge beim Prüfen eines Miners?

Prüfe der Reihe nach: Endpunkt erreicht, Anmeldung akzeptiert, Job empfangen, Hashing aktiv, Einreichungen beantwortet. Nach Verbindungsverlust verbindet sich der Client neu und baut eine Sitzung auf, bevor neue Arbeit eintrifft. Nutze genaue Logmeldung und Zeitpunkt zur Einordnung. Eine erreichbare Webseite beweist nicht, dass dieser Rechner den Mining-Port erreicht.

Öffne [My Stats](/stats) für die gewünschte Adresse und den [Poolstatus](/status) für den Dienstzustand. Gib Share-basierten Hashrate-Schätzungen nach Start oder Wiederverbindung Zeit für Beobachtungen. Trenne Protokoll und Auszahlungszeit: Anmeldung bereitet die Sitzung vor, Jobs liefern Arbeit, Shares melden Ergebnisse und die Abrechnung bestimmt die späteren Belohnungsschritte.

<CTA target="pool" title="seine mit bntpool.com verbinden" text="Nutze die Pool-Einrichtung und verfolge Jobs, Shares und dein Rig unter My Stats." />
