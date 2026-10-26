---
title: 'Akzeptierte, abgelehnte und veraltete Mining-Shares'
description: 'Verstehe akzeptierte, abgelehnte und veraltete Shares auf bntpool.com, typische Fehlermeldungen und den Einfluss der Share-Schwierigkeit auf Mining-Statistiken.'
tags: ['Mining-Anleitung', 'Shares', 'Fehlersuche']
faq:
  - q: 'Bedeutet ein akzeptierter Share einen gefundenen Block?'
    a: 'Der Pool hat ein eingereichtes Ergebnis gemäß seinen Share-Regeln angenommen. Ein Netzwerkblock muss ein separates Netzwerkziel erreichen und vom Daemon akzeptiert werden. Belohnungen folgen Blockfund, Validierung und Auszahlung. Die Bestätigung eines Shares ist deshalb keine Zahlungsbenachrichtigung.'
  - q: 'Warum wird ein veralteter Share abgelehnt?'
    a: 'Stale job bedeutet, dass die Einreichung zu einer nicht mehr zulässigen Zuweisung gehört, etwa nach Ablauf oder nach der Schonfrist eines alten Templates. Prüfe neue Jobs und wieder akzeptierte Shares. Wiederholte Meldungen erfordern eine Prüfung von Verbindung und Miner-Version.'
  - q: 'Verringert höhere Share-Schwierigkeit meine Belohnung?'
    a: 'Die Schwierigkeit verändert die Arbeit pro anrechenbarem Share und dessen erwartete Häufigkeit. Vergleiche schwierigkeitsgewichtete Arbeit in gleichen Zeiträumen statt bloßer Share-Zahlen. Die tatsächliche Belohnung hängt außerdem vom PPLNS-Fenster, anrechenbarer Arbeit, Blockfunden und Validierungsregeln ab.'
sourceHash: 5be830208e6c
translation: draft
---

Share-Ergebnisse im Miner-Log helfen dir zu entscheiden, ob du weiter minen, die Verbindung prüfen oder wiederholte Ablehnungen untersuchen solltest. Hier werden bntpool-Antworten und die Schwierigkeit hinter den Zählern erklärt. Nutze zunächst die [Pool-Einrichtung](/blog/de/mine-blocknet-on-bntpool/), falls du noch nicht verbunden bist, oder die [Tracking-Anleitung](/blog/de/track-your-miner-on-bntpool/#zero-hashrate) bei fehlender Adressaktivität.

## Was ist ein akzeptierter Mining-Share?

<Answer>
Ein Share ist ein Ergebnis für ein vom Pool zugewiesenes Ziel. Eine akzeptierte Antwort bestätigt die Aufnahme in die Share-Verarbeitung und zeigt, dass Arbeit den Pool erreicht. Blockfund, Share-Prüfung und Auszahlungsberechtigung sind getrennte Schritte. Die Annahme allein bedeutet daher noch keine Zahlung.
</Answer>

Zwischen Einreichungen berechnet der Miner viele Hashes. Die meisten erreichen das Share-Ziel nicht und werden nicht eingereicht. Der Akzeptiert-Zähler zählt Einreichungsergebnisse, nicht alle CPU- oder GPU-Hashes. Die [Hashrate-Anleitung](/blog/de/what-is-hashrate/) unterscheidet lokale Zählung von der Pool-Schätzung anhand eingereichter Arbeit.

## Wie unterscheiden sich Shares von Blöcken?

Der Pool weist deiner Verbindung Jobs und eine Share-Schwierigkeit zu. Ein passendes Ergebnis weist Arbeit dieser Schwierigkeit nach. Ein Blockkandidat muss außerdem das Netzwerkziel erreichen und die Annahmeprüfungen des Daemons bestehen. Ein Share-Fund bedeutet nicht, dass ein Block zur Kette hinzugefügt wurde.

bntpool.com erfasst verifizierte und vorläufige Shares. Auch eine akzeptierte Einreichung kann vorläufig sein; die Auszahlungsgewichtung berücksichtigt Validierung und Berechtigung. Nutze Annahmen als Verbindungssignal und [Meine Statistiken](/stats) für erfasste Arbeit und Belohnungen. Die [PPLNS-Anleitung](/blog/de/how-pplns-payouts-work/) beschreibt die Schritte nach einem Pool-Block.

## Was bedeuten abgelehnte und veraltete Shares?

Eine abgelehnte Einreichung hat eine Prüfung nicht bestanden. Der Grund hilft mehr als der Zähler allein. Stale job ist eine Ablehnungskategorie: Die Zuweisung ist nicht mehr zulässig. Vergleiche den genauen Text mit gleichzeitigen Ereignissen wie Neuverbindung oder neuem Job.

| Meldung | Bedeutung und nächste Prüfung |
|---|---|
| `stale job` | Die Zuweisung fehlt, ist abgelaufen, wurde über die Schonfrist hinaus ersetzt oder gehört zu einem nicht mehr verfügbaren Template. Prüfe neue Jobs und wieder akzeptierte Shares. |
| `duplicate share` | Dasselbe Template mit derselben Nonce wurde bereits eingereicht. Prüfe wiederholte Sendungen; dieselbe Arbeit zählt nicht erneut. |
| `low difficulty share` | Der Hash erreicht das zugewiesene Share-Ziel nicht. Prüfe bei Wiederholung Miner-Version sowie Job- und Schwierigkeitsverarbeitung. |
| `nonce out of assigned range` | Die Nonce liegt außerhalb des zugewiesenen Bereichs. Prüfe Miner-Version und eigene Proxys oder Mining-Integrationen. |

Ein veralteter Share betrifft die Arbeit einer Zuweisung, ein verwaister Block dagegen seine Position in der Blockchain. Trenne Share-Log und [Blockliste](/blocks) bei der Diagnose. Eine Stale-job-Meldung allein sagt nicht aus, dass der Pool einen verwaisten Block gefunden hat.

## Warum ändert sich die Share-Schwierigkeit?

<Answer>
Variable Schwierigkeit, oft Vardiff genannt, passt das Ziel einer Verbindung an deren beobachtete Share-Aktivität an. Höhere Schwierigkeit bedeutet meist weniger Shares mit mehr Arbeit je Share; niedrigere Schwierigkeit meist mehr Shares mit weniger Arbeit. Vergleiche gewichtete anrechenbare Arbeit im selben Zeitraum statt nur die Anzahl.
</Answer>

Rechenbeispiel: Zehn anrechenbare Shares mit Schwierigkeit 100 ergeben dieselbe Gesamtschwierigkeit wie ein Share mit 1.000, nämlich 1.000. Das zeigt die Gewichtung, verspricht aber weder eine Belohnung noch genaue Ankunftszeiten. Shares treffen in wechselnden Abständen ein; Validierungsregeln bestimmen ihre Anrechenbarkeit.

Für die Einreichung zählt die Schwierigkeit der Zuweisung. Vergleiche ältere Ergebnisse nicht allein mit der zuletzt angezeigten Schwierigkeit. Der Pool bewahrt Zuweisungsdaten zur Prüfung auf. Eine normale Anpassung ist kein Fehler; leichter eingestellte Shares erzeugen keine zusätzliche Rechenarbeit.

## Wie liest du eine Ablehnungsquote?

Vergleiche akzeptierte und abgelehnte Einreichungen im selben Zeitraum und Umfang. Miner-Zähler können eine Sitzung abdecken, Pool-Zähler einen anderen Zeitraum oder mehrere Geräte. Ein Neustart verändert das lokale Zeitfenster, löscht aber nicht die Pool-Historie deiner Adresse.

99 akzeptierte und eine abgelehnte Einreichung ergeben rechnerisch 1% Ablehnungen nach Anzahl. Das entspricht nicht automatisch 1% verlorener Belohnung: Schwierigkeiten und Anrechenbarkeit können variieren. Prüfe Gründe, Zeitpunkt und fortlaufend akzeptierte Arbeit, bevor du Einstellungen änderst.

## Was tun bei wiederholten Ablehnungen?

Beginne mit einem betroffenen Gerät. Notiere Miner-Version, genaue Meldung, Beginn und ob neue Jobs ankommen. Prüfe [Status](/status) auf Störungen und danach die Geräteverbindung auf Unterbrechungen. Aktualisiere über die [offiziellen seine-Releases](https://github.com/zpalmtree/seine/releases) und ändere zum Vergleich jeweils nur eine Sache.

Eigene Proxys oder Integrationen müssen Job, Nonce-Bereich und Schwierigkeit beim Weiterleiten erhalten. Bleibt das Problem bestehen, sende einen kurzen Logauszug und Gerätedetails über den Supportkanal des Projekts. Entferne Geheimnisse und private Kontodaten; eine Wallet-Wiederherstellungsphrase ist zur Share-Diagnose nie erforderlich.

## Wie bestätigst du den wieder funktionierenden Betrieb?

Achte auf neue Jobs und akzeptierte Shares. Öffne [Meine Statistiken](/stats) mit derselben Empfangsadresse wie im Miner und prüfe aktuelle Geräteaktivität. Lass vor einem Mittelwertvergleich mehr Arbeit in die Hashrate-Schätzung einfließen. Verfolge danach geschätzte Belohnungen, Guthaben und Auszahlungen als getrennte Schritte der PPLNS-Anleitung.

<CTA target="pool" title="Mit bntpool.com verbinden" text="Folge der Einrichtung, bestätige akzeptierte Shares und verfolge die Aktivität über deine Empfangsadresse." />
