---
sourceHash: 'e64d7723349d'
translation: draft
title: Wie Mining Blocknet-Zahlungen absichert
description: Erfahre, wie Blocknet per Proof of Work die Zahlungshistorie auswählt, warum Bestätigungen Arbeit hinzufügen und was Nodes vor der Blockannahme prüfen.
tags: [Blocknet, Zahlungssicherheit, Proof of Work]
faq:
  - q: Kann mehr Rechenleistung eine Zahlung aus meiner Wallet autorisieren?
    a: Mining-Leistung hilft, konkurrierende Blöcke zu erzeugen. Das Ausgeben von Guthaben erfordert weiterhin gültige Autorisierung. Nodes prüfen Transaktionsnachweise und Bilanz, bevor sie einen Block akzeptieren. Auch ein schwererer Zweig muss diese Regeln erfüllen. Proof of Work ersetzt nicht die privaten Schlüssel, die zum Ausgeben deiner Outputs erforderlich sind.
  - q: Folgt Blocknet immer der Kette mit den meisten Blöcken?
    a: Das veröffentlichte Core v0.22.0 erfasst kumulierte Arbeit durch Addition der Difficulty jedes Blocks. Ein gültiger Zweig mit mehr Gesamtarbeit kann unter Beachtung der Reorganisationsregeln zur ausgewählten Kette werden. Die Blockanzahl allein reicht nicht, da Blöcke unterschiedliche Difficulty-Werte haben können. Arbeit und Gültigkeit zählen gemeinsam.
  - q: Sind Zahlungsbestätigungen und Pool-Shares dasselbe?
    a: Ein Pool-Share erfasst beim Pool eingereichte Mining-Arbeit. Eine Zahlungsbestätigung betrifft die Aufnahme einer Transaktion in die ausgewählte Blockchain und die darauf folgenden Blöcke. Shares dienen der Belohnungsverteilung; Netzwerkblöcke schreiben die Zahlungshistorie fort. Mehr eigene Shares fügen einer bestimmten Zahlung nicht unmittelbar Bestätigungen hinzu.
---

Verfolge eine BNT-Zahlung von ihrer Aufnahme in einen Block bis zu einer Historie, die durch weitere Mining-Arbeit gestützt wird. Diese Anleitung erklärt, warum das Netzwerk zwischen konkurrierenden Historien wählen muss, was Bestätigungen aussagen und wie diese Auswahl mit privater Transaktionsvalidierung zusammenwirkt. Beschrieben wird das veröffentlichte Core v0.22.0, geprüft am 28. Oktober 2026.

## Was schützt Mining, wenn du BNT sendest?

<Answer>
Mining hilft dem Netzwerk, eine gemeinsame, geordnete Zahlungshistorie auszuwählen. Jeder akzeptierte Block verweist auf seinen Vorgänger und trägt Proof of Work bei. Nodes prüfen vorgeschlagene Blöcke und Transaktionen und vergleichen gültige Zweige anhand ihrer Gesamtarbeit unter ihren Reorganisationsregeln. Spätere Blöcke fügen Arbeit hinter einer aufgenommenen Zahlung hinzu und verleihen ihrer Bestätigungshistorie praktische Bedeutung.
</Answer>

Eine gültige Zahlung benötigt sowohl Autorisierung als auch einen Platz im Kassenbuch. Deine Wallet liefert die Autorisierung zum Ausgeben deiner Mittel. Mining und Kettenauswahl helfen Nodes, sich darauf zu einigen, welche gültigen Transaktionen zur aktuellen Historie gehören. Beides ergänzt sich: Der Proof of Work eines Blocks macht eine ansonsten ungültige Zahlung nicht zulässig.

Die [Anleitung zur Regeldurchsetzung](https://seineminer.com/de/blog/who-controls-blocknet/) erklärt, wer entwickelt, vorschlägt und validiert. Hier steht die Zahlung selbst im Mittelpunkt: wie ihre Aufnahme Teil der Historie wird, der deine Wallet und andere Nodes folgen.

## Wie binden Blöcke eine Zahlungshistorie?

Ein Blockheader enthält den Hash des Vorgängerblocks und eine aus seinen Transaktionen abgeleitete Merkle-Wurzel. Die Wurzel bindet den Blockinhalt, der Vorgänger-Hash die frühere Historie. Eine geänderte Transaktion verändert die Wurzel und den Header, für den gerechnet wird. Nachfolgende Blöcke müssen über ihre eigenen Verweise auf diese veränderte Historie zeigen.

Betrachte eine Zahlung in einem Block, auf den mehrere akzeptierte Blöcke folgen. Um diesen Teil der Historie zu ersetzen, braucht es einen konkurrierenden Zweig aus gültigen, verknüpften Blöcken mit ausreichender Gesamtarbeit innerhalb der Reorganisationsregeln des Nodes. Eine Transaktion in einer lokalen Datei zu ändern, erzeugt diesen Zweig nicht. Die Arbeit gehört zu einer bestimmten Folge von Headern und Transaktionen.

Der [veröffentlichte Blockcode](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) definiert diese Headerfelder, Merkle-Prüfungen und Wege der Kettenauswahl. Die [Difficulty-Anleitung](/blog/de/mining-difficulty-explained/) erklärt, wie das Netzwerk die Arbeitsanforderung für jeden neuen Block festlegt.

## Warum zählt Gesamtarbeit mehr als die Blockanzahl?

Zwei Miner können unterschiedliche gültige Blöcke für denselben Vorgänger finden. Jeder Vorschlag hat seinen eigenen Hash; Nodes können in unterschiedlicher Reihenfolge davon erfahren. So können kurzzeitig konkurrierende Zweige entstehen. Das Netzwerk braucht eine Auswahlregel, die der Arbeit hinter einer Historie folgt und nicht der Reihenfolge ihrer Ankündigung durch einen Peer.

Core v0.22.0 berechnet die kumulierte Arbeit, indem es die Difficulty eines Blocks zur Gesamtarbeit seines Vorgängers addiert. Nach der Validierung kann ein Zweig mit strikt größerer Arbeit einen Wechsel der ausgewählten Kette auslösen. Eine gleich schwere Alternative löst diesen Wechsel über den Vergleich nicht aus. Die Difficulty selbst muss zum Vorgängerkontext des Zweigs passen; ein Miner kann nicht einfach eine größere Zahl in den Header schreiben.

Vergleiche beispielsweise zwei Zweige mit einem gemeinsamen früheren Abschnitt. Dieser Abschnitt trägt zu beiden Summen dieselbe Arbeit bei. Entscheidend sind die nach der Trennung angesammelte Arbeit sowie die Gültigkeits- und Reorganisationsprüfungen. Reines Zählen der Blöcke ohne Difficulty kann diesen Unterschied übersehen. Das ist ein gedanklicher Vergleich, kein Bericht über einen aktuellen Fork.

## Wie helfen Bestätigungen beim Verfolgen einer Zahlung?

Sobald eine Zahlung in der ausgewählten Kette enthalten ist, bauen spätere Blöcke auf dieser Historie auf. Ihre Tiefe wächst mit weiteren Blöcken. Um ihre Aufnahme zu ersetzen, wäre eine geeignete alternative Historie von vor dem betreffenden Block nötig, während der aktuelle Zweig weiter Arbeit sammelt. Deshalb sagen Aufnahme und anschließende Tiefe mehr aus als die bloße Verbreitung einer Transaktion.

Eine Höhe kennzeichnet eine Position, ein Hash einen bestimmten Block. Prüfe beim Verfolgen einer Zahlung den Transaktionsdatensatz und den enthaltenden Block und dann, ob dieser weiterhin zur ausgewählten Kette gehört. Ändert sich die ausgewählte Historie, können sich Aufnahme und Tiefe ebenfalls ändern. Bestätigungszähler können den enthaltenden Block mitzählen oder nur spätere Blöcke zählen. Beachte die Konvention der Oberfläche.

Core v0.22.0 setzt außerdem eine Reorganisationsgrenze durch: Ein Wechsel wird abgelehnt, wenn der gemeinsame Vorfahr unter der aktuellen Spitzenhöhe minus 100 liegt. Dies ist eine Regel der laufenden Implementierung, kein universelles Versprechen hinter einem angezeigten Zähler. Die [Anleitung zur Übertragungsdauer](https://seineminer.com/de/blog/blocknet-transfer-time/) behandelt normale Wallet-Ausgebbarkeit; [Bestätigungen von Mining-Belohnungen](/blog/de/block-confirmations-mining-rewards/) behandelt Coinbase-Reife und Pool-Buchhaltung.

## Wie lässt sich eine private Zahlung auf doppelte Ausgabe prüfen?

Blocknets veröffentlichtes Transaktionsformat verwendet ein an den Ausgabenachweis gebundenes Key Image. Die Validierung prüft diese Bindung und lehnt ein im relevanten Kettenzustand bereits ausgegebenes Key Image ab. So erkennen Nodes eine widersprüchliche Nutzung eines Outputs, während die Ringsignatur verbirgt, welches zulässige Ringmitglied der tatsächliche Input ist. Private Beträge durchlaufen weiterhin Bilanz- und Bereichsprüfungen.

Angenommen, ein Besitzer autorisiert widersprüchliche Transaktionen für denselben Output. Eine gültige akzeptierte Historie kann nicht beide Ausgaben dieses Outputs enthalten. Die Kettenauswahl zählt, weil konkurrierende Historien unterschiedliche Entscheidungen enthalten können. Die Transaktionsvalidierung zählt, weil jede Historie die Ausgaberegeln erfüllen muss. Mehr Arbeit ersetzt weder eine gültige Signatur noch lässt sie eine unausgeglichene Transaktion die Bilanzprüfung bestehen.

Diese Prüfungen stehen im [veröffentlichten Transaktionsvalidator](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). Die weiteren Datenschutzmechanismen erklärt die [Blocknet-Privatsphäre-Anleitung](https://seineminer.com/de/blog/blocknet-privacy-features/). Zahlungssicherheit verbindet gültiges Ausgeben, eine konsistente Historie und die Kryptografie zum Schutz der darin sichtbaren Informationen.

## Welche Rolle spielt Pool-Mining für die Zahlungssicherheit?

Ein Pool koordiniert die Blocksuche der Miner und reicht einen gelösten Kandidaten bei seinem vorgeschalteten Node ein. Ein Share, der das leichtere Pool-Ziel erfüllt, erfasst beigetragene Arbeit. Nur eine Lösung, die das Netzwerkziel erfüllt, kann nach der Validierung ein Netzwerkblock werden. Die [Pool-Erklärung](/blog/de/what-is-a-mining-pool/) trennt diese Ziele und ihren jeweiligen Zweck.

Dein Miner kann somit am Fortschreiben der Zahlungshistorie mitwirken, während der Pool deine Arbeit getrennt verbucht. Ein privates Pool-Guthaben, eine Share-Anzahl und eine bestätigte Wallet-Zahlung beschreiben verschiedene Dinge. Beginne mit der [Anleitung zur Pool-Verbindung](/blog/de/mine-blocknet-on-bntpool/), um teilzunehmen. Nutze den Transaktionsdatensatz deiner Wallet, wenn du eine bestimmte BNT-Zahlung verfolgst.

<CTA target="pool" title="Trage Arbeit zu Blocknet bei" text="Verbinde seine mit bntpool.com und verfolge deine Mining-Arbeit, Belohnungen und Zahlungen über die öffentlichen Werkzeuge des Pools." />
