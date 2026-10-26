---
title: Was ist ein Mining-Pool?
description: Ein Mining-Pool bündelt die Hashrate vieler Miner und teilt jede Blockbelohnung unter ihnen auf. Erfahre, wie Pools, PPLNS, PPS und Solo-Mining funktionieren und worauf du achten solltest.
tags: [Mining-Grundlagen, Mining-Pools, PPLNS, bntpool]
sourceHash: 'a17a47ab1d73'
translation: draft
faq:
  - q: 'Verdiene ich durch den Beitritt zu einem Mining-Pool mehr?'
    a: 'Ein Pool lässt deine Hashrate und deinen Anteil an der Netzwerkarbeit unverändert. Er verändert den Eingang der Belohnungen. Allein wartest du in zufälligen Abständen auf ganze Blöcke. Im Pool erhältst du einen kleinen Anteil an jedem gefundenen Pool-Block. Zahlungen kommen dadurch häufig und in Beträgen, die deiner Hashrate entsprechen.'
  - q: 'Was unterscheidet PPLNS von PPS?'
    a: 'PPLNS zahlt einen Anteil an jedem tatsächlich gefundenen Pool-Block, aufgeteilt nach der Arbeit in einem zurückliegenden Zeitfenster. PPS zahlt für jeden akzeptierten Share einen festen Betrag nach seinem Erwartungswert, unabhängig davon, ob der Pool einen Block findet. bntpool.com nutzt PPLNS mit einem sechsstündigen Fenster.'
  - q: 'Brauche ich für Pool-Mining einen eigenen Node?'
    a: 'Nein. Der Pool betreibt den Node, erstellt Blockvorlagen, prüft Shares und reicht gefundene Blöcke ein. Auf bntpool.com brauchst du nur den Miner seine und eine Blocknet-Adresse oder einen Blocknet-ID-Namen. Starte seine, füge deine Adresse ein und akzeptiere den Standardpool. Dein Anteil an jedem Block wird an diese Adresse ausgezahlt.'
  - q: 'Wie starte ich das Mining auf bntpool.com?'
    a: 'Lade seine für Windows, macOS oder Linux von seineminer.com herunter, starte ihn und gib bei der Aufforderung deine Blocknet-Adresse oder deinen @name ein. bntpool.com ist der Standardpool; akzeptiere ihn und beginne mit dem Mining. Auf der Statistikseite des Pools findest du über deine Adresse Hashrate, Shares und Guthaben.'
---

Diese Anleitung erklärt, was ein Mining-Pool ist, warum die meisten Miner einem beitreten und wie er die Einnahmen jedes Miners bestimmt. Du lernst die gängigen Vergütungsmodelle PPLNS, PPS und Solo-Mining kennen und erfährst, was du prüfen solltest, bevor du deinen Miner mit einem Pool verbindest. Als durchgängiges Beispiel dient der Blocknet-Mining-Pool bntpool.com.

## Was ist ein Mining-Pool?

<Answer>
Ein Mining-Pool ist ein Server, der die [Hashrate](/blog/de/what-is-hashrate/) vieler Miner bündelt, damit sie gemeinsam Blöcke finden. Der Pool verteilt Arbeit am selben Block, erfasst die eingereichte Arbeit jedes Miners und teilt die Blockbelohnung entsprechend auf. Miner erhalten kleine, häufige Zahlungen statt seltener ganzer Blöcke.
</Answer>

Bei einer Proof-of-Work-Coin konkurrieren Miner um den nächsten Block: Sie hashen dessen Header mit unterschiedlichen Nonces, bis ein Ergebnis unter dem Zielwert des Netzwerks liegt. Jeder Hash ist ein unabhängiger Versuch mit derselben kleinen Erfolgschance. Wer den erfolgreichen Hash findet, erhält die Blockbelohnung.

Ein Pool macht daraus Teamarbeit. Alle mit bntpool.com verbundenen Miner arbeiten an Blöcken, die der Pool erstellt. Findet einer den erfolgreichen Hash, wird die Belohnung unter allen Minern aufgeteilt, deren Arbeit zum Fund beigetragen hat.

## Warum treten Miner einem Pool bei?

<Answer>
Miner treten Pools bei, um regelmäßig bezahlt zu werden. Allein hält ein typischer Miner nur einen winzigen Teil der Netzwerk-Hashrate und wartet womöglich Tage in zufälligen Abständen auf Blöcke. Im Pool findet die gebündelte Hashrate häufig Blöcke; jeder Miner verdient an jedem mit. So kommen die Einnahmen regelmäßig und entsprechend der eigenen Hashrate.
</Answer>

Bei Blocknet lässt sich die Rechnung einfach prüfen. Das Netzwerk strebt einen Block alle fünf Minuten an, also 288 Blöcke täglich. Allein erwartet ein Miner daher 288 × (seine Hashrate ÷ Netzwerk-Hashrate) Blöcke pro Tag.

Ein hypothetisches Beispiel mit runden Zahlen: Ein 50-H/s-Rig in einem Netzwerk mit 50.000 H/s hält 0,1 % des Netzwerks und findet im Mittel 0,288 Blöcke täglich, also etwa einen alle dreieinhalb Tage. Das ist nur ein Durchschnitt. Blockfunde sind zufällig; der tatsächliche Abstand kann erheblich kürzer oder länger sein.

In einem Pool mit insgesamt 5.000 H/s findet derselbe Rechner zusammen mit dem Pool etwa 28,8 Blöcke täglich, im Mittel einen alle 50 Minuten. Das Rig leistet 1 % der Pool-Arbeit und verdient deshalb 1 % jedes Blocks. Seine Arbeit bleibt gleich; die Belohnung trifft nun mehrmals täglich in kleinen Anteilen ein. [Pool- oder Solo-Mining für Blocknet](/blog/de/pool-vs-solo-mining-blocknet/) führt diesen Vergleich für größere Betriebe durch.

## Wie funktioniert ein Mining-Pool?

Jeder Pool folgt demselben grundlegenden Ablauf. Auf bntpool.com sieht er so aus:

1. **Der Pool erstellt eine Blockvorlage.** bntpool.com betreibt einen eigenen Blocknet-Node und ruft von ihm Blockvorlagen ab. Miner müssen daher keinen eigenen Node betreiben oder synchronisieren.
2. **Er sendet Arbeit an die Miner.** Sie verbinden sich über Stratum, das Protokoll zur Arbeitsverteilung in Pools, mit `stratum+tcp://bntpool.com:3333`. Jede Verbindung erhält einen Job aus der aktuellen Vorlage.
3. **Miner hashen und reichen Shares ein.** Ein Share ist ein Hash, der einen leichteren Zielwert des Pools erfüllt. Shares belegen die geleistete Arbeit.
4. **Ein Share, der auch das Netzwerkziel erfüllt, ist ein Block.** Der Pool prüft jeden Share gegen beide Ziele. Genügt er dem Netzwerkziel, reicht der Pool den Block bei seinem Node ein.
5. **Die Belohnung wird aufgeteilt und ausgezahlt.** Der Pool verteilt die Blockbelohnung nach seinem Vergütungsmodell und sendet jedem Miner dessen Anteil, sobald er bestätigt ist und die Mindestauszahlung erreicht.

## Was ist ein Share in einem Mining-Pool?

Ein Share ist ein Hash, der eine geringere Anforderung als ein echter Block erfüllt. Ein Block braucht einen Hash unter dem Netzwerkziel; ein Share nur einen unter dem wesentlich leichteren Share-Ziel des Pools. Die meisten Shares werden nie zu Blöcken, zeigen dem Pool aber regelmäßig, wie viel jeder Miner hasht.

Shares werden nach Difficulty gewichtet. Bei Blocknet bedeutet eine Difficulty von D ungefähr D erwartete Hashes bis zu einem Treffer. Ein Share mit Difficulty 60 steht damit für etwa 60 Hashes Arbeit. bntpool.com setzt für jede Verbindung eine Share-Difficulty und passt sie laufend an. Ziel sind etwa 10 Shares alle fünf Minuten; neue Verbindungen starten mit Difficulty 60. Ein schneller Server bekommt schwierigere Shares, ein Laptop leichtere. Beide erhalten Gutschriften nach der tatsächlich geleisteten Arbeit. Der Pool merkt sich die Difficulty jedes Rigs außerdem eine Stunde lang. Nach einem kurzen Neustart geht es dort weiter, wo es aufgehört hat.

## Gängige Vergütungsmodelle von Mining-Pools

Das Vergütungsmodell legt fest, wie ein Pool Shares in Einnahmen umsetzt. Dies sind die häufigsten Varianten.

| Modell | Wofür du bezahlt wirst | Funktionsweise |
|---|---|---|
| PPLNS (pay per last N shares) | Vom Pool gefundene Blöcke | Jede Blockbelohnung wird nach Share-Difficulty auf die Shares eines zurückliegenden Fensters verteilt |
| PPS (pay per share) | Jeden akzeptierten Share | Der Pool zahlt je Share einen festen Betrag nach dessen Erwartungswert, unabhängig von Blockfunden |
| Solo | Selbst gefundene Blöcke | Du minest mit deinem eigenen Node und behältst die volle Belohnung jedes gefundenen Blocks |

**PPLNS** blickt von jedem Pool-Block zurück. Auf bntpool.com sind die „letzten N Shares“ ein Fenster von sechs Stunden. Der Pool summiert die Difficulty aller akzeptierten Shares in diesen Stunden vor dem Block und gibt jedem Miner denselben Belohnungsanteil wie dessen Arbeitsanteil. Jeder Share zählt für jeden Block während seiner Zeit im Fenster. Kontinuierliche Miner haben deshalb stets ein volles Fenster hinter sich, und ein Neustart setzt nichts zurück. [So funktionieren PPLNS-Auszahlungen auf bntpool.com](/blog/de/how-pplns-payouts-work/) erklärt die Aufteilung Schritt für Schritt.

**PPS** bezahlt jeden Share beim Eingang nach seinem Erwartungswert: Share-Difficulty geteilt durch Netzwerk-Difficulty, multipliziert mit der Blockbelohnung. Der Pool-Betreiber übernimmt die Schwankungen der Blockfunde und zahlt Miner dazwischen aus seinem eigenen Guthaben.

**Solo-Mining** kommt ohne Pool aus. Dein Node erstellt die Vorlagen und jeder gefundene Block zahlt die volle Belohnung an deine Wallet. Das passt zu Betrieben, die groß genug sind, selbst häufig Blöcke zu finden. Der Miner seine unterstützt beides: Standardmäßig minet er auf bntpool.com, mit `./seine --mode daemon` dagegen mit deinem eigenen synchronisierten Blocknet-Node.

## Worauf solltest du bei einem Mining-Pool achten?

Beim Vergleich von Pools bestimmen diese Einzelheiten, wie du bezahlt wirst und wie leicht du das prüfen kannst:

- **Ein klar beschriebenes Vergütungsmodell.** Der Pool sollte Modell und Einstellungen, etwa das PPLNS-Fenster, nennen. bntpool.com zeigt sie unter [Get Started](/start) und veröffentlicht sie unter `https://bntpool.com/api/info`.
- **Bestätigungen und Mindestauszahlung.** Belohnungen warten normalerweise, bis der Block tief genug in der Kette liegt. Zahlungen erfolgen ab einem Mindestguthaben. Auf bntpool.com ist eine Belohnung nach 60 Blöcken bestätigt, entsprechend Blocknets Coinbase-Reife. Guthaben ab 0,1 BNT werden automatisch ausgezahlt.
- **Öffentliche, überprüfbare Zahlen.** Du solltest jeden gefundenen Block und jede Zahlung sehen können. bntpool.com zeigt sie auf den Seiten [Blocks](/blocks) und [Payouts](/payouts), den Effort jeder Runde auf [Luck](/luck) und den Dienstzustand auf [Status](/status).
- **Deine eigene Statistik.** Über deine Adresse solltest du Hashrate, Shares und Guthaben finden. Bei bntpool.com geht das auf der [Statistikseite](/stats); die [Anleitung zur Miner-Überwachung](/blog/de/track-your-miner-on-bntpool/) erklärt jede Karte.
- **Einfache Einrichtung.** Je weniger Einstellungen nötig sind, desto schneller minest du. bntpool.com ist der Standardpool von seine, der Threads, Kernel und Speicher selbst abstimmt. Statt einer vollständigen Adresse kannst du dich mit einem [Blocknet-ID-Namen](https://seineminer.com/de/blog/blocknet-id-pay-by-name/) wie `@name` anmelden.

## bntpool.com im Überblick

| Einstellung | bntpool.com |
|---|---|
| Auszahlungssystem | PPLNS |
| Share-Fenster | 6 Stunden |
| Bestätigungen vor Auszahlung | 60 Blöcke |
| Mindestauszahlung | 0,1 BNT |
| Stratum-Adresse | `stratum+tcp://bntpool.com:3333` |

Das [Pool-Dashboard](/) zeigt das aktuelle Bild: verbundene Miner, Pool-Hashrate, Netzwerk-Hashrate, gefundene Blöcke und Effort der laufenden Runde. In einer Momentaufnahme vom 25. Oktober 2026 um 02:25 Uhr EDT erreichte der Pool etwa 5.200 H/s bei rund 44.400 H/s im Netzwerk und 246 verbundenen Minern. Das sind ungefähr 12 % des Netzwerks und damit erwartete 34 Blöcke täglich, etwa einer alle 42 Minuten. Die Werte ändern sich ständig. Prüfe aktuelle Zahlen im Dashboard und nutze die [Anleitung zur Belohnungsschätzung](/blog/de/estimate-blocknet-mining-rewards/), um sie für deine Hashrate in BNT umzurechnen.

Zum Beitreten [lädst du seine herunter](https://seineminer.com/de/#download), startest ihn, fügst deine Blocknet-Adresse oder deinen `@name` ein und akzeptierst den Standardpool. Die [Mining-Anleitung für seine](https://seineminer.com/de/blog/how-to-mine-blocknet/) erklärt jeden Schritt; [Blocknet auf bntpool.com minen](/blog/de/mine-blocknet-on-bntpool/) behandelt Rig-Namen und die Kontrolle der ersten Shares.

<CTA target="pool" title="Tritt bntpool.com bei" text="Verbinde seine mit dem Pool und verdiene an jedem gefundenen Block mit. Dein Anteil wird automatisch ab 0,1 BNT an deine Adresse gesendet." />
