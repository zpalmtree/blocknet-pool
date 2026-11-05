---
title: Blocknet direkt auf eine Börse minen
description: 'Mine Blocknet auf bntpool.com direkt auf deine Einzahlungsadresse bei SafeTrade oder einer anderen Börse. Gib in seine dein Einzahlungs-Memo an, und der Pool zahlt auf dein Konto.'
tags: [Mining-Anleitung, bntpool, SafeTrade, Auszahlungen]
faq:
  - q: 'Kann ich Blocknet direkt auf eine Börse minen?'
    a: 'Ja. Starte seine v0.3.1 oder neuer mit der Einzahlungsadresse der Börse und deinem Einzahlungs-Memo, etwa ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com zahlt deine Belohnungen mit angehängtem Memo an diese Adresse, und die Börse schreibt sie deinem Konto gut.'
  - q: 'Warum fragt seine nach einem Auszahlungs-Memo?'
    a: 'Börsen wie SafeTrade nutzen eine gemeinsame Einzahlungsadresse für alle Kunden und unterscheiden Einzahlungen am Memo. seine kennt diese gemeinsamen Adressen. Gibst du eine davon ein, fragt es nach deinem Memo und speichert es mit der Adresse. Kopiere das Memo exakt von der Einzahlungsseite der Börse.'
  - q: 'Wie hoch ist die Mindestauszahlung an eine Börse auf bntpool.com?'
    a: 'Auszahlungen mit Memo beginnen bei 10 BNT, wie der Pool am 4. November 2026 meldete. Dein bestätigtes Guthaben sammelt sich auf dem Pool, bis es diesen Betrag erreicht, dann sendet der Pool es mit deinem Memo an die Börsenadresse. Auszahlungen an eine eigene Wallet beginnen bei 1 BNT.'
  - q: 'Wie prüfe ich mein Mining-Guthaben für die Börse?'
    a: 'Gib auf der Statistikseite von bntpool.com die Einzahlungsadresse der Börse ein, wähle Memo hinzufügen und gib dein Memo ein. Jedes Paar aus Adresse und Memo hat ein eigenes Guthaben. Du siehst also nur deine eigene Hashrate, deine Rigs und Auszahlungen, nicht die anderer Kunden mit derselben Börsenadresse.'
sourceHash: '8f73aae25c7a'
translation: draft
---

Diese Anleitung zeigt dir, wie du Blocknet auf bntpool.com direkt auf dein Börsenkonto minest. Du gibst seine die Einzahlungsadresse der Börse und dein Einzahlungs-Memo, und der Pool zahlt deine Belohnungen mit angehängtem Memo dorthin, sodass die Börse sie dir gutschreibt. Sie behandelt den Befehl, die Mindestauszahlung und die Prüfung deines Guthabens.

## Kannst du Blocknet direkt auf eine Börse minen?

<Answer>
Ja. bntpool.com zahlt Mining-Belohnungen an eine Einzahlungsadresse einer Börse, mit deinem Einzahlungs-Memo im Anhang. Starte seine v0.3.1 oder neuer mit der Börsenadresse und deinem Memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Auszahlungen mit Memo beginnen bei 10 BNT, und die Börse schreibt jede deinem Konto gut.
</Answer>

Mining auf eine Börse spart dir einen Schritt, wenn du das Geminte handeln willst. Belohnungen landen direkt auf deinem Börsenkonto, bereit zum Verkauf am [BNT/USDT-Markt](/blog/de/sell-blocknet-bnt-safetrade/), ohne Umweg über eine Wallet.

Börsen wie SafeTrade geben allen Kunden dieselbe Einzahlungsadresse und unterscheiden Einzahlungen an einem Memo, einem kurzen Code, der nur zu deinem Konto gehört. Deshalb ist das Memo wichtig: Es verknüpft eine Auszahlung mit dir. Der Pool führt für jedes Paar aus Adresse und Memo ein eigenes Guthaben, sodass sich deine Belohnungen nie mit denen anderer Miner auf derselben Börse vermischen.

## Wie richtest du Börsen-Mining in seine ein?

<Answer>
Kopiere die BNT-Einzahlungsadresse und dein Einzahlungs-Memo von der Einzahlungsseite der Börse. Starte seine mit beidem: --address für die Adresse und --payout-memo für das Memo. Gibst du bei der Abfrage beim ersten Start eine bekannte Börsenadresse ein, fragt seine von selbst nach dem Memo und speichert es. Prüfe im Log die Bestätigung des Pools und mine dann wie gewohnt.
</Answer>

1. **Hole deine Einzahlungsdaten.** Öffne auf der Börse die BNT-Einzahlungsseite und kopiere Einzahlungsadresse und Memo. Kopiere das Memo exakt: Großbuchstaben und führende Nullen zählen.
2. **Starte seine mit beidem.** Unter macOS oder Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Unter Windows in PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Lass seine es speichern.** seine speichert das Memo mit der Adresse, sodass spätere Starts weiter auf dein Börsenkonto zahlen. Tippst du bei der Abfrage beim ersten Start eine bekannte Börsenadresse ein, fragt seine nach dem Memo und speichert beides.

Die Seite [Get Started](/start) des Pools hat einen Abschnitt „An eine Börse auszahlen?“, der diesen Befehl für dich erstellt: Füge Adresse und Memo ein und kopiere das Ergebnis. seine verlangt ein Memo für gemeinsame Adressen bekannter Börsen wie die von SafeTrade. Dienste, Skripte und andere unbeaufsichtigte Miner müssen `--payout-memo` auf der Kommandozeile übergeben, weil es keine Abfrage gibt, die jemand beantworten könnte.

seine prüft das Memo beim Login mit dem Pool. bntpool.com bestätigt es, dann beginnt das Mining. Um deine Rigs zu benennen, übergib `--pool-worker rig-01` wie bei einer eigenen Adresse.

## Wie werden Börsenauszahlungen gezahlt?

Belohnungen nehmen denselben Weg wie jede bntpool.com-Auszahlung. Deine Shares verdienen einen Anteil an jedem Block, den der Pool in seinem sechsstündigen PPLNS-Fenster findet, und die Belohnung jedes Blocks wird nach 60 Blöcken bestätigt. Dein bestätigtes Guthaben sammelt sich dann auf dem Pool.

Der Endpunkt `/api/info` des Pools meldete am 4. November 2026 diese Mindestauszahlungen:

| Ziel | Mindestauszahlung |
|---|---|
| Börsenadresse mit Memo | 10 BNT |
| Eigene Wallet | 1 BNT |

Sobald dein bestätigtes Guthaben 10 BNT erreicht, sendet der Pool es mit deinem Memo an die Börsenadresse. Die Börse schreibt die Einzahlung nach ihrer eigenen Zahl an Bestätigungen gut; ihre Einzahlungsseite nennt, wie viele sie braucht. [So funktionieren PPLNS-Auszahlungen](/blog/de/how-pplns-payouts-work/) erklärt die Pool-Seite im Detail.

Solo-Mining funktioniert auch mit Memo. Setze `solo:` vor die Börsenadresse und behalte `--payout-memo` bei, dann zahlt der Pool die volle Belohnung jedes Blocks, den du findest, auf dein Börsenkonto. [Solo-Mining auf bntpool.com](/blog/de/solo-mine-blocknet-on-bntpool/) erklärt Solo-Belohnungen.

## Wie prüfst du dein Guthaben?

Öffne die [Statistikseite](/stats), füge die Einzahlungsadresse der Börse ein, wähle **Memo hinzufügen** und gib dein Memo ein. Die Seite zeigt dann deine eigene Hashrate, deine Rigs, dein Guthaben und deine Auszahlungen für dieses Paar aus Adresse und Memo. Andere Kunden derselben Börse haben eigene Paare und eigene Guthaben.

Jede Auszahlung unter **Recent Payouts** verlinkt auf ihre Transaktion im Explorer. Vergleiche sie mit der Einzahlungshistorie der Börse, um die Coins bis auf dein Konto zu verfolgen. [Deinen Miner auf bntpool.com überwachen](/blog/de/track-your-miner-on-bntpool/) erklärt den Rest der Statistikseite.

## Wie wechselst du zurück zu einer eigenen Wallet?

Starte seine einmal mit deiner neuen Adresse und `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine speichert die neue Adresse ohne Memo, und neue Shares zählen für deine eigene Wallet. Ein bereits dem Börsenpaar gutgeschriebenes Guthaben bleibt bei diesem Paar und wird an es ausgezahlt, sobald es das Minimum erreicht; du kannst es auf der Statistikseite verfolgen. [Mining-Wallet-Adresse ändern](/blog/de/change-mining-wallet-address/) erklärt, wie der Pool Arbeit nach einem Adresswechsel gutschreibt.

## Welche Setups unterstützen Memos?

Memos brauchen die direkte Verbindung von seine zum Pool, die Standardeinstellung. Der Daemon-Modus von seine mint mit deinem eigenen Node und zahlt an dessen Wallet, daher gibt es dort kein Memo. Der Farm-Proxy leitet keine Memos weiter, verbinde Börsen-Rigs also direkt mit dem Pool. seine mint nur auf einem Pool, der dein Memo bestätigt, daher wird deine Arbeit immer auf das Konto gezahlt, das du gewählt hast.

<CTA target="pool" title="Mine auf dein Börsenkonto" text="Starte seine mit deiner Einzahlungsadresse und deinem Memo, und bntpool.com zahlt deine Belohnungen ab 10 BNT direkt auf dein Börsenkonto." />
