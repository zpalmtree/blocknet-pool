---
title: Mina Blocknet direttamente su un exchange
description: 'Mina Blocknet direttamente sul tuo indirizzo di deposito SafeTrade o di un altro exchange con bntpool.com. Aggiungi il memo di deposito in seine e la pool paga sul tuo conto.'
tags: [Guida al mining, bntpool, SafeTrade, Pagamenti]
faq:
  - q: 'Posso minare Blocknet direttamente su un exchange?'
    a: 'Sì. Avvia seine v0.3.1 o successivo con l’indirizzo di deposito dell’exchange e il tuo memo di deposito, come in ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com paga le tue ricompense a quell’indirizzo con il memo allegato, così l’exchange le accredita sul tuo conto.'
  - q: 'Perché seine chiede un memo di pagamento?'
    a: 'Exchange come SafeTrade usano un unico indirizzo di deposito condiviso da tutti i clienti e distinguono i depositi tramite il memo. seine conosce questi indirizzi condivisi, quindi quando ne inserisci uno ti chiede il memo e lo salva insieme all’indirizzo. Copia il memo esattamente dalla pagina di deposito dell’exchange.'
  - q: 'Qual è il pagamento minimo verso un exchange su bntpool.com?'
    a: 'I pagamenti con memo partono da 10 BNT, secondo quanto riportato dalla pool il 4 novembre 2026. Il saldo confermato si accumula sulla pool fino a quella cifra, poi la pool lo invia all’indirizzo dell’exchange con il tuo memo. I pagamenti verso un wallet personale partono da 1 BNT.'
  - q: 'Come controllo il saldo del mining verso un exchange?'
    a: 'Nella pagina statistiche di bntpool.com inserisci l’indirizzo di deposito dell’exchange, scegli Aggiungi memo e inserisci il tuo memo. Ogni coppia di indirizzo e memo ha un saldo proprio, quindi vedi solo il tuo hashrate, i tuoi rig e i tuoi pagamenti, non quelli degli altri clienti che usano lo stesso indirizzo dell’exchange.'
sourceHash: '8f73aae25c7a'
translation: draft
---

Questa guida mostra come minare Blocknet direttamente sul tuo conto di un exchange con bntpool.com. Dai a seine l’indirizzo di deposito dell’exchange e il tuo memo di deposito, e la pool paga lì le tue ricompense con il memo allegato, così l’exchange te le accredita. Copre il comando, il pagamento minimo e come controllare il saldo.

## Puoi minare Blocknet direttamente su un exchange?

<Answer>
Sì. bntpool.com paga le ricompense di mining a un indirizzo di deposito di un exchange con il tuo memo di deposito allegato. Avvia seine v0.3.1 o successivo con l’indirizzo dell’exchange e il tuo memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. I pagamenti con memo partono da 10 BNT, e l’exchange accredita ciascuno sul tuo conto.
</Answer>

Minare verso un exchange ti fa risparmiare un passaggio quando pensi di scambiare ciò che mini. Le ricompense arrivano direttamente sul tuo conto dell’exchange, pronte da vendere sul [mercato BNT/USDT](/blog/it/sell-blocknet-bnt-safetrade/), senza trasferimenti da un wallet nel mezzo.

Exchange come SafeTrade danno a tutti i clienti lo stesso indirizzo di deposito e distinguono i depositi tramite un memo, un breve codice unico per il tuo conto. Per questo il memo conta: è ciò che collega un pagamento a te. La pool tiene un saldo separato per ogni coppia di indirizzo e memo, quindi le tue ricompense non si mescolano mai con quelle degli altri miner che usano lo stesso exchange.

## Come configuri il mining verso un exchange in seine?

<Answer>
Copia l’indirizzo di deposito BNT e il tuo memo di deposito dalla pagina di deposito dell’exchange. Avvia seine con entrambi: --address per l’indirizzo e --payout-memo per il memo. Se inserisci un indirizzo di exchange noto alla richiesta del primo avvio, seine chiede il memo da solo e lo salva. Controlla nel log la conferma della pool, poi mina come al solito.
</Answer>

1. **Recupera i dati di deposito.** Sull’exchange apri la pagina di deposito BNT e copia l’indirizzo di deposito e il memo. Copia il memo esattamente: maiuscole e zeri iniziali contano.
2. **Avvia seine con entrambi.** Su macOS o Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Su Windows, in PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Lascia che seine lo salvi.** seine salva il memo insieme all’indirizzo, quindi gli avvii successivi continuano a pagare sul tuo conto dell’exchange. Se digiti un indirizzo di exchange noto alla richiesta del primo avvio, seine chiede il memo e salva entrambi.

La pagina [Get Started](/start) della pool ha una sezione «Mining verso un exchange?» che costruisce questo comando per te: incolla indirizzo e memo e copia il risultato. seine richiede un memo per gli indirizzi condivisi degli exchange noti, come quelli di SafeTrade. Servizi, script e altri miner senza supervisione devono passare `--payout-memo` sulla riga di comando, perché non c’è nessuna richiesta a cui rispondere.

seine verifica il memo con la pool al login. bntpool.com lo conferma, poi il mining parte. Per dare un nome ai tuoi rig, passa `--pool-worker rig-01` come faresti con un indirizzo personale.

## Come vengono pagati i pagamenti verso un exchange?

Le ricompense seguono lo stesso percorso di qualsiasi pagamento di bntpool.com. Le tue share guadagnano una parte di ogni blocco trovato dalla pool nella sua finestra PPLNS di sei ore, e la ricompensa di ogni blocco viene confermata dopo 60 blocchi. Il saldo confermato si accumula poi sulla pool.

L’endpoint `/api/info` della pool riportava questi pagamenti minimi il 4 novembre 2026:

| Destinazione | Pagamento minimo |
|---|---|
| Indirizzo di exchange con memo | 10 BNT |
| Wallet personale | 1 BNT |

Quando il saldo confermato raggiunge 10 BNT, la pool lo invia all’indirizzo dell’exchange con il tuo memo. L’exchange accredita poi il deposito dopo il proprio numero di conferme; la sua pagina di deposito indica quante ne servono. [Come funzionano i pagamenti PPLNS](/blog/it/how-pplns-payouts-work/) spiega in dettaglio il lato della pool.

Anche il mining in solo funziona con un memo. Metti `solo:` davanti all’indirizzo dell’exchange e mantieni `--payout-memo`, e la pool paga sul tuo conto dell’exchange l’intera ricompensa di ogni blocco che trovi. [Il mining in solo su bntpool.com](/blog/it/solo-mine-blocknet-on-bntpool/) spiega le ricompense in solo.

## Come controlli il saldo?

Apri la [pagina statistiche](/stats), incolla l’indirizzo di deposito dell’exchange, scegli **Aggiungi memo** e inserisci il tuo memo. La pagina mostra allora il tuo hashrate, i tuoi rig, il saldo e i pagamenti per quella coppia di indirizzo e memo. Gli altri clienti dello stesso exchange hanno le proprie coppie e i propri saldi.

Ogni pagamento in **Recent Payouts** rimanda alla sua transazione sull’explorer. Confrontalo con la cronologia dei depositi dell’exchange per seguire le monete fino al tuo conto. [Segui il tuo miner su bntpool.com](/blog/it/track-your-miner-on-bntpool/) illustra il resto della pagina statistiche.

## Come torni a un wallet personale?

Avvia seine una volta con il nuovo indirizzo e `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine salva il nuovo indirizzo senza memo, e le nuove share contano per il tuo wallet personale. Un saldo già accreditato alla coppia dell’exchange resta a quella coppia e le viene pagato quando raggiunge il minimo; puoi seguirlo nella pagina statistiche. [Cambia l’indirizzo del wallet di mining](/blog/it/change-mining-wallet-address/) spiega come la pool accredita il lavoro dopo un cambio di indirizzo.

## Quali configurazioni supportano i memo?

I memo richiedono la connessione diretta di seine alla pool, che è quella predefinita. Il modo daemon di seine mina sul tuo nodo e paga il wallet del nodo, quindi non ha memo. Il proxy per farm non trasmette i memo, quindi collega i rig per l’exchange direttamente alla pool. seine mina solo su una pool che conferma il tuo memo, quindi il tuo lavoro viene sempre pagato sul conto che hai scelto.

<CTA target="pool" title="Mina sul tuo conto dell’exchange" text="Avvia seine con indirizzo di deposito e memo, e bntpool.com paga le tue ricompense direttamente sul tuo conto dell’exchange da 10 BNT." />
