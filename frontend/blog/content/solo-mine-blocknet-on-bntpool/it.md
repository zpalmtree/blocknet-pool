---
title: Mina Blocknet in solo su bntpool.com
description: 'Mina Blocknet in solo senza gestire un nodo. Metti solo: davanti al tuo indirizzo in seine e bntpool.com ti paga l’intera ricompensa di ogni blocco che trovi.'
tags: [Guida al mining, Mining in solo, bntpool, seine]
faq:
  - q: 'Come si mina Blocknet in solo su bntpool.com?'
    a: 'Aggiorna seine alla v0.3.1 o successiva e metti solo: davanti al tuo indirizzo di pagamento, per esempio ./seine --address solo:YOUR_ADDRESS. seine si collega a bntpool.com come sempre, e la pool ti paga l’intera ricompensa di ogni blocco trovato dal tuo rig con i pagamenti normali. Non serve gestire un nodo.'
  - q: 'Mi serve un nodo Blocknet mio per minare in solo?'
    a: 'No. Con il prefisso solo:, bntpool.com gestisce il nodo, prepara i template dei blocchi e invia i blocchi che trovi. Sul tuo rig gira solo seine. Il modo daemon di seine è l’altra via per minare in solo, sul tuo nodo sincronizzato, ed è adatto a chi ne gestisce già uno.'
  - q: 'Posso usare un nome Blocknet ID per il mining in solo?'
    a: 'Sì. Usa solo:@name o solo:$name al posto di solo:ADDRESS e mettilo tra virgolette sulla riga di comando, così la shell lo passa invariato. La pool risolve il nome nel suo indirizzo completo, poi accredita i tuoi blocchi in solo e li paga a quell’indirizzo come qualsiasi altro pagamento.'
  - q: 'Come torno alle ricompense della pool?'
    a: 'Togli il prefisso solo: dall’indirizzo e riavvia seine. Il rig torna a condividere le ricompense PPLNS di ogni blocco trovato dalla pool. Ogni rig sceglie la modalità quando si collega, quindi puoi tenere alcuni rig in solo e altri sulle ricompense condivise con lo stesso indirizzo.'
sourceHash: '623b1e0b356a'
translation: draft
---

Questa guida mostra come minare Blocknet in solo su bntpool.com: aggiungi un prefisso al tuo indirizzo in seine e la pool ti paga l’intera ricompensa di ogni blocco trovato dai tuoi rig. Non installi né sincronizzi un nodo. Lo configurerai, vedrai come vengono pagati i blocchi in solo e scoprirai dove seguirli sulla pool.

## Che cos’è il mining in solo su bntpool.com?

<Answer>
Il mining in solo su bntpool.com ti paga solo per i blocchi trovati dai tuoi rig, e ognuno paga l’intera ricompensa del blocco al tuo indirizzo. La pool continua a gestire il nodo, a preparare i template dei blocchi e a inviare i tuoi blocchi, quindi ottieni ricompense in solo con la stessa configurazione del mining in pool. Lo attivi mettendo solo: davanti al tuo indirizzo.
</Answer>

Nel normale mining in pool, bntpool.com divide ogni blocco tra i miner le cui share rientrano nella finestra PPLNS di sei ore, così guadagni una parte di ogni blocco trovato dalla pool. Un rig in solo invece tiene l’intera ricompensa dei propri blocchi e non guadagna nulla dai blocchi trovati da altri. Entrambi usano lo stesso server, lo stesso indirizzo e gli stessi pagamenti. Cambia solo il login.

Il mining in solo tramite la pool ti dà ricompense in solo senza il lavoro di un nodo:

- **Nessun nodo da gestire.** Il nodo della pool segue la catena, fornisce template aggiornati e invia i tuoi blocchi risolti.
- **Pagamenti automatici.** I tuoi blocchi vengono pagati al tuo indirizzo o nome Blocknet ID come qualsiasi altro pagamento di bntpool.com.
- **Statistiche in tempo reale.** Rig, hashrate e blocchi compaiono nella [pagina statistiche](/stats) della pool.
- **Un prefisso per cambiare.** Togli `solo:` e riavvia seine per tornare alle ricompense condivise.

Il modo daemon di seine è l’altra via per minare in solo, sul tuo nodo Blocknet sincronizzato. [Pool o solo per Blocknet](/blog/it/pool-vs-solo-mining-blocknet/) confronta le tre opzioni e mostra ogni quanto il tuo hashrate trova un blocco.

## Come inizi a minare in solo con seine?

<Answer>
Installa seine v0.3.1 o successivo, poi avvialo con solo: davanti al tuo indirizzo Blocknet, per esempio ./seine --address solo:YOUR_ADDRESS su macOS o Linux. seine si collega a bntpool.com, la sua pool predefinita, e la dashboard indica il tuo account come Solo. Puoi anche digitare solo: e il tuo indirizzo quando seine chiede un indirizzo di pagamento al primo avvio.
</Answer>

1. **Aggiorna seine.** I login in solo richiedono seine v0.3.1 o successivo. [Scarica seine](https://seineminer.com/it/#download), oppure lascia che lo installino gli aggiornamenti automatici.
2. **Avvia seine con il prefisso.** Su macOS o Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Su Windows, in PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine mina su bntpool.com per impostazione predefinita. La pagina [Get Started](/start) della pool mostra lo stesso comando con l’indirizzo della pool scritto per esteso.
3. **Controlla la dashboard.** Il pannello del saldo indica il tuo account come **Solo**, e il log mostra la conferma della pool che vieni pagato per i blocchi trovati da questo indirizzo.

Il prefisso funziona ovunque seine accetti un indirizzo: la richiesta al primo avvio, la configurazione salvata, l’API di controllo locale e i rig del proxy per farm. seine lo salva insieme all’indirizzo, quindi gli avvii successivi continuano a minare in solo.

## Puoi minare in solo con un nome Blocknet ID?

Sì. Un nome Blocknet ID funziona come un indirizzo completo. Scrivi `solo:@name` o `solo:$name` e mettilo tra virgolette, così la shell lo passa invariato:

```sh
./seine --address 'solo:@yourname'
```

La pool risolve il nome su blocknet.id e paga i tuoi blocchi in solo all’indirizzo associato. Per dare un nome a un rig, aggiungilo a un indirizzo completo dopo un punto, come in `solo:YOUR_ADDRESS.rig1`, oppure passa `--pool-worker rig1` con qualsiasi indirizzo o nome. Il prefisso accetta maiuscole e minuscole, quindi funziona anche `SOLO:`.

## Come vengono pagati i blocchi in solo?

Quando il tuo rig trova un blocco, la pool lo registra come blocco in solo. Quando arriva a 60 blocchi di profondità, circa cinque ore, la pool accredita sul tuo saldo l’intera ricompensa del blocco, al netto della commissione della pool. La commissione è dello 0% al 4 novembre 2026. Il saldo viene poi pagato al tuo indirizzo con i pagamenti normali della pool.

I pagamenti seguono le stesse regole del mining in pool. Ogni blocco attende 60 conferme, così una breve riorganizzazione della catena non può annullare una ricompensa già pagata, e [conferme dei blocchi e ricompense di mining](/blog/it/block-confirmations-mining-rewards/) spiega il motivo. L’endpoint `/api/info` della pool indicava un pagamento minimo di 1 BNT il 4 novembre 2026, e una ricompensa di blocco Blocknet è molto superiore, quindi un blocco in solo confermato viene pagato secondo il normale calendario dei pagamenti della pool. La [pagina pagamenti](/payouts) elenca ogni pagamento inviato dalla pool.

Se mini verso un indirizzo di deposito di un exchange con un memo di pagamento, il solo funziona allo stesso modo. Il memo e il suo pagamento minimo si applicano ai tuoi blocchi in solo come alle ricompense condivise.

Il lavoro del tuo rig conta comunque per hashrate, effort del round e fortuna della pool, e un blocco in solo chiude il round della pool come qualsiasi altro blocco. Le share in solo non entrano mai nella finestra PPLNS, quindi gli altri miner non guadagnano nulla dai tuoi blocchi e tu non guadagni nulla dai loro.

## Ogni quanto un rig in solo trova un blocco?

Blocknet punta a un blocco ogni cinque minuti, cioè 288 blocchi al giorno. Un rig in solo ne trova in media 288 × (il tuo hashrate ÷ hashrate di rete) al giorno. Leggi l’hashrate di rete sulla [home page di bntpool.com](/) e il tuo sulla dashboard di seine, poi dividi un giorno per il risultato per ottenere il tempo medio tra i tuoi blocchi.

Le ricompense in solo arrivano come blocchi interi, quindi sono adatte ai miner il cui hashrate trova blocchi spesso: grandi farm, parchi di server e chiunque abbia una grande quota della rete. I rig più piccoli guadagnano in media lo stesso con le ricompense PPLNS condivise, pagate in quote regolari da ogni blocco trovato dalla pool. [Pool o solo per Blocknet](/blog/it/pool-vs-solo-mining-blocknet/) sviluppa esempi con tre hashrate.

## Dove vedi i tuoi rig e blocchi in solo?

Cerca il tuo indirizzo nella [pagina statistiche](/stats). Puoi incollarlo con o senza il prefisso `solo:`. La pagina segnala poi il tuo lavoro in solo:

- **Workers** elenca ogni rig con una colonna Tipo che indica **Solo** per i rig che hanno inviato share in solo nella finestra dell’hashrate.
- **Estimated rewards** segnala i tuoi blocchi in solo mentre attendono le conferme.
- **Blocks** e **Luck** mostrano una colonna Tipo, Pool o Solo, prima dello stato di ogni blocco. La [pagina blocchi](/blocks) elenca ogni blocco trovato dalla pool, dal più recente.

[Segui il tuo miner su bntpool.com](/blog/it/track-your-miner-on-bntpool/) illustra il resto della pagina statistiche.

## Puoi combinare rig in solo e in pool?

Sì. Ogni rig sceglie la modalità quando si collega, quindi un indirizzo può avere rig in solo e rig in pool allo stesso tempo. Una farm può minare in solo sui server più grandi e tenere i desktop sulle ricompense condivise, tutto pagato su un unico saldo. La pool tiene separati i due tipi di lavoro: una share trovata su una connessione in solo conta solo per i blocchi in solo di quell’indirizzo.

Per riportare un rig in pool, togli `solo:` dal suo indirizzo, nel comando o nella configurazione salvata, e riavvia seine. Le share già inviate in modalità solo restano in solo, e le nuove share entrano subito nella finestra PPLNS.

<CTA target="pool" title="Mina in solo su bntpool.com" text="Aggiungi solo: al tuo indirizzo in seine e tieni l’intera ricompensa di ogni blocco trovato dai tuoi rig, senza un nodo da gestire." />
