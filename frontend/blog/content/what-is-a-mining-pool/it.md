---
title: Cos’è una mining pool?
description: Una mining pool unisce l’hashrate di molti miner e divide ogni ricompensa. Scopri come funziona, PPLNS, PPS e solo e cosa controllare prima di scegliere.
tags: [Basi del mining, Pool di mining, PPLNS, bntpool]
faq:
  - q: 'Entrare in una pool aumenta quanto mino?'
    a: 'La pool lascia invariati il tuo hashrate e la tua quota del lavoro di rete. Cambia come ricevi le ricompense: da solo aspetti blocchi interi a intervalli casuali; in pool guadagni una piccola parte di ogni blocco trovato, con pagamenti frequenti che seguono il tuo hashrate.'
  - q: 'Qual è la differenza tra PPLNS e PPS?'
    a: 'PPLNS paga una quota di ogni blocco realmente trovato, divisa secondo il lavoro inviato in una finestra recente. PPS paga un importo fisso per ogni share accettata, basato sul valore atteso, che la pool trovi un blocco o meno. bntpool.com usa PPLNS su sei ore.'
  - q: 'Devo eseguire un nodo per minare in pool?'
    a: 'No. La pool gestisce nodo, template, verifica delle share e invio dei blocchi. Su bntpool.com servono solo seine e un indirizzo Blocknet o nome Blocknet ID. Avvia seine, incolla l’indirizzo e accetta la pool predefinita: la quota di ogni blocco viene pagata lì.'
  - q: 'Come inizio a minare su bntpool.com?'
    a: 'Scarica seine per Windows, macOS o Linux da seineminer.com, eseguilo e incolla indirizzo Blocknet o @name. bntpool.com è la pool predefinita: accettala e inizia. Cerca l’indirizzo nella pagina statistiche per vedere hashrate, share e saldo.'
sourceHash: 7b8c04cd86ae
translation: draft
---

Questa guida spiega cos’è una mining pool, perché molti miner la scelgono e come decide i guadagni. Vedrai PPLNS, PPS e solo e cosa controllare prima di collegare il miner, usando bntpool.com, la pool Blocknet, come esempio.

## Cos’è una mining pool?

<Answer>
È un server che unisce l’[hashrate](/blog/it/what-is-hashrate/) di molti miner per trovare blocchi insieme. Distribuisce lavoro sullo stesso blocco, misura quanto ciascuno invia e divide la ricompensa in proporzione. I miner ricevono piccoli pagamenti frequenti anziché rari blocchi interi.
</Answer>

In una moneta proof of work i miner competono per il prossimo blocco, calcolando l’hash del suo header con nonce diversi finché un risultato scende sotto il target di rete. Ogni hash è un tentativo indipendente con la stessa piccola probabilità; chi trova quello vincente riceve la ricompensa.

La pool trasforma la competizione in lavoro di squadra. Tutti i collegati a bntpool.com lavorano sui blocchi preparati dalla pool. Quando uno trova un hash vincente, la ricompensa viene divisa fra chi ha contribuito alla ricerca.

## Perché i miner entrano in una pool?

<Answer>
Per ricevere pagamenti regolari. Da solo, un miner tipico ha una piccola quota della rete e può attendere giorni fra blocchi casuali. L’hashrate combinato della pool trova blocchi spesso, e ogni miner ne guadagna una parte: i ricavi arrivano regolarmente e seguono l’hashrate.
</Answer>

Il calcolo su Blocknet è semplice: un blocco ogni cinque minuti, 288 al giorno, quindi da solo puoi aspettarti 288 × (tuo hashrate ÷ hashrate di rete) blocchi al giorno.

Con numeri tondi ipotetici, un rig a 50 H/s su una rete a 50.000 H/s detiene lo 0,1% e trova in media 0,288 blocchi al giorno, uno ogni tre giorni e mezzo. È solo una media: la scoperta è casuale e l’intervallo reale può essere molto più breve o lungo.

Metti lo stesso rig in una pool da 5.000 H/s: trova circa 28,8 blocchi al giorno, uno ogni 50 minuti. Il rig ha svolto l’1% del lavoro e guadagna l’1% di ciascuno. Il contributo è uguale, ma la ricompensa arriva in piccole parti molte volte al giorno. [Pool o solo per Blocknet](/blog/it/pool-vs-solo-mining-blocknet/) estende il confronto alle grandi installazioni.

## Come funziona una mining pool?

Tutte seguono lo stesso ciclo. Su bntpool.com:

1. **La pool prepara un template di blocco.** Gestisce un proprio nodo Blocknet e ne ottiene il template, senza richiedere ai miner di eseguire o sincronizzare nodi.
2. **Invia lavoro ai miner.** Si collegano tramite Stratum, il protocollo di distribuzione del lavoro, a `stratum+tcp://bntpool.com:3333`. Ogni connessione riceve un job dal template attuale.
3. **I miner calcolano hash e inviano share.** Una share soddisfa un target più facile scelto dalla pool e dimostra il lavoro svolto.
4. **Una share che soddisfa il target di rete è un blocco.** La pool controlla entrambe le soglie e, se sufficiente per la rete, invia il blocco al nodo.
5. **La ricompensa viene divisa e pagata.** La pool applica il proprio schema e invia la quota a ogni miner quando è confermata e supera il minimo.

## Cos’è una share?

È un hash che supera una soglia meno impegnativa di un blocco. Il blocco richiede un hash sotto il target di rete; la share solo sotto il target molto più facile della pool. La maggior parte non diventa un blocco, ma mostra regolarmente quanto lavoro svolge ogni miner.

Le share sono pesate per difficoltà. Su Blocknet, difficoltà D significa circa D hash attesi per raggiungere il target: una share di difficoltà 60 rappresenta circa 60 hash. bntpool.com imposta e adatta la difficoltà per connessione mirando a circa 10 share ogni cinque minuti, partendo da 60. Un server veloce riceve share difficili, un laptop più facili, e ciascuno è accreditato per il lavoro effettivo. La difficoltà di ogni rig viene ricordata per un’ora, così un rapido riavvio riparte dal valore precedente.

## Schemi di pagamento comuni

Lo schema è la regola che trasforma share in guadagni. Questi sono i più comuni.

| Schema | Cosa viene pagato | Funzionamento |
|---|---|---|
| PPLNS (pay per last N shares) | Blocchi trovati dalla pool | Ogni ricompensa è divisa fra share di una finestra recente, pesate per difficoltà |
| PPS (pay per share) | Ogni share accettata | Importo fisso basato sul valore atteso, anche senza blocchi trovati |
| Solo | Blocchi trovati da te | Mini sul tuo nodo e conservi l’intera ricompensa |

**PPLNS** guarda indietro da ogni blocco. Su bntpool.com le “ultime N share” sono sei ore: somma la difficoltà delle share accettate e assegna la stessa frazione della ricompensa della frazione di lavoro. Ogni share conta per tutti i blocchi trovati durante la sua permanenza, quindi chi mina costantemente ha sempre una finestra piena e un riavvio non azzera nulla. [I pagamenti PPLNS su bntpool.com](/blog/it/how-pplns-payouts-work/) spiegano la divisione passo passo.

**PPS** paga ogni share all’arrivo secondo il valore atteso: difficoltà della share divisa per difficoltà di rete, per ricompensa. L’operatore assume le oscillazioni delle scoperte e paga dal proprio saldo fra un blocco e l’altro.

**Il mining in solo** salta la pool. Il tuo nodo prepara i template e ogni blocco paga tutta la ricompensa al wallet. È adatto a installazioni abbastanza grandi da trovare spesso blocchi. seine supporta entrambi: bntpool.com per impostazione predefinita e `./seine --mode daemon` per il tuo nodo sincronizzato.

## Cosa cercare in una mining pool?

Questi dettagli determinano come vieni pagato e quanto è facile verificarlo:

- **Uno schema chiaro.** La pool deve dichiarare schema e impostazioni, come la finestra PPLNS. bntpool.com li elenca su [Get Started](/start) e `https://bntpool.com/api/info`.
- **Conferme e pagamento minimo.** Le ricompense attendono una profondità sufficiente e partono oltre una soglia. Su bntpool.com sono confermate dopo 60 blocchi, come la maturità coinbase, e saldi da 0,1 BNT vengono pagati automaticamente.
- **Numeri pubblici verificabili.** Devi poter vedere ogni blocco e pagamento. bntpool.com li mostra su [Blocks](/blocks) e [Payouts](/payouts), l’effort dei round su [Luck](/luck) e la salute dei servizi su [Status](/status).
- **Le tue statistiche.** Cerca l’indirizzo per hashrate, share e saldi nella [pagina statistiche](/stats); la [guida al monitoraggio](/blog/it/track-your-miner-on-bntpool/) spiega ogni scheda.
- **Configurazione semplice.** Meno impostazioni, prima mini. bntpool.com è la pool predefinita di seine, che ottimizza thread, kernel e memoria da solo; puoi accedere con un [nome Blocknet ID](https://seineminer.com/it/blog/blocknet-id-pay-by-name/) come `@name` al posto dell’indirizzo completo.

## bntpool.com in breve

| Impostazione | bntpool.com |
|---|---|
| Schema di pagamento | PPLNS |
| Finestra delle share | 6 ore |
| Conferme prima del pagamento | 60 blocchi |
| Pagamento minimo | 0,1 BNT |
| Indirizzo Stratum | `stratum+tcp://bntpool.com:3333` |

La [dashboard](/) mostra miner collegati, hashrate pool e rete, blocchi trovati ed effort attuale. Il 25 ottobre 2026 alle 02:25 EDT, la pool aveva circa 5.200 H/s su circa 44.400 H/s di rete e 246 miner collegati: circa il 12%, sufficiente ad attendersi 34 blocchi al giorno, uno ogni 42 minuti circa. I dati cambiano continuamente: leggi quelli attuali e usa la [guida alle stime](/blog/it/estimate-blocknet-mining-rewards/) per convertirli in BNT con il tuo hashrate.

Per entrare, [scarica seine](https://seineminer.com/it/#download), eseguilo, incolla indirizzo o `@name` e accetta la pool predefinita. La [guida seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) copre ogni passaggio; [minare su bntpool.com](/blog/it/mine-blocknet-on-bntpool/) spiega nomi rig e prime share.

<CTA target="pool" title="Entra in bntpool.com" text="Collega seine alla pool e guadagna una parte di ogni blocco trovato, inviata automaticamente al tuo indirizzo da 0,1 BNT." />
