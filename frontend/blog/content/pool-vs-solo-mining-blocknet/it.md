---
title: Mining Blocknet in pool o in solo
description: Scegli pool o solo per Blocknet. Calcola i blocchi attesi al giorno, confronta ricompense condivise e in solo su bntpool.com e configura il mining in solo.
tags: [Guida al mining, Mining in solo, bntpool, PPLNS]
faq:
  - q: 'Ogni quanto troverei un blocco Blocknet in solo?'
    a: 'In media 288 × (il tuo hashrate ÷ hashrate di rete) blocchi al giorno, perché Blocknet punta a un blocco ogni cinque minuti. Con 50 H/s su 50.000 H/s di rete sono 0,288 blocchi al giorno, circa uno ogni tre giorni e mezzo. Leggi l’hashrate attuale sulla dashboard bntpool.com.'
  - q: 'Devo eseguire un nodo per minare su bntpool.com?'
    a: 'No. La pool gestisce il proprio nodo, prepara i template e invia i blocchi trovati. Ti servono solo seine e un indirizzo Blocknet o nome Blocknet ID. Esegui seine, incolla l’indirizzo e accetta la pool predefinita: la tua quota verrà pagata lì.'
  - q: 'seine può minare Blocknet in solo?'
    a: 'Sì, in due modi. Metti solo: davanti all’indirizzo, come in ./seine --address solo:YOUR_ADDRESS, e bntpool.com ti paga l’intera ricompensa di ogni blocco che trovi, senza un nodo da gestire. Oppure avvia seine con --mode daemon per minare sul tuo nodo Blocknet sincronizzato.'
  - q: 'Posso passare da pool a solo più avanti?'
    a: 'Sì. Il download è lo stesso per ogni opzione. Aggiungi o togli il prefisso solo: dall’indirizzo, oppure aggiungi --mode daemon per il tuo nodo, e riavvia seine quando vuoi cambiare. Le share già inviate per le ricompense condivise restano nella finestra PPLNS di sei ore e continuano a guadagnare finché vi rimangono.'
sourceHash: 'aecc3e8f992f'
translation: draft
---

Questa guida aiuta a scegliere fra le ricompense condivise della pool e il mining in solo di Blocknet, in solo tramite bntpool.com o sul tuo nodo. Calcolerai quanti blocchi trova il tuo hashrate da solo, vedrai come la pool li trasforma in pagamenti regolari e imparerai a configurare ogni opzione con seine.

## Conviene minare Blocknet in pool o in solo?

<Answer>
Il calcolo è uguale: la tua quota di hashrate di rete determina la quota di blocchi. Su bntpool.com guadagni una parte di ogni blocco trovato, con pagamenti regolari al ritmo della pool. In solo conservi la ricompensa intera di ogni blocco trovato, una scelta adatta a grandi installazioni. bntpool.com offre entrambe le opzioni.
</Answer>

Per la maggior parte dei miner bntpool.com è il punto di partenza semplice. È la pool predefinita di seine: basta l’indirizzo, senza eseguire o sincronizzare un nodo. Anche il solo è pienamente supportato. Metti `solo:` davanti all’indirizzo e bntpool.com ti paga l’intera ricompensa di ogni blocco trovato dai tuoi rig, senza un nodo da gestire. seine ha anche un modo daemon che mina direttamente sul tuo nodo Blocknet. Il resto della guida mostra come scegliere in base all’hardware.

## Quanti blocchi troveresti in solo?

<Answer>
Blocknet punta a un blocco ogni cinque minuti, 288 al giorno. Da solo puoi aspettarti 288 × (il tuo hashrate ÷ hashrate di rete) blocchi al giorno. Dividi un giorno per quel numero per ottenere il tempo medio tra blocchi. Leggi l’hashrate di rete attuale sulla dashboard bntpool.com.
</Answer>

La formula funziona perché Blocknet regola la difficoltà affinché la rete trovi un blocco ogni 300 secondi in media. La difficoltà è circa il numero di hash attesi prima del successo. Ogni hash ha la stessa probabilità: la frazione di hash prodotti dalla tua macchina è la frazione di blocchi che puoi aspettarti.

Servono due numeri:

- **Il tuo hashrate.** seine lo mostra durante il mining e la [pagina statistiche](/stats) della pool dopo che hai iniziato a minare lì.
- **Hashrate di rete.** La [dashboard bntpool.com](/) lo mostra in Network Hashrate, accanto a Pool Hashrate. Entrambi cambiano: controllali quando fai il calcolo.

Ogni blocco paga la ricompensa del suo mese. Il [calendario delle ricompense](/blog/it/blocknet-block-reward-schedule/) le elenca mese per mese.

## Un esempio di calcolo

<Note>
Questi sono numeri tondi ipotetici per mostrare il calcolo, non valori in tempo reale. Usa l’hashrate attuale della dashboard per la tua stima.
</Note>

Supponiamo una rete a 50.000 H/s. Un rig a 50 H/s, circa il risultato seine su un server AMD EPYC Zen 5, detiene lo 0,1%. Ecco cosa troverebbero quel rig e due installazioni maggiori in solo:

| Il tuo hashrate (esempio) | Quota di rete | Blocchi attesi al giorno | Intervallo medio |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | circa 83 ore (3,5 giorni) |
| 500 H/s | 1% | 2,88 | circa 8,3 ore |
| 5.000 H/s | 10% | 28,8 | circa 50 minuti |

Ora metti il rig da 50 H/s in una pool che totalizza 5.000 H/s. Trova circa 28,8 blocchi al giorno, uno ogni 50 minuti in media, e il rig guadagna l’1% di ciascuno grazie al suo 1% del lavoro. Il lavoro contato è sempre 0,288 blocchi al giorno. Cambia il ritmo: una piccola parte circa ogni 50 minuti, invece di un blocco intero ogni pochi giorni.

La tabella mostra anche perché il solo è adatto a grandi installazioni. A 5.000 H/s un miner trova blocchi tanto spesso quanto una pool di pari dimensioni: i suoi guadagni sono regolari anche da solo.

Per convertire blocchi al giorno in BNT per il tuo hardware, con un esempio datato basato sui dati reali, leggi [come stimare le ricompense Blocknet](/blog/it/estimate-blocknet-mining-rewards/).

## Perché una pool paga più regolarmente?

bntpool.com unisce l’hashrate di tutti, trovando blocchi molto più spesso di un solo rig. Divide ogni ricompensa fra i miner che hanno contribuito tramite PPLNS: guarda le sei ore prima del blocco e ripartisce in proporzione alla difficoltà delle share.

Per te significa:

- **Guadagni da ogni blocco.** Finché le share sono nella finestra di sei ore ricevi una quota di ogni blocco. Puoi seguirli nella [pagina blocchi](/blocks).
- **Pagamenti automatici.** A 60 blocchi di profondità la quota entra nel saldo confermato, inviato al tuo indirizzo al raggiungimento del pagamento minimo, 1 BNT al 4 novembre 2026. Gli invii sono nella [pagina pagamenti](/payouts).
- **Continuità dopo i riavvii.** Le share restano sei ore: riavvio o aggiornamento driver non azzerano la posizione.
- **Solo il miner da eseguire.** La pool gestisce nodo, template e invio dei blocchi. Più rig possono minare allo stesso indirizzo o `@name` Blocknet ID.

La [guida PPLNS](/blog/it/how-pplns-payouts-work/) spiega la divisione passo passo con un esempio. [Get Started](/start) elenca le impostazioni attuali.

## Perché minare Blocknet in solo?

Il solo ti paga l’intera ricompensa di ogni blocco che trovi. È più adatto quando il tuo hashrate trova blocchi spesso. Nell’esempio, il 10% della rete trova un blocco circa ogni 50 minuti, quindi i pagamenti in solo sono regolari come quelli di una pool.

Con seine puoi minare in solo in due modi:

- **Solo su bntpool.com.** Metti `solo:` davanti all’indirizzo. La pool gestisce il nodo, invia i tuoi blocchi e paga l’intera ricompensa di ciascuno, al netto della commissione della pool, al tuo indirizzo con i pagamenti normali. Puoi combinare rig in solo e condivisi sullo stesso indirizzo.
- **Solo sul tuo nodo.** Il modo daemon di seine mina sui template del tuo nodo, e ogni blocco paga direttamente al wallet del nodo. Se esegui già un nodo Blocknet per il wallet o altri servizi, il solo vi aggiunge il miner.

seine è progettato anche per il modo daemon. Mina sui template del nodo e, se questo ne dimentica uno, per esempio dopo un riavvio, invia nuovamente il blocco risolto completo, affinché un blocco trovato raggiunga comunque la catena.

## Come configurare il mining in solo con seine?

Per minare in solo su bntpool.com, installa seine v0.3.1 o successivo e avvialo con il prefisso sull’indirizzo:

```sh
./seine --address solo:YOUR_BLOCKNET_ADDRESS
```

La dashboard indica allora il tuo account come **Solo**. [Mina Blocknet in solo su bntpool.com](/blog/it/solo-mine-blocknet-on-bntpool/) spiega nomi Blocknet ID, nomi dei rig, pagamenti e dove compaiono i tuoi blocchi in solo nella pagina statistiche.

Per minare in solo sul tuo nodo:

1. **Installa e sincronizza un nodo Blocknet.** Con il wrapper `blocknet`, esegui:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Il cookie API viene scritto in `~/.config/bnt/data/mainnet/api.cookie`. Per abbreviare la prima sincronizzazione, bntpool.com pubblica `checkpoints.dat` nella [pagina checkpoint](/checkpoints); [sincronizzare con i checkpoint](/blog/it/blocknet-checkpoints-fast-sync/) spiega l’uso.
2. **Aspetta la fine della sincronizzazione.** I messaggi `[sync] progress` arrivano al 100%. Il nodo serve template solo quando ha recuperato la catena.
3. **Avvia seine in modo daemon.**

   ```sh
   ./seine --mode daemon
   ```

   Per una configurazione personalizzata indica API e cookie:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

La [guida solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) copre password e configurazioni personalizzate; la [guida al mining seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) tratta download e prime share. Puoi [scaricare seine](https://seineminer.com/it/#download) per Windows, macOS o Linux.

## Quale scegliere?

Applica la formula al tuo hashrate e a quello attuale della [dashboard](/), poi guarda l’intervallo medio tra blocchi.

- **Inizia su bntpool.com** con desktop, laptop o pochi server, oppure se vuoi pagamenti regolari senza un nodo. Esegui seine, incolla indirizzo o `@name` e accetta la pool predefinita.
- **Mina in solo su bntpool.com** se possiedi una grande quota della rete e trovi spesso blocchi da solo. Aggiungi `solo:` all’indirizzo e mantieni nodo, statistiche e pagamenti della pool.
- **Mina in solo sul tuo nodo** se hai quell’hashrate ed esegui già, o vuoi eseguire, un nodo sincronizzato.

seine rende facile cambiare scelta. Tutte le opzioni usano lo stesso download: bastano un prefisso o un flag e un riavvio quando cresce l’hardware. Se esegui un nodo locale mentre mini in pool, seine mostra anche il saldo del wallet del nodo accanto a quello della pool.

<Note>
Blocknet prevede Faraday intorno al 16 febbraio 2027, secondo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), con un cambiamento previsto della proof of work. seine v0.3.1 è già pronto: il mining in pool passa da solo alla nuova proof of work e, con gli aggiornamenti automatici attivi, seine installa per te le nuove versioni.
</Note>

<CTA target="pool" title="Inizia a minare su bntpool.com" text="Esegui seine, incolla l’indirizzo e guadagna una parte di ogni blocco trovato dalla pool, oppure aggiungi solo: e tieni ogni blocco che trovi." />
