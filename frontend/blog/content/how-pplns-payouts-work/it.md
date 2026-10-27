---
title: Come funzionano i pagamenti PPLNS su bntpool.com
description: bntpool.com paga i miner Blocknet con PPLNS su sei ore. Scopri la divisione delle ricompense, le 60 conferme e il pagamento minimo di 0,1 BNT.
tags: [Pagamenti, PPLNS, bntpool, Guida al mining]
faq:
  - q: 'Quanto dura la finestra PPLNS su bntpool.com?'
    a: 'Sei ore. Quando la pool trova un blocco, guarda le sei ore precedenti e divide la ricompensa fra chi ha inviato share in quel periodo, in proporzione alla loro difficoltà. Il valore attuale è indicato nella pagina Get Started.'
  - q: 'Qual è il pagamento minimo su bntpool.com?'
    a: 'È 0,1 BNT. Quando il saldo confermato raggiunge quella cifra, la pool lo invia automaticamente all’indirizzo Blocknet, senza richieste o riscatti. Un saldo inferiore rimane sul conto e cresce a ogni blocco confermato a cui hai contribuito, finché supera la soglia.'
  - q: 'Perché Estimated Rewards e Confirmed Rewards sono diversi?'
    a: 'Estimated Rewards è la tua quota dei blocchi che stanno ancora raccogliendo 60 conferme, quindi può cambiare. Confirmed Rewards deriva dai blocchi profondi 60 blocchi e attende il pagamento. Quando un blocco matura, l’importo passa dalla prima scheda alla seconda.'
  - q: 'Le ricompense si fermano appena smetto di minare?'
    a: 'No. Ogni share resta nella finestra PPLNS sei ore dopo l’invio, quindi i blocchi trovati in quel periodo ti accreditano ancora una quota. Le ricompense già confermate restano nel saldo e vengono pagate quando raggiunge il minimo di 0,1 BNT.'
  - q: 'Cosa significa effort sopra il 100% nella pagina Luck?'
    a: 'Confronta il lavoro della pool nel round con la difficoltà del blocco, cioè il lavoro atteso. Sopra il 100% il round ha richiesto più tempo della media, sotto è stato più rapido. Entrambi sono normali e su molti round l’effort medio si avvicina al 100%.'
sourceHash: 'f216680e1944'
translation: draft
---

Questa guida spiega come bntpool.com trasforma le share del miner in pagamenti Blocknet. Imparerai come si divide la ricompensa, perché attende circa cinque ore prima del pagamento, quando viene inviato il saldo e come leggere le schede nella [pagina statistiche](/stats) e la [pagina Luck](/luck).

Se non conosci il mining in pool, [cos’è una mining pool?](/blog/it/what-is-a-mining-pool/) spiega le basi. Se non hai iniziato, la [guida al mining con seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) ti porta dal download alle prime share in pochi minuti.

## Cos’è PPLNS?

<Answer>
PPLNS significa “pay per last N shares”. Quando bntpool.com trova un blocco, ne divide la ricompensa fra le share inviate nelle sei ore precedenti, in proporzione alla difficoltà. Più lavoro costante fornisci nella finestra, maggiore è la tua parte di ogni blocco.
</Answer>

Su bntpool.com le “ultime N” corrispondono a un intervallo: sei ore. [Get Started](/start) mostra schema, finestra, minimo e conferme; gli stessi valori sono pubblicati su `https://bntpool.com/api/info`.

| Impostazione | Valore su bntpool.com |
|---|---|
| Schema di pagamento | PPLNS |
| Finestra delle share | 6 ore |
| Conferme prima del pagamento | 60 blocchi |
| Pagamento minimo | 0,1 BNT |

Le share sono pesate per difficoltà. La pool assegna e adatta una difficoltà a ogni connessione per mantenere un flusso costante. Una macchina veloce riceve share più difficili, ciascuna con più peso. Conta il lavoro totale svolto: una misura equa per laptop e server.

## Come divide la ricompensa bntpool.com?

Ogni blocco trovato segue gli stessi passaggi:

1. **Il blocco viene registrato.** Appare nella [pagina blocchi](/blocks) come in attesa.
2. **Si misura la finestra.** La pool somma la difficoltà di ogni share accettata per indirizzo nelle sei ore che terminano con il blocco.
3. **Si divide la ricompensa.** Ogni indirizzo riceve la stessa frazione della ricompensa della propria frazione del lavoro totale.
4. **Il blocco matura.** La quota resta stimata fino a 60 blocchi di profondità, poi entra nel saldo confermato.
5. **Il saldo viene pagato.** A 0,1 BNT confermati, la pool lo invia al tuo indirizzo.

## Un esempio di calcolo

<Note>
L’esempio usa cifre tonde inventate per illustrare il calcolo. I totali reali dipendono dagli hashrate tuo e della pool e dalla [ricompensa del blocco](/blog/it/blocknet-block-reward-schedule/) in quel momento.
</Note>

Supponiamo che nelle sei ore prima di un blocco tutte le share sommino una difficoltà di 1.000.000 e le tue 20.000. La tua frazione è 20.000 ÷ 1.000.000 = 2%. Se la pool distribuisce 400 BNT, il tuo credito è 400 × 0,02 = 8 BNT.

Un secondo miner con il tuo stesso hashrate si collega 30 minuti prima del blocco. È un dodicesimo di sei ore, quindi ha circa 1.667 di difficoltà e riceve circa 0,67 BNT. Il lavoro continua però a contare: ogni share rimane nella finestra per le sei ore successive e guadagna una parte di ogni blocco trovato in quel tempo.

| Miner (esempio) | Lavoro nella finestra | Frazione | Credito da 400 BNT |
|---|---|---|---|
| Tu, per tutte le sei ore | 20.000 | 2% | 8 BNT |
| Stesso hashrate, entrato 30 minuti prima | 1.667 | circa 0,17% | circa 0,67 BNT |
| Piccolo miner, per tutte le sei ore | 100 | 0,01% | 0,04 BNT |

L’ultimo miner guadagna 0,04 BNT per blocco: dopo tre blocchi simili il saldo confermato supera 0,1 BNT e viene pagato.

## Perché PPLNS premia chi mina con costanza?

Ogni share conta per tutti i blocchi trovati nelle sei ore successive. Chi mina continuamente ha sempre una finestra piena di lavoro, quindi ogni nuovo blocco accredita la quota completa. I guadagni seguono da vicino l’hashrate da blocco a blocco.

PPLNS tollera anche le interruzioni. Riavvio, aggiornamento driver o breve disconnessione non azzerano la posizione: le share già inviate restano sei ore. Il lavoro aggiunto dopo un blocco contribuisce ai successivi, quindi l’hashrate extra viene sempre usato.

Per risultati regolari lascia il miner attivo e fallo ottimizzare. seine adatta thread e kernel CPU senza riconnettersi alla pool, mantenendo continuo il flusso di share. Puoi [scaricare seine](https://seineminer.com/it/#download) per Windows, macOS o Linux.

## Perché attendere 60 conferme per la ricompensa?

Blocknet richiede 60 blocchi prima di spendere le monete appena minate. Il daemon chiama questa regola maturità coinbase: circa cinque ore con blocchi da cinque minuti (60 × 5 minuti = 300 minuti). bntpool.com usa gli stessi 60 blocchi prima di confermare la ricompensa, quindi ogni pagamento deriva da monete realmente spendibili.

Durante la maturazione, la pool controlla che il blocco resti nella catena principale. A 60 blocchi viene confermato e la ricompensa aggiunta ai saldi. Nel raro caso in cui la rete scelga un altro blocco a quell’altezza, quello della pool è segnato orfano e le sue ricompense stimate rimosse.

## Quando arrivano i pagamenti bntpool.com?

<Answer>
Dopo 60 conferme, circa cinque ore, la quota passa in Confirmed Rewards sulla pagina statistiche. A 0,1 BNT di saldo confermato bntpool.com invia automaticamente al tuo indirizzo. Non devi richiedere o riscattare nulla.
</Answer>

Per un nuovo miner la prima stima compare quando la pool trova un blocco con le sue share nella finestra. Il primo pagamento segue maturazione e raggiungimento del minimo. Gli invii sono nella [pagina pagamenti](/payouts) e nella cronologia personale delle statistiche.

## Come sono mostrate le ricompense provvisorie?

Cerca indirizzo o `@name` nella [pagina statistiche](/stats). Troverai tre schede:

- **Estimated Rewards:** la quota dei blocchi non ancora maturati, provvisoria fino alla conferma o allo stato orfano.
- **Confirmed Rewards:** saldo maturato in attesa del pagamento.
- **Paid Balance:** tutto ciò che è già stato inviato al tuo indirizzo.

La pool separa il provvisorio affinché il saldo confermato contenga solo importi definitivi. Sotto le schede, ogni blocco non confermato è elencato con la tua quota stimata, così puoi seguirne la maturazione. La [guida al monitoraggio del miner](/blog/it/track-your-miner-on-bntpool/) copre il resto della pagina.

## Cosa indicano fortuna ed effort?

L’effort misura il lavoro per trovare un blocco: lavoro accettato nel round diviso per difficoltà, espresso in percentuale. La difficoltà Blocknet equivale agli hash attesi per blocco, quindi 100% è esattamente il lavoro medio. Sotto il 100% il blocco è arrivato prima; sopra ha richiesto più tempo.

La [pagina Luck](/luck) elenca ogni round con altezza, effort, durata, stato e momento della scoperta. La [dashboard](/) mostra l’effort corrente e la fortuna media. I singoli round oscillano sopra e sotto il 100%, normale per ogni pool proof of work, e su molti round la media si avvicina al 100%.

La fortuna influenza la frequenza dei blocchi. La tua frazione di ogni blocco dipende solo dalla quota di lavoro nella finestra, e il mining costante la mantiene piena sia nei round brevi sia in quelli lunghi.

<CTA target="pool" title="Entra nella pool" text="Mina con costanza su bntpool.com: la tua quota di ogni blocco viene pagata automaticamente da 0,1 BNT." />
