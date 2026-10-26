---
title: 'Pagamento inviato, ma assente dal wallet?'
description: 'Segui un pagamento Blocknet tramite transazione, indirizzo ricevente e sincronizzazione del wallet e raccogli i dati per chiarire la voce mancante.'
tags: ['Pagamenti', 'Wallet', 'Risoluzione dei problemi']
faq:
  - q: 'Un pagamento in coda è già arrivato nel wallet?'
    a: 'In coda è uno stato di elaborazione del pool. Verifica prima se il record ha un identificatore di transazione e quale sia il suo stato attuale. Record del pagamento, arrivo della transazione sulla catena e riconoscimento dell''output nel wallet sono passaggi separati. Segui la transazione precisa quando disponibile.'
  - q: 'L''explorer mostra il saldo del mio wallet privato?'
    a: 'L''explorer permette di verificare lo stato pubblico della transazione sulla catena. Blocknet usa output privati, quindi una normale ricerca per indirizzo non mostra il saldo del wallet né prova che un output ti appartenga. Il wallet scansiona la catena per riconoscere i propri output.'
  - q: 'Quali dati fornire per un pagamento mancante?'
    a: 'Fornisci indirizzo ricevente completo usato sul pool, identificatore di transazione, orario approssimativo, stato del pool e avanzamento della sincronizzazione. Indica se appare nella cronologia. Condividi dati dell''account solo nel contesto di assistenza appropriato, mai frase seed, chiavi private o password del wallet.'
sourceHash: '915075cf4e2b'
translation: draft
---

Segui un pagamento Blocknet dal record del pool al wallet destinatario. Questa guida parte da una voce di pagamento identificabile su bntpool.com. Se vedi soltanto ricompense stimate o confermate ancora da pagare, consulta prima la [guida PPLNS](/blog/it/how-pplns-payouts-work/): è una fase precedente con proprie regole contabili e di pagamento.

## Come rintracciare un pagamento assente dal wallet?

<Answer>
Trova il record per l'indirizzo ricevente completo, annota identificatore e stato attuale e verifica la transazione sulla catena. Apri il wallet della stessa destinazione e lascia completare sincronizzazione e scansione. Confronta la cronologia prima di decidere se approfondire il pagamento o la visualizzazione locale del wallet.
</Answer>

*Un record del pool si collega alla transazione sulla catena, poi al wallet che scansiona i propri output in entrata.*

## Parti da un pagamento preciso

Apri My Stats e cerca l'indirizzo completo usato quando sono state ottenute le ricompense. Annota importo, orario, stato e identificatore, se presente. Usa la singola voce anziché il totale storico pagato, che può riunire più pagamenti. La [guida al monitoraggio](/blog/it/track-your-miner-on-bntpool/) spiega dove trovare i campi.

La pagina pubblica Payouts descrive lotti che possono includere più destinatari e link di transazioni. Conserva il record del tuo indirizzo insieme alla transazione esaminata. Il totale del lotto non è l'importo assegnato al tuo wallet. Se ci sono più identificatori, espandi l'elenco e conserva quelli pertinenti anziché copiare soltanto un'etichetta abbreviata.

## Il pagamento è in coda, non confermato o confermato?

Leggi lo stato prima di presumere che una voce abbia completato ogni passaggio. In coda indica ancora elaborazione lato pool; una transazione non confermata necessita di conferma sulla catena. Aggiorna il record e segui il link quando disponibile. La [pagina di stato](/status) offre contesto operativo se più osservazioni indicano un'interruzione.

Distingui maturazione della ricompensa e conferma della transazione di pagamento. Il blocco che ha generato il premio e la transazione successiva hanno identificatori e ruoli diversi. La [guida alle conferme](/blog/it/block-confirmations-mining-rewards/) chiarisce la distinzione. Un identificatore offre un elemento concreto da indagare, ma non prova che il wallet locale abbia già scansionato e riconosciuto il pagamento.

## Verifica la transazione sulla catena

Apri il link dell'explorer associato al pagamento e confronta l'identificatore completo con i tuoi appunti. Leggi stato sulla catena e informazioni del blocco disponibili. Se l'explorer non la trova ancora, confronta record aggiornato e stato del servizio prima di concludere. Un risultato temporaneo da solo non identifica la causa.

La privacy di Blocknet implica che una normale ricerca per indirizzo non mostri un saldo pubblico del destinatario. Stato della transazione e controlli di proprietà del wallet rispondono a domande diverse. Il wallet riconosce output in entrata scansionando la catena. La [guida agli indirizzi stealth](https://seineminer.com/it/blog/stealth-addresses-explained/) spiega il processo; un elenco pubblico non riproduce la cronologia privata del wallet.

## Stai guardando il wallet ricevente corretto?

Confronta l'indirizzo cercato nel pool con quello ricevente del wallet aperto. Usa valori completi: prefissi abbreviati simili non bastano. Con un Blocknet ID conserva l'indirizzo completo usato per il lavoro originale. Dopo una modifica, il nome attuale può puntare altrove mentre un pagamento precedente resta collegato alla destinazione originale.

Controlla eventuali vecchi wallet usati prima di modificare il mining. Cambiare indirizzo del miner o associazione del nome non sposta pagamenti già assegnati. La [guida al cambio indirizzo](/blog/it/change-mining-wallet-address/) spiega entrambe le cronologie. Conserva wallet originale e backup; distingui l'apertura di un wallet esistente dalla creazione di uno nuovo con un indirizzo diverso.

## Lascia sincronizzare e scansionare il wallet

Verifica che il nodo avanzi verso la catena corrente e che la scansione raggiunga il blocco della transazione. Sincronizzazione del nodo e scansione del wallet sono progressi distinti. Un wallet appena aperto o ripristinato può richiedere tempo per individuare gli output anche se l'explorer mostra già la transazione in un blocco.

Mantieni il wallet connesso e segui il flusso di sincronizzazione documentato. Confronta cronologia entrante e saldo principale: spese successive possono modificare il totale. Se una voce esiste, controllane conferma o disponibilità. Parti da queste osservazioni prima di recupero o scansione completa e conserva backup prima della manutenzione. La [guida al wallet](https://seineminer.com/it/blog/blocknet-wallet-for-mining/) copre accesso e backup.

## Quali dettagli chiariscono una discrepanza restante?

Annota indirizzo completo del pool, identificatore della transazione, orario approssimativo e stato attuale. Aggiungi versione del wallet, avanzamento del nodo e della scansione e presenza della transazione nella cronologia. Indica le verifiche svolte. Così distingui stato della transazione, indirizzo diverso e wallet ancora in aggiornamento.

Usa l'assistenza ufficiale del progetto e condividi dati dell'account solo dove appropriato. L'indagine non richiede frase di recupero, chiavi private o password. Esamina i file diagnostici prima di pubblicarli integralmente. Abbina infine lo stesso record, la stessa transazione e il wallet destinatario: hashrate recente o un altro riavvio del miner non confermano un pagamento precedente.

<CTA target="pool" title="Segui i tuoi pagamenti su bntpool.com" text="Usa l&#x27;indirizzo completo per trovare il pagamento e confrontarne la transazione con la cronologia del wallet sincronizzato." />
