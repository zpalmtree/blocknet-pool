---
title: 'Minare Blocknet da più computer su un portafoglio'
description: 'Aggiungi un secondo miner Blocknet allo stesso portafoglio su bntpool.com. Nomina ogni computer, verifica le share e comprendi ricompense e nomi duplicati.'
tags: ['Guida al mining', 'Computer', 'Configurazione']
faq:
  - q: 'Serve un portafoglio per ogni computer?'
    a: 'No. Ogni computer può usare lo stesso indirizzo di ricezione su bntpool.com, con un nome distinto per identificare l''installazione. Il pool contabilizza le ricompense per indirizzo. Ai miner bastano l''indirizzo o il tuo Blocknet ID, non la frase di recupero.'
  - q: 'Cosa succede con due miner dallo stesso nome?'
    a: 'Sotto lo stesso indirizzo, lo storico delle share viene raggruppato sotto quel nome. Diventa più difficile riconoscere la macchina associata a una lettura. Assegna un nome distinto a ogni installazione e riavviala con esso; le share già registrate conservano l''etichetta originale.'
  - q: 'Aggiungere una macchina divide il pagamento?'
    a: 'I nomi organizzano l''attività visualizzata, senza creare saldi separati sotto lo stesso indirizzo. Il lavoro valido dei computer contribuisce alla partecipazione PPLNS di quell''indirizzo. Il saldo segue le normali regole di conferma e pagamento del pool.'
sourceHash: '1e7424aee51d'
translation: draft
---

Aggiungi un secondo computer a bntpool.com mantenendo un portafoglio di ricezione. Nomina ogni installazione, verificala separatamente e poi controlla l'indirizzo comune. Questa guida tratta l'aggiunta e i nomi duplicati. Per il primo miner, completa prima la [configurazione del pool](/blog/it/mine-blocknet-on-bntpool/).

## Più computer possono minare sullo stesso portafoglio?

<Answer>
Sì. Avvia seine su ciascun computer con lo stesso indirizzo di ricezione e un nome macchina distinto. bntpool.com registra lavoro e ricompense per indirizzo; i nomi separano l'attività nelle statistiche. Verifica le share accettate di ogni macchina prima di affidarti alla vista complessiva.
</Answer>

*Computer distinti inviano lavoro attraverso il pool verso un indirizzo di ricezione; ciascuno mantiene il proprio nome.*

## Scegli indirizzo e nomi

Usa l'indirizzo di ricezione del portafoglio o un Blocknet ID che vi punti. Controlla l'indirizzo risolto su ogni miner. Il nome semplifica l'inserimento, ma la contabilità segue l'indirizzo risultante. Se cambi la destinazione dell'ID, un'esecuzione esistente può conservare la risoluzione precedente fino al riavvio. Le ricompense già guadagnate restano sull'indirizzo originale.

La [guida al portafoglio per il mining](https://seineminer.com/it/blog/blocknet-wallet-for-mining/) spiega come ottenere l'indirizzo. I computer richiedono solo questa destinazione pubblica; frase di recupero e chiavi private restano con il portafoglio. Usa nomi brevi come `desktop-01` e `server-02`. Il pool elimina gli spazi esterni e conserva fino a 64 caratteri: metti la parte distintiva all'inizio dei nomi lunghi.

## Aggiungi il secondo miner con il suo nome

Installa la versione adatta di seine sul secondo computer ed eseguila dalla cartella estratta. Il primo esempio PowerShell imposta esplicitamente nome destinatario, pool e macchina. Sostituisci `@yourname` con il tuo ID o indirizzo completo prima dell'esecuzione. Mantieni le virgolette e aggiungi le opzioni di backend o ottimizzazione già provate per quel computer.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Su Linux o macOS, usa il comando seguente con l'eseguibile corrispondente e un altro nome macchina. I sistemi operativi possono differire, l'indirizzo deve coincidere. Ogni esempio avvia mining reale. Esegui quello appropriato sulla macchina prevista e attendi l'avvio e l'ottimizzazione normali. Non occorre eseguire entrambi gli esempi sullo stesso computer.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Quali impostazioni si condividono?

Condividi indirizzo di ricezione ed endpoint del pool. Mantieni cartella dati locale e nome separati per ogni installazione. seine salva la connessione in `seine.config.json` nel suo percorso dati; il valore predefinito `./seine-data` dipende dalla cartella di avvio. Un'altra cartella può trovare un'altra configurazione. Una posizione stabile semplifica la manutenzione.

Se copi la configurazione dal primo computer, imposta un nome distinto nel comando del secondo e verificalo all'avvio. Mantienilo esplicito nei comandi futuri: una sostituzione da riga di comando non riscrive un nome già salvato. Adatta le scelte hardware e lascia ottimizzare ogni computer localmente. L'argomento indirizzo determina il pagamento, quello del nome l'etichetta: inseriscili separatamente come sopra.

## Come verifichi il contributo di entrambi?

Apri My Stats su bntpool.com per l'indirizzo comune. Cerca entrambi i nomi e una share recente per ciascuno. Controlla invii accettati anche nei miner locali. Procedi un computer alla volta per collegare processo e riga del pool: è più facile trovare errori che osservando solo l'hashrate totale.

Il pool stima l'hashrate dalle share inviate; una macchina nuova richiede tempo e lavoro accettato per una lettura utile. Tasso locale e stima del pool coprono misure e intervalli diversi. Conferma prima l'attività con share recenti e contatori accettati, poi confronta periodi più lunghi. La [guida alle statistiche](/blog/it/track-your-miner-on-bntpool/) spiega schede e righe senza dedurre dispositivi dal totale delle connessioni.

## Cosa fare se mancano macchine o i nomi coincidono?

Nomi uguali sotto lo stesso indirizzo raggruppano le share registrate. Rinomina un miner con l'opzione del nome e riavvialo. I nuovi invii usano la nuova etichetta, quelli precedenti mantengono la vecchia. La vecchia riga può restare nel periodo storico mostrato. Rinominare non sposta il saldo guadagnato dall'indirizzo.

Per una macchina assente, confronta indirizzo realmente risolto, endpoint e nome con la pagina aperta. Verifica poi connessione e share accettate. CPU e GPU nello stesso processo seine non richiedono nomi separati in questo flusso. Per share rifiutate o obsolete, segui la [guida agli stati](/blog/it/accepted-rejected-stale-shares/) invece di cambiare ripetutamente indirizzo del portafoglio.

## Come funzionano ricompense e pagamenti complessivi?

Il lavoro valido dei computer contribuisce alla partecipazione di un indirizzo nella finestra PPLNS. I nomi aiutano a seguire i contributi senza creare saldi distinti. Una macchina aggiunge il lavoro utile effettivamente inviato, non una ricompensa fissa per dispositivo o nome. Hardware e tempo di attività diversi possono contribuire diversamente; più nomi da soli non aumentano il lavoro.

La [guida PPLNS](/blog/it/how-pplns-payouts-work/) spiega la ripartizione quando viene trovato un blocco. Conferme e regole di pagamento valgono ancora per il saldo comune. Non occorre unire portafogli né trasferire saldi tra nomi. Prima di lasciare tutto in esecuzione, verifica destinazione, nome distinto e share accettate su ogni computer. Riusa questa lista per il prossimo.

<CTA target="pool" title="Collega un altro computer a bntpool.com" text="Usa un indirizzo di ricezione, assegna nomi distinti alle installazioni e verifica le share accettate." />
