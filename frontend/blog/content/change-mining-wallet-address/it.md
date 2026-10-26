---
title: 'Cambiare l’indirizzo wallet per il mining Blocknet'
description: 'Cambia l’indirizzo di ricezione dei miner Blocknet, verifica le nuove share su bntpool.com e segui le ricompense precedenti associate al vecchio indirizzo.'
tags: ['Guida al mining', 'Wallet', 'Pagamenti']
faq:
  - q: 'Cambiare indirizzo trasferisce il saldo precedente?'
    a: 'La nuova destinazione assegna il lavoro futuro al nuovo indirizzo. Share, accrediti e pagamenti esistenti restano associati all’indirizzo originale. Cerca separatamente entrambi in Le mie statistiche e conserva l’accesso al vecchio wallet mentre segui le ricompense rimanenti.'
  - q: 'Devo cambiare ogni computer?'
    a: 'Sì. Ogni processo usa la propria destinazione configurata. Aggiorna e verifica prima un computer, poi gli altri, inclusi configurazione salvata e argomenti di avvio. Nomi distinti consentono di riconoscere le macchine che inviano lavoro al nuovo indirizzo.'
  - q: 'Cambiare Blocknet ID aggiorna il miner attivo?'
    a: 'seine risolve l’ID salvato all’avvio e mantiene l’indirizzo durante l’esecuzione, riconnessioni incluse. Dopo una modifica dell’associazione riavvia ogni miner e verifica l’indirizzo completo. Il cambiamento non riassegna lavoro o saldi già registrati all’indirizzo precedente.'
sourceHash: '645b2eff8a61'
translation: draft
---

Cambia la destinazione del lavoro futuro mantenendo traccia delle ricompense precedenti. La guida riguarda seine già collegato a bntpool.com. Per iniziare usa la [guida al pool](/blog/it/mine-blocknet-on-bntpool/); per un nuovo wallet di ricezione, la [guida al wallet](https://seineminer.com/it/blog/blocknet-wallet-for-mining/).

## Come cambiare l’indirizzo di mining?

<Answer>
Arresta un miner, aggiorna la destinazione nella configurazione o nel comando abituale e riavvialo. Verifica indirizzo completo risolto e share accettate, poi cerca l’indirizzo su bntpool.com. Ripeti sugli altri computer. Share e ricompense precedenti restano associate all’indirizzo usato al momento della registrazione.
</Answer>

*Il nuovo lavoro va al secondo wallet; i registri delle vecchie ricompense restano collegati al primo.*

## Annota entrambe le destinazioni

Copia il nuovo indirizzo direttamente dal wallet previsto. Il miner richiede la destinazione pubblica o un Blocknet ID registrato, senza frase di recupero o chiave privata. Conserva anche il vecchio indirizzo completo: resta la chiave per consultare l’attività passata.

Apri Le mie statistiche per il vecchio indirizzo e annota ricompense pendenti, saldo confermato, pagamenti recenti e nomi attivi. Questo documenta il passaggio; la nuova pagina non contiene la cronologia precedente. Mantieni accesso al wallet originale mentre segui ricompense e pagamenti residui.

## Aggiorna un miner e controlla il comando

Arresta un miner e controllane l’avvio abituale. Con configurazione esistente, `--address` prevale sul valore salvato senza sostituirlo. L’esempio PowerShell sceglie la destinazione per questa esecuzione. Sostituisci `@yourname` con nuovo ID o indirizzo completo e mantieni gli argomenti verificati per pool, dati e hardware.

```powershell
.\seine.exe --address "@yourname"
```

Su Linux o macOS usa la sintassi seguente dalla directory abituale. Entrambi gli esempi avviano mining reale: scegli quello per il tuo sistema. L’installazione esistente fornisce gli altri parametri. Mantieni lo stesso nome macchina per riconoscere le share sotto il nuovo indirizzo.

```sh
./seine --address "@yourname"
```

## Conserva il nuovo indirizzo al riavvio normale

A miner fermo, modifica `address` in `seine.config.json`, conservando gli altri campi e JSON valido. Controlla la directory: `./seine-data` è relativa a quella di lavoro; `--data-dir` può indicarne un’altra. Modifica il file usato davvero dall’avvio abituale.

Aggiorna o rimuovi i vecchi `--address` da collegamento, script, attività o servizio. Un comando manuale riuscito non modifica questi altri avvii. Riavvia normalmente e verifica. La [guida alla configurazione](https://seineminer.com/it/blog/configure-seine-miner/) spiega priorità e selezione di un file diverso quando cambia la directory di lavoro.

## Cosa cambia usando un Blocknet ID?

seine v0.2.37 risolve e valida `@name` all’avvio. L’indirizzo completo resta fisso per l’esecuzione, riconnessioni incluse. Se cambi l’associazione del nome, riavvia ogni miner e controlla la destinazione. Riconnettere il processo esistente non ripete la risoluzione iniziale.

Annota i due indirizzi completi anche se il nome leggibile resta uguale. La contabilità usa l’indirizzo risolto: una nuova associazione non reindirizza vecchie share o saldi. Se il nome non si risolve, verifica l’associazione oppure usa esplicitamente l’indirizzo completo controllato nel wallet.

## Verifica ogni computer al nuovo indirizzo

Controlla destinazione e invii accettati, poi cerca l’indirizzo completo in Le mie statistiche. Abbina nome macchina e share recenti. La nuova stima di hashrate richiede lavoro e tempo: verifica prima gli arrivi. La [guida alle statistiche](/blog/it/track-your-miner-on-bntpool/) spiega contatori e macchine mancanti.

Dopo il primo computer, ripeti sugli altri. Nomi distinti facilitano il controllo. Prova anche ogni avvio normale, così un vecchio comando non ripristinerà in seguito la destinazione precedente. La [guida a più computer](/blog/it/mine-multiple-computers-one-wallet/) spiega come organizzarli sotto un solo indirizzo.

## Cosa succede alle vecchie ricompense?

Accrediti e pagamenti esistenti restano al vecchio indirizzo. Le share idonee precedenti possono ancora contribuire alle relative finestre PPLNS; le ricompense pendenti seguono le conferme. Hashrate zero non significa contabilità completata. Le guide [PPLNS](/blog/it/how-pplns-payouts-work/) e [conferme](/blog/it/block-confirmations-mining-rewards/) distinguono lavoro, maturazione e pagamenti.

Il cambio non unisce saldi né aggira il minimo di pagamento. Consulta le regole attuali e segui entrambe le pagine fino a comprendere gli importi residui. Nemmeno rinominare una macchina trasferisce ricompense. Conferma infine che tutti i miner previsti inviano nuovo lavoro al nuovo indirizzo e che conservi vecchio wallet e cronologia.

<CTA target="pool" title="Controlla la configurazione su bntpool.com" text="Verifica destinazione e lavoro accettato di ogni macchina e segui separatamente vecchie e nuove ricompense." />
