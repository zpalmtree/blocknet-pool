---
title: Sincronizzare rapidamente Blocknet con i checkpoint bntpool.com
description: I nodi Blocknet si sincronizzano rapidamente con checkpoints.dat di bntpool.com. Scopri come vincolano la catena e come scaricare, verificare e caricare il file.
tags: [Nodo Blocknet, Checkpoint, Sincronizzazione rapida, bntpool]
faq:
  - q: 'Da dove ottiene i checkpoint un nodo Blocknet?'
    a: 'Da bntpool.com. Al primo avvio, se la directory dei dati non contiene checkpoints.dat, il daemon lo scarica da https://bntpool.com/checkpoints.dat e lo usa per la sincronizzazione rapida. Puoi anche scaricarlo dalla pagina Checkpoints, verificarne SHA-256 e inserirlo nella directory prima di avviare il nodo.'
  - q: 'Ogni quanto bntpool.com aggiorna checkpoints.dat?'
    a: 'Il daemon della pool riscrive il file mentre la catena cresce, con una voce ogni 100 blocchi. Ogni voce è almeno 100 blocchi sotto la punta, oltre la profondità massima di riorganizzazione di Blocknet: copre quindi solo la storia consolidata. La pagina Checkpoints mostra l’ultima altezza e quando il file è cambiato.'
  - q: 'I checkpoint cambiano saldo o cronologia del wallet?'
    a: 'No. Il nodo memorizza comunque ogni blocco e registra ogni output e key image negli indici, quindi scansione e saldi funzionano come dopo una sincronizzazione completa. I checkpoint permettono solo di saltare una nuova verifica di proof of work e firme della storia già garantita dai loro hash.'
  - q: 'Posso sincronizzare un nodo senza checkpoint?'
    a: 'Sì. Avvia il daemon con --full-sync oppure imposta full_sync su true nella configurazione blocknet. Il nodo salta il download e verifica di nuovo proof of work e firme di ogni blocco dal genesi. Richiede più tempo, per questo i checkpoint sono la scelta predefinita dei nuovi nodi.'
sourceHash: 7a3491743f7d
translation: draft
---

Un nuovo nodo Blocknet deve recuperare tutti i blocchi dal genesi prima di servire un wallet o un miner in solo. I checkpoint accelerano il recupero. Questa guida spiega cosa contiene il file di bntpool.com, come il daemon salta i controlli più lenti restando vincolato alla catena reale e come scaricare, verificare e caricare il file da te.

## Cosa sono i checkpoint Blocknet?

<Answer>
Sono un elenco di hash di blocchi noti, uno ogni 100 blocchi, distribuito da bntpool.com come checkpoints.dat. Un nodo in sincronizzazione controlla che i blocchi ricevuti corrispondano agli hash e salta la nuova verifica di proof of work e firme fino all’ultimo checkpoint, completando molto prima la prima sincronizzazione.
</Answer>

Il file è testo semplice con una coppia `height:hash` per riga. Le prime righe il 25 ottobre 2026 erano:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Ogni hash è l’ID del blocco, lo SHA3-256 del suo header. Il daemon della pool scrive il file dalla propria catena e include solo blocchi profondi almeno 100 blocchi. Cento blocchi sono la finalità rigida di Blocknet, la massima riorganizzazione consentita: ogni checkpoint appartiene alla storia consolidata.

Il 25 ottobre 2026 alle 02:04 EDT il file conteneva 689 checkpoint, dall’altezza 100 alla 68.900, in 48.811 byte. Cresce di una riga circa ogni 100 blocchi, approssimativamente ogni otto ore con blocchi da cinque minuti. Consulta [Checkpoints](/checkpoints) per l’altezza attuale.

## Come usa i checkpoint un nodo Blocknet?

Il nodo prende l’ultima altezza caricata come obiettivo della sincronizzazione rapida. Fino a raggiungerla si affida ai checkpoint pubblicati per i due controlli più costosi ed esegue tutti gli altri normalmente:

- **La proof of work si affida ai checkpoint.** Ogni blocco Blocknet usa un hash Argon2id su 2 GiB di memoria. Ricalcolarlo per ogni blocco storico è costoso e gli hash dei checkpoint garantiscono già quella storia.
- **Anche firme RingCT e range proof si affidano ai checkpoint.** I commenti del daemon definiscono la verifica delle range proof degli output il costo principale della sincronizzazione: ogni output ne ha una, compresa la coinbase di ciascun blocco.
- **Gli altri controlli rimangono attivi.** Collegamenti tra blocchi, difficoltà LWMA, regole dei timestamp, dimensione e forma dei blocchi, doppie spese tramite key image e legami tra input e firma vengono controllati normalmente. Output e key image entrano negli indici come sempre: scansione del wallet e saldi sono identici a una sincronizzazione completa.

Il salto si applica solo ai blocchi che estendono la punta attuale del nodo e solo sotto l’ultimo checkpoint. Oltre quell’altezza, ogni nuovo blocco viene verificato integralmente, proof of work compresa.

### Come i checkpoint vincolano la catena fidata

Durante il recupero, ogni blocco a un’altezza di checkpoint deve avere esattamente l’hash del file. Un hash diverso viene rifiutato con un errore di mancata corrispondenza. Poiché i checkpoint distano 100 blocchi, un peer non può guidare il nodo lungo un’altra storia: il checkpoint successivo la smaschererebbe.

I checkpoint accelerano anche ogni riavvio. Se ci sono già blocchi su disco, il daemon trova il checkpoint più alto corrispondente alla catena memorizzata e ricontrolla solo i blocchi successivi, anziché ripercorrere tutto dal genesi.

## Un nuovo nodo scarica automaticamente i checkpoint?

<Answer>
Sì. Se all’avvio manca checkpoints.dat nella directory dei dati, il daemon lo scarica da https://bntpool.com/checkpoints.dat, lo carica e si sincronizza rapidamente fino all’ultimo checkpoint. Il download è best effort: se non riesce, il nodo parte comunque e verifica l’intera catena.
</Answer>

L’indirizzo predefinito è scritto nel core Blocknet stesso: ogni nuovo nodo usa quindi subito il file pubblicato da bntpool.com. Il daemon lo recupera solo se assente, concede 15 secondi alla richiesta e limita il download a 32 MiB.

## Come usare i checkpoint bntpool.com passo passo?

Se preferisci scaricare e verificare il file da te, o vuoi aggiornare una vecchia copia prima di una lunga risincronizzazione, segui questi passaggi manuali.

### 1. Trova la directory dei dati

Il file sta nella directory dei dati del nodo, accanto al database della catena:

- **Il gestore `blocknet`:** `~/.config/bnt/data/mainnet/` su macOS e Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` su Windows, salvo un `data_dir` personalizzato nella configurazione.
- **Il core avviato direttamente:** `./blocknet-data-mainnet` nella cartella di avvio, oppure il percorso passato con `--data`.

### 2. Scarica checkpoints.dat

Apri [Checkpoints](/checkpoints) e premi **Download checkpoints.dat**, oppure usa il terminale. Questo esempio scrive nella directory predefinita del core:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Scaricare sopra un file esistente lo sostituisce: è il modo per aggiornare una vecchia copia.

### 3. Verifica SHA-256

La pagina pubblica lo SHA-256 del file attuale. Calcola quello della tua copia e confrontalo:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

Su macOS usa invece `shasum -a 256`. L’hash cambia a ogni nuovo checkpoint, quindi confrontalo con la pagina subito dopo il download.

### 4. Controlla un checkpoint sul block explorer

Puoi verificare qualsiasi riga sul [block explorer pubblico](https://explorer.blocknetcrypto.com). Apri un blocco a un’altezza del file, per esempio `explorer.blocknetcrypto.com/block/68900`, e confronta l’hash con la riga 68.900. Lo abbiamo fatto per l’ultimo checkpoint il 25 ottobre 2026 e gli hash corrispondevano.

### 5. Avvia il nodo

Avvia o riavvia il daemon. Legge `checkpoints.dat` all’avvio e si sincronizza rapidamente fino all’ultimo checkpoint. Con la shell interattiva del core (`--cli`), `load-checkpoints` carica il file nel nodo già attivo e riporta l’altezza di sincronizzazione rapida:

```text
> load-checkpoints
```

I checkpoint aiutano quando il nodo è indietro. Oltre l’ultima altezza, il nodo verifica già tutto integralmente e un file più recente non cambia nulla.

## Come controllare il file da uno script?

La pool pubblica i dettagli in JSON su `https://bntpool.com/api/checkpoints`, senza chiave API:

| Campo | Significato |
|---|---|
| `available` | Se la pool ha un file disponibile |
| `url` | Dove scaricarlo |
| `entries` | Numero di checkpoint |
| `latest_height`, `latest_hash` | Checkpoint più alto e hash del blocco |
| `bytes`, `sha256` | Dimensione del file e SHA-256 |
| `updated_at` | Ultima modifica come timestamp Unix |

Il download include gli header `X-Checkpoint-Height`, `X-Checkpoint-Sha256` e un `ETag` ricavato dall’hash. Uno script può inviare `If-None-Match` e ricevere rapidamente `304 Not Modified` se nulla è cambiato:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Le risposte possono restare in cache per 60 secondi, più che adeguato per un file che cambia circa ogni otto ore.

<Note>
blocknetcrypto.com indica che l’aggiornamento Faraday previsto porterà una proof of work più leggera, così i nuovi nodi recupereranno la catena più velocemente. Fino ad allora, i checkpoint sono il modo rapido per aggiornare un nodo.
</Note>

## Cosa puoi fare con un nodo sincronizzato?

Il nodo serve il wallet e l’API Blocknet ed è ciò a cui seine si collega quando mini in solo con `--mode daemon`. [Pool o solo per Blocknet](/blog/it/pool-vs-solo-mining-blocknet/) aiuta a capire se il solo è adatto al tuo hardware. Per iniziare senza alcun nodo, seine mina su bntpool.com per impostazione predefinita: la [guida d’installazione di seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) ti porta alle prime share e [minare su bntpool.com](/blog/it/mine-blocknet-on-bntpool/) copre il lato pool.

<CTA target="pool" title="Mina sulla pool che distribuisce i checkpoint Blocknet" text="Entra in bntpool.com in pochi minuti e segui hashrate, share e pagamenti in tempo reale." />
