---
title: Minare Blocknet su bntpool.com
description: Entra in bntpool.com, la pool Blocknet predefinita di seine. Collegati con indirizzo o @name, assegna nomi ai rig e controlla share e statistiche.
tags: [Guida al mining, bntpool, Blocknet ID, seine]
faq:
  - q: 'Qual è l’indirizzo Stratum di bntpool.com?'
    a: 'È stratum+tcp://bntpool.com:3333, la pool suggerita al primo avvio di seine. Premi Invio alla richiesta oppure passalo con --pool-url. Il tuo indirizzo Blocknet o nome Blocknet ID è il login e la pool paga le ricompense a quell’indirizzo.'
  - q: 'Posso minare su bntpool.com con un nome Blocknet ID?'
    a: 'Sì. Inserisci @name o $name ovunque seine o la pool chiedano un indirizzo. La pool verifica la mappatura firmata di blocknet.id, poi accredita share e pagamenti all’indirizzo completo. Serve seine v0.2.16 o successivo. Nella riga di comando racchiudi il nome tra apici affinché la shell non lo interpreti.'
  - q: 'Serve un account per minare su bntpool.com?'
    a: 'No. Nessuna registrazione, password o email. L’indirizzo Blocknet, o un nome Blocknet ID che vi punta, è login e destinazione dei pagamenti. Avvia seine e cercalo su My Stats per vedere hashrate, share, rig, saldo e pagamenti.'
  - q: 'Posso minare allo stesso indirizzo da più computer?'
    a: 'Sì. Esegui seine su ogni computer con lo stesso indirizzo o nome e un nome rig distinto. La pool conta share, saldo e pagamenti per indirizzo: ogni rig contribuisce allo stesso saldo e pagamento, e My Stats mostra una riga per rig.'
sourceHash: 'df269126bb76'
translation: draft
---

Questa guida spiega come entrare in bntpool.com con seine: indirizzo della pool, login con indirizzo Blocknet o `@name` Blocknet ID, nomi dei rig e controllo dell’arrivo delle share. Se non hai installato seine, segui prima la [guida d’installazione](https://seineminer.com/it/blog/how-to-mine-blocknet/), poi torna qui.

## Qual è l’indirizzo della pool bntpool.com?

<Answer>
L’indirizzo Stratum è `stratum+tcp://bntpool.com:3333`. seine lo propone al primo avvio: premi Invio per collegarti. Accedi con indirizzo Blocknet o `@name` Blocknet ID. Non c’è un account da creare e le ricompense vanno a quell’indirizzo.
</Answer>

Queste sono le impostazioni riportate dall’endpoint pubblico `/api/info` il 25 ottobre 2026:

| Impostazione | Valore |
|---|---|
| Indirizzo Stratum | `stratum+tcp://bntpool.com:3333` |
| Login | Indirizzo Blocknet, `@name` o `$name` |
| Schema di pagamento | PPLNS su una finestra di 6 ore |
| Conferme prima del pagamento | 60 blocchi |
| Pagamento minimo | 0,1 BNT |

Le stesse informazioni sono sempre disponibili su [Get Started](/start).

## Perché bntpool.com è la pool predefinita di seine?

Al primo avvio seine chiede la pool e propone `stratum+tcp://bntpool.com:3333`. Pool e miner si integrano in diversi modi pratici:

- **Un protocollo comune.** bntpool.com usa lo Stratum di seine, versione 2: basta il tuo indirizzo, senza altre impostazioni.
- **Nomi su entrambi i lati.** seine e la pool accettano Blocknet ID e verificano lo stesso record firmato su blocknet.id.
- **Controlli di versione.** Ogni rig comunica la versione di seine con le share. La pagina statistiche segnala i rig con versioni vecchie tramite “update available”, collegato alle release.
- **Una guida per seine.** Get Started mostra download e avvio, con un comando da copiare.

## Come collegare seine a bntpool.com?

Al primo avvio seine fa due domande:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Incolla l’indirizzo o scrivi il nome, poi premi Invio per accettare bntpool.com. Le risposte vengono salvate in `seine-data/seine.config.json`, quindi gli avvii successivi passano direttamente al mining. seine sceglie anche un nome rig e lo salva nello stesso file, come vedremo sotto.

Su un rig headless, un servizio o uno script, passa entrambi i valori come flag al primo avvio, con lo stesso comando di Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Per cambiare pool o indirizzo, modifica o elimina `seine-data/seine.config.json`, oppure sovrascrivi i valori con `--address` e `--pool-url`.

## Puoi minare verso un @name Blocknet ID?

<Answer>
Sì. bntpool.com accetta uno shortname Blocknet ID, `@name` o `$name`, ovunque accetti un indirizzo. Verifica la mappatura firmata da blocknet.id, valida l’indirizzo associato e accredita share, saldo e pagamenti a quell’indirizzo completo per tutta la connessione.
</Answer>

I nomi non distinguono maiuscole e minuscole: `@Alice` e `@alice` sono lo stesso login. Il supporto è arrivato con seine v0.2.16 ed è presente in tutte le release attuali. Prima di minare, seine stampa l’indirizzo risolto per confrontarlo con il wallet. Nella riga di comando usa apici singoli: senza, la shell proverebbe a espandere `$name` come variabile.

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Se in seguito sposti il nome su un altro wallet, riavvia seine. Il nome salvato viene risolto a ogni avvio e l’indirizzo conservato per tutta l’esecuzione; la pool fa lo stesso per ogni connessione. Il saldo già guadagnato rimane legato all’indirizzo originale e viene pagato lì: spostare un nome non sposta mai le monete.

Registra un nome su [blocknet.id](https://blocknet.id). La [guida Blocknet ID](https://seineminer.com/it/blog/blocknet-id-pay-by-name/) spiega i pagamenti tramite nome.

## Come assegnare un nome ai rig?

Ogni connessione invia un nome rig insieme all’indirizzo. Al primo avvio seine sceglie `seine-` seguito da quattro cifre e lo salva, così la macchina conserva il nome dopo i riavvii. Per uno riconoscibile, impostalo da riga di comando:

```sh
./seine --pool-worker rig-01
```

La pool conserva fino a 64 caratteri. Un nome stabile aiuta dopo il riavvio: la pool ricorda per un’ora la difficoltà delle share di ogni rig, identificato da indirizzo e nome, così riparte dalla difficoltà già stabilizzata.

## Puoi minare allo stesso indirizzo da più rig?

Sì. Avvia seine su ogni macchina con lo stesso indirizzo o `@name` e un nome rig diverso. Share, saldi e pagamenti sono contati per indirizzo: tutti i rig alimentano un unico saldo e pagamento. Ogni rig ha una riga nelle statistiche, rendendo facile verificare che tutte le macchine stiano minando.

## Come verificare che il miner funzioni?

La dashboard di seine mostra hashrate e share accettate sulla macchina. Per il punto di vista della pool apri [My Stats](/stats), inserisci indirizzo, `@name` o `$name` e premi **Lookup**. Se usi un nome, appare l’indirizzo risolto accanto alla ricerca. La pagina ricorda l’ultima ricerca nel browser; la [guida al monitoraggio](/blog/it/track-your-miner-on-bntpool/) spiega ogni scheda.

Controlla tre cose:

- **Hashrate e Shares Accepted.** L’hashrate è quello visto dalla pool e il conteggio cresce mentre i rig inviano lavoro.
- **Una riga per rig.** Mostra nome, hashrate, share accettate e rifiutate e ora dell’ultima share. Una share recente indica un rig collegato e operativo.
- **Recent Shares.** Le ultime share sono elencate con rig, difficoltà e stato.

La difficoltà viene adattata separatamente per connessione: anche una piccola CPU invia share regolari e le prime accettate appaiono entro pochi minuti. La [guida hardware Blocknet](https://seineminer.com/it/blog/blocknet-mining-hardware-hashrate/) indica gli hashrate attesi.

Poi arrivano le ricompense. Estimated Rewards compare quando la pool trova un blocco con le tue share nella finestra di sei ore. Dopo 60 conferme, la quota passa in Confirmed Rewards e viene inviata automaticamente da 0,1 BNT. [I pagamenti PPLNS](/blog/it/how-pplns-payouts-work/) spiegano la divisione. La cronologia generale è su [Blocks](/blocks) e [Payouts](/payouts).

## Cosa contiene Get Started?

La pagina [Get Started](/start) raccoglie tutto in una schermata:

- **Informazioni della pool.** Indirizzo Stratum cliccabile per copiarlo, schema, minimo e conferme necessarie, letti in tempo reale.
- **Avvio rapido in tre passaggi.** Scarica seine da [seineminer.com](https://seineminer.com/it/#download), eseguilo e inserisci indirizzo o nome e URL della pool, poi mina e segui hashrate e saldo su My Stats.
- **Un comando da copiare.** Il comando `./seine` completo con URL già compilato. Inserisci il tuo indirizzo o `'@yourname'` tra apici.
- **Note di configurazione.** Dove viene salvata, come azzerarla e quali flag la sovrascrivono.
- **Come vengono contate le ricompense.** Una breve spiegazione PPLNS: le share precedenti al blocco ricevono la sua ricompensa; il lavoro successivo conta per i blocchi seguenti. Le ricompense non confermate restano separate su My Stats fino alla conferma.
- **Screenshot** della prima configurazione e della dashboard di seine, così sai cosa aspettarti.

<CTA target="pool" title="Inizia a minare su bntpool.com" text="Collega seine a bntpool.com con il tuo indirizzo o @name e guarda arrivare le share su My Stats." />
