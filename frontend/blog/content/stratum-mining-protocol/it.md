---
title: 'Cos''è Stratum? Come i miner parlano con il pool'
description: 'Segui Stratum tra seine e bntpool.com: accesso, lavori, target delle share, invii e risposte, per capire le diverse fasi della connessione del tuo miner.'
tags: ['Fondamenti di mining', 'Stratum', 'Mining in pool']
faq:
  - q: 'A cosa serve Stratum nel mining in pool?'
    a: 'Stratum trasporta la conversazione tra miner e pool: identifica la sessione, consegna lavoro e trasferisce share e risposte. Il miner calcola gli hash localmente. Il pool collega questo lavoro al proprio nodo e tiene la contabilità necessaria per il sistema di pagamenti.'
  - q: 'Un indirizzo Stratum è come un indirizzo web?'
    a: 'Servono connessioni differenti. Il sito bntpool.com usa HTTPS per pagine e API; l''endpoint di mining documentato è stratum+tcp://bntpool.com:3333. Inseriscilo nell''impostazione del pool del miner. Aprire correttamente il sito non stabilisce da solo una connessione di mining.'
  - q: 'Una share Stratum accettata significa un pagamento inviato?'
    a: 'No. La risposta indica l''esito di un invio nella sessione. Verifica, blocco valido per la rete, idoneità alla ricompensa e pagamento completato sono fasi distinte. Segui l''attività accettata in My Stats e leggi la guida ai pagamenti per conferme, PPLNS e soglia.'
sourceHash: '5cc5086d7b28'
translation: draft
---

Segui un lavoro da bntpool.com a seine e una share di ritorno al pool. Capire questo dialogo chiarisce messaggi di connessione, aggiornamenti e risposte. La guida descrive l'implementazione di bntpool.com; per endpoint e indirizzo di pagamento iniziali, usa la [guida del pool](/blog/it/mine-blocknet-on-bntpool/).

## Cos'è Stratum nel mining?

<Answer>
Stratum è lo strato di comunicazione per ricevere lavoro dal pool e inviare risultati. Con seine e bntpool.com una connessione TCP persistente porta messaggi JSON per login, lavori e share. Il computer calcola gli hash localmente; il pool ottiene modelli di blocco dal nodo e processa il lavoro inviato.
</Answer>

*Un miner invia login e share al pool, riceve lavori e risposte, mentre il pool scambia modelli e candidati a blocco con il nodo.*

## L'endpoint identifica la connessione di mining

L'endpoint documentato è `stratum+tcp://bntpool.com:3333`. Lo schema indica il trasporto, l'hostname identifica il servizio e la porta il listener di mining. La connessione del browser a `https://bntpool.com` ha un altro scopo. Copia l'endpoint completo da [Get Started](/start) nelle impostazioni del pool per aprire la connessione prevista.

La connessione persistente consente messaggi da entrambi i lati quando c'è lavoro. bntpool usa JSON delimitato da nuove righe e ID che collegano risposte e richieste. Questo è il formato del pool: il miner deve supportarlo, insieme alle capacità e all'algoritmo Blocknet. Il [codice del protocollo](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) registra i campi attuali.

## Cosa succede al login del miner?

Il client invia `login` con indirizzo di pagamento, nome del rig, versione del protocollo e capacità supportate. Il pool verifica e risponde con dati del protocollo e capacità richieste. Così stabilisce come scambiare lavoro. La connessione TCP è il primo passo; login accettato conferma l'avvio del dialogo applicativo.

Il nome del rig distingue una macchina sotto un indirizzo. Usa nomi diversi per più rig mantenendo l'indirizzo di pagamento desiderato. Serve l'indirizzo ricevente, non la frase di recupero o la chiave privata di spesa. Dopo l'accettazione della sessione, se c'è lavoro, il pool invia `job` per iniziare l'hashing.

## Quali dati contiene un lavoro di mining?

Un lavoro contiene ID dell'assegnazione e del modello, dati dell'header, altezza, target share e rete, e un intervallo di nonce. Il nonce è il valore modificato durante l'hashing. L'intervallo definisce lo spazio di ricerca della sessione. L'ID collega un invio successivo al lavoro e alle condizioni originariamente assegnate.

Il target share stabilisce quali risultati inviare come prova del lavoro. Il target di rete stabilisce i possibili candidati a blocco. Molte share alimentano la contabilità senza diventare blocchi. La [guida alla difficoltà](/blog/it/mining-difficulty-explained/) spiega target e lavoro atteso; il protocollo fornisce questi target nell'assegnazione.

## I lavori possono cambiare a connessione aperta

Un nuovo lavoro da solo non indica una disconnessione. Il pool può aggiornare il lavoro quando cambia il modello del nodo o inviare un'assegnazione con diversa difficoltà share. La difficoltà variabile adatta la soglia di invio al flusso osservato. Il miner segue la nuova assegnazione mentre la connessione continua a portare messaggi nei due sensi.

Distingui ID di lavoro e di modello nei log tecnici. Più assegnazioni possono usare lo stesso modello con dettagli di sessione diversi. L'altezza non descrive ogni modifica. Per l'operatore conta che arrivino lavori, l'hashing proceda e gli invii ricevano risposta; la [guida al monitoraggio](/blog/it/track-your-miner-on-bntpool/) mostra dove vedere l'attività.

## Cosa contiene l'invio di una share?

Quando trova un risultato idoneo, seine invia `submit` con lavoro, nonce e hash dichiarato. Il pool processa il contesto dell'assegnazione e risponde con l'ID della richiesta. Il valore dichiarato fa parte del messaggio; verifica e validazione del blocco determinano il seguito. Un candidato a blocco viene inviato al daemon per la validazione.

La risposta può riportare accettazione, stato di verifica e difficoltà. Leggi i campi separatamente, senza trattare ogni risposta riuscita come una scoperta di blocco. Contabilità e pagamento seguono oltre questo scambio. La [guida alle share](/blog/it/accepted-rejected-stale-shares/) spiega gli esiti; la [guida PPLNS](/blog/it/how-pplns-payouts-work/) collega lavoro idoneo, ricompense e pagamenti.

## Come usare la sequenza per controllare un miner?

Leggi le fasi in ordine: endpoint raggiunto, login accettato, lavoro ricevuto, hashing attivo, invii risposti. Dopo una perdita di connessione il client riconnette e crea una sessione prima di ricevere altro lavoro. Usa messaggio esatto e orario per individuare la fase. Il sito accessibile non dimostra che la porta di mining sia raggiungibile da quella macchina.

Apri [My Stats](/stats) per l'indirizzo previsto e lo [stato del pool](/status) per il servizio. Dopo avvio o riconnessione lascia accumulare osservazioni alle stime basate sulle share. Separa protocollo e tempi di pagamento: login prepara la sessione, i lavori danno compiti, le share riportano risultati e la contabilità determina le successive ricompense.

<CTA target="pool" title="Collega seine a bntpool.com" text="Usa la pagina di configurazione e segui lavori, share e il tuo rig su My Stats." />
