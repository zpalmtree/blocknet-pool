---
sourceHash: 'e64d7723349d'
translation: draft
title: Come il mining protegge i pagamenti Blocknet
description: Scopri come Blocknet sceglie la cronologia dei pagamenti tramite proof of work, cosa aggiungono le conferme e quali controlli eseguono i nodi sui blocchi.
tags: [Blocknet, Sicurezza dei pagamenti, Proof of work]
faq:
  - q: Più potenza di mining può autorizzare un pagamento dal mio wallet?
    a: La potenza di mining aiuta a produrre blocchi concorrenti. Spendere fondi richiede comunque un’autorizzazione valida, e i nodi verificano prove e bilancio delle transazioni prima di accettare un blocco. Anche un ramo più pesante deve rispettare queste regole. Il proof of work non sostituisce le chiavi private necessarie per spendere i tuoi output.
  - q: Blocknet segue sempre la catena con più blocchi?
    a: Core v0.22.0 calcola il lavoro cumulativo sommando la difficoltà di ogni blocco. Un ramo valido con più lavoro può diventare la catena scelta, nel rispetto delle regole di riorganizzazione. Il numero di blocchi non basta, perché le difficoltà possono essere diverse. Contano sia il lavoro sia la validità.
  - q: Conferme dei pagamenti e share del pool sono la stessa cosa?
    a: Una share registra lavoro inviato al pool. Una conferma riguarda l’inclusione della transazione nella catena scelta e i blocchi costruiti successivamente. Le share aiutano a dividere le ricompense, mentre i blocchi di rete fanno avanzare la cronologia dei pagamenti. Aumentare le share non aggiunge direttamente conferme a un pagamento specifico.
---

Segui un pagamento BNT dall’inclusione in un blocco a una cronologia sostenuta dal lavoro di mining successivo. Questa guida spiega perché la rete deve scegliere tra cronologie concorrenti, cosa indicano le conferme e come la scelta si affianca alla validazione delle transazioni private. L’implementazione descritta è core v0.22.0, verificata il 28 ottobre 2026.

## Cosa protegge il mining quando invii BNT?

<Answer>
Il mining aiuta la rete a scegliere una cronologia dei pagamenti condivisa e ordinata. Ogni blocco accettato si collega al precedente e apporta proof of work. I nodi controllano blocchi e transazioni proposti, poi confrontano i rami validi per lavoro cumulativo secondo le regole di riorganizzazione. I blocchi successivi aggiungono lavoro dietro un pagamento incluso, dando significato concreto alle sue conferme.
</Answer>

Un pagamento valido richiede autorizzazione e un posto nel registro. Il wallet fornisce l’autorizzazione a spendere i tuoi fondi. Mining e selezione della catena aiutano i nodi a concordare quali transazioni valide appartengano alla cronologia attuale. Questi compiti si completano: il proof of work di un blocco non rende accettabile un pagamento altrimenti invalido.

La [guida all’applicazione delle regole](https://seineminer.com/it/blog/who-controls-blocknet/) spiega chi sviluppa, propone e valida. Qui seguiamo il pagamento: come la sua inclusione entra nella cronologia consultata dal wallet e dagli altri nodi.

## Come vincolano i blocchi una cronologia dei pagamenti?

L’header contiene l’hash del blocco precedente e una radice di Merkle derivata dalle transazioni. La radice vincola crittograficamente il contenuto, mentre l’hash precedente collega il blocco alla cronologia passata. Modificare una transazione cambia la radice e l’header su cui si esegue il mining. I blocchi discendenti devono quindi riferirsi alla cronologia modificata tramite i propri collegamenti.

Considera un pagamento in un blocco seguito da diversi altri blocchi accettati. Sostituire quel tratto richiede un ramo concorrente con blocchi validi collegati e lavoro cumulativo sufficiente, entro le regole di riorganizzazione del nodo. Modificare una transazione in un file locale non produce quel ramo. Il lavoro è legato a una precisa sequenza di header e transazioni.

Il [codice dei blocchi pubblicato](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) definisce questi campi, i controlli Merkle e i percorsi di selezione della catena. La [guida alla difficoltà](/blog/it/mining-difficulty-explained/) spiega come la rete stabilisce il lavoro richiesto per ogni nuovo blocco.

## Perché il lavoro cumulativo conta più del numero di blocchi?

Due minatori possono trovare blocchi validi diversi che estendono lo stesso genitore. Ogni proposta ha un hash proprio, e i nodi possono riceverle in ordini diversi. Possono così comparire brevemente rami concorrenti. Serve una regola che segua il lavoro a sostegno della cronologia, anziché l’ordine in cui un peer l’ha annunciata.

Core v0.22.0 somma la difficoltà del blocco al lavoro cumulativo del genitore. Dopo la validazione, un ramo con lavoro strettamente maggiore può far cambiare la catena scelta. Un’alternativa con lavoro uguale non provoca quel cambio tramite questo confronto. La difficoltà deve essere valida nel contesto del genitore del ramo: un minatore non può semplicemente scrivere un numero maggiore nell’header.

Confronta due rami con una parte iniziale comune. Questa contribuisce allo stesso modo a entrambi i totali. La differenza decisiva è il lavoro accumulato dopo la separazione, insieme ai controlli di validità e riorganizzazione. Contare i blocchi senza considerarne la difficoltà può nascondere la differenza. È un confronto concettuale, non il resoconto di una biforcazione in corso.

## Come aiutano le conferme a seguire un pagamento?

Dopo l’inclusione del pagamento nella catena scelta, i blocchi successivi si costruiscono su quella cronologia. La sua profondità cresce con l’arrivo dei blocchi. Sostituirne l’inclusione richiederebbe una cronologia alternativa ammissibile da prima del blocco interessato, mentre il ramo attuale continua ad accumulare lavoro. Per questo inclusione e profondità dicono più della semplice diffusione di una transazione.

L’altezza identifica una posizione; l’hash identifica un blocco preciso. Controlla il record della transazione e il blocco che la contiene, poi verifica che sia ancora nella catena scelta. Se cambia la cronologia selezionata, possono cambiare inclusione e profondità. I contatori possono includere il blocco contenitore oppure contare soltanto quelli successivi: leggi la convenzione dell’interfaccia.

Core v0.22.0 applica anche un limite di riorganizzazione: rifiuta un cambio se l’antenato comune è sotto l’altezza della punta attuale meno 100. È una regola dell’implementazione in esecuzione, non una promessa universale associata a un contatore. La [guida ai tempi di trasferimento](https://seineminer.com/it/blog/blocknet-transfer-time/) tratta la spendibilità ordinaria nel wallet; le [conferme delle ricompense](/blog/it/block-confirmations-mining-rewards/) spiegano maturazione coinbase e contabilità del pool.

## Come si verifica la doppia spesa di un pagamento privato?

Il formato pubblicato di Blocknet usa una key image legata alla prova di spesa. La validazione controlla il legame e rifiuta una key image già spesa nello stato pertinente della catena. I nodi rilevano così usi incompatibili di un output, mentre la firma ad anello nasconde quale membro ammissibile sia l’input reale. Gli importi privati restano soggetti a controlli di bilancio e intervallo.

Supponi che il proprietario autorizzi transazioni incompatibili che spendono lo stesso output. Una cronologia valida accettata non può contenerle entrambe. La selezione della catena conta perché cronologie concorrenti possono contenere scelte diverse; la validazione conta perché ciascuna deve rispettare le regole di spesa. Più lavoro non sostituisce una firma valida e non fa superare il controllo contabile a una transazione sbilanciata.

Questi controlli sono nel [validatore di transazioni pubblicato](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). La [guida alla privacy di Blocknet](https://seineminer.com/it/blog/blocknet-privacy-features/) spiega gli altri meccanismi. La sicurezza dei pagamenti combina spese valide, cronologia coerente e crittografia che protegge ciò che questa rivela.

## Che ruolo ha il mining in pool nella sicurezza dei pagamenti?

Un pool coordina la ricerca del blocco e invia il candidato risolto al nodo a monte. Una share che soddisfa il target più facile del pool registra il lavoro apportato; solo una soluzione che soddisfa il target di rete può diventare un blocco dopo la validazione. La [spiegazione dei pool](/blog/it/what-is-a-mining-pool/) distingue i due target e i loro scopi.

Il tuo miner può quindi contribuire ad estendere la cronologia mentre il pool contabilizza il lavoro separatamente. Un saldo privato del pool, un numero di share e un pagamento confermato nel wallet indicano cose diverse. Parti dalla [guida alla connessione al pool](/blog/it/mine-blocknet-on-bntpool/) per partecipare e usa il record del wallet per seguire un pagamento BNT preciso.

<CTA target="pool" title="Contribuisci al lavoro di Blocknet" text="Collega seine a bntpool.com e segui lavoro di mining, ricompense e pagamenti tramite gli strumenti pubblici del pool." />
