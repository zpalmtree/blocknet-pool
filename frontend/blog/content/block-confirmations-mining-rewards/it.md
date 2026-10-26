---
title: 'Conferme dei blocchi e ricompense di mining immature'
description: 'Segui un blocco Blocknet fino alla maturità e al pagamento. Scopri come bntpool conta la profondità e perché le conferme si misurano in blocchi.'
tags: ['Ricompense di mining', 'Conferme', 'Blocknet']
faq:
  - q: 'Quante conferme richiede bntpool?'
    a: 'Alla verifica del 26 ottobre 2026 bntpool.com pubblica 60 blocchi. Sottrae l''altezza trovata da quella corrente e confronta l''hash con il nodo. Consulta Get Started o l''API pubblica per il valore attuale.'
  - q: '60 blocchi significano esattamente cinque ore?'
    a: 'No. Sessanta intervalli obiettivo da cinque minuti danno una stima di 300 minuti, cinque ore. L''avanzamento reale dipende dai blocchi aggiunti a intervalli variabili. Maturità, soglia del saldo e transazione successiva sono fasi separate.'
  - q: 'Devo continuare a minare perché una ricompensa maturi?'
    a: 'Un blocco trovato guadagna profondità con i nuovi blocchi della rete anche se il miner è scollegato. La tua quota segue le regole contabili del pool. Fermarsi non accelera né azzera la maturazione; segui la ricompensa in My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Segui un blocco Blocknet attraverso conferma, maturità e pagamento al wallet. Questa guida spiega la profondità dietro le schede di bntpool.com. La [guida PPLNS](/blog/it/how-pplns-payouts-work/) tratta la divisione; la [guida di configurazione](/blog/it/mine-blocknet-on-bntpool/) spiega come iniziare.

## Cosa sono conferme e ricompense immature?

<Answer>
Le conferme descrivono la posizione di un blocco sotto la punta corrente. Una ricompensa immatura viene da un blocco recente ancora sotto la profondità richiesta. bntpool verifica profondità e identità prima di confermare; idoneità del saldo e pagamento seguono separatamente.
</Answer>

*Il blocco guadagna profondità, raggiunge la maturità e passa dal saldo idoneo a un pagamento distinto.*

## Come conta bntpool la profondità?

Il pool sottrae l'altezza del blocco da quella corrente. Il 26 ottobre 2026 il requisito pubblicato era 60 blocchi. Con questa convenzione il blocco parte da zero. Altrove il blocco contenitore può contare come prima conferma: confronta anche la regola.

Esempio: un blocco a 70.000 ha profondità zero con punta 70.000, uno a 70.001 e 60 a 70.060. Sono altezze illustrative. Il pool verifica anche l'hash con il nodo. L'[implementazione](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) documenta entrambi i controlli.

## Perché cinque ore è una stima?

Blocknet punta a cinque minuti tra blocchi. Sessanta per cinque dà 300 minuti, cinque ore. Aiuta a pianificare, ma la condizione resta un conteggio di blocchi. Osserva l'avanzamento dalla quota trovata alla profondità necessaria, non una scadenza fissa.

Qualsiasi blocco della catena accettata aumenta la profondità, non solo quelli di bntpool. La velocità del tuo miner non crea un orologio privato. Gli arrivi variano e la durata può differire dalla stima. Le guide a [difficoltà](/blog/it/mining-difficulty-explained/) e [fortuna](/blog/it/mining-pool-luck-explained/) spiegano regolazione e variabilità dei round.

## Distingui in attesa, confermato e pagato

Un nuovo blocco resta in attesa mentre guadagna profondità. La quota stimata può apparire in [My Stats](/stats) se hai lavoro idoneo. La ricompensa crea monete; la regola coinbase del wallet Blocknet richiede 60 blocchi prima di spenderle. L'attesa corrispondente del pool segue questa maturazione.

Superati i controlli, il pool può confermare e distribuire la ricompensa ai saldi idonei. Non significa pagare istantaneamente ogni indirizzo. Il processo valuta separatamente saldi confermati e condizioni d'invio. Stato del blocco, saldo e storico descrivono parti diverse dello stesso flusso.

## Perché verificare l'hash?

L'altezza identifica una posizione, l'hash un blocco preciso. I nodi possono conoscere blocchi concorrenti; la catena accettata stabilisce quale occupa la posizione. Il pool interroga il nodo e confronta l'hash. La sola profondità non prova che il blocco originale sia ancora nella catena.

Un blocco uscito può essere marcato orfano e la sua ricompensa provvisoria non è più idonea come ricompensa della catena accettata. Il pool tratta lo stato separatamente. Usa il link hash in [Blocks](/blocks): due voci alla stessa altezza non sono necessariamente lo stesso evento. La stima resta legata al blocco.

## Come differisce il pagamento dalla ricompensa?

La ricompensa nasce nell'output coinbase. Il pagamento del pool è una transazione successiva verso il tuo indirizzo. Alla data verificata il minimo pubblicato è 0,1 BNT. Maturità e soglia del saldo sono condizioni diverse; la [guida PPLNS](/blog/it/how-pplns-payouts-work/) dettaglia ripartizione e pagamento.

Dopo l'invio, segui la transazione e la ricezione e spendibilità nel wallet. Le sue conferme appartengono al pagamento, non al blocco originale. Non serve cambiare indirizzo o riconnettere il miner durante la maturazione. Confronta lo [storico pagamenti](/payouts) e il wallet con lo stesso indirizzo.

## Come seguire tutta la ricompensa?

In [Blocks](/blocks) identifica altezza, hash e stato. Apri [My Stats](/stats) con l'indirizzo del miner e controlla stime e ricompense confermate. Quando esiste un pagamento, segui il link della transazione. Ogni osservazione riguarda blocco, saldo del conto o pagamento al wallet.

Verifica i valori in [Get Started](/start) o `https://bntpool.com/api/info` e lo [stato del pool](/status). Per indagare, annota indirizzo, identificatore del blocco o transazione e ora; non condividere parole di recupero. La [guida di monitoraggio](/blog/it/track-your-miner-on-bntpool/) completa le statistiche. I blocchi avanzano anche offline; l'idoneità segue lavoro registrato e catena.

<CTA target="pool" title="Segui le tue ricompense" text="Collegati a bntpool.com e segui stime, saldi confermati e pagamenti in My Stats." />
