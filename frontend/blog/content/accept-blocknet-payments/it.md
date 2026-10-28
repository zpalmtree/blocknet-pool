---
title: "Accettare pagamenti Blocknet per beni e servizi"
description: "Accetta BNT per ordini e fatture, identifica i pagamenti nel wallet Blocknet, controlla le conferme e gestisci con chiarezza pagamenti parziali e rimborsi."
tags: [Blocknet, BNT]
faq:
  - q: "Come verifico che un cliente abbia pagato in Blocknet?"
    a: "Trova il pagamento nel tuo wallet sincronizzato, confronta importo e riferimento con l’ordine e controlla le conferme. Registra l’ID completo della transazione. Uno screenshot del cliente o un link di pagamento non dimostra la ricezione; l’explorer non mostra l’importo privato inviato al tuo indirizzo."
  - q: "Cosa faccio se il cliente invia meno BNT del richiesto?"
    a: "Lascia l’ordine parzialmente pagato e calcola il residuo dai pagamenti verificati. Richiedi la differenza con lo stesso riferimento e conserva entrambi gli ID. Conta ogni ricezione una sola volta. Se concordate l’annullamento, conferma un indirizzo di rimborso nella conversazione già aperta per l’ordine."
  - q: "Il memo segna automaticamente un ordine come pagato?"
    a: "No. Il memo aiuta ad associare un pagamento all’ordine, ma il pagatore può modificarlo o ometterlo. Controlla importo e stato della transazione. Il wallet crea richieste e mostra ricezioni; questo flusso manuale non aggiunge un checkout automatico né un sistema di gestione ordini."
sourceHash: '3309a069e5de'
translation: draft
---

Accetta Blocknet indicando un importo chiaro in BNT, controllando la ricezione nel tuo wallet e associandola all’ordine. L’esempio è un ordine fittizio di **1.25 BNT** con riferimento **order-1042**. Usiamo il wallet desktop pubblicato v0.22.0; le schermate contengono dati dimostrativi segnalati.

## Come si accetta un pagamento Blocknet?

<Answer>
Concorda importo in BNT e riferimento, invia una richiesta verso un indirizzo che controlli e attendi la ricezione nel wallet sincronizzato. Confronta importo e riferimento, verifica le conferme, registra l’ID della transazione e segna l’ordine come pagato secondo la regola di conferma concordata con il cliente.
</Answer>

La [guida al wallet desktop](https://seineminer.com/it/blog/blocknet-desktop-wallet/) spiega come crearlo o aprirlo. Conserva il backup di recupero separato dagli ordini. Ricevere un pagamento non richiede un miner o l’adesione a un pool.

## Definisci importo e riferimento dell’ordine

Annota riferimento, importo in BNT, indirizzo di ricezione e acquisto. Se il prezzo è espresso in un’altra valuta, concorda importo in BNT e validità del preventivo prima del pagamento. Il link contiene l’importo richiesto; non aggiorna un cambio né impone una scadenza.

Nell’esempio richiedi **1.25 BNT** con memo **order-1042**. Basta il numero d’ordine: indirizzi di consegna e dati del cliente restano nel gestionale privato. Un [memo cifrato](https://seineminer.com/it/blog/blocknet-encrypted-memos/) accompagna il pagamento, ma il memo scritto in un link condiviso è leggibile da chi riceve quel link.

In **Receive → Payment Request**, inserisci importo e memo e scegli **Generate Payment Link**. Controlla destinatario e importo prima di condividere. La [guida ai link di pagamento](https://seineminer.com/it/blog/blocknet-payment-links/) spiega formati e destinatari per nome. Per una fattura fissa conserva l’indirizzo completo insieme alla richiesta.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Wallet Blocknet pubblicato con richiesta di 1.25 BNT, memo order-1042 e indirizzo dimostrativo." loading="lazy" decoding="async"></a>
<figcaption>Interfaccia inglese reale con dati dimostrativi. Generare la richiesta non trasferisce fondi. Apri l’immagine per ingrandirla.</figcaption>
</figure>

## Controlla la ricezione nel tuo wallet

Chiedi al cliente di includere il riferimento e conservare l’ID della transazione. Quando nodo e wallet sono aggiornati, apri **History** e trova il pagamento entrante. Confronta importo e memo con l’ordine. Il pagatore può cambiare entrambi, quindi la richiesta non dimostra cosa sia stato inviato.

Salva l’ID completo con l’importo ricevuto. Non contare due volte una transazione perché compare in Dashboard e History o perché il cliente ti rimanda l’ID. Associa ricezioni specifiche, non la variazione del saldo totale: può includere mining, altri clienti e trasferimenti tuoi.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History del wallet Blocknet mostra 1.25 BNT ricevuti con memo order-1042 e dati dimostrativi." loading="lazy" decoding="async"></a>
<figcaption>La ricezione d’esempio corrisponde a importo e riferimento richiesti. Controlla lo stato prima di evadere l’ordine.</figcaption>
</figure>

Se il pagamento manca, confronta indirizzo completo e ID con il cliente e controlla la sincronizzazione. Lo screenshot di invio dà contesto; il tuo wallet ricevente stabilisce l’importo ricevuto.

## Quando segnare l’ordine come pagato?

Distingui pagamento entrante non confermato, transazione inclusa in un blocco e fondi spendibili. Il wallet pubblicato attende dieci blocchi successivi a quello del pagamento ordinario prima di rendere spendibili i suoi output. La [guida ai tempi di trasferimento](https://seineminer.com/it/blog/blocknet-transfer-time/) spiega contatori e attese variabili.

Comunica la regola sulle conferme prima del pagamento. Un flusso manuale semplice attende che la ricezione corrispondente sia spendibile prima di evadere l’ordine. Un importo visibile non completa il controllo. Non promettere una consegna a un’ora precisa basandoti sul conto alla rovescia: l’arrivo dei blocchi varia. [Come il mining protegge i pagamenti](/blog/it/how-mining-secures-blocknet-payments/) spiega la profondità di conferma.

## Gestisci pagamenti parziali, in eccesso o non associati

| Ricezione verificata | Azione successiva |
|---|---|
| 1.00 BNT per l’ordine da 1.25 BNT | Registra il pagamento parziale e richiedi i restanti 0.25 BNT. |
| Altri 0.25 BNT successivamente | Conserva entrambi gli ID, conta una sola volta il totale di 1.25 BNT e verifica le conferme di entrambi. |
| Più di 1.25 BNT | Concorda rimborso o credito e annota la decisione. |
| Importo corretto senza memo | Conferma ID completo e destinazione con il cliente prima dell’associazione. |
| Pagamento in attesa | Lascia l’ordine in attesa di conferma e ricontrolla lo stato. |

Per rimborsare, chiedi conferma dell’indirizzo nella conversazione esistente dell’ordine. Non ricavarlo dalla transazione pubblica: gli input Blocknet non forniscono un normale indirizzo del mittente. Controlla importo, destinazione e commissione prima di approvare il nuovo invio; conserva il suo ID con il pagamento originale.

## Condividi solo le prove necessarie

L’explorer aiuta a controllare l’inclusione, ma non rivela l’importo privato ricevuto dal tuo indirizzo. Se la ricezione è contestata, il mittente può fornire prove specifiche seguendo la [guida alle prove di pagamento](https://seineminer.com/it/blog/blocknet-payment-proof/). Verifica destinatario, importo e stato in catena: la sola validità della chiave non basta.

Conserva privatamente ordine, richiesta, ID entranti, importi verificati, decisione sulle conferme ed eventuale rimborso. Condividi solo la ricevuta o prova pertinente. Non fornire mai parole di recupero, password o chiavi private di visualizzazione dell’intero wallet come prova.

Se fai anche mining, separa i [pagamenti del pool](/blog/it/track-your-miner-on-bntpool/) dagli ordini anche nello stesso wallet, così ogni fattura resta associata al trasferimento corretto.


<CTA target="pool" title="Ricevi anche le ricompense di mining" text="La guida del pool mostra come collegare un miner e seguire i suoi pagamenti insieme alle altre ricezioni di Blocknet." />
