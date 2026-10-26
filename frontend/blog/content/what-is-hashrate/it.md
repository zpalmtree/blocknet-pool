---
title: Cos’è l’hashrate nel mining di criptovalute?
description: L’hashrate conta gli hash calcolati al secondo. Scopri le unità, le differenze fra miner, pool e rete e perché ciascuno di questi valori oscilla.
tags: [Hashrate, Basi del mining, Blocknet, bntpool]
faq:
  - q: 'Quale hashrate raggiunge un computer su Blocknet?'
    a: 'Nei test seine, i migliori risultati per generazione sono 49,4 H/s su AMD EPYC Zen 5, 35,7 H/s su AMD EPYC Zen 4, 30,9 H/s su un laptop Apple M4 Max e 21,5 H/s su AWS Graviton4 Arm. Il tuo dato dipende soprattutto dalla banda di memoria e da quanti thread da 2 GB contiene la RAM.'
  - q: 'Perché Blocknet usa H/s anziché MH/s o TH/s?'
    a: 'Ogni hash è un calcolo Argon2id che scrive circa 2 GiB di memoria e li rilegge: anche un grande server completa solo decine di hash al secondo. Le proof of work basate su hash rapidi come SHA-256 raggiungono numeri maggiori e usano unità più grandi. Confronta hashrate solo dello stesso algoritmo.'
  - q: 'Perché la pool mostra un hashrate diverso dal miner?'
    a: 'Il miner conta ogni hash completato. La pool vede solo le share e stima l’hashrate dalla loro difficoltà nell’ultima ora. Le share arrivano casualmente, quindi la stima oscilla di più e si stabilizza nel tempo. Confronta il grafico 24 ore di My Stats con la media del miner.'
  - q: 'Un hashrate maggiore significa più ricompense?'
    a: 'Sì, in proporzione. Ogni hash ha la stessa probabilità di trovare un blocco: la quota attesa di blocchi è la quota di hashrate di rete. Raddoppiando l’hashrate raddoppi le ricompense attese; se cresce la rete, la tua quota scende. Su bntpool.com ricevi una parte di ogni blocco trovato.'
sourceHash: '5af3a8b3f18b'
translation: draft
---

L’hashrate è il primo numero mostrato da miner, pool e block explorer. Questa guida spiega cosa misura, come leggere le unità da H/s in su, il rapporto fra il tuo hashrate, quello della pool e della rete e perché cambiano minuto per minuto, usando Blocknet e bntpool.com come esempio.

## Cos’è l’hashrate?

<Answer>
È il numero di hash calcolati al secondo. Nel mining proof of work ogni hash è un tentativo di risolvere un blocco, con la stessa piccola probabilità di successo. Più hashrate significa più tentativi: la quota attesa di blocchi cresce in proporzione alla quota dell’hashrate totale.
</Answer>

Una catena proof of work chiede un header il cui hash sia sotto un target. Non ci sono scorciatoie: il miner cambia il nonce nell’header, calcola l’hash, confronta il risultato e ripete. L’hashrate conta i tentativi completati ogni secondo.

Su Blocknet un tentativo è un hash Argon2id. Il nonce entra come password, il resto dell’header come salt, e la funzione lavora su 2 GiB di memoria prima di produrre la risposta da 32 byte. seine e pool riportano hash completati al secondo: 1 H/s equivale a un intero calcolo Argon2id al secondo.

## Cosa significano H/s, kH/s e MH/s?

Le unità seguono i prefissi metrici: ogni passo vale mille volte il precedente.

| Unità | Hash al secondo | Uso tipico |
|---|---|---|
| H/s | 1 | Un computer su Blocknet |
| kH/s (KH/s) | 1.000 | Totali pool e rete Blocknet |
| MH/s | 1.000.000 | Algoritmi proof of work più rapidi |
| GH/s | 1.000.000.000 | Hash rapidi come SHA-256 |
| TH/s e oltre | 1.000.000.000.000 e più | Totali di rete per hash rapidi |

bntpool.com scrive i migliaia come KH/s e sale di unità ogni volta che il valore supera 1.000: 44,44 KH/s significa 44.440 H/s. Prima di dividere due hashrate convertili entrambi in H/s.

## Perché gli hashrate Blocknet sono piccoli?

La proof of work è memory-hard. Ogni hash scrive circa 2 GiB e ne rilegge circa 2 GiB, molto più della cache di qualsiasi processore: la velocità della memoria determina il ritmo. Ogni thread ha bisogno di 2 GiB propri, perciò la RAM determina quanti thread possono funzionare.

Questi sono i migliori risultati seine per generazione:

| Hardware | Thread | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Server Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Anche il server Zen 5 a 32 thread completa circa 1,5 hash per thread al secondo. Gli algoritmi rapidi producono numeri maggiori: il wiki Bitcoin ricorda che i miner double SHA-256 erano già valutati in miliardi di hash al secondo nel 2014. I numeri misurano quantità di lavoro diverse per hash, quindi hanno senso solo all’interno dello stesso algoritmo. [La proof of work Argon2id](https://seineminer.com/it/blog/argon2id-proof-of-work-explained/) approfondisce l’algoritmo; la [guida hardware Blocknet](https://seineminer.com/it/blog/blocknet-mining-hardware-hashrate/) spiega banda di memoria e RAM.

## Il tuo hashrate, quello della pool e della rete

Chi mina in [pool](/blog/it/what-is-a-mining-pool/) deve considerare tre hashrate, misurati diversamente.

### Il tuo hashrate

seine conta ogni hash e mostra **Current**, il ritmo attuale, e **Average**, la media della sessione. Per una lettura stabile usa la media dopo qualche minuto di ottimizzazione di thread e kernel CPU su lavoro reale.

La pool non vede gli hash, ma le share: hash che superano un target più facile assegnato alla connessione. bntpool.com stima l’hashrate dalla difficoltà totale accettata nell’ultima ora e lo mostra su [My Stats](/stats). [Segui il tuo miner](/blog/it/track-your-miner-on-bntpool/) spiega la pagina.

### Hashrate della pool

**Pool Hashrate** somma il lavoro delle share di tutti i miner nella stessa finestra di un’ora. Indica quanta parte del lavoro di rete svolge bntpool.com e quanto spesso può aspettarsi un blocco, mostrato come **Expected Block Time**.

### Hashrate della rete

Nessuno può contare tutti gli hash della rete, quindi il dato si ricava dai blocchi. La difficoltà è il numero atteso di hash per blocco e Blocknet punta a 300 secondi.

## Come si calcola l’hashrate di rete?

<Answer>
Si stima da difficoltà e tempi di blocco. Su Blocknet la difficoltà è circa il numero di hash attesi per blocco; con un target di 300 secondi, l’hashrate è circa difficoltà divisa per 300. La dashboard usa lavoro e timestamp effettivi degli ultimi dieci blocchi.
</Answer>

Ecco il calcolo con dati letti il 25 ottobre 2026 alle 02:26 EDT. Il [block explorer](https://explorer.blocknetcrypto.com) mostrava una difficoltà successiva di 16.943.147, quindi circa 16,9 milioni di hash per blocco. Divisi per 300 secondi danno circa 56.477 H/s.

Nello stesso momento bntpool.com mostrava 44,44 KH/s, cioè 44.440 H/s. Sono stime della stessa cosa da angoli diversi. La difficoltà indica il ritmo che produrrebbe blocchi esattamente ogni cinque minuti. La dashboard divide il lavoro reale degli ultimi dieci blocchi per il tempo reale: se hanno richiesto più di cinque minuti, legge meno. La difficoltà si adatta a ogni blocco, tenendo i valori vicini nel tempo.

## Perché l’hashrate oscilla?

Ogni numero cambia per ragioni diverse.

- **La macchina.** Nei primi minuti seine confronta thread e kernel e sceglie i più rapidi: il ritmo sale durante l’ottimizzazione. Poi altri usi di memoria e core possono prendere parte del tempo di calcolo.
- **La vista della pool.** Le share sono casuali. bntpool.com punta a circa 10 ogni cinque minuti, circa 120 all’ora: un periodo fortunato o tranquillo muove la stima. Un nuovo rig viene mediato sui minuti trascorsi finché si riempie l’ora.
- **La rete.** Anche i tempi di blocco sono casuali e dieci blocchi sono pochi. Il 25 ottobre 2026 la dashboard leggeva circa 50.057 H/s alle 00:26 EDT, 62.329 H/s alle 02:03 e 44.440 H/s alle 02:26. Arrivi e partenze dei miner aggiungono variazioni reali al rumore.

Per decidere usa medie: quella della sessione seine, il grafico 24 ore di My Stats e più rilevamenti della rete durante un giorno.

## Cosa significa l’hashrate per le ricompense?

Ogni hash ha la stessa probabilità: la quota dell’hashrate di rete è la quota attesa dei blocchi. Con 288 blocchi al giorno, chi ha lo 0,1% può aspettarsi nel tempo ricompense equivalenti a circa 0,29 blocchi al giorno. [Stimare le ricompense Blocknet](/blog/it/estimate-blocknet-mining-rewards/) converte in BNT usando i dati della [dashboard](/).

Su bntpool.com la quota arriva come parte di ogni blocco trovato, accreditata tramite le share accettate: i guadagni crescono a piccoli passi tutto il giorno. La [guida d’installazione seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) ti porta dal download alle prime share in pochi minuti.

<Note>
Gli hashrate descrivono l’attuale proof of work Argon2id. Blocknet prevede Faraday intorno al 16 febbraio 2027, secondo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), con un cambiamento previsto della proof of work. Tieni seine aggiornato: controlla nuove versioni all’avvio e ogni 12 ore.
</Note>

<CTA target="seine" title="Scopri il tuo hashrate" text="Scarica seine, lascialo ottimizzare per qualche minuto e segui l’hashrate nella sua dashboard e su My Stats." />
