---
title: "Calcolatore di mining Blocknet"
description: "Stima i BNT al giorno e alla settimana con il tuo hashrate, i dati di rete in tempo reale e il calendario delle ricompense, con commissioni separate."
tags: [Ricompense del mining, Guida al mining, Hashrate, bntpool]
faq:
  - q: 'Come si calcolano le ricompense Blocknet?'
    a: 'Dividi il tuo hashrate per quello di rete, moltiplica per 288, i blocchi prodotti al giorno, e per la ricompensa attuale. Ottieni i BNT lordi attesi al giorno. Leggi l’hashrate di rete sulla dashboard bntpool.com e il tuo in seine o My Stats.'
  - q: 'Dove vedo l’hashrate della rete Blocknet?'
    a: 'La scheda Network Hashrate della dashboard lo mostra solitamente in KH/s: moltiplica per 1.000 per ottenere H/s. Lo stesso valore è disponibile in H/s nel campo network_hashrate su bntpool.com/api/stats. È misurato sugli ultimi dieci blocchi e si aggiorna a ogni blocco.'
  - q: 'La ricompensa Blocknet cambia nel tempo?'
    a: 'Sì. Rimane fissa in ciascun mese di 30 giorni e 8.640 blocchi e scende all’inizio del successivo, fino alla coda permanente di 2,0 BNT dal mese 48. Dividi l’altezza per 8.640, arrotonda per difetto e cerca il mese nel calendario delle ricompense.'
  - q: 'Perché le ricompense giornaliere della pool differiscono dalla stima?'
    a: 'La stima è una media. In un singolo giorno la pool può trovare più o meno blocchi del previsto e l’hashrate della rete cambia con l’arrivo di miner. Su una settimana o più di mining costante le oscillazioni si compensano. Puoi seguire le ricompense accreditate su My Stats.'
  - q: 'Minare in pool cambia i BNT che posso aspettarmi?'
    a: 'La quota attesa delle ricompense di rete resta uguale, perché dipende solo dalla tua quota di hashrate. La pool cambia il ritmo: anziché aspettare giorni per un blocco intero, ricevi una parte di ogni blocco trovato da bntpool.com, in importi piccoli e regolari.'
sourceHash: 0e1d600e0b00
translation: draft
---

<RewardsCalculator />

Questa guida mostra come stimare quanti BNT mina il tuo hardware al giorno: la formula, dove leggere i dati sulla [dashboard bntpool.com](/) e un esempio datato costruito con dati reali della pool e i migliori hashrate misurati da seine.

## Quanti BNT minerà il tuo hardware al giorno?

<Answer>
I BNT attesi al giorno sono il tuo hashrate diviso per quello della rete, per 288 blocchi al giorno, per la ricompensa attuale. Il tuo hashrate viene da seine o My Stats, quello di rete dalla dashboard e la ricompensa dall’altezza attuale e dal calendario Blocknet.
</Answer>

In formula:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Ogni parte ha una spiegazione semplice:

- **288 blocchi al giorno.** Blocknet punta a un blocco ogni cinque minuti: 86.400 secondi al giorno divisi per 300 secondi sono 288. La difficoltà si adatta a ogni blocco mantenendo il ritmo quando arrivano miner.
- **La tua quota di hashrate è la tua quota di blocchi.** La difficoltà è circa il numero di hash attesi per trovare un blocco. Ogni hash ha la stessa probabilità: una macchina che produce l’1% degli hash di rete può aspettarsi l’1% dei blocchi nel tempo.
- **La ricompensa dipende dall’altezza.** Ogni blocco nello stesso mese di 30 giorni e 8.640 blocchi paga uguale, quindi la ricompensa è una costante nota per ogni giorno di quel mese.

Il risultato è la ricompensa lorda attesa, mediata nel tempo.

## Dove trovi ogni dato?

### Il tuo hashrate

seine mostra l’hashrate sulla dashboard del terminale appena parte. Aspetta qualche minuto: ottimizza thread e kernel CPU sul lavoro reale, e il valore si stabilizza trovate le impostazioni migliori.

Quando mini nella pool, **Hashrate** su [My Stats](/stats) mostra la stima della pool, mediata sull’ultima ora. Per stimare usa il valore stabile di seine o il grafico a 24 ore della pool.

### Hashrate della rete

**Network Hashrate** mostra quello dell’intera rete in KH/s: 62,33 KH/s significa 62.330 H/s. Converti in H/s prima di dividere, altrimenti sbagli di un fattore mille.

La pool usa lavoro e timestamp degli ultimi dieci blocchi, quindi il valore cambia da blocco a blocco. [Cos’è l’hashrate?](/blog/it/what-is-hashrate/) spiega calcolo e oscillazioni. Per un dato più stabile controllalo più volte in un giorno e fai la media. Puoi leggerlo in H/s anche dall’API:

```sh
curl -s https://bntpool.com/api/stats
```

La sezione `chain` contiene `network_hashrate` in H/s e `current_job_height`, il blocco su cui lavora la pool.

### Ricompensa del blocco

**Current Block** mostra l’altezza su cui lavora la rete. Dividila per 8.640 e arrotonda per difetto per ottenere il mese, poi cercalo nel [calendario delle ricompense](/blog/it/blocknet-block-reward-schedule/). La [pagina blocchi](/blocks) permette un rapido riscontro: Reward mostra la ricompensa completa di ogni blocco trovato dalla pool.

## Un esempio del 25 ottobre 2026

<Note>
Questo esempio usa i dati di https://bntpool.com/api/stats alle 02:03 EDT del 25 ottobre 2026. Hashrate di rete e ricompensa cambiano: inserisci i valori odierni prima di basarti sul risultato.
</Note>

| Dato | Valore al momento del rilevamento |
|---|---|
| Blocco attuale | 69.020 (mese 7, perché 69.020 ÷ 8.640 = 7,99) |
| Ricompensa | 467,67 BNT |
| Hashrate di rete | 62.329 H/s (mostrato come 62,33 KH/s) |
| Hashrate bntpool.com | 5.240 H/s |

Prendiamo il server AMD EPYC Zen 5 dei benchmark seine, a 49,4 H/s:

1. Quota di rete: 49,4 ÷ 62.329 = 0,0793%.
2. Blocchi attesi al giorno: 0,000793 × 288 = 0,2283.
3. BNT attesi al giorno: 0,2283 × 467,67 = circa 106,8 BNT.

Il mese 8 inizia all’altezza 69.120, circa 100 blocchi dopo, e paga 439,46 BNT. Allo stesso hashrate di rete la stima diventa 0,2283 × 439,46 = circa 100,3 BNT al giorno. Ecco perché conviene controllare la ricompensa all’inizio di ogni mese.

Lo stesso calcolo, all’hashrate di rete rilevato, per il miglior risultato seine di ogni generazione:

| Hardware | Thread | Hashrate | Quota di rete | BNT/giorno a 467,67 | BNT/giorno a 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Server Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

Conta il valore della tua macchina. Lascia seine in funzione, prendi l’hashrate stabile e usalo al posto della tabella. La guida [hardware e hashrate Blocknet](https://seineminer.com/it/blog/blocknet-mining-hardware-hashrate/) spiega il ruolo di banda di memoria e RAM; i risultati completi sono nella [pagina prestazioni](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## Come cambia la stima minando su bntpool.com?

La quota attesa rimane uguale; cambia come arriva. bntpool.com paga con PPLNS: ogni ricompensa è divisa fra le share inviate nelle sei ore precedenti, in proporzione alla difficoltà. [Come funzionano i pagamenti PPLNS](/blog/it/how-pplns-payouts-work/) spiega la divisione.

Calcolando dal lato pool ottieni la stessa risposta. Nel rilevamento, 5.240 H/s erano l’8,4% della rete: 288 × 5.240 ÷ 62.329 = circa 24,2 blocchi al giorno, circa uno all’ora. La dashboard mostra **Expected Block Time** in Round Progress. Un server a 49,4 H/s è lo 0,94% della pool; lo 0,94% di 24,2 blocchi a 467,67 BNT è di nuovo circa 106,8 BNT al giorno. L’hashrate della pool si semplifica.

La differenza è il ritmo. In solo il server Zen 5 attenderebbe un blocco circa ogni 4,4 giorni; [pool o solo per Blocknet](/blog/it/pool-vs-solo-mining-blocknet/) approfondisce. Su bntpool.com riceve una parte dei circa 24 blocchi giornalieri della pool, facendo crescere il saldo a piccoli passi tutto il giorno. Commissione della pool e commissione di sviluppo di seine vengono sottratte dal lordo della formula.

Due dettagli influenzano il primo giorno:

- **La finestra si riempie in sei ore.** La quota del nuovo miner cresce mentre le share riempiono la finestra PPLNS, poi resta al livello pieno finché continua a minare.
- **Le ricompense maturano prima del pagamento.** Passano in Confirmed Rewards dopo 60 conferme, circa cinque ore, e la pool invia il saldo automaticamente da 0,1 BNT.

## Come confrontare la stima con i risultati reali?

La stima è una media e ogni giorno oscilla con la fortuna della pool. Su molti round l’effort medio si avvicina al 100%, quindi una settimana di mining costante è un buon confronto. Annota la somma di Confirmed Rewards e Paid Balance su [My Stats](/stats) a inizio e fine settimana. L’aumento è l’accredito dei blocchi maturati quella settimana: confrontalo con la stima degli stessi giorni, usando ricompensa e hashrate di rete di quei giorni.

Ricalcola quando cambia un dato:

- **A ogni nuovo mese**, quando scende la ricompensa: ogni 8.640 blocchi, circa 30 giorni.
- **Quando cambia l’hashrate di rete**, con l’arrivo di miner.
- **Quando cambia il tuo hashrate**, aggiungendo una macchina, riservando huge pages su Linux o aggiungendo una GPU NVIDIA, usata da seine insieme alla CPU.

<Note>
Gli hashrate descrivono l’attuale proof of work Argon2id. Blocknet prevede Faraday intorno al 16 febbraio 2027, secondo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), con un cambiamento previsto della proof of work. Tieni seine aggiornato: controlla nuove versioni all’avvio e ogni 12 ore.
</Note>

<CTA target="seine" title="Misura il tuo hashrate" text="Scarica seine, lascialo ottimizzare per qualche minuto e usa il suo hashrate nella formula con i dati odierni della dashboard." />
