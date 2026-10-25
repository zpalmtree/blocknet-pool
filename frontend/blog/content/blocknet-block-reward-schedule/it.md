---
title: Calendario delle ricompense dei blocchi Blocknet
description: Le ricompense Blocknet mese per mese, da 723,25 BNT al lancio alla coda permanente di 2,0 BNT al mese 48, con 288 blocchi al giorno e 98,6 milioni prima della coda.
tags: [Blocknet, Ricompensa del blocco, Emissione, Mining]
faq:
  - q: 'Qual è la ricompensa attuale di un blocco Blocknet?'
    a: 'Dipende dal mese raggiunto dalla catena, perché scende ogni 8.640 blocchi. Il 25 ottobre 2026 la catena era nel mese 7, con 467,67 BNT per blocco; il mese 8 inizia all’altezza 69.120 con 439,46 BNT. Leggi l’altezza attuale sulla dashboard di bntpool.com e cerca la riga nella tabella.'
  - q: 'Quanti blocchi Blocknet vengono minati al giorno?'
    a: 'Blocknet punta a un blocco ogni cinque minuti: 288 al giorno e 8.640 per mese di 30 giorni. La difficoltà si adatta a ogni blocco usando una media ponderata degli ultimi 60 tempi di blocco, mantenendo la media vicina a cinque minuti quando i miner entrano o escono dalla rete.'
  - q: 'Qual è l’offerta totale di Blocknet?'
    a: 'Prima della coda, che inizia al mese 48, vengono minati circa 98,56 milioni di BNT. Il codice e il sito ufficiale arrotondano a circa 100 milioni. Poi ogni blocco paga 2,0 BNT per sempre: 576 BNT al giorno, circa 210.240 BNT all’anno, quindi l’offerta continua a crescere lentamente.'
  - q: 'Quando inizia l’emissione di coda di Blocknet?'
    a: 'Inizia al mese 48, all’altezza 414.720: 48 mesi di 30 giorni, cioè 1.440 giorni di blocchi da cinque minuti dopo il lancio, circa quattro anni. La data esatta dipende dai tempi di blocco reali. Da quell’altezza ogni blocco paga 2,0 BNT.'
  - q: 'Quando si possono spendere i BNT minati?'
    a: 'La ricompensa diventa spendibile dopo 60 blocchi, circa cinque ore con i blocchi da cinque minuti di Blocknet. Si chiama maturità coinbase. Anche bntpool.com attende 60 blocchi prima di pagare una ricompensa, distribuendo quindi solo ricompense maturate.'
sourceHash: abea18c2c21b
translation: draft
---

Questa pagina elenca la ricompensa Blocknet per ogni mese del calendario di emissione, spiega come viene calcolata e cosa significa per te che mini.

## Qual è la ricompensa di un blocco Blocknet?

<Answer>
La ricompensa è partita da 723,25 BNT per blocco e scende una volta per mese di 30 giorni e 8.640 blocchi, seguendo una curva di decadimento regolare. Rimane uguale per tutti i blocchi dello stesso mese. Dal mese 48, all’altezza 414.720, ogni blocco paga una coda permanente di 2,0 BNT.
</Answer>

Blocknet punta a un blocco ogni cinque minuti (300 secondi): 288 al giorno e 8.640 per mese di 30 giorni. La difficoltà si adatta dopo ogni blocco sulla base di una media ponderata degli ultimi 60 tempi di blocco, mantenendo il ritmo vicino a cinque minuti mentre cambia l’hashrate della rete.

La ricompensa dipende solo dal mese in cui cade l’altezza. Il mese è l’altezza divisa per 8.640, arrotondata per difetto: le altezze da 0 a 8.639 sono il mese 0, da 8.640 a 17.279 il mese 1, e così via.

## Come si calcola la ricompensa?

Il daemon calcola la ricompensa mensile con una formula:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

Il primo termine è la parte sopra la coda di 2,0 BNT. Si riduce di un fattore e^-0,75 ogni anno: dopo dodici mesi è circa il 47% del valore iniziale. Ripartita sui mesi, questa parte vale circa il 93,9% di quella del mese precedente, quindi la ricompensa scende a piccoli passi regolari.

Il risultato viene arrotondato per difetto a unità atomiche intere (1 BNT equivale a 100.000.000 di unità atomiche). La tabella mostra ogni ricompensa arrotondata a due decimali.

## Tabella delle ricompense Blocknet per mese

| Mese | Primo blocco | Ricompensa per blocco (BNT) | Totale minato a fine mese (milioni di BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| 48 e oltre | 414.720 | 2,00 | +0,017 al mese |

Il blocco genesi all’altezza 0 non ha ricompensa, quindi il mese 0 contiene 8.639 blocchi pagati. Ogni BNT proviene da queste ricompense, senza premine, come spiega il [fair launch di Blocknet](/blog/it/blocknet-fair-launch/). Il 25 ottobre 2026 la pool lavorava al blocco 69.016, verso la fine del mese 7. La [dashboard bntpool.com](/) mostra l’altezza attuale per trovare la riga corrispondente e la [pagina dei blocchi](/blocks) elenca quelli trovati dalla pool.

## Cosa succede al mese 48?

<Answer>
Il mese 48 inizia all’altezza 414.720. Da lì ogni blocco Blocknet paga 2,0 BNT fissi per sempre: è l’emissione di coda. Mantiene una ricompensa su ogni blocco finché la catena funziona, così i miner vengono sempre pagati per proteggere la rete.
</Answer>

La formula di decadimento si applica ai mesi da 0 a 47, terminando con 40,22 BNT per blocco nel mese 47. Dal mese 48 il daemon usa direttamente il valore di coda: la ricompensa diventa 2,0 BNT e rimane tale. Con 288 blocchi al giorno, la coda aggiunge 576 BNT al giorno, circa 210.240 BNT all’anno.

Il mese 48 inizia dopo 1.440 giorni di blocchi da cinque minuti dal lancio, circa quattro anni. La data dipende dai tempi reali, quindi l’altezza del blocco è il riferimento affidabile.

## Quanti BNT vengono minati prima della coda?

Sommando tutti i blocchi dall’altezza 1 alla 414.719 si ottengono 98.561.905,08 BNT, circa 98,6 milioni. Il codice e il sito ufficiale descrivono circa 100 milioni di monete in quattro anni più la coda; la somma esatta è poco sotto quella cifra tonda.

La maggior parte viene minata all’inizio. Raggruppando per anno del calendario:

| Mesi | BNT minati | Quota del totale prima della coda |
|---|---|---|
| 0-11 (anno 1) | 54,48 milioni | 55,3% |
| 12-23 (anno 2) | 25,84 milioni | 26,2% |
| 24-35 (anno 3) | 12,32 milioni | 12,5% |
| 36-47 (anno 4) | 5,93 milioni | 6,0% |

## Quando puoi spendere i BNT minati?

La ricompensa si può spendere dopo che altri 60 blocchi sono stati aggiunti sopra, circa cinque ore dopo. È la maturità coinbase. bntpool.com attende gli stessi 60 blocchi prima di pagare e la [pagina dei pagamenti](/payouts) elenca gli invii.

## Cosa significa il calendario per i miner?

Le ricompense sono maggiori nei primi mesi. Un blocco al mese 12 paga 342,69 BNT, circa il 47% dei 723,25 BNT del mese 0, e uno al mese 24 paga 162,93 BNT. Ogni mese paga meno per blocco del precedente, fino alla coda.

La tua quota segue la tua parte dell’hashrate di rete. In solo, i blocchi attesi al giorno sono 288 × (il tuo hashrate ÷ hashrate di rete), ciascuno con la ricompensa del suo mese. In una pool lo stesso calcolo si applica all’hashrate della pool, e la ricompensa viene divisa tra chi ha contribuito lavoro: guadagni da ogni blocco trovato invece di aspettare di trovarne uno intero. La [dashboard bntpool.com](/) mostra gli hashrate di rete e pool e la [pagina statistiche](/stats) quello del tuo miner dopo l’avvio.

Per iniziare ora:

1. Scarica seine da [seineminer.com](https://seineminer.com/it/#download). Funziona su Windows, macOS e Linux e si ottimizza per il tuo hardware.
2. Eseguilo e incolla il tuo indirizzo Blocknet o `@name`.
3. Accetta la pool predefinita, `stratum+tcp://bntpool.com:3333`.

La [guida al mining su bntpool.com](/blog/it/mine-blocknet-on-bntpool/) copre i passaggi lato pool, [Get Started](/start) elenca le impostazioni e la [guida al mining di seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) spiega la configurazione passo passo. Per confrontare i processori, leggi [hardware e hashrate per Blocknet](https://seineminer.com/it/blog/blocknet-mining-hardware-hashrate/).

<Note>
I dati provengono dal codice delle ricompense del daemon Blocknet, verificato il 25 ottobre 2026. Blocknet prevede l’aggiornamento Faraday intorno al 16 febbraio 2027, secondo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). Tieni seine aggiornato: controlla nuove versioni all’avvio e ogni 12 ore.
</Note>

<CTA target="seine" title="Mina Blocknet con seine" text="seine è gratuito per Windows, macOS e Linux e mina su bntpool.com per impostazione predefinita." />
