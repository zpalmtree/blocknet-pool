---
title: 'Difficoltà di mining: rete e share'
description: 'Scopri come la difficoltà determina il target, come Blocknet la regola e perché difficoltà di rete e delle share influenzano statistiche diverse del mining.'
tags: ['Basi del mining', 'Difficoltà', 'Blocknet']
faq:
  - q: 'La difficoltà maggiore riduce l''hashrate?'
    a: 'Cambia il target da soddisfare, non direttamente i calcoli al secondo dell''hardware. A hashrate costante, una difficoltà di rete superiore allunga l''intervallo medio previsto tra blocchi. Verifica le prestazioni del miner separatamente dalla frequenza di blocchi e share.'
  - q: 'Una share più difficile paga di più?'
    a: 'Rappresenta più lavoro atteso, ma a hashrate costante viene inviata meno spesso. Confronta il lavoro ponderato per difficoltà nello stesso periodo. Su bntpool.com contano anche i blocchi accettati e il calcolo PPLNS, non il semplice numero di share.'
  - q: 'Quanto spesso Blocknet regola la difficoltà?'
    a: 'Dopo ogni blocco calcola quella del successivo dalla storia recente. La regola attuale usa una media mobile ponderata linearmente di 60 intervalli e punta a 300 secondi per blocco. Gli intervalli recenti pesano di più; ogni blocco può arrivare prima o dopo.'
sourceHash: '5c203e8c7523'
translation: draft
---

La difficoltà collega il lavoro dell'hardware ai blocchi accettati dalla rete. Questa guida spiega target, regolazione di Blocknet e difficoltà separata delle share. Interpreta il valore prima di confrontare hashrate, tempo atteso o [calcolatore delle ricompense](/blog/it/estimate-blocknet-mining-rewards/#mining-calculator).

## Cos'è la difficoltà di mining?

<Answer>
Descrive il lavoro di hashing atteso per trovare un risultato che soddisfi un target. Aumentandola, i risultati validi diventano meno frequenti a hashrate costante. In Blocknet la difficoltà di rete equivale circa agli hash attesi per blocco; quella delle share riguarda un target di invio distinto.
</Answer>

Ogni hash produce un numero da confrontare con una soglia. Un target minore accetta meno risultati possibili e ha difficoltà maggiore. Blocknet divide il massimo valore a 256 bit per la difficoltà per ottenere il target a 256 bit. Raddoppiarla raddoppia circa il lavoro atteso senza modificare l'operazione di hashing.

## Come Blocknet regola la difficoltà di rete?

La calcola dopo ogni blocco. La regola mainnet attuale usa una media mobile ponderata linearmente, LWMA, su 60 intervalli recenti, dando più peso ai nuovi. Il target è 300 secondi per blocco. Combinando difficoltà passate e tempi ponderati, arrivi persistentemente rapidi tendono ad aumentarla, quelli lenti a ridurla.

Il codice limita gli intervalli impiegati e impone una difficoltà minima. Per 60 intervalli servono 61 timestamp: non è una regolazione ogni 60 blocchi. Le regole avvicinano il ritmo al target mentre varia la potenza. Cinque minuti sono una media; un singolo blocco può arrivare molto prima o dopo.

## Difficoltà di rete e delle share

Il pool assegna un target per osservare il lavoro tra le scoperte di blocchi. In genere è più facile di quello della rete. Entrambi descrivono lavoro atteso, ma rispondono a domande diverse e dipendono da parti diverse del sistema:

| Difficoltà | Cosa controlla |
|---|---|
| Rete | Soglia di lavoro del blocco candidato, derivata dalle regole di consenso. |
| Share | Soglia di invio assegnata dal pool a una connessione o attività di mining. |

bntpool.com usa difficoltà variabile, vardiff, per regolare gli invii. A parità di hashrate, difficoltà maggiore significa generalmente meno share, ciascuna con più lavoro atteso. Non cambia la difficoltà di rete. Un risultato che soddisfa anche il target di rete può diventare un blocco candidato, ancora da validare dal daemon. La [guida alle share](/blog/it/accepted-rejected-stale-shares/) spiega accettazioni e rifiuti.

## Come cambia il tempo atteso per blocco?

Per Blocknet dividi difficoltà di rete per hashrate che svolge il lavoro, in H/s, per stimare l'intervallo medio in secondi. Confronta valori dello stesso periodo. L'esempio mantiene entrambe le variabili costanti per mostrare l'aritmetica: non sono misure attuali né previsioni per un miner specifico.

**Secondi attesi per blocco ≈ difficoltà di rete ÷ hashrate in H/s**

| Difficoltà di rete | Hashrate | Intervallo atteso |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Con 30.000 e 100 H/s la media è 300 secondi. Raddoppiando a 60.000 con lo stesso hashrate diventa 600 secondi. Se anche l'hashrate raddoppia a 200 H/s, torna a 300 secondi. Sono medie su tentativi ripetuti. La [guida alla fortuna del pool](/blog/it/mining-pool-luck-explained/) spiega le deviazioni delle singole ronde.

## Perché cambiano le ricompense a hashrate costante?

Una difficoltà di rete maggiore riduce i blocchi attesi per unità di tempo finché resta tale. Se cresce l'hashrate totale e la difficoltà segue, diminuisce la tua quota di lavoro. Un altro fattore è la ricompensa per blocco: il [calendario](/blog/it/blocknet-block-reward-schedule/) la cambia indipendentemente. La difficoltà non determina i BNT della ricompensa programmata.

Usa dati recenti nel calcolatore per esplorare i BNT attesi. Distingui hashrate del miner e del pool per gestire correttamente la commissione, usando quella della tua versione. Le stime sono medie sotto certe ipotesi. Il pagamento dipende anche da blocchi trovati, conferme e [PPLNS](/blog/it/how-pplns-payouts-work/); la sola difficoltà non dice quando arriverà.

## Quale difficoltà devi controllare?

Parti dalla domanda. Per i blocchi usa difficoltà di rete e hashrate del miner o pool considerato. Per la frequenza delle share usa la difficoltà dei relativi lavori. Per saldo e pagamenti consulta [My Stats](/stats) e lo [storico](/payouts). Conserva timestamp, ambito e unità insieme ai valori confrontati.

Il [pannello del pool](/) e lo [storico blocchi](/blocks) danno il contesto attuale. L'hashrate di rete è stimato da lavoro recente e tempo trascorso, quindi può differire dalla prossima difficoltà divisa per 300 secondi. La [guida all'hashrate](/blog/it/what-is-hashrate/) chiarisce la distinzione. Difficoltà, hashrate e fortuna descrivono aspetti diversi: separali nella diagnosi.

<CTA target="pool" title="Collega il miner a bntpool.com" text="Segui la pagina di configurazione e poi le share e l&#x27;attività in My Stats." />
