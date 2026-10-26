---
title: 'Fortuna del pool di mining: sforzo e tempi dei blocchi'
description: 'Capisci la fortuna del pool, lo sforzo al 100% e i round lunghi. Leggi lo storico di bntpool.com e distingui la scoperta dei blocchi dai tuoi pagamenti.'
tags: ['Pool di mining', 'Fortuna del pool', 'Blocknet']
faq:
  - q: 'Uno sforzo del 100% significa che il pool sta per trovare un blocco?'
    a: 'No. Significa che il lavoro registrato tramite le share ha raggiunto la quantità attesa per un blocco alla difficoltà usata nel calcolo. È un confronto con una media, non una percentuale di completamento. Ogni nuovo hash deve comunque soddisfare il target della rete.'
  - q: 'Su bntpool.com è meglio una percentuale di fortuna più bassa?'
    a: 'Leggi prima l’etichetta e la definizione. La colonna Sforzo e la scheda Fortuna media di bntpool.com esprimono il lavoro rispetto a quello atteso: valori inferiori indicano meno lavoro registrato per blocco. Altri siti possono usare la convenzione inversa. Confronta metriche e dimensioni dei campioni equivalenti, non solo percentuali.'
  - q: 'Un round lungo aumenterà il mio prossimo pagamento?'
    a: 'Un round lungo non garantisce un pagamento personale maggiore. La scoperta del blocco determina quando diventa disponibile una ricompensa; la contabilità PPLNS stabilisce la tua quota usando il lavoro accettato idoneo. Seguono le regole di conferma e pagamento. Controlla i saldi in Le mie statistiche e la sequenza nella guida ai pagamenti.'
sourceHash: 'f7051fa67591'
translation: draft
---

La dashboard del pool mostra il lavoro di mining e i blocchi trovati con quel lavoro. Questa guida spiega come leggere percentuali di sforzo, tempi attesi tra i blocchi e storico dei round su bntpool.com. Potrai distinguere una media statistica da un conto alla rovescia e verificare la tua connessione senza attendere il prossimo blocco del pool.

## Che cos’è la fortuna di un pool di mining?

<Answer>
La fortuna del pool descrive come i blocchi effettivamente trovati si confrontano con la media attesa dal lavoro svolto. I blocchi arrivano casualmente: alcuni richiedono poco lavoro, altri di più. Su bntpool.com, lo sforzo del round confronta il lavoro delle share accettate con la difficoltà del blocco; uno sforzo minore indica che il round ha trovato il blocco con meno lavoro registrato.
</Answer>

Un pool combina i tentativi di molti miner, ma deve comunque trovare un hash che soddisfi il target della rete. Le share accettate misurano i contributi con un target più facile e aiutano a stimare quanto lavoro di hashing è avvenuto tra le scoperte. L’[introduzione ai pool](/blog/it/what-is-a-mining-pool/) spiega questa relazione; la fortuna descrive la variazione dei blocchi ottenuti.

## Come calcola lo sforzo il pool?

Per un round concluso, bntpool.com divide il lavoro delle share accettate per la difficoltà del blocco e moltiplica per 100. Il lavoro è ponderato per la difficoltà: non coincide semplicemente con il numero di share inviate. Una share con difficoltà assegnata maggiore rappresenta più lavoro di hashing atteso di una con difficoltà inferiore.

**Sforzo (%) = lavoro delle share accettate ÷ difficoltà del blocco × 100**

Supponiamo che un blocco abbia difficoltà 10.000 e che il pool registri 5.000 unità di lavoro accettato nel suo round. Lo sforzo è del 50%. Con 20.000 unità alla stessa difficoltà sarebbe del 200%. Sono esempi aritmetici, non misurazioni di uno specifico blocco in produzione.

| Sforzo | Come interpretarlo |
|---|---|
| 50% | Metà del lavoro atteso per quella difficoltà |
| 100% | La quantità di lavoro attesa per un blocco |
| 200% | Il doppio del lavoro atteso per quella difficoltà |

Lo sforzo del round attuale usa la difficoltà corrente, che può cambiare mentre la catena avanza. Le righe storiche usano la difficoltà del proprio blocco. Il valore confronta quindi il lavoro: non è una barra di avanzamento che deve fermarsi al 100%. È distinto anche dalla durata del round: un pool più veloce può svolgere lo stesso lavoro in meno tempo.

## Che cosa significa tempo atteso tra i blocchi?

È l’intervallo medio suggerito dalla quota stimata del pool nell’hashrate di rete. La dashboard di bntpool.com lo calcola moltiplicando 300 secondi per l’hashrate di rete e dividendo per quello del pool. Usa la stessa unità per entrambi. La [guida all’hashrate](/blog/it/what-is-hashrate/) spiega perché queste stime cambiano all’arrivo di share accettate e nuovi blocchi.

**Secondi attesi = 300 × H/s della rete ÷ H/s del pool**

Se la rete è stimata a 100.000 H/s e il pool a 10.000 H/s, il pool ha un decimo del totale stimato. L’intervallo atteso è di 3.000 secondi, cioè 50 minuti. Questa media può descrivere un insieme di round brevi e lunghi; non programma il prossimo blocco esattamente tra 50 minuti.

La dashboard confronta anche tempo trascorso e tempo atteso. Questa percentuale basata sul tempo può differire dallo sforzo basato sulle share perché usa dati diversi. Leggi l’etichetta prima di confrontare due numeri. Cambiamenti dell’hashrate del pool o della difficoltà di rete possono modificare la stima durante lo stesso round.

## Dopo il 100% di sforzo un blocco è dovuto?

<Answer>
Il pool non ha diritto a un blocco quando lo sforzo raggiunge il 100%. Gli hash precedenti senza successo non rendono il prossimo più probabile rispetto a un target di rete invariato. Con hashrate e difficoltà costanti, l’attesa media da adesso resta uguale, anche dopo un round lungo. Più lavoro crea più tentativi, non una scadenza.
</Answer>

Un semplice modello probabilistico aiuta a capirlo. Con tentativi indipendenti e un tasso costante di scoperta dei blocchi, l’attesa è approssimativamente esponenziale. La probabilità di non avere ancora un blocco dopo un intervallo atteso è circa il 36,8%; dopo due il 13,5%; dopo tre il 5,0%. Queste percentuali descrivono molti round ipotetici a condizioni fisse, non prevedono il round attuale del pool.

| Intervalli attesi trascorsi | Probabilità approssimativa di nessun blocco ancora |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Il calcolo è `exp(-t / T)`, dove `T` è l’intervallo medio e `t` il tempo trascorso. Il [riferimento NIST sulla distribuzione esponenziale](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) riporta questa funzione di sopravvivenza. Hashrate e difficoltà reali variano: considera la tabella una spiegazione della casualità. Anche una serie di blocchi rapidi lascia invariata la probabilità del prossimo hash se il target resta uguale.

## Come si legge lo storico della fortuna di bntpool.com?

Apri [Fortuna](/luck) per confrontare i round conclusi. Ogni riga mostra blocco, sforzo, durata del round, stato e ora della scoperta. Osserva più round insieme invece di sceglierne uno insolitamente breve o lungo. Il link del blocco apre il relativo record nella catena; la paginazione consente di esaminare uno storico più ampio.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Pagina reale dello storico della fortuna di bntpool.com con sforzo, durata dei round e stato dei blocchi" width="1200" height="833" loading="lazy" />

Questa schermata è stata acquisita il 26 ottobre 2026. Righe e stati sono un’istantanea storica: consulta la pagina live per i valori attuali. Lo stato in attesa o confermato descrive la conferma del blocco. Non cambia il significato della percentuale di sforzo registrata ed è separato da un pagamento al tuo indirizzo.

Nella dashboard, Fortuna media è la media aritmetica delle percentuali di sforzo registrate per i blocchi non orfani fino all’altezza attuale della catena. Nonostante il nome, segue la convenzione dello sforzo: un valore inferiore indica meno lavoro registrato per blocco. Le medie riassumono quel campione. Non promettono che i round futuri compensino uno specifico risultato precedente.

## Come influisce la fortuna del pool sulle ricompense?

La fortuna del pool cambia quando vengono trovati i blocchi. PPLNS decide come distribuire la ricompensa di un blocco trovato tra il lavoro accettato idoneo. Sono passaggi diversi. Il confine di un round non trasforma la finestra PPLNS in una regola di pagamento limitata a quel round; leggi [come funzionano i pagamenti PPLNS](/blog/it/how-pplns-payouts-work/) per finestra, conferme e soglia effettive.

Il saldo può quindi cambiare a scatti mentre il miner continua a inviare share accettate. Il [calcolatore delle ricompense](/blog/it/estimate-blocknet-mining-rewards/#mining-calculator) stima la produzione da hashrate e dati della rete. È utile per aspettative nel tempo; non prevede il blocco o l’istante in cui un pagamento arriverà nel wallet.

## Che cosa controllare durante un round lungo?

Controlla ciò che puoi osservare direttamente: miner in esecuzione, share ancora accettate e indirizzo corretto in [Le mie statistiche](/stats). Consulta [Stato](/status) per la salute pubblica del servizio. Se le tue share accettate si fermano, segui la [guida alla risoluzione dei problemi delle statistiche](/blog/it/track-your-miner-on-bntpool/#zero-hashrate); un round lungo e un rig disconnesso richiedono controlli diversi.

Quando la connessione funziona, leggi la fortuna su uno storico significativo mantenendo le stesse definizioni. Ogni numero della dashboard ha così un ruolo chiaro: l’hashrate stima lavoro al secondo, lo sforzo lo confronta con quello atteso per un blocco e i saldi mostrano la contabilità delle ricompense. Nulla richiede di indovinare quando arriverà il prossimo hash vincente.

<CTA target="pool" title="Inizia a minare su bntpool.com" text="Collega seine, verifica le share accettate e segui round e ricompense nella dashboard del pool." />
