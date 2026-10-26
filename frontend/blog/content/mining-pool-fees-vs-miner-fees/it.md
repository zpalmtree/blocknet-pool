---
title: 'Commissioni del pool e dello sviluppatore del miner'
description: 'Scopri dove si applicano le commissioni del pool e del miner Blocknet, come combinarle e come evitare di sottrarle due volte.'
tags: ['Guida al mining', 'Commissioni pool', 'Ricompense']
faq:
  - q: 'Sono la stessa commissione?'
    a: 'No. Quella del pool riguarda la distribuzione delle ricompense; quella dello sviluppatore appartiene al software. seine v0.2.36 dedica il 2% del tempo soggetto alla commissione allo sviluppo dopo il periodo iniziale. La commissione separata di bntpool era 0% il 26 ottobre 2026.'
  - q: 'Come si combinano?'
    a: 'Moltiplica la stima lorda per uno meno la frazione del miner, poi per uno meno quella del pool. Usa versione effettiva e impostazione corrente. Si stimano ricompense trattenute, non pagamenti garantiti o profitto monetario.'
  - q: 'Devo sottrarre la commissione dall''hashrate del pool?'
    a: 'Identifica prima il dato. Se stima già il lavoro accreditato al tuo indirizzo dopo il tempo dello sviluppatore, sottrarla ancora duplica l''effetto. Il calcolatore permette zero per tale correzione già inclusa. Non sottrarre nuovamente commissioni da saldi confermati o pagamenti.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Segui i due livelli di commissione in una stima Blocknet. La guida usa seine v0.2.36 e impostazioni bntpool verificate il 26 ottobre 2026 per compilare il [calcolatore](/blog/it/estimate-blocknet-mining-rewards/#mining-calculator) e leggere [My Stats](/stats).

## Che differenza c'è tra commissioni del pool e del miner?

<Answer>
La commissione del pool appartiene alla distribuzione delle ricompense; quella dello sviluppatore alla pianificazione del mining. Responsabili e fasi differiscono. Verifica entrambe, identifica se il dato è lordo o già corretto e applica ogni correzione pertinente una sola volta.
</Answer>

*Il lavoro passa attraverso una correzione del tempo del miner e una della ricompensa del pool prima dell'importo trattenuto stimato.*

## A cosa si applica la commissione del pool?

Il pool raccoglie lavoro ammissibile e distribuisce secondo le proprie regole. bntpool calcola la commissione sulla ricompensa registrata del blocco prima di assegnare il resto tramite PPLNS. La [guida PPLNS](/blog/it/how-pplns-payouts-work/) spiega partecipazione e accrediti.

Il 26 ottobre 2026 `https://bntpool.com/api/info` pubblica `pool_fee_pct` pari a 0,0, cioè 0%. Controlla il valore corrente su [Get Started](/start) o nell'API quando aggiorni la stima. Zero descrive questa impostazione del pool; la politica del software resta separata.

## Come funziona la commissione di seine?

v0.2.36 documenta il 2% su ogni pool e in solo. È tempo di mining per l'indirizzo dello sviluppatore, non un prelievo successivo dal portafoglio. I primi 10 minuti sono esenti; il tracker conta il tempo seguente. La [guida di configurazione](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) descrive la politica.

La pianificazione avviene a turni: una finestra breve non deve coincidere esattamente col 2%. È l'obiettivo a lungo termine dopo il periodo iniziale, non una promessa per minuto o blocco. Annota la versione: altri programmi o guide storiche possono differire. La commissione pool non sostituisce questa informazione.

## Come combinare le percentuali?

Usa le frazioni trattenute. Con m per il miner e p per il pool, espressi come frazioni, resta `(1 - m) * (1 - p)` del lordo. Lo usa lo [stimatore](/blog/it/estimate-blocknet-mining-rewards/). Sommare percentuali non è la regola generale: la seconda agisce sul resto.

Esempio puramente aritmetico: 100 BNT lordi ipotetici, miner 2% e pool verificato 0% danno 100 × 0,98 × 1,00 = 98 BNT. È un esempio a regime, non una misura hardware, una promessa o una simulazione breve del periodo iniziale.

## Scegli il dato di hashrate corretto

Un dato locale o benchmark descrive lavoro della macchina. Prima della correzione, verifica se include tutto il tempo o solo lavoro per il tuo indirizzo. Controlla origine e intervallo. La [guida hashrate](/blog/it/what-is-hashrate/) distingue osservazioni locali, pool e rete.

L'hashrate del pool per il tuo indirizzo può già riflettere tempo accreditato altrove. In tal caso inserisci zero come correzione aggiuntiva del miner nel calcolatore. Cambia il calcolo, non la politica effettiva. La commissione pool resta pertinente alle ricompense lorde ammissibili: controlla ogni dato.

## Perché non sottrarre di nuovo dal saldo?

Il saldo confermato è un risultato contabile, non una stima lorda intatta. Il pool ha già assegnato gli accrediti. Sottrarre nuovamente da saldo o pagamento modifica il dato senza un nuovo evento. Riconcilia usando l'importo visualizzato e identifica la fase.

Ricompense in attesa, saldo confermato e pagamenti differiscono anche nei tempi. Maturità e soglia spiegano le transizioni, senza nuove percentuali. Consulta la [guida alle conferme](/blog/it/block-confirmations-mining-rewards/) e la [cronologia](/payouts). L'eventuale commissione della transazione del portafoglio è un campo separato.

## Conserva le fonti della stima

Salva data, versione, commissioni, origine dell'hashrate e intervallo. Il calcolatore combina dati live pool/rete con i tuoi dati; aggiorna la rete quando torni al risultato. Accrediti reali dipendono anche da share ammissibili, blocchi e PPLNS: formula e registrazione successiva rispondono a domande diverse.

Usa [My Stats](/stats) e la [guida al monitoraggio](/blog/it/track-your-miner-on-bntpool/). Confronta stesso indirizzo, periodo e fase prima di cambiare commissioni. Il calcolatore stima BNT; elettricità, hardware e prezzo monetario sono esclusi. Non serve cambiare indirizzo del portafoglio.

<CTA target="pool" title="Segui le ricompense su bntpool.com" text="Usa il calcolatore per le stime e My Stats per lavoro, saldi e pagamenti registrati." />
