---
title: 'Share accettate, rifiutate e obsolete nel mining'
description: 'Comprendi le share accettate, rifiutate e obsolete su bntpool.com, interpreta i messaggi e scopri come la difficoltà influisce sulle statistiche di mining.'
tags: ['Guida al mining', 'Share', 'Risoluzione problemi']
faq:
  - q: 'Una share accettata significa che ho trovato un blocco?'
    a: 'Significa che il pool ha accettato un risultato secondo le regole di elaborazione delle share. Un blocco deve raggiungere un obiettivo di rete distinto ed essere accettato dal daemon. Le ricompense seguono scoperta, validazione e pagamento: la conferma di una share non notifica un pagamento.'
  - q: 'Perché viene rifiutata una share obsoleta?'
    a: 'Stale job indica un’assegnazione non più valida, per esempio scaduta o legata a un template oltre il periodo di tolleranza. Controlla nuovi job e la ripresa delle share accettate. Messaggi ripetuti richiedono di verificare connessione e versione del miner.'
  - q: 'Una difficoltà maggiore riduce la ricompensa?'
    a: 'La difficoltà cambia il lavoro rappresentato da ogni share idonea e la frequenza attesa. Confronta il lavoro ponderato per difficoltà in periodi equivalenti, non il conteggio grezzo. La ricompensa dipende anche dalla finestra PPLNS, dal lavoro idoneo, dai blocchi trovati e dalle regole di validazione.'
sourceHash: 5be830208e6c
translation: draft
---

Usa il risultato delle share nel log per decidere se proseguire, verificare la connessione o indagare rifiuti ripetuti. Qui trovi le risposte di bntpool.com e la difficoltà dietro i contatori. Inizia dalla [guida di connessione](/blog/it/mine-blocknet-on-bntpool/) se necessario, o dalla [guida di monitoraggio](/blog/it/track-your-miner-on-bntpool/#zero-hashrate) se un indirizzo non mostra attività.

## Cos’è una share accettata?

<Answer>
Una share è un risultato inviato rispetto a un obiettivo assegnato dal pool. La risposta di accettazione conferma l’ingresso nel flusso di elaborazione e dimostra che il lavoro arriva. Scoperta dei blocchi, verifica delle share e idoneità ai pagamenti sono passaggi distinti: l’accettazione da sola non è un pagamento.
</Answer>

Tra le share il miner calcola molti hash. La maggior parte non raggiunge l’obiettivo e non viene inviata. Il contatore delle accettate registra gli esiti degli invii, non tutti gli hash di CPU o GPU. La [guida all’hashrate](/blog/it/what-is-hashrate/) distingue conteggio locale e stima del pool dal lavoro inviato.

## Che differenza c’è tra share e blocchi?

Il pool assegna job e difficoltà alla connessione. Raggiungere l’obiettivo share dimostra lavoro a quella difficoltà. Un candidato blocco deve anche raggiungere l’obiettivo di rete e superare i controlli del daemon. Trovare una share non implica l’aggiunta di un blocco alla catena.

bntpool.com registra share verificate e provvisorie durante la validazione. Anche un invio accettato può essere provvisorio; la ponderazione dei pagamenti segue regole di validazione e idoneità. Usa l’accettazione come segnale di connessione e [Le mie statistiche](/stats) per lavoro e ricompense registrati. La [guida PPLNS](/blog/it/how-pplns-payouts-work/) spiega la sequenza dopo un blocco.

## Cosa significano share rifiutate e obsolete?

Un invio rifiutato non ha superato il controllo applicabile. Il motivo è più utile del solo contatore. Stale job è una categoria di rifiuto: l’assegnazione non è più idonea. Confronta il messaggio esatto con eventi contemporanei, come riconnessione o nuovo job.

| Messaggio | Significato e controllo successivo |
|---|---|
| `stale job` | Assegnazione assente, scaduta, sostituita oltre la tolleranza o collegata a un vecchio template non disponibile. Controlla la ripresa di job nuovi e share accettate. |
| `duplicate share` | Stesso template e nonce già inviati. Cerca invii ripetuti: lo stesso lavoro non aggiunge un’altra contribuzione. |
| `low difficulty share` | L’hash non soddisfa l’obiettivo assegnato. Se si ripete, verifica versione del miner e gestione di job e difficoltà. |
| `nonce out of assigned range` | Il nonce è fuori dall’intervallo assegnato. Controlla versione del miner ed eventuali proxy o integrazioni personalizzate. |

Una share obsoleta riguarda il lavoro di un’assegnazione; un blocco orfano riguarda il posto del blocco nella catena. Distingui log delle share e [lista dei blocchi](/blocks). Il solo messaggio stale job non indica che il pool abbia trovato un blocco orfano.

## Perché cambia la difficoltà delle share?

<Answer>
La difficoltà variabile, o vardiff, adatta l’obiettivo della connessione all’attività osservata. Una difficoltà maggiore produce generalmente meno share con più lavoro ciascuna; una minore, più share con meno lavoro. Confronta lavoro idoneo ponderato nello stesso periodo anziché solo i conteggi.
</Answer>

Esempio aritmetico: dieci share idonee di difficoltà 100 totalizzano quanto una di difficoltà 1.000: entrambe sommano 1.000. Illustra la ponderazione, senza promettere ricompense o intervalli esatti. Gli arrivi variano e la validazione determina il lavoro conteggiabile.

Conta la difficoltà dell’assegnazione al momento dell’invio. Non confrontare un risultato vecchio solo con l’ultima difficoltà visualizzata. Il pool conserva le assegnazioni per i controlli. Un normale adeguamento non è un errore; imporre share più facili non crea lavoro computazionale aggiuntivo.

## Come leggere un tasso di rifiuto?

Confronta invii accettati e rifiutati nello stesso intervallo e ambito. Il miner può contare una sessione mentre il pool copre un altro periodo o più macchine. Riavviare cambia la finestra locale senza cancellare lo storico dell’indirizzo nel pool.

99 invii accettati e uno rifiutato danno l’1% di rifiuti per conteggio. Non equivale automaticamente alla percentuale di ricompensa persa: difficoltà e idoneità possono variare. Controlla motivi, tempi e prosecuzione del lavoro accettato prima di modificare impostazioni.

## Cosa fare se i rifiuti si ripetono?

Parti da una macchina interessata. Annota versione, messaggio esatto, inizio e arrivo di nuovi job. Controlla [Stato](/status) e poi eventuali interruzioni della connessione. Aggiorna dalle [release ufficiali di seine](https://github.com/zpalmtree/seine/releases) e confronta dopo una sola modifica alla volta.

Un proxy o un’integrazione personalizzata deve preservare job, intervallo nonce e difficoltà negli invii. Se il problema continua, fornisci un breve log e i dettagli della macchina al canale di supporto del progetto. Rimuovi segreti e dati privati: la frase di recupero non serve mai per diagnosticare share.

## Come confermare che il mining funziona di nuovo?

Cerca nuovi job seguiti da share accettate e apri [Le mie statistiche](/stats) con l’indirizzo di ricezione del miner. Verifica attività recente della macchina. Lascia accumulare lavoro alla stima dell’hashrate prima di confrontare medie. Segui poi ricompense stimate, saldo e pagamenti come fasi distinte nella guida PPLNS.

<CTA target="pool" title="Connettiti a bntpool.com" text="Segui la configurazione, conferma le share accettate e controlla l’attività tramite l’indirizzo di ricezione." />
