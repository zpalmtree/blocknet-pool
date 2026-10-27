---
title: 'Si può minare Blocknet a tempo parziale?'
description: 'Mina Blocknet in sessioni brevi: segui le share nella finestra PPLNS di sei ore, torna con lo stesso indirizzo e controlla i saldi non pagati.'
tags: ['Guida al mining', 'PPLNS', 'Orari']
faq:
  - q: 'Devo minare sei ore prima che le share contino?'
    a: 'No. Le share idonee possono partecipare alla finestra di un blocco senza aspettare sei ore. La tua quota dipende dal lavoro ponderato per difficoltà rispetto al totale del pool nella finestra; per assegnare una ricompensa deve essere trovato un blocco.'
  - q: 'Il miner deve restare collegato per ricevere un pagamento esistente?'
    a: 'Una connessione attiva non è un requisito. I saldi confermati seguono le regole di pagamento del pool, compresa la soglia minima attuale. Scollegarsi non cancella il saldo né richiede automaticamente un pagamento immediato.'
  - q: 'Ricollegarsi azzera l’età delle share precedenti?'
    a: 'No. Le share conservano i timestamp. Il lavoro nuovo entra nelle finestre pertinenti dopo il ritorno; ricollegarsi non rinnova share vecchie e non aggiunge lavoro per le ore offline. Verifica nuove share accettate dopo il riavvio.'
sourceHash: '0a11c393062e'
translation: draft
---

Puoi usare un miner Blocknet in determinati orari e tornare in seguito. Distingui connessione attuale, share già inviate e ricompense già assegnate. Questa guida segue una breve sessione per aiutarti a fermare, riavviare e controllare l’account bntpool.com sapendo cosa aspettarti.

## Una sessione breve può ricevere ricompense?

<Answer>
Sì. Il lavoro idoneo di una sessione breve può partecipare ai blocchi le cui finestre PPLNS includono quelle share. Non devi prima completare sei ore. Le ricompense dipendono dai blocchi trovati e dalla tua quota ponderata. Fermandoti non aggiungi lavoro; le share invecchiano normalmente e i saldi seguono le regole di pagamento.
</Answer>

*Una linea temporale mostra sessione, pausa e ritorno, con segni distinti per idoneità delle share e saldo esistente.*

## Distingui i diversi tempi

bntpool pubblica attualmente una finestra PPLNS di sei ore, 60 blocchi prima dell’idoneità al pagamento e un minimo di 0,1 BNT sulla [pagina iniziale](/start). Sono lavoro considerato, maturazione del blocco e soglia del saldo. Le sei ore non sono un timer che promette un pagamento.

La [guida PPLNS](/blog/it/how-pplns-payouts-work/) spiega la formula. Le share idonee sono ponderate per difficoltà rispetto a tutto il lavoro idoneo nella finestra. I minuti online non fissano una percentuale del prossimo premio. Hardware, partecipazione al pool ed esiti delle share possono cambiare i pesi anche a parità di durata.

## Segui una share durante la pausa

Esempio in ora locale: mini dalle 18:00 alle 20:00 e una share idonea è registrata alle 19:00. Un blocco con finestra conclusa alle 22:00 guarda sei ore indietro, alle 16:00. La share rientra anche se sei offline. È un esempio di idoneità, non una previsione di trovare un blocco a quell’ora.

Una finestra che finisce alle 02:00 del giorno successivo parte dalle 20:00: la share delle 19:00 è fuori. Tornare alle 08:00 non modifica il timestamp. Il lavoro nuovo contribuisce alle finestre successive. La [guida alla fortuna](/blog/it/mining-pool-luck-explained/) spiega gli intervalli variabili: non pianificare presumendo che un blocco sia dovuto.

## Fermati conservando un riferimento

Prima della pausa annota indirizzo, nome del rig, ora approssimativa e ultimi messaggi di share accettate. Controlla l’account con la [guida al monitoraggio](/blog/it/track-your-miner-on-bntpool/). Avrai un riferimento per distinguere una macchina ferma da un indirizzo diverso o da una schermata non ancora aggiornata.

Esci normalmente da seine con `q` nel pannello o `Ctrl+C` nel terminale. Conserva comando e configurazione. La [guida all’uso del PC](https://seineminer.com/it/blog/mine-while-using-pc/) spiega come conciliare altre attività; fermarsi correttamente non richiede di cancellare impostazioni, cambiare indirizzo o chiudere l’account del pool.

## Cosa succede al saldo non pagato mentre sei offline?

Quando una share esce dalla finestra di un blocco successivo, non cancella una ricompensa già assegnata a uno precedente. Le stime seguono stato e maturazione del blocco; gli importi confermati non pagati restano nell’account fino all’elaborazione. La [guida alle conferme](/blog/it/block-confirmations-mining-rewards/) distingue credito provvisorio e saldo maturo.

Un saldo confermato sotto 0,1 BNT resta sotto la soglia automatica attuale finché nuovi crediti raggiungono il minimo. Ricollegarsi non lo aumenta né forza il pagamento. Un saldo idoneo non richiede neppure il miner online durante l’elaborazione. La [cronologia dei pagamenti](/payouts) distingue saldo aperto e transazione inviata.

## Torna allo stesso account e verifica lavoro nuovo

Riavvia il comando salvato e controlla pool, indirizzo e nome del rig prima di confrontare le statistiche. L’indirizzo identifica l’account seguito. Per una nuova destinazione intenzionale consulta la [guida al cambio d’indirizzo](/blog/it/change-mining-wallet-address/): il saldo vecchio non viene trasferito automaticamente nel nuovo account.

Cerca un job attuale e share accettate nuove, poi attendi l’aggiornamento della finestra di misurazione del pool. La media di una sessione appena iniziata può differire dalla stima mobile del pool. La [guida all’hashrate](/blog/it/what-is-hashrate/) spiega questi intervalli. Verifica lavoro reale e account corretto prima di dedurre ricompense mancanti.

## Pianifica secondo la tua routine

Scegli gli orari secondo uso del computer e budget elettrico. La [guida all’elettricità](https://seineminer.com/it/blog/mining-electricity-use/) calcola il consumo del programma reale, comprese eventuali fasce tariffarie. Una sessione più breve riduce il tempo di funzionamento, ma l’energia esatta dipende da hardware e carico misurati.

Registra inizi, arresti, lavoro accettato e saldi per più sessioni. Il [calcolatore di ricompense](/blog/it/estimate-blocknet-mining-rewards/) fornisce un’aspettativa sotto le sue ipotesi, non una promessa per una sera. Valuta un periodo rappresentativo e distingui connessione, idoneità delle share, maturazione e pagamenti inviati.

<CTA target="pool" title="Segui le sessioni su bntpool.com" text="Torna con lo stesso indirizzo, verifica share nuove e segui le normali fasi del pagamento." />
