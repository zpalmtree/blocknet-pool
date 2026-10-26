---
title: Segui il tuo miner su bntpool.com
description: Cerca indirizzo o @name su bntpool.com per hashrate, share, saldo e pagamenti. Segui blocchi, fortuna e disponibilità o usa l’API pubblica nei tuoi script.
tags: [bntpool, Statistiche di mining, API della pool, Blocknet]
faq:
  - q: 'Perché l’hashrate su bntpool.com differisce da seine?'
    a: 'seine conta ogni hash calcolato; la pool stima l’hashrate dalla difficoltà delle share accettate nell’ultima ora. Le share arrivano in momenti casuali, quindi la stima oscilla di più e si stabilizza nel tempo. Confronta il grafico a 24 ore di My Stats con la media di seine.'
  - q: 'Posso cercare le statistiche con un @name?'
    a: 'Sì. Scrivi lo shortname Blocknet ID, come @name o $name, nella ricerca My Stats. La pagina lo risolve tramite blocknet.id, mostra le statistiche e inserisce nella casella l’indirizzo completo. Usa quell’indirizzo per chiamare l’API da uno script.'
  - q: 'L’API bntpool.com richiede una chiave?'
    a: 'No. Statistiche, blocchi, pagamenti, fortuna, stato ed endpoint dei miner sono JSON pubblico via HTTPS, leggibile con curl o una dashboard domestica senza registrazione. Gli importi sono in unità atomiche: dividi per 100.000.000 per ottenere BNT e interroga ogni 30-60 secondi.'
  - q: 'Perché My Stats mostra zero Estimated Rewards?'
    a: 'Le stime appaiono quando la pool trova un blocco con le tue share nella finestra di pagamento. Per un nuovo miner possono servire alcuni round. Share accettate e hashrate compaiono prima, poi le ricompense stimate, che maturano in Confirmed Rewards e vengono pagate automaticamente.'
sourceHash: f7e0a568c6cf
translation: draft
---

Quando seine mina su bntpool.com, la pool mostra tutto ciò che vede: hashrate, ogni share accettata, guadagni e pagamenti inviati al tuo indirizzo. Questa guida presenta My Stats, le pagine pubbliche dell’intera pool e l’API per script e dashboard domestiche.

Se non hai iniziato, la [guida d’installazione di seine](https://seineminer.com/it/blog/how-to-mine-blocknet/) ti porta alle prime share in pochi minuti.

## Come controllare le statistiche del miner?

<Answer>
Apri My Stats su bntpool.com/stats, incolla indirizzo Blocknet o @name e premi Lookup. Trovi hashrate, share accettate e rifiutate, saldi stimati, confermati e pagati, grafico dell’hashrate, una riga per rig e pagamenti recenti. La pagina si aggiorna da sola finché la scheda è aperta.
</Answer>

Vai su [My Stats](/stats) e inserisci lo stesso indirizzo o [shortname Blocknet ID](https://seineminer.com/it/blog/blocknet-id-pay-by-name/) dato a seine. `@name` o `$name` viene risolto tramite blocknet.id e la casella mostra poi l’indirizzo completo. La pagina ricorda l’ultima ricerca nel browser, così la prossima volta trovi già i tuoi numeri.

## Cosa mostra My Stats?

Schede e tabelle sono raggruppate per rispondere a una domanda sul miner.

### Saldo

- **Estimated Rewards:** la tua quota dei blocchi ancora in maturazione.
- **Confirmed Rewards:** saldo maturato in attesa del pagamento.
- **Paid Balance:** tutto ciò che è stato inviato al tuo indirizzo.

Quando hai ricompense in corso, **Estimated From Recent Blocks** elenca ogni blocco con altezza, credito stimato, conferme restanti e momento della scoperta. Ogni altezza collega il blocco sul [block explorer](https://explorer.blocknetcrypto.com), per verificarlo personalmente. [Come funzionano i pagamenti PPLNS](/blog/it/how-pplns-payouts-work/) spiega divisione e tempi.

### Mining e share

Le schede **Mining** mostrano **Hashrate**, quanti **Blocks Found** hanno risolto i tuoi rig e la data iniziale (**Mining Since**). Sotto trovi **Shares Accepted**, **Shares Rejected**, **Reject Rate** e **Avg Difficulty**. Un rig sano invia share accettate regolarmente e ha un tasso di rifiuto vicino a zero.

**Hashrate History** traccia 1h, 24h, 7d o 30d. Gli intervalli lunghi sono il modo migliore per verificare che un rig abbia lavorato tutta la notte.

### Una riga per rig

Assegna a ogni macchina un nome distinto: la pool mostra separatamente hashrate, share accettate e rifiutate e ora dell’ultima share. Al primo avvio seine sceglie `seine-` più quattro cifre. Puoi impostarlo da riga di comando:

```sh
./seine --pool-worker rig-01
```

Un rig con una vecchia versione riceve **update available**, collegato alle [release seine](https://github.com/zpalmtree/seine/releases), per individuare facilmente una macchina dimenticata negli aggiornamenti.

### Pagamenti e share recenti

**Recent Payouts** elenca ogni pagamento con importo, link alla transazione e stato in coda, non confermato o confermato. **Recent Shares** mostra le ultime 20 share con job, rig, difficoltà e stato: il modo più rapido per verificare che una macchina appena avviata raggiunga la pool.

## Perché l’hashrate della pool differisce da seine?

seine conta tutti gli hash. La pool vede solo le share inviate e stima l’hashrate dalla loro difficoltà totale accettata nell’ultima ora. Ogni share è casuale, e bntpool.com imposta la difficoltà per connessione mirando a circa 10 share ogni 5 minuti: una finestra breve è quindi rumorosa.

Due abitudini aiutano il confronto:

- Confronta le medie: grafico 24h di My Stats e media seine nello stesso periodo.
- Dai tempo a un nuovo rig. All’inizio la media copre solo i minuti trascorsi, quindi si basa su poche share. Dopo un’ora si applica la finestra completa.

Recent Shares conferma subito l’arrivo del lavoro, prima che l’hashrate si stabilizzi.

<span id="zero-hashrate"></span>

## Perché seine sta minando ma My Stats mostra un hashrate pari a zero?

Controlla prima l’indirizzo di pagamento, la connessione al pool e le share accettate. L’hashrate locale indica che la macchina calcola hash; la stima del pool richiede share accettate per l’indirizzo cercato.

1. **Verifica l’indirizzo.** Cerca lo stesso indirizzo di ricezione o `@name` registrato usato da seine. Se hai cambiato l’indirizzo associato a un nome breve, riavvia seine per risolverlo nuovamente, poi controlla l’indirizzo risultante in My Stats.
2. **Verifica il pool.** Controlla che seine usi `stratum+tcp://bntpool.com:3333`. Una macchina che mina altrove o in modalità solo non aggiunge share qui. La [guida alla configurazione del pool](/blog/it/mine-blocknet-on-bntpool/) spiega la connessione.
3. **Controlla Recent Shares.** Attendi share accettate e l’aggiornamento della pagina. Le share arrivano a intervalli variabili; un hashrate locale attivo non garantisce una stima immediata del pool. Se arrivano share accettate, confronta le medie nel tempo.
4. **Leggi il messaggio di connessione.** Usa la tabella finché il miner non invia share accettate. Consulta [Status](/status) per eventuali incidenti in corso.

| Messaggio in seine | Cosa controllare |
|---|---|
| `failed to connect to pool at` | Verifica nome host e porta, la connessione e che sia consentito il traffico TCP in uscita verso la porta 3333. |
| `login rejected:` | Leggi il motivo che segue; verifica l’indirizzo di ricezione o il nome breve e usa la versione attuale di seine. |
| `stale job` | La share si riferisce a un vecchio incarico. Controlla l’arrivo di nuovi incarichi e share accettate; se continua, verifica connessione e versione del miner. |
| `submit interrupted during reconnect` | Seine si sta riconnettendo. Attendi l’accettazione del login e nuove share accettate dopo il ripristino della connessione. |

Uno zero in **Estimated Rewards** è una questione diversa: le share possono essere accettate prima che un blocco del pool accrediti ricompense. La [guida PPLNS](/blog/it/how-pplns-payouts-work/) spiega la sequenza.

## Quali pagine pubbliche mostrano l’intera pool?

Non richiedono una ricerca e mostrano apertamente l’attività. La [dashboard](/) raccoglie miner collegati, hashrate di pool e rete, blocchi trovati e fortuna media.

### Blocchi

[Blocks](/blocks) elenca ogni blocco trovato dal più recente: altezza collegata all’explorer, ricompensa, effort, durata, stato e momento della scoperta. Filtra per Confirmed, Pending o Orphaned per vedere la situazione.

### Pagamenti

[Payouts](/payouts) elenca ogni lotto con importo totale, numero di miner pagati, link alle transazioni e stato. Appare non confermato appena trasmesso e diventa confermato poco dopo la prima conferma in un blocco: puoi seguire il pagamento dalla pool e dall’explorer.

### Fortuna

[Luck](/luck) mostra l’effort di ogni round: lavoro delle share accettate diviso per difficoltà del blocco. Il 100% è il lavoro esattamente atteso, meno significa una scoperta anticipata. I singoli round oscillano molto, quindi osserva **Average Luck** sulla dashboard. **Pool Luck History** vi collega tramite **View All**.

### Stato

[Status](/status) è la pagina di salute della pool. Mostra se pool, HTTP pubblico, API, Stratum, database e daemon sono online, quanto è recente il template, l’altezza, la disponibilità negli ultimi 10 minuti, 6 ore, 24 ore e 7 giorni e la cronologia degli incidenti. La disponibilità è misurata due volte: da un monitor sul server e da una sonda esterna su Cloudflare.

## Come usare l’API bntpool.com?

<Answer>
L’API è JSON pubblico via HTTPS e non richiede chiavi. Chiama https://bntpool.com/api/miner/ seguito dall’indirizzo per hashrate, rig, share e pagamenti; aggiungi /balance per i saldi e usa /api/stats per pool e rete. Gli importi sono unità atomiche: dividi per 100.000.000 per ottenere BNT.
</Answer>

Tutte le pagine leggono questi endpoint: puoi recuperare personalmente ogni dato visibile.

| Endpoint | Risultato |
|---|---|
| `/api/stats` | Miner collegati, hashrate pool, blocchi trovati, hashrate rete, altezza attuale |
| `/api/stats/history?range=24h` | Hashrate pool nel tempo (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Round attuale, effort medio, round recenti |
| `/api/info` | Porta Stratum, schema e finestra di pagamento, minimo |
| `/api/blocks?status=confirmed` | Blocchi trovati filtrabili per stato |
| `/api/payouts/recent` | Lotti di pagamenti con hash delle transazioni |
| `/api/luck` | Effort e durata di ogni round |
| `/api/status` | Salute dei servizi, finestre di disponibilità, incidenti |
| `/api/miner/{address}` | Il tuo hashrate, totali, rig, ultime share, pagamenti |
| `/api/miner/{address}/balance` | Saldi stimato, confermato e pagato |
| `/api/miner/{address}/hashrate?range=24h` | La tua cronologia di hashrate |

Gli endpoint elenco accettano `limit` (25 predefinito, fino a 200) e `offset`, e restituiscono `items` e `total`. Quelli miner richiedono l’indirizzo completo: dopo una ricerca @name, copialo dalla casella My Stats.

Per leggere il miner dal terminale:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Con `jq` puoi estrarre singoli valori, come hashrate e share accettate, oppure totale pagato in BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Mostra il miner su una dashboard domestica

Lo stesso endpoint funziona come sensore. In Home Assistant l’integrazione RESTful lo legge con poche righe di `configuration.yaml`:

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Interrogare ogni 30-60 secondi basta: l’hashrate è una media oraria e My Stats aggiorna i dettagli ogni 30 secondi. L’API limita le richieste per client. Se uno script riceve HTTP 429 deve rallentare e attendere il tempo indicato da `Retry-After` prima di riprovare.

## E la dashboard di seine?

La pool mostra quanto ti ha accreditato. Per la macchina in diretta, la dashboard terminale mostra hashrate e share immediatamente. seine può anche esporre un’API locale REST con eventi server-sent e metriche Prometheus su `/metrics`, legata a `127.0.0.1:9977` per impostazione predefinita. La [guida API di controllo](https://github.com/zpalmtree/seine/blob/master/docs/API.md) descrive flag ed endpoint. Molti miner usano entrambi: metriche seine per ogni macchina e API pool per saldi e pagamenti.

Non conosci seine? [Scaricalo](https://seineminer.com/it/#download), usa la pool predefinita e l’indirizzo comparirà su My Stats entro pochi minuti dalla prima share accettata.

<CTA target="pool" title="Inizia a minare e segui tutto in diretta" text="Entra in bntpool.com in pochi minuti, poi segui hashrate, share e saldo su My Stats." />
