---
title: 'Il fair launch di Blocknet: ogni moneta viene minata'
description: Blocknet è nato senza premine, fondo sviluppatori o tesoreria. Verifica il genesi vuoto e la coinbase a un solo output, poi inizia a minare BNT.
tags: [Blocknet, Fair launch, Blocco genesi, Mining, bntpool]
faq:
  - q: 'Blocknet ha avuto un premine?'
    a: 'No. Il blocco genesi, creato il 15 febbraio 2026, non contiene transazioni: nessuna moneta esisteva prima del mining. Le prime sono arrivate dalla ricompensa del blocco 1 e ogni BNT successivo è stato pagato a chi ha minato il blocco da cui proviene.'
  - q: 'Blocknet ha un fondo sviluppatori o una tesoreria?'
    a: 'No. Il consenso permette esattamente una transazione coinbase per blocco, con esattamente un output pari alla ricompensa. Non esiste un secondo output per sviluppatori, fondatori o tesoreria e ogni nodo rifiuta un blocco che provi ad aggiungerlo.'
  - q: 'Quando è stato creato il blocco genesi?'
    a: 'Il timestamp è il 15 febbraio 2026 alle 20:58:38 EST, cioè le 01:58:38 UTC del 16 febbraio. Contiene il messaggio “Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything”, il cui SHA3-256 è l’hash del blocco precedente.'
  - q: 'Come posso verificare il fair launch di Blocknet?'
    a: 'Apri il blocco 0 sul block explorer: vedrai zero transazioni e una radice Merkle di soli zeri. Poi leggi il codice aperto del daemon: il genesi è definito con una lista di transazioni vuota e il controllo coinbase accetta un solo output pari alla ricompensa.'
  - q: 'Come inizio a minare Blocknet?'
    a: 'Scarica seine per Windows, macOS o Linux, eseguilo e incolla l’indirizzo Blocknet o lo shortname Blocknet ID quando richiesto. Premi Invio per accettare bntpool.com, la pool predefinita, e il mining parte subito. Poi cerca il tuo indirizzo nella pagina statistiche della pool.'
sourceHash: 814839500c3d
translation: draft
---

Blocknet è partito senza assegnare nulla a nessuno. Questa pagina mostra il contenuto del blocco genesi, le regole di consenso che escludono premine, fondo sviluppatori e tesoreria, come verificarle sul block explorer e nel codice e come iniziare a minare BNT su bntpool.com.

## Blocknet ha avuto un premine?

<Answer>
No. Il blocco genesi di Blocknet, creato il 15 febbraio 2026, non contiene transazioni: non esistevano monete prima del mining. Dal blocco 1 ogni blocco paga la ricompensa in un solo output coinbase, esattamente pari all’importo previsto. Ogni BNT in circolazione è quindi stato minato da qualcuno che eseguiva la proof of work.
</Answer>

Non ci sono assegnazioni a fondatori, sviluppatori, investitori o fondazioni, né nel genesi né nei blocchi successivi. L’offerta deriva soltanto dalle ricompense dei blocchi. Le regole fanno parte del consenso, quindi ogni nodo le verifica per ogni blocco.

## Cosa contiene il blocco genesi di Blocknet?

Il genesi è definito nel daemon, così ogni nodo parte dallo stesso blocco 0. I campi sono fissi:

| Campo | Valore |
|---|---|
| Altezza | 0 |
| Timestamp | 15 febbraio 2026, 20:58:38 EST (01:58:38 UTC del 16 febbraio) |
| Messaggio | “Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything” |
| Hash precedente | SHA3-256 del messaggio: `a94a7f2b…d909df` |
| Difficoltà | 4, il minimo della rete |
| Nonce | 0 |
| Transazioni | nessuna |
| Radice Merkle | tutti zeri |

Il messaggio è il titolo di un articolo Wired del 15 febbraio 2026, citato nel codice. Poiché l’hash precedente deriva dal messaggio, il titolo è fissato nell’identità del blocco 0 e in tutto ciò che vi viene costruito sopra.

Il commento sopra la funzione genesi è chiaro: “Il genesi non ha transazioni: nessuna coinbase, nessuna moneta bruciata. Il primo vero blocco all’altezza 1 avrà la prima coinbase.” La validazione lo impone. Un nodo rifiuta il blocco 0 se non ha esattamente zero transazioni e una radice Merkle di soli zeri. Neppure un nodo modificato potrebbe inserirvi monete senza essere rifiutato dagli altri.

## Come il consenso esclude un fondo sviluppatori o una tesoreria?

<Answer>
Ogni blocco Blocknet deve iniziare con esattamente una coinbase, senza input, con commissione zero e un solo output pari alla ricompensa prevista per quell’altezza. Non c’è spazio per un secondo pagamento: un output per sviluppatori, fondatori o tesoreria non può esistere sulla catena.
</Answer>

Diversi controlli del daemon operano insieme:

- **Una coinbase per blocco.** La prima transazione deve essere coinbase e nessun’altra può esserlo.
- **Un output, nessun input.** Una coinbase con zero o due output, oppure con un input, non è valida.
- **La ricompensa esatta.** Blocknet nasconde gli importi con commitment di Pedersen, ma la coinbase è speciale. Il fattore di blinding deriva da dati pubblici, la chiave pubblica della transazione e l’altezza, così ogni nodo può ricostruire il commitment della ricompensa prevista e confrontarlo. Un importo diverso fa rifiutare il blocco.
- **Un memo vuoto.** Il memo coinbase deve essere vuoto, quindi la ricompensa non può nemmeno trasportare dati aggiuntivi.

Le transazioni ordinarie non creano monete. Devono bilanciarsi: input uguali a output più commissione, verificati sui commitment stessi. Ogni output porta una range proof Bulletproofs che dimostra un importo nel range valido e non negativo. Gli importi rimangono privati mentre l’emissione rispetta esattamente il calendario.

## Come verificare personalmente il fair launch?

Non devi fidarti della parola di qualcuno. Catena e codice sono pubblici.

**Sul block explorer.** Apri il [blocco 0](https://explorer.blocknetcrypto.com/block/0). Mostra 0 transazioni, una radice Merkle di 64 zeri, nonce 0 e hash precedente che inizia con `a94a7f2b`. La riga della ricompensa mostra l’importo previsto per l’altezza, ma senza transazioni non c’è un output che lo riceva. Apri poi il [blocco 1](https://explorer.blocknetcrypto.com/block/1): l’unica transazione è una coinbase senza input e con un output, le prime monete della catena.

Puoi confermare che l’hash precedente sia quello del messaggio genesi. Con OpenSSL 1.1.1 o successivo, esegui:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Il risultato termina con `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, il valore mostrato per il blocco 0.

**Nel codice.** Il daemon è open source su [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). In [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go), cerca queste funzioni:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` crea il blocco 0 con lista vuota, `validateGenesisBlock` rifiuta un blocco 0 con transazioni e `validateCoinbaseConsensus` impone un solo output pari alla ricompensa. In `transaction.go`, `validateCoinbase` e `verifyCommitmentBalance` controllano forma della coinbase e bilancio delle transazioni ordinarie.

## Perché il fair launch conta per chi mina?

Significa che le ricompense spiegano interamente da dove arrivano le monete.

- **Tutte le nuove monete vanno ai miner.** Senza premine e tesoreria, il 100% dell’emissione viene pagato a chi trova i blocchi. Il blocco 1 ha pagato 723,25093035 BNT nel suo unico output coinbase e ogni blocco successivo ha pagato l’intera ricompensa nello stesso modo.
- **Le regole sono uguali per tutti.** Dal blocco 1 l’unico modo di ottenere nuovi BNT è eseguire la proof of work. Il [calendario delle ricompense](/blog/it/blocknet-block-reward-schedule/) è nel codice e vale per il primo miner come per chi inizia oggi.
- **La proof of work è adatta ai computer comuni.** Blocknet usa Argon2id con 2 GiB per hash, progettato per resistere agli ASIC. Il mining rimane aperto a desktop, laptop e server già posseduti. La [spiegazione di Argon2id](https://seineminer.com/it/blog/argon2id-proof-of-work-explained/) approfondisce il motivo.
- **L’emissione è facile da verificare.** Ogni moneta risale a un output coinbase controllato da ogni nodo. Il totale emesso a qualsiasi altezza deriva direttamente dal calendario.

Per una panoramica della moneta, [Cos’è Blocknet?](https://seineminer.com/it/blog/what-is-blocknet/) tratta privacy, pagamenti tramite nome e wallet.

## Come minare Blocknet personalmente?

Partecipare a un fair launch significa minare. Il modo più rapido è usare seine su bntpool.com:

1. **Ottieni un indirizzo.** Installa il [wallet da riga di comando](https://github.com/blocknetprivacy/blocknet/releases/latest) o il [wallet desktop](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) ufficiale e copia l’indirizzo, oppure usa uno shortname Blocknet ID come `@name`.
2. **Scarica seine.** Prendi l’archivio Windows, macOS o Linux da [seineminer.com](https://seineminer.com/it/#download) ed estrailo. Non c’è nulla da installare.
3. **Eseguilo e accetta i valori predefiniti.** Incolla l’indirizzo o `@name` e premi Invio per `stratum+tcp://bntpool.com:3333`. seine inizia subito e ottimizza thread e kernel durante il mining, su CPU e schede NVIDIA in Windows e Linux.
4. **Segui il miner.** Cerca l’indirizzo nella [pagina statistiche](/stats) per hashrate, share e saldo, e segui le scoperte nella [pagina blocchi](/blocks).

La [guida al mining su bntpool.com](/blog/it/mine-blocknet-on-bntpool/) tratta accesso tramite indirizzo o nome e verifica del miner, [Get Started](/start) elenca le impostazioni di pagamento e la [guida a minare Blocknet](https://seineminer.com/it/blog/how-to-mine-blocknet/) mostra come installare seine su ogni sistema.

<CTA target="seine" title="Mina la tua parte" text="Ogni BNT viene minato. Scarica seine e inizia su bntpool.com in pochi minuti." />
