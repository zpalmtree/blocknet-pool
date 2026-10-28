---
title: 'Blocknets eerlijke start: geen premine, elke munt gemined'
description: "Controleer Blocknets lege genesisblok, eerste beloning en uitgifteregels. Lees hoe BNT in omloop kwam en hoe gemijnde munten andere houders bereiken."
tags: ['Blocknet', 'Eerlijke start', 'Genesisblok', 'Mining', 'bntpool']
sourceHash: a283d654ab64
translation: draft
faq:
  - q: 'Had Blocknet een premine?'
    a: 'Nee. Het genesisblok van Blocknet, gemaakt op 15 februari 2026, bevat helemaal geen transacties. Er bestonden dus geen munten voordat het minen begon. De eerste munten kwamen uit de beloning van blok 1. Elke BNT sindsdien is betaald aan degene die het bijbehorende blok mined heeft.'
  - q: 'Heeft Blocknet een ontwikkelfonds of treasury?'
    a: "De gepubliceerde regels kennen geen door het protocol verplichte toewijzing aan een ontwikkelfonds of treasury. Elk blok na genesis heeft één coinbase-output gelijk aan de geplande beloning. Dit beschrijft nieuwe uitgifte, niet de identiteit van latere houders. Iedereen met BNT kan gewone overdrachten uitvoeren, waaronder vrijwillige betalingen aan een ontwikkelaar."
  - q: 'Wanneer is het genesisblok van Blocknet gemaakt?'
    a: 'De tijdstempel is 15 februari 2026 om 20:58:38 EST, oftewel 16 februari om 01:58:38 UTC. Het bevat de boodschap «Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything». De hash van het vorige blok is de SHA3-256-hash van die boodschap.'
  - q: 'Hoe controleer ik zelf de eerlijke start van Blocknet?'
    a: 'Open blok 0 in de Blocknet-explorer: je ziet nul transacties en een merkle root met uitsluitend nullen. Lees daarna de opensource-daemoncode. Daar is genesis vastgelegd met een lege transactielijst en accepteert de coinbasecontrole één output gelijk aan de blokbeloning.'
  - q: 'Hoe begin ik met Blocknet minen?'
    a: 'Download seine voor Windows, macOS of Linux, start het en plak je Blocknet-adres of korte Blocknet ID-naam wanneer daarom wordt gevraagd. Druk op Enter voor de standaardpool bntpool.com en seine begint direct. Daarna kun je je adres opzoeken op de statistiekenpagina van bntpool.com.'
---

Zo kwam BNT in omloop: een leeg genesisblok, de eerste gemijnde beloning en latere overdrachten tussen houders. Deze gids controleert de startgegevens en uitgifteregels, legt uit wat ze aantonen en laat zien hoe je nieuw uitgegeven BNT ontvangt door op bntpool.com te minen.

## Had Blocknet een premine?

<Answer>
Nee. Het genesisblok van Blocknet, gemaakt op 15 februari 2026, bevat geen transacties. Er bestonden geen munten voordat mining begon. Sinds blok 1 betaalt elk blok zijn beloning in één coinbase-output, precies ter waarde van het vastgelegde bedrag. Elke BNT in omloop is dus gemined door iemand die de proof of work uitvoerde.
</Answer>

Het genesisblok wijst geen reserve toe aan oprichters, ontwikkelaars, investeerders of een stichting. Latere blokken creëren de geplande miningbeloning, zonder extra treasury-output die het protocol voorschrijft. Dit zijn uitgifteregels: ze identificeren niet wie een beloning heeft gemijnd of wie bestaande munten later via een overdracht heeft ontvangen.

## Wat bevat het genesisblok van Blocknet?

Het genesisblok ligt vast in de daemon, zodat iedere node begint met hetzelfde blok 0. De velden zijn:

| Veld | Waarde |
|---|---|
| Hoogte | 0 |
| Tijdstempel | 15 februari 2026, 20:58:38 EST (16 februari om 01:58:38 UTC) |
| Boodschap | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Vorige hash | SHA3-256 van de boodschap: `a94a7f2b…d909df` |
| Moeilijkheid | 4, het netwerkminimum |
| Nonce | 0 |
| Transacties | geen |
| Merkle root | uitsluitend nullen |

De boodschap is de titel van een Wired-artikel van 15 februari 2026, dat de code als bron noemt. Omdat de vorige hash uit de boodschap wordt afgeleid, zit de titel vast in de identiteit van blok 0 en alles wat daarop voortbouwt.

Het commentaar boven de genesisfunctie in de daemon zegt het duidelijk: genesis heeft geen transacties, geen coinbase en geen verbrande munten; het eerste echte blok op hoogte 1 krijgt de eerste coinbase. De validatie dwingt dit af. Een node accepteert blok 0 alleen met precies nul transacties en een merkle root van uitsluitend nullen. Zelfs een aangepaste node kan er dus geen munten in stoppen zonder door de rest te worden afgewezen.

## Hoe sluit consensus een ontwikkelfonds of treasury uit?

<Answer>
Na genesis begint elk blok met een coinbase-transactie zonder inputs, met nul kosten en precies één output gelijk aan de geplande beloning. De gepubliceerde regels voorzien in die transactie niet in een aparte output voor oprichters of een treasury. Houders kunnen bestaande BNT gewoon blijven overdragen.
</Answer>

Verschillende controles in de daemon werken hierbij samen:

- **Eén coinbase per blok.** De eerste transactie moet een coinbase zijn; geen andere transactie in het blok mag dat zijn.
- **Eén output, geen inputs.** Een coinbase met nul of twee outputs, of met een input, is ongeldig.
- **De exacte beloning.** Blocknet verbergt bedragen met Pedersen commitments, maar coinbase is een speciaal geval. De blinding factor wordt afgeleid uit openbare gegevens: de publieke transactiesleutel en de blokhoogte. Iedere node kan daardoor de commitment voor de vastgelegde beloning opnieuw opbouwen en vergelijken. Bij een ander bedrag wordt het blok afgewezen.
- **Een lege memo.** De coinbase-memo moet leeg zijn; blokbeloningen kunnen dus ook geen extra gegevens bevatten.

Gewone transacties kunnen geen munten maken. Ze moeten in balans zijn: inputs zijn gelijk aan outputs plus transactiekosten, gecontroleerd op de commitments zelf. Elke output draagt een Bulletproofs range proof die aantoont dat het bedrag binnen een geldig bereik ligt en niet negatief is. Zo blijven bedragen privé terwijl de uitgifte exact het schema volgt.

## Hoe controleer je zelf de eerlijke start van Blocknet?

Je hoeft niemand op zijn woord te geloven. Zowel de keten als de code is openbaar.

**In de explorer.** Open [blok 0 in de Blocknet-explorer](https://explorer.blocknetcrypto.com/block/0). Het toont 0 transacties, een merkle root van 64 nullen, nonce 0 en een vorige hash die begint met `a94a7f2b`. De regel voor de blokbeloning toont het bedrag uit het schema voor die hoogte, maar zonder transacties bestaat er geen output om het te ontvangen. Open daarna [blok 1](https://explorer.blocknetcrypto.com/block/1): de enige transactie is een coinbase zonder inputs en met één output, de eerste munten op de keten.

Controleer ook dat de vorige hash werkelijk de hash van de genesisboodschap is. Met OpenSSL 1.1.1 of nieuwer:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

De uitvoer eindigt op `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, dezelfde waarde als voor blok 0 in de explorer.

**In de code.** De daemon is opensource op [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). Zoek in [`block.go`](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) deze functies:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` maakt blok 0 met een lege transactielijst. `validateGenesisBlock` wijst elk blok 0 met transacties af. `validateCoinbaseConsensus` dwingt één output ter waarde van de blokbeloning af. In `transaction.go` controleren `validateCoinbase` en `verifyCommitmentBalance` de coinbasevorm en de balans van gewone transacties.

## Hoe kwam nieuw gemijnde BNT bij andere houders terecht?

De explorer vermeldt voor genesis 15 februari 2026 om 20:58:38 EST. Blok 1 draagt 20 februari 2026 om 14:29:20 EST en bevat de eerste coinbase-transactie. Dit zijn aparte records: genesis definieert het gezamenlijke beginblok, terwijl blok 1 de betaalde uitgifte begint. De beloning die de explorer bij het lege genesisblok toont, is het geplande bedrag; geen output ontvangt het.

Zodra een miningbeloning besteedbaar is, kan de eigenaar BNT aan iemand anders overdragen. Een pool verstuurt op dezelfde manier latere betalingen uit ontvangen beloningen. Die overdrachten herverdelen bestaande munten; ze herhalen de uitgifte niet. Je kunt dus BNT bezitten en gebruiken zonder zelf het oorspronkelijke blok te hebben gemijnd. De [gids voor verzenden en ontvangen](https://seineminer.com/nl/blog/send-receive-blocknet/) behandelt gewone betalingen; de [aanbodgids](https://seineminer.com/nl/blog/blocknet-supply/) legt het algemene monetaire schema uit.

## Waarom is een eerlijke start belangrijk voor miners?

Voor miners betekent een eerlijke start dat blokbeloningen de volledige herkomst van de munten verklaren.

- **Alle nieuwe munten gaan naar miners.** Zonder premine of treasury-output gaat 100% van de uitgifte naar de mensen die blokken vinden. Blok 1 betaalde 723,25093035 BNT in zijn enige coinbase-output. Elk volgend blok betaalde op dezelfde manier zijn volledige beloning.
- **Iedereen volgt dezelfde regels.** Sinds blok 1 is proof of work de enige manier om nieuwe BNT te verdienen. Het [beloningsschema](/blog/nl/blocknet-block-reward-schedule/) staat in code en geldt gelijk voor de eerste miner en iemand die vandaag begint.
- **De proof of work past bij gewone computers.** Blocknet gebruikt Argon2id met 2 GiB geheugen per hash, ontworpen om ASICs tegen te gaan. Daardoor blijft mining toegankelijk voor desktops, laptops en servers die mensen al bezitten. De [Argon2id-uitleg op seineminer.com](https://seineminer.com/nl/blog/argon2id-proof-of-work-explained/) legt uit waarom.
- **De uitgifte is eenvoudig te controleren.** Iedere munt komt uit een coinbase-output die elke node heeft gecontroleerd. De totale uitgifte op elke hoogte volgt daardoor direct uit het beloningsschema.

Nieuw met de munt? [Wat is Blocknet?](https://seineminer.com/nl/blog/what-is-blocknet/) behandelt privacy, betalen op naam en wallets.

## Hoe mine je zelf Blocknet?

Gebruik de seine-miner met bntpool.com om via mining nieuw uitgegeven BNT te ontvangen:

1. **Haal een adres op.** Installeer de officiële Blocknet-[opdrachtregelwallet](https://github.com/blocknetprivacy/blocknet/releases/latest) of [desktopwallet](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) en kopieer je adres, of gebruik een Blocknet ID-naam zoals `@name`.
2. **Download seine.** Download het archief voor Windows, macOS of Linux van [seineminer.com](https://seineminer.com/nl/#download) en pak het uit. Installeren is niet nodig.
3. **Start en accepteer de standaardwaarden.** Open seine, plak je adres of `@name` en druk op Enter voor `stratum+tcp://bntpool.com:3333`. Seine begint meteen met hashen en stemt threads en kernels tijdens het draaien af, op je CPU en op NVIDIA-kaarten onder Windows en Linux.
4. **Volg je miner.** Zoek je adres op de [statistiekenpagina](/stats) voor hashrate, shares en saldo. Volg de gevonden blokken op de [blokkenpagina](/blocks).

De [gids voor minen op bntpool.com](/blog/nl/mine-blocknet-on-bntpool/) behandelt inloggen met adres of naam en het controleren van je miner. [Get Started](/start) toont de uitbetalingsinstellingen en de [Blocknet-mininggids](https://seineminer.com/nl/blog/how-to-mine-blocknet/) legt de installatie van seine stap voor stap uit voor elk systeem.

<CTA target="seine" title="Mine jouw aandeel" text="Elke BNT is gemined. Download seine en begin binnen enkele minuten op bntpool.com." />
