---
title: 'Hoe PPLNS-uitbetalingen werken op bntpool.com'
description: 'bntpool.com betaalt Blocknet-miners via PPLNS over zes uur. Lees hoe beloningen worden verdeeld, waarom 60 bevestigingen nodig zijn en wanneer 0,1 BNT wordt betaald.'
tags: ['Uitbetalingen', 'PPLNS', 'bntpool', 'Mininggids']
sourceHash: 'f216680e1944'
translation: draft
faq:
  - q: 'Hoe lang is het PPLNS-venster op bntpool.com?'
    a: 'Zes uur. Wanneer de pool een blok vindt, kijkt hij terug naar de zes uur ervoor en verdeelt de beloning onder iedereen die in die periode shares instuurde, naar verhouding van hun moeilijkheid. De huidige waarde staat op de pagina Get Started van de pool.'
  - q: 'Wat is de minimumuitbetaling op bntpool.com?'
    a: 'De minimumuitbetaling is 0,1 BNT. Zodra je bevestigde saldo dat bedrag bereikt, stuurt de pool het automatisch naar je Blocknet-adres. Je hoeft niets aan te vragen of te claimen. Een kleiner saldo blijft staan en groeit met elk bevestigd blok waarin je shares meetellen, totdat de grens is bereikt.'
  - q: 'Waarom verschillen Estimated Rewards en Confirmed Rewards?'
    a: 'Estimated Rewards zijn je aandeel in gevonden blokken die hun 60 bevestigingen nog verzamelen; dat bedrag kan nog veranderen. Confirmed Rewards komen uit blokken die 60 blokken diep liggen en wachten op uitbetaling. Zodra een blok rijp is, verhuist het bedrag van de eerste naar de tweede kaart.'
  - q: 'Stoppen mijn beloningen zodra ik stop met minen?'
    a: 'Nee. Elke share blijft na insturen zes uur in het PPLNS-venster. Blokken die de pool in die tijd vindt, leveren je dus nog beloningen op. Bevestigde beloningen blijven op je saldo staan en worden uitbetaald zodra het saldo het minimum van 0,1 BNT bereikt.'
  - q: 'Wat betekent effort boven 100% op de luck-pagina?'
    a: 'Effort vergelijkt het werk tijdens een ronde met de blokmoeilijkheid, de verwachte hoeveelheid werk. Boven 100% betekent dat de ronde langer duurde dan gemiddeld, onder 100% dat hij sneller was. Beide zijn normaal. Over veel rondes komt de gemiddelde effort rond 100% uit.'
---

Deze gids legt uit hoe bntpool.com de shares van je miner omzet in Blocknet-uitbetalingen. Je leert hoe een blokbeloning wordt verdeeld, waarom die ongeveer vijf uur wacht voordat ze kan worden betaald, wanneer je saldo wordt verstuurd en hoe je de saldokaarten op de [statistiekenpagina](/stats) en de [luck-pagina](/luck) leest.

Nieuw met poolmining? [Wat is een miningpool?](/blog/nl/what-is-a-mining-pool/) behandelt de basis. Ben je nog niet begonnen, dan brengt de [mininggids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) je in enkele minuten van download naar je eerste shares.

## Wat is PPLNS?

<Answer>
PPLNS betekent «pay per last N shares». Als bntpool.com een blok vindt, verdeelt de pool de beloning over de shares uit de zes uur vóór dat blok, naar verhouding van de moeilijkheid van elke share. Hoe meer doorlopend werk je binnen dat venster levert, hoe groter je aandeel in elk blok.
</Answer>

Op bntpool.com is «last N» een tijdsduur: zes uur. [Get Started](/start) toont het uitbetalingssysteem, het venster, het minimum en het aantal bevestigingen. Dezelfde waarden staan op `https://bntpool.com/api/info`.

| Instelling | Waarde op bntpool.com |
|---|---|
| Uitbetalingssysteem | PPLNS |
| Sharevenster | 6 uur |
| Bevestigingen voor uitbetaling | 60 blokken |
| Minimumuitbetaling | 0,1 BNT |

Shares tellen naar moeilijkheid. De pool geeft elke verbinding een eigen sharemoeilijkheid en past die aan voor een regelmatige stroom shares. Een snellere machine krijgt moeilijkere shares, zodat elke share zwaarder weegt. Je aandeel wordt bepaald door je totale werk, een eerlijke maat voor zowel laptop als server.

## Hoe verdeelt bntpool.com een blokbeloning?

Bij ieder gevonden blok doorloopt de beloning dezelfde stappen:

1. **Het blok wordt vastgelegd.** Het verschijnt op de [blokkenpagina](/blocks) als pending.
2. **Het venster wordt gemeten.** De pool neemt de zes uur die eindigen bij het blok en telt per adres de moeilijkheid van alle geaccepteerde shares op.
3. **De beloning wordt verdeeld.** Elk adres ontvangt hetzelfde aandeel in de beloning als zijn aandeel in het totale werk van het venster.
4. **Het blok rijpt.** Je aandeel blijft een schatting totdat het blok 60 blokken diep ligt en verhuist dan naar je bevestigde saldo.
5. **Het saldo wordt betaald.** Bij een bevestigd saldo van 0,1 BNT stuurt de pool het naar je adres.

## Een rekenvoorbeeld

<Note>
Dit voorbeeld gebruikt verzonnen ronde getallen om de berekening te tonen. Werkelijke totalen hangen af van de hashrate van de pool, jouw hashrate en de [blokbeloning](/blog/nl/blocknet-block-reward-schedule/) op dat moment.
</Note>

Stel dat alle miners in de zes uur vóór een blok shares met een totale moeilijkheid van 1.000.000 hebben ingestuurd, waarvan jouw shares 20.000 bijdragen. Jouw aandeel is 20.000 ÷ 1.000.000 = 2%. Verdeelt de pool 400 BNT, dan krijg je 400 × 0,02 = 8 BNT.

Neem nu een tweede miner met precies jouw hashrate, die 30 minuten vóór het blok verbond. Dertig minuten is een twaalfde van zes uur. Die miner heeft ongeveer 1.667 moeilijkheid in het venster en ontvangt circa 0,67 BNT voor dit blok. Het werk blijft wel meetellen: elke share blijft de volgende zes uur in het venster en verdient aan elk blok dat de pool dan vindt.

| Miner (voorbeeld) | Werk in venster | Aandeel | Credit uit een uitbetaling van 400 BNT |
|---|---|---|---|
| Jij, de volle zes uur aan het minen | 20.000 | 2% | 8 BNT |
| Dezelfde hashrate, 30 minuten vóór het blok begonnen | 1.667 | circa 0,17% | circa 0,67 BNT |
| Kleine miner, de volle zes uur aan het minen | 100 | 0,01% | 0,04 BNT |

De kleine miner in de laatste regel verdient 0,04 BNT per blok. Na drie zulke bevestigde blokken passeert het saldo het minimum van 0,1 BNT en volgt uitbetaling.

## Waarom beloont PPLNS doorlopend minen?

Elke ingestuurde share telt mee voor elk blok dat de pool in de volgende zes uur vindt. Een miner die dag en nacht draait, heeft altijd een vol werkvenster achter zich. Elk nieuw blok schrijft daardoor het volledige aandeel bij. De opbrengst volgt de hashrate van blok tot blok nauwkeurig.

PPLNS vangt ook korte onderbrekingen op. Een herstart, driverupdate of korte netwerkuitval reset je positie niet: eerdere shares blijven zes uur in het venster. Werk na een gevonden blok telt voor volgende blokken, zodat extra hashrate altijd wordt benut.

Laat je miner voor de regelmatigste resultaten draaien en zichzelf afstemmen. Seine past tijdens het minen het aantal threads en de CPU-kernel aan zonder opnieuw met de pool te verbinden, zodat de stroom shares doorgaat. Je kunt [seine downloaden](https://seineminer.com/nl/#download) voor Windows, macOS en Linux.

## Waarom wacht een blokbeloning op 60 bevestigingen?

Blocknet laat nieuw geminede munten 60 blokken wachten voordat ze besteedbaar zijn. De daemon noemt dit coinbase maturity: ongeveer vijf uur bij blokken van vijf minuten (60 × 5 minuten = 300 minuten). bntpool.com gebruikt dezelfde 60 blokken voordat een beloning bevestigd is, zodat elke uitbetaling bestaat uit munten die de pool daadwerkelijk kan versturen.

Tijdens het rijpen controleert de pool of het blok nog bij de hoofdketen hoort. Op 60 blokken diepte wordt het bevestigd en wordt de beloning bij de bevestigde saldi opgeteld. In het zeldzame geval dat het netwerk voor een ander blok op die hoogte kiest, wordt het blok orphaned en worden de geschatte beloningen verwijderd.

## Wanneer komen uitbetalingen van bntpool.com aan?

<Answer>
Na 60 bevestigingen, ongeveer vijf uur bij de bloktijd van Blocknet, verhuist je aandeel naar Confirmed Rewards op de statistiekenpagina. Zodra je bevestigde saldo 0,1 BNT bereikt, stuurt bntpool.com het automatisch naar je Blocknet-adres. Je hoeft niets aan te vragen of te claimen.
</Answer>

Voor een nieuwe miner verschijnt de eerste schatting zodra de pool een blok vindt terwijl zijn shares in het venster zitten. De eerste betaling volgt nadat dat blok rijp is en het saldo het minimum bereikt. Verstuurde betalingen staan op de [uitbetalingspagina](/payouts) en in je eigen geschiedenis op My Stats.

## Hoe worden voorlopige beloningen getoond?

Zoek je adres of `@name` op de [statistiekenpagina](/stats). Je ziet drie saldokaarten:

- **Estimated Rewards:** je aandeel in nog niet gerijpte blokken. Deze voorlopige bedragen kunnen nog veranderen tot het blok bevestigd of orphaned is.
- **Confirmed Rewards:** je gerijpte saldo dat wacht op uitbetaling.
- **Paid Balance:** alles wat al naar je adres is gestuurd.

De pool houdt voorlopige beloningen apart, zodat het bevestigde saldo alleen definitieve bedragen bevat. Onder de kaarten staat elk onbevestigd blok met je geschatte aandeel, zodat je de voortgang kunt volgen. De [gids voor het volgen van je miner](/blog/nl/track-your-miner-on-bntpool/) behandelt de rest van de pagina.

## Wat vertellen luck en effort je?

Effort meet hoeveel werk de pool nodig had voor een blok: het geaccepteerde werk in de ronde gedeeld door de blokmoeilijkheid, als percentage. De moeilijkheid van Blocknet is het verwachte aantal hashes per blok. Bij 100% effort kostte het dus precies de gemiddelde hoeveelheid werk. Lager betekent sneller gevonden, hoger betekent langer gewacht.

De [luck-pagina](/luck) toont elke ronde met blokhoogte, effort, rondetijd, status en vindmoment. Het [pooldashboard](/) toont de oplopende effort van de huidige ronde en de gemiddelde luck. Losse rondes schommelen boven en onder 100%, zoals normaal is bij proof-of-work-pools. Over veel rondes komt effort gemiddeld rond 100% uit.

Luck beïnvloedt hoe vaak blokken aankomen. Je aandeel in elk blok hangt uitsluitend af van je aandeel in het werkvenster. Doorlopend minen houdt dat aandeel vol, of de ronde nu kort of lang duurt.

<CTA target="pool" title="Sluit je aan bij de pool" text="Mine doorlopend op bntpool.com. Je aandeel in elk blok wordt automatisch betaald zodra je 0,1 BNT bereikt." />
