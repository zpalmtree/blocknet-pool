---
title: "Blocknet-miningcalculator"
description: "Schat BNT per dag en week met je hashrate, actuele netwerkgegevens en het beloningsschema. Kosten worden apart getoond."
tags: ['Miningbeloningen', 'Mininggids', 'Hashrate', 'bntpool']
sourceHash: c8369c1db7c0
translation: draft
faq:
  - q: 'Hoe bereken je Blocknet-miningbeloningen?'
    a: 'Deel je hashrate door de netwerkhashrate, vermenigvuldig met 288, het aantal Blocknet-blokken per dag, en met de huidige blokbeloning. Dit is je verwachte bruto BNT per dag. De netwerkhashrate staat op het dashboard van bntpool.com; je eigen waarde lees je in seine of op My Stats.'
  - q: 'Waar zie ik de netwerkhashrate van Blocknet?'
    a: 'Het dashboard van bntpool.com toont die op Network Hashrate, meestal in KH/s. Vermenigvuldig met 1.000 voor H/s. Dezelfde waarde staat in H/s in het veld network_hashrate van bntpool.com/api/stats. Ze wordt over de tien recentste blokken gemeten en bij elk blok bijgewerkt.'
  - q: 'Verandert de blokbeloning van Blocknet met de tijd?'
    a: 'Ja. Ze is vast binnen elke maand van 30 dagen of 8.640 blokken en daalt aan het begin van de volgende maand, tot permanent 2,0 BNT per blok vanaf maand 48. Deel de huidige blokhoogte door 8.640, rond naar beneden af en zoek de maand op in het beloningsschema.'
  - q: 'Waarom wijken mijn dagelijkse poolbeloningen af van de schatting?'
    a: 'De schatting is een gemiddelde. Op een dag kan de pool meer of minder blokken vinden dan verwacht, terwijl de netwerkhashrate verandert als miners aansluiten. Over een week of langer doorlopend minen middelen dagelijkse schommelingen uit. Volg je bijgeschreven beloningen op My Stats.'
  - q: 'Verandert poolmining hoeveel BNT ik kan verwachten?'
    a: 'Je verwachte aandeel in de netwerkbeloningen blijft gelijk: het hangt alleen af van je aandeel in de netwerkhashrate. Een pool verandert de timing. In plaats van dagen op een heel blok te wachten, krijg je een deel van elk blok van bntpool.com, in kleine, regelmatige bedragen.'
---

<RewardsCalculator />

Deze gids laat zien hoe je schat hoeveel BNT je hardware per dag mint. Je leert de formule, waar elke invoer op het [dashboard van bntpool.com](/) staat en hoe je rekent met een gedateerd voorbeeld uit echte poolgegevens en de beste gemeten hashrates van seine.

## Hoeveel BNT mint jouw hardware per dag?

<Answer>
Verwachte BNT per dag is jouw hashrate gedeeld door de netwerkhashrate, maal 288 blokken per dag, maal de huidige blokbeloning. Je eigen hashrate komt uit seine of My Stats, de netwerkhashrate uit het dashboard en de beloning uit de huidige blokhoogte en het beloningsschema van Blocknet.
</Answer>

Als formule:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Elk onderdeel heeft een eenvoudige reden:

- **288 blokken per dag.** Blocknet mikt op één blok per vijf minuten. De 86.400 seconden in een dag gedeeld door 300 per blok geven 288. De moeilijkheid wordt elk blok aangepast, zodat de keten dat tempo blijft benaderen als miners aansluiten.
- **Je aandeel in hashrate is je aandeel in blokken.** De moeilijkheid benadert het verwachte aantal hashes voor één blok. Elke hash heeft dezelfde kans. Een machine met 1% van de netwerkhashes kan op termijn 1% van de blokken verwachten.
- **De beloning ligt vast per hoogte.** Elk blok in dezelfde maand van 30 dagen of 8.640 blokken betaalt gelijk. Voor elke dag binnen een maand is de beloning dus een bekende constante.

Het resultaat is je verwachte brutobeloning, gemiddeld over tijd.

## Waar vind je elke invoer?

### Jouw hashrate

Seine toont je hashrate op zijn terminaldashboard zodra het start. Geef het enkele minuten: het stemt threads en CPU-kernel af op echt miningwerk. De waarde stabiliseert zodra de beste instellingen voor jouw machine gevonden zijn.

Als je op de pool mint, toont **Hashrate** op [My Stats](/stats) de poolschatting over het laatste uur. Gebruik voor je berekening de stabiele seine-waarde of de 24-uursgrafiek van de pool.

### Netwerkhashrate

**Network Hashrate** toont het hele netwerk, in KH/s. Een waarde van 62,33 KH/s betekent 62.330 H/s. Reken vóór het delen om naar H/s, anders wijkt het resultaat een factor duizend af.

De pool gebruikt werk en tijdstempels van de tien recentste blokken, dus de waarde verandert per blok. [Wat is hashrate?](/blog/nl/what-is-hashrate/) legt de berekening en schommelingen uit. Meet enkele keren per dag en neem het gemiddelde voor een stabielere invoer. Je kunt de waarde ook in H/s uit de API lezen:

```sh
curl -s https://bntpool.com/api/stats
```

De sectie `chain` bevat `network_hashrate` in H/s en `current_job_height`, het blok waaraan de pool werkt.

### Blokbeloning

**Current Block** toont de hoogte waaraan het netwerk werkt. Deel die door 8.640 en rond naar beneden af voor de maand. Zoek die op in het [blokbeloningsschema van Blocknet](/blog/nl/blocknet-block-reward-schedule/). De kolom Reward op de [blokkenpagina](/blocks) toont de volledige beloning van elk poolblok als snelle kruiscontrole.

## Een rekenvoorbeeld van 25 oktober 2026

<Note>
Dit is een momentopname van https://bntpool.com/api/stats op 25 oktober 2026 om 02:03 EDT. Netwerkhashrate en blokbeloning veranderen. Vul daarom actuele dashboardwaarden in voordat je op de uitkomst vertrouwt.
</Note>

| Invoer | Waarde op dat moment |
|---|---|
| Huidig blok | 69.020 (maand 7, want 69.020 ÷ 8.640 = 7,99) |
| Blokbeloning | 467,67 BNT |
| Netwerkhashrate | 62.329 H/s (getoond als 62,33 KH/s) |
| Hashrate bntpool.com | 5.240 H/s |

Neem de AMD EPYC Zen 5-server uit de benchmarks van seine met 49,4 H/s:

1. Netwerkaandeel: 49,4 ÷ 62.329 = 0,0793%.
2. Verwachte blokken per dag: 0,000793 × 288 = 0,2283.
3. Verwachte BNT per dag: 0,2283 × 467,67 = circa 106,8 BNT.

Maand 8 begint op hoogte 69.120, ongeveer 100 blokken na de momentopname, en betaalt 439,46 BNT per blok. Bij gelijke netwerkhashrate wordt de schatting voor dezelfde server 0,2283 × 439,46 = circa 100,3 BNT per dag. Daarom is het goed de beloning bij elke nieuwe maand te controleren.

Dezelfde berekening bij deze netwerkhashrate voor het beste gemeten resultaat per hardwaregeneratie:

| Hardware | Threads | Hashrate | Netwerkaandeel | BNT/dag bij 467,67 | BNT/dag bij 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Arm-server (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

Het getal van jouw machine telt. Laat seine een tijd draaien en gebruik de stabiele hashrate in plaats van de tabel. [Blocknet-mininghardware en hashrate](https://seineminer.com/nl/blog/blocknet-mining-hardware-hashrate/) legt uit waarom bandbreedte en RAM bepalend zijn. Volledige resultaten staan op de [prestatiepagina van seine](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## Hoe verandert minen op bntpool.com de schatting?

Je verwachte aandeel blijft op de pool gelijk. Alleen de ontvangst verandert. bntpool.com gebruikt PPLNS: elke beloning wordt verdeeld over shares uit de zes uur ervoor, naar hun moeilijkheid. [Hoe PPLNS-uitbetalingen werken](/blog/nl/how-pplns-payouts-work/) legt die verdeling uit.

Je kunt ook vanaf de pool rekenen en krijgt hetzelfde antwoord. De 5.240 H/s van de pool was op dat moment 8,4% van het netwerk. Hij kon dus 288 × 5.240 ÷ 62.329 = circa 24,2 blokken per dag verwachten, ongeveer één per uur. Dit staat als **Expected Block Time** in Round Progress. Een server van 49,4 H/s is 0,94% van de pool. Dat aandeel van 24,2 blokken à 467,67 BNT is opnieuw ongeveer 106,8 BNT per dag. De poolhashrate valt uit de berekening weg.

Het verschil is timing. Solo zou deze Zen 5-server ongeveer elke 4,4 dagen één heel blok verwachten. [Pool versus solo voor Blocknet](/blog/nl/pool-vs-solo-mining-blocknet/) vergelijkt dit uitgebreider. Op bntpool.com krijgt hij een deel van elk van de circa 24 dagelijkse poolblokken, zodat het saldo dag en nacht in kleine stappen groeit. De poolkosten en ontwikkelaarsvergoeding van seine gaan van het brutobedrag uit de formule af.

Twee pooldetails bepalen je eerste dag:

- **Het venster vult zich in zes uur.** Het aandeel van een nieuwe miner groeit terwijl zijn shares het PPLNS-venster vullen. Daarna blijft het volledig zolang hij doorgaat.
- **Beloningen rijpen vóór betaling.** Na 60 bevestigingen, circa vijf uur, gaat de beloning naar Confirmed Rewards. Vanaf 0,1 BNT stuurt de pool je saldo automatisch.

## Hoe vergelijk je de schatting met echte resultaten?

De schatting is een gemiddelde. Afzonderlijke dagen bewegen eromheen doordat de luck per ronde varieert. Over veel rondes komt effort gemiddeld rond 100% uit. Een week doorlopend minen is daarom geschikt om te vergelijken. Noteer aan het begin en eind de som van Confirmed Rewards en Paid Balance op [My Stats](/stats). De stijging is wat de pool heeft bijgeschreven uit blokken die die week rijpten. Vergelijk dit met je schatting voor dezelfde dagen, met de bijbehorende beloning en netwerkhashrate.

Bereken opnieuw wanneer een invoer verandert:

- **Bij elke nieuwe maand**, wanneer de beloning daalt. Maandgrenzen vallen elke 8.640 blokken, ongeveer 30 dagen.
- **Wanneer de netwerkhashrate verandert** doordat miners aansluiten.
- **Wanneer jouw hashrate verandert**, na een extra machine, het reserveren van huge pages op Linux of een NVIDIA-GPU die seine naast de CPU gebruikt.

<Note>
Deze hashrates gelden voor de huidige Argon2id-proof-of-work. Volgens [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) plant Blocknet rond 16 februari 2027 Faraday, die de proof of work moet veranderen. Houd seine actueel: het controleert bij het starten en elke 12 uur op nieuwe releases.
</Note>

<CTA target="seine" title="Meet je eigen hashrate" text="Download seine, laat het enkele minuten afstemmen en gebruik je hashrate met de actuele dashboardwaarden in de formule." />
