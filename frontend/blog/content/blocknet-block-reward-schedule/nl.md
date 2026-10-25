---
title: 'Het blokbeloningsschema van Blocknet'
description: 'De Blocknet-beloning per maand: van 723,25 BNT bij de start naar permanent 2,0 BNT vanaf maand 48, met 288 blokken per dag en 98,6 miljoen BNT vóór de staart.'
tags: ['Blocknet', 'Blokbeloning', 'Uitgifte', 'Mining']
sourceHash: abea18c2c21b
translation: draft
faq:
  - q: 'Wat is de huidige blokbeloning van Blocknet?'
    a: 'Dat hangt af van de maand van de keten: de beloning daalt elke 8.640 blokken. Op 25 oktober 2026 zat de keten in maand 7 met 467,67 BNT per blok. Maand 8 begint op hoogte 69.120 met 439,46 BNT. Zoek de huidige hoogte op het dashboard van bntpool.com en vergelijk die met de tabel.'
  - q: 'Hoeveel Blocknet-blokken worden per dag gemined?'
    a: 'Blocknet mikt op één blok per vijf minuten: 288 per dag en 8.640 per maand van 30 dagen. De moeilijkheid wordt elk blok aangepast met een gewogen gemiddelde van de laatste 60 bloktijden. Daardoor blijft het gemiddelde dicht bij vijf minuten wanneer miners aansluiten of vertrekken.'
  - q: 'Wat is de totale voorraad Blocknet?'
    a: 'Vóór de staart vanaf maand 48 worden ongeveer 98,56 miljoen BNT gemined. De code en officiële site ronden dat af op ongeveer 100 miljoen. Daarna betaalt elk blok voorgoed 2,0 BNT, goed voor 576 BNT per dag of circa 210.240 BNT per jaar. De voorraad blijft dus langzaam groeien.'
  - q: 'Wanneer begint de permanente uitgifte van Blocknet?'
    a: 'De staart begint in maand 48, op blokhoogte 414.720. Dat zijn 48 maanden van 30 dagen, of 1.440 dagen met blokken van vijf minuten na de start, ongeveer vier jaar. De precieze kalenderdatum hangt van echte bloktijden af. Vanaf die hoogte betaalt elk blok 2,0 BNT.'
  - q: 'Wanneer kun je geminede Blocknet-munten uitgeven?'
    a: 'Een blokbeloning is na 60 blokken besteedbaar, ongeveer vijf uur bij de bloktijd van vijf minuten. Dit heet coinbase maturity. Ook bntpool.com wacht 60 blokken voordat een beloning wordt uitbetaald, zodat alleen beloningen van gerijpte blokken worden verstuurd.'
---

Deze pagina vermeldt de Blocknet-blokbeloning voor elke maand van het uitgifteschema, legt de berekening uit en laat zien wat het schema voor jou als miner betekent.

## Wat is de blokbeloning van Blocknet?

<Answer>
De blokbeloning begon op 723,25 BNT en daalt eenmaal per maand van 30 dagen, oftewel 8.640 blokken, langs een vloeiende afnamecurve. Binnen een maand blijft de beloning gelijk. Vanaf maand 48, op hoogte 414.720, betaalt ieder blok een permanente staartbeloning van 2,0 BNT.
</Answer>

Blocknet mikt op één blok per vijf minuten, 300 seconden. Dat geeft 288 blokken per dag en 8.640 per maand van 30 dagen. De moeilijkheid wordt na ieder blok aangepast op basis van een gewogen gemiddelde van de laatste 60 bloktijden, zodat het tempo bij veranderende netwerkhashrate rond vijf minuten blijft.

De beloning hangt alleen af van de maand waarin de blokhoogte valt. Deel de hoogte door 8.640 en rond naar beneden af: hoogtes 0 tot 8.639 zijn maand 0, 8.640 tot 17.279 zijn maand 1, enzovoort.

## Hoe wordt de beloning berekend?

De daemon berekent elke maand met één formule:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

De eerste term is het deel boven de staart van 2,0 BNT. Dit krimpt jaarlijks met een factor e^-0,75 en is na twaalf maanden ongeveer 47% van de beginwaarde. Per maand is het deel boven de staart ongeveer 93,9% van de vorige maand. De beloning daalt dus in kleine, regelmatige stappen.

Het resultaat wordt naar beneden afgerond op hele atomaire eenheden: 1 BNT is 100.000.000 atomaire eenheden. De tabel toont beloningen afgerond op twee decimalen.

## Blocknet-beloning per maand

| Maand | Eerste blok | Beloning per blok (BNT) | Totaal gemined aan einde maand (miljoen BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| Vanaf 48 | 414.720 | 2,00 | +0,017 per maand |

Het genesisblok op hoogte 0 bevat geen beloning. Maand 0 heeft dus 8.639 betaalde blokken. Elke BNT komt uit deze blokbeloningen, zonder premine, zoals de pagina over [Blocknets eerlijke start](/blog/nl/blocknet-fair-launch/) uitlegt. Op 25 oktober 2026 werkte de pool aan blok 69.016, bijna aan het einde van maand 7. Het [dashboard van bntpool.com](/) toont de huidige hoogte om de juiste regel te vinden; de [blokkenpagina](/blocks) vermeldt de vondsten van de pool.

## Wat gebeurt er in maand 48?

<Answer>
Maand 48 begint op hoogte 414.720. Vanaf dat punt betaalt ieder Blocknet-blok voorgoed 2,0 BNT. Dit is de staartuitgifte, of tail emission. Zolang de keten draait, blijft elk blok een beloning geven. Miners worden dus altijd betaald voor het beveiligen van het netwerk.
</Answer>

De afnameformule geldt voor maanden 0 tot en met 47 en eindigt op 40,22 BNT per blok in maand 47. Vanaf maand 48 gebruikt de daemon rechtstreeks de staartwaarde: de beloning wordt 2,0 BNT en blijft daar. Bij 288 blokken per dag voegt dit 576 BNT per dag toe, of circa 210.240 BNT per jaar.

Maand 48 begint na 1.440 dagen met blokken van vijf minuten, ongeveer vier jaar na de start. De kalenderdatum hangt af van werkelijke bloktijden; de blokhoogte is daarom het betrouwbare ijkpunt.

## Hoeveel BNT wordt vóór de staart gemined?

Alle blokken van hoogte 1 tot 414.719 bij elkaar leveren 98.561.905,08 BNT op, ongeveer 98,6 miljoen. De code en officiële site omschrijven het schema als circa 100 miljoen munten in vier jaar plus de staart. De exacte som ligt net onder dat ronde getal.

Het grootste deel wordt vroeg gemined. Per jaar van het schema:

| Maanden | Geminede BNT | Aandeel van het totaal vóór de staart |
|---|---|---|
| 0-11 (jaar 1) | 54,48 miljoen | 55,3% |
| 12-23 (jaar 2) | 25,84 miljoen | 26,2% |
| 24-35 (jaar 3) | 12,32 miljoen | 12,5% |
| 36-47 (jaar 4) | 5,93 miljoen | 6,0% |

## Wanneer kun je geminede BNT uitgeven?

Een blokbeloning is besteedbaar zodra er 60 nieuwe blokken bovenop zijn gebouwd, ongeveer vijf uur later. Deze regel heet coinbase maturity. bntpool.com wacht dezelfde 60 blokken vóór uitbetaling. De [uitbetalingspagina](/payouts) toont wat al is verstuurd.

## Wat betekent het schema voor miners?

In eerdere maanden zijn de beloningen groter. Een blok in maand 12 betaalt 342,69 BNT, ongeveer 47% van de 723,25 BNT in maand 0. In maand 24 is dat 162,93 BNT. Elke nieuwe maand betaalt minder per blok dan de vorige, totdat de staart begint.

Jouw aandeel volgt je aandeel in de netwerkhashrate. Bij solo is het verwachte aantal blokken per dag 288 × (jouw hashrate ÷ netwerkhashrate), met voor elk blok de beloning van zijn maand. Bij een pool geldt dezelfde berekening voor de poolhashrate. De beloning wordt verdeeld onder de miners die werk hebben bijgedragen. Je verdient zo aan elk gevonden poolblok in plaats van op een volledig eigen blok te wachten. Het [dashboard van bntpool.com](/) toont de netwerk- en poolhashrate. De [statistiekenpagina](/stats) toont jouw hashrate zodra je mint.

Om nu te beginnen:

1. Download seine van [seineminer.com](https://seineminer.com/nl/#download). Het werkt op Windows, macOS en Linux en stemt zichzelf op je hardware af.
2. Start het en plak je Blocknet-adres of `@name`.
3. Accepteer de standaardpool, `stratum+tcp://bntpool.com:3333`.

De [gids voor minen op bntpool.com](/blog/nl/mine-blocknet-on-bntpool/) behandelt de poolstappen, [Get Started](/start) vermeldt de instellingen en de [mininggids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) loopt de installatie stap voor stap door. Zie voor prestaties van verschillende processors [Blocknet-mininghardware en hashrate](https://seineminer.com/nl/blog/blocknet-mining-hardware-hashrate/).

<Note>
De cijfers komen uit de beloningscode van de Blocknet-daemon, gecontroleerd op 25 oktober 2026. Volgens [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) plant Blocknet de netwerkupgrade Faraday rond 16 februari 2027. Houd seine actueel: het controleert bij het starten en elke 12 uur op nieuwe releases.
</Note>

<CTA target="seine" title="Mine Blocknet met seine" text="seine is gratis voor Windows, macOS en Linux en mint standaard op bntpool.com." />
