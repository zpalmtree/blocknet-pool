---
title: 'Wat is hashrate bij cryptomining?'
description: 'Hashrate is het aantal hashes dat een miner per seconde berekent. Lees over de eenheden, jouw hashrate, die van pool en netwerk, en waarom ze schommelen.'
tags: ['Hashrate', 'Miningbasis', 'Blocknet', 'bntpool']
sourceHash: 0bfdcdca348e
translation: draft
faq:
  - q: 'Welke hashrate haalt een computer bij Blocknet-mining?'
    a: 'De beste metingen van seine per hardwaregeneratie zijn 49,4 H/s voor AMD EPYC Zen 5, 35,7 H/s voor AMD EPYC Zen 4, 30,9 H/s voor een Apple M4 Max-laptop en 21,5 H/s voor een AWS Graviton4 Arm-server. Jouw resultaat hangt vooral af van geheugenbandbreedte en hoeveel threads van 2 GB in je RAM passen.'
  - q: 'Waarom wordt Blocknet-hashrate gemeten in H/s in plaats van MH/s of TH/s?'
    a: 'Elke Blocknet-hash is een Argon2id-berekening die ongeveer 2 GiB geheugen schrijft en terugleest. Zelfs een grote server doet daardoor maar tientallen hashes per seconde. Proofs of work met snelle hashes zoals SHA-256 bereiken veel grotere aantallen en gebruiken grotere eenheden. Vergelijk hashrates alleen binnen hetzelfde algoritme.'
  - q: 'Waarom toont de pool een andere hashrate dan mijn miner?'
    a: 'Je miner telt elke voltooide hash. De pool ziet alleen ingestuurde shares en schat je hashrate uit hun moeilijkheid over het laatste uur. Shares komen op willekeurige momenten, dus de poolwaarde beweegt sterker en stabiliseert met de tijd. Vergelijk de 24-uursgrafiek op My Stats met het gemiddelde van je miner.'
  - q: 'Betekent een hogere hashrate meer miningbeloningen?'
    a: 'Ja, naar verhouding. Elke hash heeft dezelfde kans op een blok. Je verwachte aandeel in de netwerkblokken is dus je aandeel in de netwerkhashrate. Een verdubbeling verdubbelt je verwachte beloningen; stijgende netwerkhashrate verkleint je aandeel. Op bntpool.com ontvang je dat aandeel als een deel van elk gevonden poolblok.'
---

Hashrate is het getal dat iedere miner, pool en blokexplorer als eerste toont. Deze gids legt uit wat het meet, hoe je eenheden vanaf H/s leest, hoe jouw hashrate zich verhoudt tot pool en netwerk en waarom elke waarde van minuut tot minuut beweegt. Blocknet en het dashboard van bntpool.com vormen het rekenvoorbeeld.

## Wat is hashrate?

<Answer>
Hashrate is het aantal hashes dat een miner per seconde berekent. Bij proof-of-work-mining is elke hash één poging om een blok op te lossen, met steeds dezelfde kleine kans op succes. Een hogere hashrate geeft meer pogingen per seconde. Je verwachte aandeel in de blokken groeit dus evenredig met je aandeel in de totale hashrate.
</Answer>

Een proof-of-work-keten vraagt miners een blokheader te vinden waarvan de hash onder een doel ligt. Er is geen kortere route: de miner verandert een getal in de header, de nonce, berekent de hash, vergelijkt die met het doel en herhaalt dit. Hashrate telt hoeveel van die pogingen de machine per seconde afrondt.

Bij Blocknet is één poging één Argon2id-hash. De nonce dient als password en de rest van de header als salt. De functie verwerkt 2 GiB geheugen voordat ze een antwoord van 32 bytes geeft. Seine en de pool rapporteren voltooide hashes per seconde: 1 H/s is dus één volledige Argon2id-berekening per seconde.

## Wat betekenen H/s, kH/s en MH/s?

Hashrate-eenheden gebruiken de gewone metrische voorvoegsels. Elke stap is duizendmaal de vorige:

| Eenheid | Hashes per seconde | Gebruikelijk gebruik |
|---|---|---|
| H/s | 1 | Eén computer op Blocknet |
| kH/s (KH/s) | 1.000 | Totalen van Blocknet-pool en netwerk |
| MH/s | 1.000.000 | Snellere proof-of-work-algoritmen |
| GH/s | 1.000.000.000 | Snelle hashes zoals SHA-256 |
| TH/s en hoger | 1.000.000.000.000 en meer | Netwerktotalen voor snelle hashes |

Het dashboard schrijft de duizendeenheid als KH/s en schakelt naar een hogere eenheid zodra een waarde 1.000 passeert. Zo betekent 44,44 KH/s 44.440 H/s. Zet bij het delen van twee hashrates beide eerst om naar H/s.

## Waarom zijn Blocknet-hashrates zo klein?

De proof of work van Blocknet is geheugenintensief. Elke hash schrijft ongeveer 2 GiB en leest ongeveer 2 GiB terug, veel meer dan in een processorcache past. De snelheid van het geheugensysteem bepaalt daardoor het tempo. Elke miningthread heeft eigen 2 GiB nodig; daarom bepaalt RAM hoeveel threads een machine kan draaien.

Dit zijn de beste resultaten die seine per hardwaregeneratie heeft gemeten:

| Hardware | Threads | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Arm-server (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Zelfs de Zen 5-server met 32 threads haalt ongeveer 1,5 hashes per thread per seconde. Snelle hash-algoritmen geven veel grotere getallen: volgens de Bitcoin-wiki werden Bitcoin-miners met dubbele SHA-256 al in 2014 in miljarden hashes per seconde beoordeeld. De cijfers vertegenwoordigen verschillende hoeveelheden werk per hash. Een hashrate heeft dus alleen betekenis naast resultaten van hetzelfde algoritme. [Argon2id-proof-of-work uitgelegd](https://seineminer.com/nl/blog/argon2id-proof-of-work-explained/) behandelt het algoritme diepgaand. De [hardwaregids voor Blocknet](https://seineminer.com/nl/blog/blocknet-mining-hardware-hashrate/) laat zien hoe bandbreedte en RAM deze prestaties bepalen.

## Jouw hashrate, die van de pool en die van het netwerk

Voor miners op een [pool](/blog/nl/what-is-a-mining-pool/) zijn drie hashrates van belang. Ze worden elk anders gemeten.

### Jouw hashrate

Seine telt elke voltooide hash en toont twee waarden: **Current**, je huidige snelheid, en **Average**, het gemiddelde over de sessie. Gebruik voor een stabiele indruk het gemiddelde nadat seine enkele minuten zijn threadaantal en CPU-kernel op echt miningwerk heeft afgestemd.

De pool ziet je hashes niet. Hij ziet alleen je shares: hashes die een eenvoudiger doel voor je verbinding halen. bntpool.com schat je hashrate uit de totale moeilijkheid van geaccepteerde shares in het laatste uur en toont die op [My Stats](/stats). [Je miner volgen op bntpool.com](/blog/nl/track-your-miner-on-bntpool/) bespreekt die pagina.

### Poolhashrate

**Pool Hashrate** telt het sharewerk van alle verbonden miners in hetzelfde uurvenster op. Het vertelt hoeveel netwerkwerk bntpool.com doet en daarmee hoe vaak de pool naar verwachting een blok vindt. Het dashboard toont dat als **Expected Block Time**.

### Netwerkhashrate

Niemand kan alle hashes in het netwerk tellen, dus netwerkhashrate wordt uit blokken afgeleid. Moeilijkheid is het verwachte aantal hashes om een blok te vinden. Blocknet mikt op één blok per 300 seconden.

## Hoe wordt netwerkhashrate berekend?

<Answer>
Netwerkhashrate wordt geschat uit moeilijkheid en bloktijden. Bij Blocknet is de moeilijkheid ongeveer het verwachte aantal hashes per blok. Met een doel van 300 seconden is de netwerkhashrate dus ruwweg moeilijkheid gedeeld door 300. Het dashboard van bntpool.com gebruikt het werkelijke werk en de tijdstempels van de tien recentste blokken.
</Answer>

Neem de actuele waarden van 25 oktober 2026 om 02:26 EDT. De [Blocknet-explorer](https://explorer.blocknetcrypto.com) toonde een volgende moeilijkheid van 16.943.147: het netwerk verwachtte ongeveer 16,9 miljoen hashes per blok. Gedeeld door 300 seconden is dat circa 56.477 H/s.

Tegelijk toonde het pooldashboard 44,44 KH/s, oftewel 44.440 H/s. Beide schatten hetzelfde vanuit een andere invalshoek. De moeilijkheidswaarde geeft de snelheid die precies blokken van vijf minuten oplevert. Het dashboard deelt het werkelijke werk van de laatste tien blokken door hun werkelijke tijd. Duren recente blokken langer dan vijf minuten, dan leest het dus lager. Blocknet past de moeilijkheid na ieder blok aan, zodat beide over tijd dicht bij elkaar blijven.

## Waarom schommelt hashrate?

Elk van de drie getallen beweegt om eigen redenen.

- **Je machine.** In de eerste minuten vergelijkt seine threadaantallen en kernels tijdens het minen en kiest de snelste. De snelheid stijgt tijdens het afstemmen. Daarna kunnen andere programma’s die geheugen of cores gebruiken een deel van de hashtijd innemen.
- **De poolweergave van jou.** Shares zijn willekeurige gebeurtenissen. bntpool.com mikt met de moeilijkheid per verbinding op circa 10 shares per vijf minuten, ongeveer 120 per uur. Een gelukkige of stille periode verschuift de schatting. Een pas verbonden rig wordt gemiddeld over zijn verstreken miningminuten tot het volle uur gevuld is.
- **Het netwerk.** Bloktijden zijn ook willekeurig en tien blokken is een kleine steekproef. Op 25 oktober 2026 toonde het dashboard ongeveer 50.057 H/s om 00:26 EDT, 62.329 H/s om 02:03 en 44.440 H/s om 02:26. Aansluitende en vertrekkende miners voegen echte veranderingen aan die ruis toe.

Gebruik gemiddelden voor beslissingen: het sessiegemiddelde van seine, de 24-uursgrafiek op My Stats en netwerkmetingen op meerdere momenten van de dag.

## Wat betekent je hashrate voor beloningen?

Omdat elke hash dezelfde kans heeft, is je aandeel in de netwerkhashrate je verwachte aandeel in blokken. Blocknet produceert 288 blokken per dag. Met 0,1% van het netwerk kun je op termijn dus circa 0,29 blokbeloningen per dag verwachten. [Hoe je Blocknet-miningbeloningen schat](/blog/nl/estimate-blocknet-mining-rewards/) rekent dit om naar BNT per dag met gegevens van het [dashboard](/).

Op bntpool.com ontvang je dat verwachte aandeel als een deel van elk poolblok, toegeschreven op basis van geaccepteerde shares. Beloningen komen daardoor dag en nacht in kleine stappen. Ben je nog niet begonnen, dan brengt de [installatiegids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) je in enkele minuten van download naar eerste shares.

<Note>
Deze hashrates gelden voor Blocknets huidige Argon2id-proof-of-work. Volgens [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) plant Blocknet rond 16 februari 2027 Faraday, een netwerkupgrade die de proof of work moet veranderen. Houd seine actueel: het controleert bij het starten en elke 12 uur op nieuwe releases.
</Note>

<CTA target="seine" title="Bekijk je eigen hashrate" text="Download seine, laat het enkele minuten afstemmen en bekijk je hashrate op het dashboard en op My Stats." />
