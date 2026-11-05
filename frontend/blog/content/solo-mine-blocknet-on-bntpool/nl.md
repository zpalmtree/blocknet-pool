---
title: Solo Blocknet minen op bntpool.com
description: 'Mine Blocknet solo zonder eigen node. Zet solo: voor je adres in seine en bntpool.com betaalt je de volledige beloning van elk blok dat je vindt.'
tags: ['Mininggids', 'Solomining', 'bntpool', 'seine']
sourceHash: '623b1e0b356a'
translation: draft
faq:
  - q: 'Hoe mine ik Blocknet solo op bntpool.com?'
    a: 'Werk seine bij naar v0.3.1 of nieuwer en zet solo: voor je uitbetalingsadres, bijvoorbeeld ./seine --address solo:YOUR_ADDRESS. Seine maakt zoals gewoonlijk verbinding met bntpool.com, en de pool betaalt je via de normale uitbetalingen de volledige beloning van elk blok dat je rig vindt. Je hoeft geen node te draaien.'
  - q: 'Heb ik een eigen Blocknet-node nodig om solo te minen?'
    a: 'Nee. Met het voorvoegsel solo: draait bntpool.com de node, bouwt het de bloksjablonen en verstuurt het de blokken die jij vindt. Op je rig draait alleen seine. De daemonmodus van seine is de andere manier om solo te minen, tegen je eigen gesynchroniseerde node, en past bij miners die er al een draaien.'
  - q: 'Kan ik een Blocknet ID-naam gebruiken voor solo minen?'
    a: 'Ja. Gebruik solo:@name of solo:$name in plaats van solo:ADDRESS, en zet het op de opdrachtregel tussen aanhalingstekens zodat je shell het ongewijzigd doorgeeft. De pool zet de naam om naar het volledige adres, schrijft je soloblokken bij en betaalt ze naar dat adres uit zoals elke andere uitbetaling.'
  - q: 'Hoe schakel ik terug naar poolbeloningen?'
    a: 'Haal het voorvoegsel solo: van je adres en start seine opnieuw. Je rig deelt dan weer in de PPLNS-beloningen van elk blok dat de pool vindt. Elke rig kiest zijn modus bij het verbinden, dus met hetzelfde adres kun je sommige rigs solo laten minen en andere op gedeelde beloningen.'
---

Deze gids laat zien hoe je Blocknet solo minet op bntpool.com: voeg één voorvoegsel toe aan je adres in seine, en de pool betaalt je de volledige beloning van elk blok dat je rigs vinden. Je installeert of synchroniseert geen node. Je stelt het in, ziet hoe soloblokken worden uitbetaald en leert waar je ze op de pool volgt.

## Wat is solo minen op bntpool.com?

<Answer>
Solo minen op bntpool.com betaalt je alleen voor de blokken die je eigen rigs vinden, en elk daarvan betaalt de volledige blokbeloning naar je adres. De pool draait nog steeds de node, bouwt de bloksjablonen en verstuurt je blokken, dus je krijgt solobeloningen met dezelfde setup als bij poolminen. Je zet het aan door solo: voor je adres te zetten.
</Answer>

Bij normaal poolminen verdeelt bntpool.com elk blok over de miners van wie de shares in het PPLNS-venster van zes uur vallen, dus je verdient een deel van elk blok dat de pool vindt. Een solorig houdt in plaats daarvan de volledige beloning van zijn eigen blokken en verdient niets aan blokken die anderen vinden. Beide gebruiken dezelfde server, hetzelfde adres en dezelfde uitbetalingen. Alleen de login verandert.

Solo minen via de pool geeft je solobeloningen zonder het werk van een node:

- **Geen node om te draaien.** De node van de pool volgt de chain, levert verse sjablonen en verstuurt je opgeloste blokken.
- **Automatische uitbetalingen.** Je blokken worden naar je adres of Blocknet ID-naam uitbetaald zoals elke andere bntpool.com-uitbetaling.
- **Live statistieken.** Je rigs, hashrate en blokken verschijnen op de [statistiekenpagina](/stats) van de pool.
- **Eén voorvoegsel om te wisselen.** Haal `solo:` weg en start seine opnieuw om terug te gaan naar gedeelde beloningen.

De daemonmodus van seine is de andere manier om solo te minen, tegen je eigen gesynchroniseerde Blocknet-node. [Pool of solo minen voor Blocknet](/blog/nl/pool-vs-solo-mining-blocknet/) vergelijkt alle drie de opties en laat zien hoe vaak jouw hashrate een blok vindt.

## Hoe begin je met solo minen in seine?

<Answer>
Installeer seine v0.3.1 of nieuwer en start het met solo: voor je Blocknet-adres, bijvoorbeeld ./seine --address solo:YOUR_ADDRESS op macOS of Linux. Seine maakt verbinding met bntpool.com, zijn standaardpool, en het dashboard labelt je account als Solo. Je kunt ook solo: en je adres typen wanneer seine bij de eerste start om een uitbetalingsadres vraagt.
</Answer>

1. **Werk seine bij.** Sologins vereisen seine v0.3.1 of nieuwer. [Download seine](https://seineminer.com/nl/#download), of laat automatische updates het installeren.
2. **Start seine met het voorvoegsel.** Op macOS of Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Op Windows, in PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Seine minet standaard op bntpool.com. De [Get Started](/start)-pagina van de pool toont dezelfde opdracht met het pooladres voluit geschreven.
3. **Controleer het dashboard.** Het saldopaneel labelt je account als **Solo**, en het logboek toont de bevestiging van de pool dat je wordt betaald voor de blokken die dit adres vindt.

Het voorvoegsel werkt overal waar seine een adres aanneemt: de vraag bij de eerste start, de opgeslagen config, de lokale control-API en rigs achter de farmproxy. Seine slaat het op met je adres, dus latere starts blijven solo minen.

## Kun je solo minen met een Blocknet ID-naam?

Ja. Een Blocknet ID-naam werkt net als een volledig adres. Schrijf `solo:@name` of `solo:$name` en zet het tussen aanhalingstekens zodat je shell het ongewijzigd doorgeeft:

```sh
./seine --address 'solo:@yourname'
```

De pool zet de naam om via blocknet.id en betaalt je soloblokken uit aan het adres erachter. Om een rig een naam te geven, voeg je die met een punt aan een volledig adres toe, zoals in `solo:YOUR_ADDRESS.rig1`, of geef je `--pool-worker rig1` mee met elk adres of elke naam. Het voorvoegsel accepteert elke hoofdlettervorm, dus `SOLO:` werkt ook.

## Hoe worden soloblokken uitbetaald?

Wanneer je rig een blok vindt, registreert de pool het als soloblok. Zodra het 60 blokken diep is, na ongeveer vijf uur, schrijft de pool de volledige blokbeloning, min de poolfee, bij op je saldo. De fee is 0% per 4 november 2026. Je saldo wordt daarna via de normale uitbetalingen van de pool naar je adres gestuurd.

Uitbetalingen volgen dezelfde regels als bij poolminen. Elk blok wacht op 60 bevestigingen, zodat een korte chain-reorganisatie een al uitbetaalde beloning niet ongedaan kan maken; [blokbevestigingen en miningbeloningen](/blog/nl/block-confirmations-mining-rewards/) legt uit waarom. Het `/api/info`-endpoint van de pool meldde op 4 november 2026 een minimale uitbetaling van 1 BNT, en één Blocknet-blokbeloning ligt daar ver boven, dus een bevestigd soloblok wordt uitbetaald volgens het normale uitbetalingsschema van de pool. De [uitbetalingenpagina](/payouts) toont elke betaling die de pool verstuurt.

Als je minet naar een stortingsadres van een exchange met een uitbetalingsmemo, werkt solo op dezelfde manier. De memo en zijn eigen minimale uitbetaling gelden voor je soloblokken net zoals voor gedeelde beloningen.

Het werk van je rig telt nog steeds mee voor de hashrate, ronde-effort en luck van de pool, en een soloblok beëindigt de ronde van de pool zoals elk ander blok. Soloshares komen nooit in het PPLNS-venster, dus andere miners verdienen niets aan jouw blokken en jij niets aan die van hen.

## Hoe vaak vindt een solorig een blok?

Blocknet mikt op één blok per vijf minuten, oftewel 288 blokken per dag. Een solorig vindt daarvan gemiddeld 288 × (jouw hashrate ÷ netwerkhashrate) per dag. Lees de netwerkhashrate af op de [voorpagina van bntpool.com](/) en je eigen hashrate op het dashboard van seine, en deel één dag door de uitkomst voor de gemiddelde tijd tussen je blokken.

Solobeloningen komen als hele blokken, dus ze passen bij miners van wie de hashrate vaak blokken vindt: grote farms, serverparken en iedereen met een groot aandeel in het netwerk. Kleinere rigs verdienen gemiddeld hetzelfde met gedeelde PPLNS-beloningen, uitbetaald in regelmatige delen van elk blok dat de pool vindt. [Pool of solo minen voor Blocknet](/blog/nl/pool-vs-solo-mining-blocknet/) werkt voorbeelden uit bij drie hashrates.

## Waar zie je je solorigs en blokken?

Zoek je adres op de [statistiekenpagina](/stats). Je kunt het plakken met of zonder het voorvoegsel `solo:`. De pagina markeert dan je solowerk:

- **Workers** toont elke rig met een Type-kolom die **Solo** aangeeft voor rigs die soloshares stuurden in het hashratevenster.
- **Estimated rewards** markeert je soloblokken terwijl ze op bevestigingen wachten.
- **Blocks** en **Luck** tonen vóór de status van elk blok een Type-kolom, Pool of Solo. De [blokkenpagina](/blocks) toont elk blok dat de pool heeft gevonden, nieuwste eerst.

[Je miner volgen op bntpool.com](/blog/nl/track-your-miner-on-bntpool/) loopt de rest van de statistiekenpagina door.

## Kun je solo- en poolrigs combineren?

Ja. Elke rig kiest zijn modus bij het verbinden, dus één adres kan tegelijk solorigs en poolrigs draaien. Een farm kan solo minen op zijn grootste servers en desktops op gedeelde beloningen houden, allemaal uitbetaald naar één saldo. De pool houdt de twee soorten werk gescheiden: een share die op een soloverbinding is gevonden, telt alleen voor de soloblokken van dat adres.

Om een rig terug te zetten, haal je `solo:` van zijn adres in je opdracht of opgeslagen config en start je seine opnieuw. Shares die de rig al in solomodus stuurde, blijven solo, en nieuwe shares komen meteen in het PPLNS-venster.

<CTA target="pool" title="Solo minen op bntpool.com" text="Voeg solo: toe aan je adres in seine en houd de volledige beloning van elk blok dat je rigs vinden, zonder node om te draaien." />
