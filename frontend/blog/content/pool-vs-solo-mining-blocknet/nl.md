---
title: 'Blocknet minen: pool of solo?'
description: 'Kies pool- of solomining voor Blocknet. Bereken je verwachte blokken per dag, vergelijk gedeelde en solobeloningen op bntpool.com en stel solo in.'
tags: ['Mininggids', 'Solomining', 'bntpool', 'PPLNS']
sourceHash: 'aecc3e8f992f'
translation: draft
faq:
  - q: 'Hoe vaak zou ik solo een Blocknet-blok vinden?'
    a: 'Gemiddeld vind je 288 × (jouw hashrate ÷ netwerkhashrate) blokken per dag, want Blocknet mikt op één blok per vijf minuten. Met 50 H/s op een netwerk van 50.000 H/s is dat 0,288 per dag, ongeveer één blok per drieënhalve dag. De actuele netwerkhashrate staat op het dashboard van bntpool.com.'
  - q: 'Moet ik een Blocknet-node draaien om op bntpool.com te minen?'
    a: 'Nee. De pool draait zijn eigen node, bouwt de bloktemplates en dient gevonden blokken in. Je hebt alleen seine en een Blocknet-adres of Blocknet ID-naam nodig. Start seine, plak je adres en accepteer de standaardpool. De pool betaalt je aandeel in elk blok aan dat adres.'
  - q: 'Kan seine Blocknet solo minen?'
    a: 'Ja, op twee manieren. Zet solo: voor je adres, zoals in ./seine --address solo:YOUR_ADDRESS, en bntpool.com betaalt je de volledige beloning van elk blok dat je vindt, zonder node om te draaien. Of start seine met --mode daemon om op je eigen gesynchroniseerde Blocknet-node te minen.'
  - q: 'Kan ik later wisselen tussen pool en solo?'
    a: 'Ja. Elke optie gebruikt dezelfde seine-download. Voeg het voorvoegsel solo: toe aan je adres of haal het weg, of voeg --mode daemon toe voor je eigen node, en herstart seine wanneer je wilt wisselen. Shares die je al voor gedeelde beloningen stuurde, blijven in het PPLNS-venster van zes uur en blijven opleveren zolang ze daarin zitten.'
---

Deze gids helpt je kiezen tussen gedeelde poolbeloningen en solomining voor Blocknet, solo via bntpool.com of op je eigen node. Je berekent hoeveel blokken je hashrate zelf vindt, ziet hoe de pool dat omzet in regelmatige uitbetalingen en leert elke optie met seine instellen.

## Moet je Blocknet in een pool of solo minen?

<Answer>
Beide volgen dezelfde berekening: je aandeel in de netwerkhashrate bepaalt je aandeel in de blokken. Op bntpool.com verdien je een deel van elk gevonden poolblok, waardoor uitbetalingen regelmatig komen terwijl de pool blokken vindt. Solo behoud je de volledige beloning van elk eigen blok, passend bij grote opstellingen. bntpool.com biedt allebei.
</Answer>

Voor de meeste miners is bntpool.com een eenvoudige start. Het is de standaardpool van seine: behalve je adres hoef je niets in te stellen en je hoeft geen node te draaien of synchroniseren. Solo wordt ook volledig ondersteund. Zet `solo:` voor je adres en bntpool.com betaalt je de volledige beloning van elk blok dat je rigs vinden, zonder node om te draaien. Seine heeft daarnaast een daemonmodus die rechtstreeks op je eigen Blocknet-node mint. Hieronder zie je welke optie bij je hardware past.

## Hoeveel blokken zou je solo vinden?

<Answer>
Blocknet mikt op één blok per vijf minuten, oftewel 288 per dag. Solo kun je dagelijks 288 × (jouw hashrate ÷ netwerkhashrate) blokken verwachten. Deel één dag door dat aantal voor de gemiddelde tijd tussen je blokken. De actuele netwerkhashrate staat op het dashboard van bntpool.com.
</Answer>

De formule werkt doordat Blocknet zijn moeilijkheid afstemt op gemiddeld één netwerkblok per 300 seconden. De blokmoeilijkheid is ongeveer het aantal hashes dat nodig wordt geacht voordat één slaagt. Elke hash heeft dezelfde kans. Jouw aandeel in alle netwerkhashes is dus je verwachte aandeel in de blokken.

Je hebt twee getallen nodig:

- **Jouw hashrate.** Seine toont die op zijn dashboard tijdens het minen. De [statistiekenpagina](/stats) van de pool toont hem nadat je daar hebt gemined.
- **De netwerkhashrate.** Het [dashboard van bntpool.com](/) toont die in Network Hashrate naast Pool Hashrate. Beide veranderen, dus controleer ze wanneer je rekent.

Elk gevonden blok betaalt de beloning voor die maand. Het [blokbeloningsschema](/blog/nl/blocknet-block-reward-schedule/) vermeldt de maandwaarden.

## Een rekenvoorbeeld

<Note>
Hieronder staan hypothetische ronde getallen om de berekening te tonen, geen actuele waarden. Gebruik de huidige netwerkhashrate van het dashboard voor je eigen schatting.
</Note>

Stel dat het netwerk op 50.000 H/s draait. Een rig van 50 H/s, ongeveer wat seine op één AMD EPYC Zen 5-server mat, bezit 0,1% van het netwerk. Dit vinden die rig en twee grotere opstellingen naar verwachting solo:

| Jouw hashrate (voorbeeld) | Netwerkaandeel | Verwachte blokken per dag | Gemiddelde tijd tussen blokken |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | circa 83 uur (3,5 dagen) |
| 500 H/s | 1% | 2,88 | circa 8,3 uur |
| 5.000 H/s | 10% | 28,8 | circa 50 minuten |

Zet de rig van 50 H/s nu op een pool met in totaal 5.000 H/s. De pool vindt ongeveer 28,8 blokken per dag, gemiddeld elke 50 minuten. De rig levert 1% van het poolwerk en verdient 1% van elk blok. Het getelde werk blijft hetzelfde: 0,288 blokken per dag. Alleen de timing verandert: een klein deel ongeveer elke 50 minuten, in plaats van een volledig blok om de paar dagen.

De tabel laat ook zien waarom solo bij grote opstellingen past. Met 5.000 H/s vindt een solominer even vaak blokken als een pool van dat formaat en heeft hij zelfstandig een regelmatige opbrengst.

Zie [hoe je Blocknet-miningbeloningen schat](/blog/nl/estimate-blocknet-mining-rewards/) om blokken per dag voor jouw hardware om te rekenen naar BNT, met een gedateerd voorbeeld uit echte poolgegevens.

## Waarom betaalt een pool regelmatiger?

bntpool.com combineert de hashrate van iedereen die erop mint en vindt dus veel vaker blokken dan één rig. De beloning wordt via PPLNS verdeeld onder de miners die werk hebben geleverd: de pool kijkt zes uur terug en verdeelt naar verhouding van de moeilijkheid van hun shares.

Voor jou betekent dit:

- **Opbrengst uit elk blok.** Zolang je shares in het venster zitten, verdien je aan elk poolblok. Volg ze op de [blokkenpagina](/blocks).
- **Automatische uitbetaling.** Zodra een blok 60 blokken diep ligt, gaat je aandeel naar het bevestigde saldo. Zodra het de minimale uitbetaling bereikt, 1 BNT per 4 november 2026, wordt het verstuurd. Betalingen staan op de [uitbetalingspagina](/payouts).
- **Continuïteit na herstarts.** Ingestuurde shares blijven zes uur meetellen. Een herstart of driverupdate reset je positie niet.
- **Alleen de miner draaien.** De pool draait de node, bouwt templates en dient blokken in. Je kunt met meerdere rigs naar één adres of Blocknet ID `@name` minen.

De [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) doorloopt de verdeling met een eigen rekenvoorbeeld. [Get Started](/start) toont de actuele uitbetalingsinstellingen.

## Waarom Blocknet solo minen?

Solomining betaalt je de volledige beloning van elk blok dat je vindt. Dat past het beste als je hashrate vaak blokken vindt. In het voorbeeld vindt een opstelling met 10% van het netwerk ongeveer elke 50 minuten een blok, dus even regelmatig als een pool.

Met seine kun je op twee manieren solo minen:

- **Solo op bntpool.com.** Zet `solo:` voor je adres. De pool draait de node, verstuurt je blokken en betaalt de volledige beloning van elk blok, min de poolfee, via de normale uitbetalingen naar je adres. Je kunt solo- en gedeelde rigs op één adres combineren.
- **Solo op je eigen node.** De daemonmodus van seine mint op de bloktemplates van je eigen node, en elk blok betaalt rechtstreeks aan je nodewallet. Draai je al een Blocknet-node voor je wallet of andere diensten, dan voeg je voor solo de miner daaraan toe.

Seine is ook voor de daemonmodus gebouwd. Het gebruikt de bloktemplates van de node. Is de node een template vergeten, bijvoorbeeld na een herstart, dan dient seine het volledige opgeloste blok opnieuw in, zodat het gevonden blok de keten nog bereikt.

## Hoe stel je solomining met seine in?

Om solo te minen op bntpool.com installeer je seine v0.3.1 of nieuwer en start je het met het voorvoegsel voor je adres:

```sh
./seine --address solo:YOUR_BLOCKNET_ADDRESS
```

Het dashboard labelt je account dan als **Solo**. [Solo Blocknet minen op bntpool.com](/blog/nl/solo-mine-blocknet-on-bntpool/) behandelt Blocknet ID-namen, rignamen, uitbetalingen en waar je soloblokken op de statistiekenpagina verschijnen.

Om solo te minen op je eigen node:

1. **Installeer en synchroniseer een Blocknet-node.** Met de `blocknet`-wrapper:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   De API-cookie komt in `~/.config/bnt/data/mainnet/api.cookie`. Om de eerste synchronisatie te verkorten publiceert bntpool.com `checkpoints.dat` op de [checkpoints-pagina](/checkpoints). [Een node synchroniseren met checkpoints](/blog/nl/blocknet-checkpoints-fast-sync/) legt uit hoe de node die gebruikt.
2. **Wacht tot synchronisatie klaar is.** De berichten `[sync] progress` bereiken 100%. Pas als de node de keten heeft ingehaald, levert hij bloktemplates.
3. **Start seine in daemonmodus.**

   ```sh
   ./seine --mode daemon
   ```

   Geef bij een aangepaste node-indeling zelf de API en cookie op:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

De [sologids](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) behandelt walletwachtwoorden en aangepaste opstellingen. De [mininggids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) behandelt downloaden en eerste shares. Je kunt [seine downloaden](https://seineminer.com/nl/#download) voor Windows, macOS of Linux.

## Welke optie kies je?

Gebruik je eigen hashrate en de huidige netwerkhashrate van het [dashboard](/) in de formule en bekijk de gemiddelde tijd tussen blokken.

- **Begin op bntpool.com** als je een desktop, laptop of enkele servers gebruikt, of regelmatig betaald wilt worden zonder eigen node. Start seine, plak je adres of `@name` en accepteer de standaardpool.
- **Mine solo op bntpool.com** als je opstelling een groot netwerkaandeel heeft en zelfstandig vaak blokken vindt. Zet `solo:` voor je adres en houd de node, statistieken en uitbetalingen van de pool.
- **Mine solo op je eigen node** als je die hashrate hebt en al een gesynchroniseerde node draait of dat wilt doen.

Seine maakt heroverwegen eenvoudig. Elke optie gebruikt dezelfde download. Je wisselt met één voorvoegsel of optie en een herstart wanneer je hardware groeit. Draai je lokaal een node terwijl je op de pool mint, dan toont seine het nodewalletsaldo naast het poolsaldo.

<Note>
Volgens [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) plant Blocknet rond 16 februari 2027 de netwerkupgrade Faraday, die de proof of work moet veranderen. Seine v0.3.1 is er klaar voor: poolminen schakelt vanzelf over op de nieuwe proof of work, en met automatische updates aan installeert seine nieuwere releases voor je.
</Note>

<CTA target="pool" title="Begin met minen op bntpool.com" text="Start seine, plak je adres en verdien een deel van elk poolblok, of voeg solo: toe en houd elk blok dat je vindt." />
