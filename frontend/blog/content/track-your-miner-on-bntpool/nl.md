---
title: 'Je miner volgen op bntpool.com'
description: 'Zoek je adres of @name op bntpool.com voor hashrate, shares, saldo en betalingen. Volg blokken, luck en uptime of gebruik de openbare API in je scripts.'
tags: ['bntpool', 'Miningstatistieken', 'Pool-API', 'Blocknet']
sourceHash: bac503690f3a
translation: draft
faq:
  - q: 'Waarom verschilt mijn hashrate op bntpool.com van seine?'
    a: 'Seine telt elke berekende hash. De pool schat je hashrate uit de moeilijkheid van geaccepteerde shares, gemiddeld over het laatste uur. Shares komen willekeurig binnen, waardoor de poolwaarde sterker beweegt en met de tijd stabiliseert. Vergelijk de 24-uursgrafiek op My Stats met het gemiddelde van seine.'
  - q: 'Kan ik mijn statistieken opzoeken met een @name?'
    a: 'Ja. Typ je korte Blocknet ID-naam, zoals @name of $name, in het zoekveld van My Stats. De pagina zoekt die op via blocknet.id, toont je statistieken en zet het volledige gevonden adres in het zoekveld. Gebruik dat volledige adres voor API-aanroepen vanuit scripts.'
  - q: 'Vereist de API van bntpool.com een API-sleutel?'
    a: 'Nee. Endpoints voor poolstatistieken, blokken, betalingen, luck, status en individuele miners zijn openbare JSON via HTTPS. Een curl-opdracht of huisdashboard kan ze zonder registratie lezen. Bedragen staan in atomaire eenheden: deel door 100.000.000 voor BNT en vraag elke 30 tot 60 seconden gegevens op.'
  - q: 'Waarom toont My Stats nul Estimated Rewards?'
    a: 'Estimated Rewards verschijnen zodra de pool een blok vindt terwijl je shares in het uitbetalingsvenster zitten. Voor een nieuwe miner kan dat enkele rondes duren. Geaccepteerde shares en hashrate verschijnen eerst, dan geschatte beloningen. Die rijpen tot Confirmed Rewards en worden automatisch uitbetaald.'
---

Zodra seine op bntpool.com mint, toont de pool alles wat hij ziet: je hashrate, elke geaccepteerde share, je verdiende bedragen en elke betaling naar je adres. Deze gids bespreekt My Stats, de openbare poolpagina’s en de API voor scripts en huisdashboards.

Nog niet begonnen? De [installatiegids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) brengt je binnen enkele minuten naar je eerste shares.

## Hoe bekijk je je minerstatistieken op bntpool.com?

<Answer>
Open My Stats op bntpool.com/stats, plak je Blocknet-adres of @name en druk op Lookup. Je ziet hashrate, geaccepteerde en afgewezen shares, geschat, bevestigd en betaald saldo, een hashrategrafiek, één regel per rig en recente uitbetalingen. De pagina blijft zichzelf vernieuwen zolang de tab openstaat.
</Answer>

Voer op [My Stats](/stats) hetzelfde adres of dezelfde [Blocknet ID-naam](https://seineminer.com/nl/blog/blocknet-id-pay-by-name/) in als in seine. Een naam als `@name` of `$name` wordt via blocknet.id opgezocht; daarna toont het zoekveld het volledige adres. De browser onthoudt je laatste zoekopdracht, zodat je cijfers bij een volgend bezoek al klaarstaan.

## Wat toont My Stats?

De pagina is verdeeld in kaarten en tabellen, elk met een antwoord op een vraag over je miner.

### Saldo

- **Estimated Rewards:** je aandeel in gevonden poolblokken die nog rijpen.
- **Confirmed Rewards:** gerijpt saldo dat wacht op de volgende uitbetaling.
- **Paid Balance:** alles wat al naar je adres is gestuurd.

Bij lopende beloningen toont **Estimated From Recent Blocks** ieder blok met hoogte, geschat bedrag, resterende bevestigingen en vindmoment. Elke hoogte linkt naar de [Blocknet-explorer](https://explorer.blocknetcrypto.com), zodat je het zelf controleert. [Hoe PPLNS-uitbetalingen werken](/blog/nl/how-pplns-payouts-work/) legt de verdeling en het betaalmoment uit.

### Mining en shares

De kaarten onder **Mining** tonen je huidige **Hashrate**, hoeveel **Blocks Found** je eigen rigs voor de pool hebben opgelost en je begindatum, **Mining Since**. Daaronder staan **Shares Accepted**, **Shares Rejected**, **Reject Rate** en **Avg Difficulty**. Een gezonde rig levert doorlopend geaccepteerde shares en heeft een afwijzingspercentage dicht bij nul.

**Hashrate History** toont je hashrate over 1h, 24h, 7d of 30d. De langere perioden zijn het geschiktst om te zien of een rig de hele nacht draaide.

### Eén regel per rig

Geef bij meerdere machines elke rig een eigen naam. De pool vermeldt ze apart met hashrate, geaccepteerde en afgewezen shares en het tijdstip van de laatste share. Seine kiest bij de eerste start `seine-` gevolgd door vier cijfers. Een eigen naam geef je op de opdrachtregel:

```sh
./seine --pool-worker rig-01
```

Een rig met een oudere seine-versie krijgt een badge **update available**, met een link naar de [releasepagina van seine](https://github.com/zpalmtree/seine/releases). Zo zie je snel welke machine je nog moet bijwerken.

### Uitbetalingen en recente shares

**Recent Payouts** vermeldt elke betaling aan je adres, inclusief bedrag, explorerlink en status queued, unconfirmed of confirmed. **Recent Shares** toont de laatste 20 shares met taak, rig, moeilijkheid en status. Dit is de snelste controle of een pas gestarte machine de pool bereikt.

## Waarom verschilt de poolhashrate van die in seine?

Seine telt elke hash. De pool ziet alleen ingestuurde shares en schat je hashrate uit de totale moeilijkheid van geaccepteerde shares in het laatste uur. Elke share is willekeurig. bntpool.com mikt met de moeilijkheid per verbinding op ongeveer 10 shares per 5 minuten, dus korte perioden bevatten ruis.

Twee gewoonten maken de cijfers vergelijkbaar:

- Vergelijk gemiddelden: de 24h-grafiek op My Stats met het seine-gemiddelde over dezelfde periode.
- Geef een nieuwe rig tijd. De pool middelt aanvankelijk alleen over de minuten dat hij draait. De eerste waarde rust dus op enkele shares; na een uur geldt het volledige uurvenster.

Recent Shares toont meteen dat shares aankomen, nog voordat de hashrate stabiel is.

## Welke openbare pagina’s tonen de hele pool?

Deze pagina’s vereisen geen adreszoekopdracht en tonen openlijk wat de pool doet. Het [dashboard](/) verzamelt verbonden miners, pool- en netwerkhashrate, gevonden blokken en gemiddelde luck.

### Blokken

[Blocks](/blocks) vermeldt alle gevonden blokken, nieuwste eerst, met hoogte en explorerlink, beloning, effort, rondetijd, status en vindmoment. Filter op Confirmed, Pending of Orphaned om de status te bekijken.

### Uitbetalingen

[Payouts](/payouts) toont elke betaalbatch met totaalbedrag, aantal betaalde miners, transactielinks en status. Na uitzending is de batch unconfirmed; kort na de eerste blokbevestiging wordt hij confirmed. Je kunt de betaling zo volgen aan poolkant én in de explorer.

### Luck

[Luck](/luck) toont de effort per ronde: geaccepteerd sharewerk gedeeld door de blokmoeilijkheid. 100% betekent precies de verwachte hoeveelheid werk; lager betekent eerder gevonden. Losse rondes kunnen sterk in beide richtingen schommelen. Let daarom op **Average Luck** op het dashboard. **View All** bij **Pool Luck History** brengt je naar deze pagina.

### Status

[Status](/status) is de gezondheidspagina van de pool. Ze toont of pool, openbare HTTP, API, Stratum, database en daemon online zijn, hoe vers de huidige taaktemplate is, de ketenhoogte, uptime over 10 minuten, 6 uur, 24 uur en 7 dagen, plus incidentgeschiedenis. Uptime wordt dubbel gemeten: door een monitor op de poolserver en een externe probe op Cloudflare.

## Hoe gebruik je de API van bntpool.com?

<Answer>
De API biedt openbare JSON via HTTPS zonder sleutel. Gebruik https://bntpool.com/api/miner/ gevolgd door je adres voor hashrate, rigs, shares en betalingen. Voeg /balance toe voor je saldi en gebruik /api/stats voor pool- en netwerkhashrate. Bedragen zijn atomaire eenheden: deel door 100.000.000 voor BNT.
</Answer>

Alle genoemde pagina’s lezen deze endpoints. Wat je op de site ziet, kun je dus zelf ophalen:

| Endpoint | Resultaat |
|---|---|
| `/api/stats` | Verbonden miners, poolhashrate, gevonden blokken, netwerkhashrate, huidige hoogte |
| `/api/stats/history?range=24h` | Poolhashrate door de tijd (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Huidige ronde, gemiddelde effort, recente rondes |
| `/api/info` | Poolinstellingen: Stratum-poort, uitbetalingssysteem en venster, minimum |
| `/api/blocks?status=confirmed` | Gevonden blokken, te filteren op status |
| `/api/payouts/recent` | Betaalbatches met transactiehashes |
| `/api/luck` | Effort en duur per ronde |
| `/api/status` | Gezondheid, uptimevensters, incidenten |
| `/api/miner/{address}` | Jouw hashrate, totalen, rigs, laatste shares en betalingen |
| `/api/miner/{address}/balance` | Geschat, bevestigd en betaald saldo |
| `/api/miner/{address}/hashrate?range=24h` | Jouw hashrategeschiedenis |

Lijstendpoints accepteren `limit`, standaard 25 en maximaal 200, plus `offset`. Ze antwoorden met `items` en `total`. Minerendpoints gebruiken een volledig adres. Kopieer dat na een @name-zoekopdracht uit het zoekveld van My Stats.

Om je miner vanuit een terminal te lezen:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Met `jq` haal je afzonderlijke waarden op, zoals hashrate en geaccepteerde shares, of je betaalde totaal in BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Toon je miner op een huisdashboard

Hetzelfde endpoint werkt als sensor in een huisdashboard. De RESTful-integratie van Home Assistant leest het met enkele regels in `configuration.yaml`:

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

Elke 30 tot 60 seconden pollen is voldoende: hashrate is een uurgemiddelde en My Stats vernieuwt de minergegevens zelf elke 30 seconden. De openbare API begrenst verzoeken per client. Krijgt een script HTTP 429, dan moet het wachten volgens de `Retry-After`-header voordat het opnieuw probeert.

## En het dashboard van seine zelf?

De pool toont wat aan je is toegeschreven. Voor de machine zelf toont het terminaldashboard van seine live hashrate en shares. Seine kan ook een lokale control-API aanbieden met REST, een server-sent-eventstream en Prometheus-metrieken op `/metrics`, standaard gebonden aan `127.0.0.1:9977`. De [control-API-gids](https://github.com/zpalmtree/seine/blob/master/docs/API.md) behandelt opties en endpoints. Veel miners gebruiken beide: seine-metrieken per machine en de pool-API voor saldi en betalingen.

Nieuw met seine? [Download het](https://seineminer.com/nl/#download), gebruik de standaardpool en je adres verschijnt binnen enkele minuten na je eerste geaccepteerde share op My Stats.

<CTA target="pool" title="Begin met minen en volg het live" text="Sluit binnen enkele minuten aan op bntpool.com en volg je hashrate, shares en saldo op My Stats." />
