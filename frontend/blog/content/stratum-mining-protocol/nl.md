---
title: 'Wat is Stratum? Hoe miners met een pool praten'
description: 'Volg Stratum tussen seine en bntpool.com: aanmelden, miningtaken, sharedoelen, inzendingen en antwoorden, en begrijp de stappen van de verbinding van je miner.'
tags: ['Miningbasis', 'Stratum', 'Poolmining']
faq:
  - q: 'Wat doet Stratum bij poolmining?'
    a: 'Stratum draagt het gesprek tussen miner en pool: sessie-identificatie, werklevering, shares en antwoorden. De miner berekent hashes lokaal. De pool verbindt dit werk met zijn achterliggende node en houdt de administratie bij die nodig is voor zijn uitbetalingssysteem.'
  - q: 'Is een Stratum-adres hetzelfde als een webadres?'
    a: 'Ze dienen verschillende verbindingen. De website bntpool.com gebruikt HTTPS voor pagina''s en API-data; het gedocumenteerde miningadres is stratum+tcp://bntpool.com:3333. Vul dit in bij de poolinstelling van de miner. Alleen succesvol de website openen vormt nog geen miningverbinding.'
  - q: 'Betekent een geaccepteerde Stratum-share dat er is uitbetaald?'
    a: 'Nee. Een shareantwoord meldt het resultaat van een inzending binnen de sessie. Verificatie, een netwerkgeldig blok, beloningsrecht en afgeronde uitbetaling zijn aparte stappen. Volg geaccepteerde activiteit in My Stats en lees de uitbetalingsgids voor bevestigingen, PPLNS en de drempel.'
sourceHash: '1dba87acd7c0'
translation: draft
---

Volg een miningtaak van bntpool.com naar seine en een share terug naar de pool. Dit gesprek helpt verbindingsmeldingen, taakupdates en antwoorden begrijpen. De gids beschrijft bntpool.com's implementatie; gebruik de [poolinstallatiegids](/blog/nl/mine-blocknet-on-bntpool/) voor het endpoint en uitbetalingsadres.

## Wat is Stratum bij mining?

<Answer>
Stratum is de communicatielaag waarmee een miner poolwerk ontvangt en resultaten instuurt. Bij seine en bntpool.com draagt een blijvende TCP-verbinding JSON-berichten voor aanmelden, taken en shares. Je computer berekent hashes lokaal; de pool haalt bloktemplates van zijn node en verwerkt ingestuurd werk.
</Answer>

*Een miner stuurt aanmelding en shares naar de pool, ontvangt taken en antwoorden, en de pool wisselt templates en blokkandidaten met zijn node uit.*

## Het endpoint wijst de miningverbinding aan

Het gedocumenteerde endpoint is `stratum+tcp://bntpool.com:3333`. Het schema kiest het transport, de hostnaam identificeert de dienst en de poort kiest de mininglistener. De browserverbinding met `https://bntpool.com` dient een ander doel. Kopieer het volledige endpoint van [Get Started](/start) naar de poolinstelling voor de bedoelde verbinding.

Via de blijvende verbinding kunnen beide kanten berichten sturen wanneer werk beschikbaar is. bntpool gebruikt JSON gescheiden door nieuwe regels, met aanvraag-ID's die antwoorden koppelen. Dit beschrijft het formaat van deze pool; een miner moet dit, de mogelijkheden en Blocknets hashalgoritme ondersteunen. De [protocolbroncode](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) legt de huidige velden vast.

## Wat gebeurt er bij het aanmelden?

De client stuurt `login` met uitbetalingsadres, rignaam, protocolversie en ondersteunde mogelijkheden. De pool controleert dit en antwoordt met protocolinformatie en vereiste mogelijkheden. Daarmee staat de werkuitwisseling vast. TCP-verbinding is de eerste stap; geaccepteerde login toont dat ook het gesprek op toepassingsniveau is begonnen.

De rignaam onderscheidt één machine onder een adres. Gebruik verschillende namen voor meerdere rigs en behoud het gewenste uitbetalingsadres. Login vraagt het ontvangstadres, niet de herstelzin of privé-uitgavesleutel. Zodra de sessie is geaccepteerd en werk beschikbaar is, stuurt de pool `job` zodat het hashen kan beginnen.

## Welke informatie bevat een miningtaak?

Een taak bevat toewijzings- en template-ID's, headerdata, blokhoogte, sharedoel, netwerkdoel en een noncebereik. De nonce is de veranderende waarde tijdens het hashen. Het bereik definieert de zoekruimte van de sessie. De taak-ID koppelt een latere inzending aan oorspronkelijk toegewezen werk en voorwaarden.

Het sharedoel bepaalt welke resultaten als werkbewijs worden ingestuurd. Het netwerkdoel bepaalt mogelijke blokkandidaten. De meeste shares dienen de administratie zonder blok te worden. De [moeilijkheidsgids](/blog/nl/mining-difficulty-explained/) legt doelgrootte en verwacht werk uit; het protocol levert deze doelen bij de taak.

## Taken kunnen veranderen bij open verbinding

Een nieuwe taak betekent op zichzelf geen verbroken verbinding. De pool kan nieuw werk sturen bij een gewijzigde nodetemplate of een toewijzing met andere sharemoeilijkheid. Variabele moeilijkheid past de inzenddrempel aan het waargenomen werk aan. De miner volgt de nieuwe taak terwijl de verbinding berichten in beide richtingen blijft dragen.

Onderscheid taak-ID's en template-ID's in technische logs. Verschillende taken kunnen dezelfde template gebruiken met andere sessiedetails. Alleen hoogte beschrijft niet elke werkverandering. Controleer als operator of taken aankomen, hashing voortgaat en inzendingen antwoord krijgen. De [volggids](/blog/nl/track-your-miner-on-bntpool/) toont waar de activiteit staat.

## Wat bevat een share-inzending?

Vindt seine een passend resultaat, dan stuurt het `submit` met taak, nonce en opgegeven hash. De pool verwerkt de taakcontext en antwoordt met de aanvraag-ID. De opgegeven hash is onderdeel van het bericht; verificatie en blokvalidatie bepalen de verdere verwerking. Een blokkandidaat gaat naar de daemon voor validatie.

Het antwoord kan acceptatie, verificatiestatus en moeilijkheid bevatten. Lees deze apart in plaats van elk succesvol antwoord als blokvondst te zien. Administratie en uitbetaling gebeuren buiten deze uitwisseling. De [sharegids](/blog/nl/accepted-rejected-stale-shares/) verklaart inzendresultaten; de [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) verbindt geldig werk met beloningen en betalingen.

## Hoe helpt deze volgorde bij het controleren?

Lees de stappen op volgorde: endpoint bereikt, login geaccepteerd, taak ontvangen, hashing actief, inzendingen beantwoord. Na verbindingsverlies verbindt de client opnieuw en vormt een sessie voordat nieuw werk komt. Gebruik exacte logmelding en tijdstip om de stap te vinden. Een werkende website bewijst niet dat die computer de miningpoort bereikt.

Open [My Stats](/stats) voor het juiste adres en [poolstatus](/status) voor de dienst. Laat na start of herverbinding sharegebaseerde hashrate voldoende waarnemingen verzamelen. Houd protocol en uitbetalingstijd apart: login bereidt de sessie voor, taken leveren werk, shares melden resultaten en de administratie bepaalt volgende beloningsstappen.

<CTA target="pool" title="Verbind seine met bntpool.com" text="Gebruik de poolinstellingenpagina en volg taken, shares en je rig in My Stats." />
