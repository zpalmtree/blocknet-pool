---
title: 'Geaccepteerde, afgewezen en verouderde mining-shares'
description: 'Begrijp geaccepteerde, afgewezen en verouderde shares op bntpool.com, lees foutmeldingen en ontdek hoe sharemoeilijkheid je miningstatistieken beïnvloedt.'
tags: ['Mininggids', 'Shares', 'Problemen oplossen']
faq:
  - q: 'Betekent een geaccepteerde share dat ik een blok vond?'
    a: 'De pool heeft een resultaat volgens zijn shareverwerkingsregels aangenomen. Een blok moet een apart netwerkdoel halen en door de daemon worden geaccepteerd. Beloningen volgen blokvondst, validatie en uitbetaling; de bevestiging van een share is geen betalingsmelding.'
  - q: 'Waarom wijst de pool een verouderde share af?'
    a: 'Stale job betekent dat de inzending hoort bij een niet meer geldige toewijzing, bijvoorbeeld verlopen of gekoppeld aan een template voorbij de respijtperiode. Controleer nieuwe jobs en opnieuw geaccepteerde shares. Herhaalde meldingen vragen om controle van verbinding en minerversie.'
  - q: 'Verlaagt hogere sharemoeilijkheid mijn beloning?'
    a: 'Moeilijkheid verandert hoeveel werk een geldige share vertegenwoordigt en hoe vaak deze wordt verwacht. Vergelijk op moeilijkheid gewogen werk over gelijke perioden, niet alleen aantallen. De beloning hangt ook af van het PPLNS-venster, meetellend werk, blokvondsten en validatieregels.'
sourceHash: 5be830208e6c
translation: draft
---

Gebruik shareresultaten in het log om te beslissen of je doorgaat, de verbinding controleert of terugkerende afwijzingen onderzoekt. Deze gids verklaart bntpool-antwoorden en de moeilijkheid achter de tellers. Begin zo nodig met de [poolinstallatie](/blog/nl/mine-blocknet-on-bntpool/), of de [trackinggids](/blog/nl/track-your-miner-on-bntpool/#zero-hashrate) als een adres geen activiteit toont.

## Wat is een geaccepteerde mining-share?

<Answer>
Een share is een resultaat voor een door de pool toegewezen doel. Een geaccepteerd antwoord bevestigt opname in de shareverwerking en toont dat werk de pool bereikt. Blokvondst, shareverificatie en recht op uitbetaling zijn afzonderlijke stappen. Acceptatie alleen is dus nog geen betaling.
</Answer>

Tussen inzendingen berekent de miner veel hashes. De meeste halen het sharedoel niet en worden niet ingezonden. De acceptatieteller telt inzendingsresultaten, niet alle CPU- of GPU-hashes. De [hashrategids](/blog/nl/what-is-hashrate/) onderscheidt lokaal tellen van de poolschatting uit ingediend werk.

## Hoe verschillen shares van blokken?

De pool geeft je verbinding jobs en sharemoeilijkheid. Een resultaat dat het sharedoel haalt, toont werk op die moeilijkheid. Een blok moet daarnaast het netwerkdoel halen en daemoncontroles doorstaan. Een share vinden betekent niet dat een blok aan de keten is toegevoegd.

bntpool.com registreert geverifieerde en voorlopige shares. Een geaccepteerde inzending kan voorlopig zijn; uitbetalingsweging volgt validatie- en toelatingsregels. Gebruik acceptatie als verbindingssignaal en [Mijn statistieken](/stats) voor geregistreerd werk en beloningen. De [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) behandelt de stappen na een poolblok.

## Wat betekenen afgewezen en verouderde shares?

Een afgewezen inzending voldeed niet aan een controle. De reden helpt meer dan de teller alleen. Stale job is één afwijzingscategorie: de toewijzing is niet meer geldig. Vergelijk de exacte tekst met gelijktijdige gebeurtenissen zoals opnieuw verbinden of een nieuwe job.

| Melding | Betekenis en volgende controle |
|---|---|
| `stale job` | De toewijzing ontbreekt, is verlopen, vervangen voorbij de respijtperiode of gekoppeld aan een onbeschikbaar oud template. Controleer hervatting van nieuwe jobs en geaccepteerde shares. |
| `duplicate share` | Hetzelfde template en dezelfde nonce zijn al ingezonden. Zoek herhaalde inzendingen; hetzelfde werk telt niet nogmaals mee. |
| `low difficulty share` | De hash haalt het toegewezen sharedoel niet. Controleer bij herhaling minerversie en verwerking van jobs en moeilijkheid. |
| `nonce out of assigned range` | De nonce ligt buiten het toegewezen bereik. Controleer minerversie en aangepaste proxy’s of integraties. |

Een verouderde share gaat over toegewezen werk, een verweesd blok over de plaats van een blok in de keten. Houd sharelog en [bloklijst](/blocks) apart bij diagnose. Stale job alleen betekent niet dat de pool een verweesd blok vond.

## Waarom verandert sharemoeilijkheid?

<Answer>
Variabele moeilijkheid, vardiff, past het doel van een verbinding aan de waargenomen shareactiviteit aan. Hoger betekent doorgaans minder shares met meer werk per share; lager meer shares met minder werk. Vergelijk gewogen meetellend werk over dezelfde periode in plaats van alleen aantallen.
</Answer>

Rekenvoorbeeld: tien geldige shares met moeilijkheid 100 wegen samen evenveel als één met 1.000: beide totaal 1.000. Dit illustreert weging zonder beloning of exacte aankomsttijd te beloven. Shares komen onregelmatig en validatieregels bepalen welk werk meetelt.

Bij inzenden telt de moeilijkheid van de toewijzing. Vergelijk een ouder resultaat niet alleen met de laatst getoonde moeilijkheid. De pool bewaart toewijzingen voor controles. Normale aanpassing is geen fout; gemakkelijker shares afdwingen creëert geen extra rekenwerk.

## Hoe lees je een afwijzingspercentage?

Vergelijk geaccepteerde en afgewezen inzendingen binnen dezelfde periode en scope. De miner kan één sessie tellen, de pool een andere periode of meerdere apparaten. Herstarten verandert het lokale venster, maar wist de poolhistorie van je adres niet.

99 geaccepteerde en één afgewezen inzending geven 1% afwijzing naar aantal. Dat is niet automatisch het percentage verloren beloning: moeilijkheden en meetelregels kunnen verschillen. Bekijk redenen, timing en voortgaand geaccepteerd werk voordat je instellingen verandert.

## Wat doe je bij herhaalde afwijzingen?

Begin met één getroffen apparaat. Noteer versie, exacte melding, begintijd en of nieuwe jobs aankomen. Controleer [Status](/status) en vervolgens verbindingsonderbrekingen. Werk bij via de [officiële seine-releases](https://github.com/zpalmtree/seine/releases) en vergelijk na één wijziging tegelijk.

Een eigen proxy of integratie moet job, noncebereik en moeilijkheid bij doorsturen behouden. Blijft het probleem, deel dan een kort logfragment en apparaatdetails via het supportkanaal van het project. Verwijder geheimen en privégegevens; een herstelzin is nooit nodig voor sharediagnose.

## Hoe bevestig je dat mining weer werkt?

Zoek nieuwe jobs gevolgd door geaccepteerde shares. Open [Mijn statistieken](/stats) met hetzelfde ontvangstadres en controleer recente activiteit van het apparaat. Laat de hashrateschatting meer werk verzamelen voordat je gemiddelden vergelijkt. Volg daarna geschatte beloningen, saldo en uitbetalingen afzonderlijk via de PPLNS-gids.

<CTA target="pool" title="Verbind met bntpool.com" text="Volg de installatiegids, bevestig geaccepteerde shares en volg activiteit via je ontvangstadres." />
