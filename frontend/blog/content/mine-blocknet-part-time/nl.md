---
title: 'Kun je Blocknet in deeltijd minen?'
description: 'Mine Blocknet in korte sessies: volg shares door het PPLNS-venster van zes uur, keer terug met hetzelfde adres en controleer onbetaald saldo.'
tags: ['Mininggids', 'PPLNS', 'Planning']
faq:
  - q: 'Moet ik zes uur minen voordat mijn shares meetellen?'
    a: 'Nee. Geschikte shares kunnen zonder zes uur wachten deelnemen aan het beloningsvenster van een blok. Je aandeel hangt af van je naar moeilijkheid gewogen werk tegenover het totaal van de pool; er moet een blok worden gevonden om een blokbeloning toe te delen.'
  - q: 'Moet mijn miner verbonden blijven voor een bestaande betaling?'
    a: 'Een actieve miningverbinding is geen betalingsvoorwaarde. Bevestigde saldi worden volgens de poolregels behandeld, inclusief het huidige minimumbedrag. Verbreken van de verbinding wist het saldo niet en vraagt op zichzelf geen onmiddellijke betaling aan.'
  - q: 'Verjongt opnieuw verbinden mijn oude shares?'
    a: 'Nee. Bestaande shares behouden hun tijdstempel. Nieuw werk komt na terugkeer in de betreffende vensters; opnieuw verbinden vernieuwt oude shares niet en voegt geen werk toe voor offline uren. Controleer nieuwe geaccepteerde shares na herstart.'
sourceHash: '0a11c393062e'
translation: draft
---

Je kunt een Blocknet-miner op gekozen uren laten draaien en later terugkeren. Onderscheid je huidige verbinding, al ingediende shares en al toegewezen beloningen. Deze gids volgt een korte sessie zodat je met duidelijke verwachtingen stopt, herstart en je bntpool.com-account controleert.

## Kan een korte sessie poolbeloningen opleveren?

<Answer>
Ja. Geschikt werk uit een korte sessie kan deelnemen aan blokken waarvan het PPLNS-venster die shares bevat. Eerst zes uur volmaken is niet nodig. Beloningen hangen af van gevonden blokken en je gewogen aandeel in elk venster. Stoppen voegt geen nieuw werk toe; bestaande shares verouderen normaal en saldi volgen de betalingsregels.
</Answer>

*Een tijdlijn toont mining, een offline pauze en terugkeer, met afzonderlijke markeringen voor geschikte shares en bestaand saldo.*

## Onderscheid de verschillende tijdstappen

bntpool publiceert momenteel een PPLNS-venster van zes uur, 60 blokken voor uitbetaling en een minimum van 0,1 BNT op de [startpagina](/start). Dat zijn meegeteld werk, rijping van een blok en een saldodrempel. Het venster van zes uur is geen timer die een betaling na zes uur belooft.

De [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) beschrijft de verdeling. Geschikte shares worden naar moeilijkheid gewogen tegenover al het geschikte werk in hetzelfde venster. Online minuten leggen geen percentage van de volgende beloning vast. Apparaten, pooldeelname en share-uitkomsten kunnen gewichten veranderen bij even lange sessies.

## Volg één share tijdens een pauze

Voorbeeld in lokale tijd: je minet van 18:00 tot 20:00 en een geschikte share wordt om 19:00 vastgelegd. Een blok met venstereinde 22:00 kijkt zes uur terug tot 16:00. De share valt daarin, ook al ben je offline. Dit illustreert geschiktheid, niet de voorspelling dat de pool dan een blok vindt.

Een venster tot 02:00 de volgende dag begint om 20:00, zodat de share van 19:00 erbuiten valt. Opnieuw verbinden om 08:00 verandert zijn tijdstempel niet. Vers werk telt daarna voor latere vensters. De [geluksgids](/blog/nl/mining-pool-luck-explained/) verklaart wisselende bloktijden; plan niet omdat een blok zogenaamd aan de beurt is.

## Stop met een herkenbaar verslag

Noteer voor een pauze ontvangstadres, rignaam, geschatte stoptijd en laatste berichten over geaccepteerde shares. Controleer het account via de [volggids](/blog/nl/track-your-miner-on-bntpool/). Dit geeft een referentie en helpt een gestopte rig te onderscheiden van een ander adres of een nog achterlopende weergave.

Sluit seine normaal af met `q` in het dashboard of `Ctrl+C` in de terminal. Bewaar werkend startcommando en configuratie. De [desktopgids](https://seineminer.com/nl/blog/mine-while-using-pc/) behandelt afstemming met andere taken; netjes stoppen vereist geen verwijderde instellingen, ander walletadres of sluiting van een poolaccount.

## Wat gebeurt er offline met onbetaald saldo?

Als een share buiten het venster voor een later blok valt, wist dat geen beloning die al aan een eerder blok is toegewezen. Geschatte beloningen volgen blokstatus en rijping; bevestigd onbetaald saldo blijft tot verwerking op het account. De [bevestigingsgids](/blog/nl/block-confirmations-mining-rewards/) onderscheidt voorlopig bloktegoed en gerijpt saldo.

Een bevestigd saldo onder 0,1 BNT blijft onder de huidige automatische drempel totdat extra tegoed het minimum bereikt. Opnieuw verbinden verhoogt het saldo niet en dwingt geen betaling af. Andersom vereist een geschikt saldo geen online miner tijdens verwerking. De [betalingshistorie](/payouts) onderscheidt onbetaald saldo en verzonden transactie.

## Keer terug naar hetzelfde account en controleer nieuw werk

Herstart het opgeslagen commando en controleer pool, ontvangstadres en rignaam voordat je statistieken vergelijkt. Het adres identificeert het gevolgde account. Kies je bewust een andere bestemming, lees de [gids voor adreswijziging](/blog/nl/change-mining-wallet-address/); oud saldo verhuist niet stilzwijgend naar het nieuwe account.

Zoek een actuele job en nieuwe geaccepteerde shares, en laat vervolgens het meetvenster van de pool bijwerken. Een nieuw sessiegemiddelde kan verschillen van de terugkijkende poolschatting. De [hashrategids](/blog/nl/what-is-hashrate/) verklaart die vensters. Controleer werkelijk werk en het juiste account voordat je een kort verschil als ontbrekende beloningen ziet.

## Plan sessies rond je eigen routine

Kies uren volgens computergebruik en stroombudget. De [elektriciteitsgids](https://seineminer.com/nl/blog/mining-electricity-use/) berekent verbruik voor het echte schema, inclusief verschillende tariefperioden waar nodig. Een kortere sessie verkort de draaitijd, maar exacte energie hangt nog steeds af van gemeten apparatuur en belasting.

Noteer starts, stops, geaccepteerd werk en saldi over meerdere sessies. De [beloningscalculator](/blog/nl/estimate-blocknet-mining-rewards/) geeft een verwachting onder zijn aannames, geen belofte voor één avond. Beoordeel een representatieve periode en houd verbinding, sharevenster, blokrijping en verzonden betalingen afzonderlijk bij.

<CTA target="pool" title="Volg je sessies op bntpool.com" text="Keer terug met hetzelfde ontvangstadres, controleer verse shares en volg de normale betalingsstappen van je saldo." />
