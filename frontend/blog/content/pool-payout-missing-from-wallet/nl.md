---
title: 'Poolbetaling verzonden, maar niet in je wallet?'
description: 'Volg een Blocknet-poolbetaling via transactie, ontvangstadres en walletsynchronisatie en verzamel de gegevens om een ontbrekende betaling te onderzoeken.'
tags: ['Uitbetalingen', 'Wallet', 'Problemen oplossen']
faq:
  - q: 'Betekent een betaling in de wachtrij dat mijn wallet die heeft?'
    a: 'In de wachtrij is een verwerkingsstatus bij de pool. Controleer eerst of de vermelding een transactie-ID heeft en wat de huidige status is. De betalingsregistratie, opname van de transactie in de keten en herkenning van de output door je wallet zijn afzonderlijke stappen. Volg de specifieke transactie zodra die beschikbaar is.'
  - q: 'Kan de explorer mijn privésaldo tonen?'
    a: 'De explorer helpt de openbare ketenstatus van een transactie te controleren. Blocknet gebruikt private outputs. Een gewone adreszoekopdracht toont daarom niet je walletsaldo en bewijst niet dat een bepaalde output van jou is. Je wallet scant de keten om zijn eigen outputs te herkennen.'
  - q: 'Welke gegevens zijn nodig bij een ontbrekende betaling?'
    a: 'Geef het volledige ontvangstadres van de poolzoekopdracht, transactie-ID, geschatte tijd, poolstatus en synchronisatievoortgang. Beschrijf of de transactie in de walletgeschiedenis staat. Deel accountgegevens alleen in de juiste ondersteuningscontext en nooit je herstelzin, privésleutels of walletwachtwoord.'
sourceHash: '915075cf4e2b'
translation: draft
---

Volg een Blocknet-uitbetaling vanaf de poolregistratie naar de ontvangende wallet. Deze handleiding begint bij een herkenbare betaling op bntpool.com. Zie je alleen geschatte of bevestigde miningbeloningen die nog uitbetaald moeten worden, begin dan met de [PPLNS-handleiding](/blog/nl/how-pplns-payouts-work/): dat is een eerdere fase met eigen administratie- en uitbetalingsregels.

## Hoe volg je een betaling die in je wallet ontbreekt?

<Answer>
Zoek de betaling voor het volledige ontvangstadres, noteer transactie-ID en huidige status en controleer de ketenstatus. Open de wallet voor dezelfde bestemming en laat synchronisatie en scan bijwerken. Vergelijk de transactiegeschiedenis voordat je beslist of de betaling of de lokale walletweergave aandacht nodig heeft.
</Answer>

*Een poolbetaling sluit aan op een ketentransactie en daarna op de wallet die eigen inkomende outputs scant.*

## Begin met één specifieke betaling

Open My Stats en zoek het volledige adres dat werd gebruikt toen de beloningen werden verdiend. Noteer bij de juiste betaling bedrag, tijd, status en eventueel transactie-ID. Gebruik die vermelding in plaats van het totale ooit betaalde bedrag, dat meerdere betalingen kan omvatten. De [trackinghandleiding](/blog/nl/track-your-miner-on-bntpool/) toont waar deze velden staan.

De openbare Payouts-pagina beschrijft batches met mogelijk meerdere ontvangers en transactielinks. Bewaar jouw adresregistratie naast de onderzochte transactie. Het batchtotaal is niet het bedrag voor jouw wallet. Klap bij meerdere transactie-ID’s de lijst open en bewaar de relevante volledige ID’s in plaats van alleen een verkort label.

## Staat de betaling in de wachtrij, onbevestigd of bevestigd?

Lees de huidige status voordat je aanneemt dat alle stappen klaar zijn. Een wachtrijvermelding wordt nog aan poolzijde verwerkt; een onbevestigde transactie heeft ketenbevestiging nodig. Vernieuw de registratie en volg de link zodra beschikbaar. De [statuspagina](/status) geeft operationele context als meerdere waarnemingen op een onderbreking wijzen.

Houd rijping van miningbeloningen en bevestiging van de betalingstransactie apart. Het blok dat de beloning opleverde en de latere betaling hebben verschillende ID’s en functies. De [bevestigingshandleiding](/blog/nl/block-confirmations-mining-rewards/) legt dat uit. Een transactie-ID geeft een concreet onderzoekspunt, maar bewijst niet dat je lokale wallet de betaling al heeft gescand en herkend.

## Controleer de transactie op de keten

Open de explorerlink bij de betaling en vergelijk het volledige ID met je notities. Lees ketenstatus en beschikbare blokgegevens. Vindt de explorer de transactie nog niet, vergelijk dan de vernieuwde poolregistratie en servicestatus voordat je concludeert. Een tijdelijk zoekresultaat bepaalt op zichzelf geen oorzaak.

Door Blocknets privacy toont een gewone adreszoekopdracht geen openbaar ontvangersaldo. Transactiestatus en eigendomscontrole door de wallet beantwoorden andere vragen. Je wallet herkent inkomende outputs door de keten te scannen. De [stealth-adressenhandleiding](https://seineminer.com/nl/blog/stealth-addresses-explained/) beschrijft dit; een openbare adreslijst reproduceert niet de private walletgeschiedenis.

## Bekijk je de juiste ontvangende wallet?

Vergelijk het poolzoekadres met het ontvangstadres van de geopende wallet. Gebruik volledige waarden; vergelijkbare verkorte voorvoegsels zijn onvoldoende. Bewaar bij een Blocknet ID het volledige adres van het oorspronkelijke werk. Een huidige naamzoekopdracht kan na een wijziging elders uitkomen, terwijl een eerdere betaling bij de oorspronkelijke bestemming blijft.

Controleer oudere wallets die je vóór gewijzigde mininginstellingen gebruikte. Het wijzigen van mineradres of naamkoppeling verplaatst geen toegewezen betaling. De [adreswijzigingshandleiding](/blog/nl/change-mining-wallet-address/) beschrijft beide geschiedenissen. Bewaar oorspronkelijke wallet en back-up en onderscheid het openen van een bestaande wallet van het maken van een nieuwe met een ander adres.

## Laat de wallet synchroniseren en scannen

Controleer of de walletnode de huidige keten nadert en de walletscan het blok van de betalingstransactie heeft bereikt. Nodesynchronisatie en walletscan zijn aparte voortgangscontroles. Een net geopende of herstelde wallet kan tijd nodig hebben om outputs te vinden, ook wanneer de explorer de transactie al in een blok toont.

Houd de wallet verbonden en volg de gedocumenteerde synchronisatiewerkwijze. Vergelijk inkomende geschiedenis en hoofdsaldo, want latere uitgaven veranderen het totaal. Controleer bij een aanwezige vermelding bevestiging of beschikbaarheid. Begin hiermee vóór herstel of een volledige herscan en behoud back-ups vóór onderhoud. De [wallethandleiding](https://seineminer.com/nl/blog/blocknet-wallet-for-mining/) behandelt toegang en back-ups.

## Welke gegevens helpen een resterend verschil op te lossen?

Noteer volledig pooladres, transactie-ID, geschatte tijd en actuele poolstatus. Voeg walletversie, node- en scanvoortgang en aanwezigheid van de inkomende transactie toe. Beschrijf uitgevoerde controles. Zo onderscheid je transactiestatus, een verkeerd adres en een wallet die nog aan het bijwerken is.

Gebruik de officiële ondersteuningsroute en deel accountgegevens alleen waar passend. Onderzoek vereist geen herstelzin, privésleutels of wachtwoord. Bekijk diagnosebestanden voordat je ze volledig publiceert. Koppel uiteindelijk dezelfde betaling, transactie en ontvangende wallet; recente hashrate of nog een minerherstart bewijst geen ontvangst van een eerdere betaling.

<CTA target="pool" title="Volg je uitbetalingen op bntpool.com" text="Gebruik je volledige ontvangstadres om de betaling te vinden en vergelijk de transactie met de gesynchroniseerde walletgeschiedenis." />
