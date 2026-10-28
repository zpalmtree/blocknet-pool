---
sourceHash: 'e64d7723349d'
translation: draft
title: Hoe mining Blocknet-betalingen beveiligt
description: Lees hoe Blocknet met proof of work de betaalgeschiedenis kiest, waarom bevestigingen werk toevoegen en wat nodes controleren voordat ze blokken accepteren.
tags: [Blocknet, Betalingsveiligheid, Proof of work]
faq:
  - q: Kan meer miningkracht een betaling uit mijn wallet autoriseren?
    a: Miningkracht helpt om concurrerende blokken te produceren. Geld uitgeven vereist nog steeds geldige autorisatie. Nodes controleren transactiebewijzen en de balans voordat ze een blok accepteren. Ook een zwaardere tak moet aan die regels voldoen. Proof of work vervangt de privésleutels voor het uitgeven van jouw outputs niet.
  - q: Volgt Blocknet de keten met de meeste blokken?
    a: Core v0.22.0 houdt cumulatief werk bij door de difficulty van elk blok op te tellen. Een geldige tak met meer werk kan de geselecteerde keten worden, binnen de reorganisatieregels. Alleen blokken tellen is onvoldoende, omdat hun difficulty kan verschillen. Zowel werk als geldigheid telt.
  - q: Zijn betalingsbevestigingen en poolshares hetzelfde?
    a: Een poolshare registreert miningwerk dat bij de pool is ingediend. Een betalingsbevestiging gaat over opname van een transactie in de geselecteerde blockchain en de daaropvolgende blokken. Shares helpen beloningen verdelen; netwerkblokken verlengen de betaalgeschiedenis. Meer eigen shares voegen niet rechtstreeks bevestigingen aan een bepaalde betaling toe.
---

Volg een BNT-betaling vanaf opname in een blok tot een geschiedenis die door later miningwerk wordt ondersteund. Deze gids legt uit waarom het netwerk tussen concurrerende geschiedenissen moet kiezen, wat bevestigingen vertellen en hoe die keuze samengaat met validatie van privétransacties. De beschreven implementatie is het uitgebrachte core v0.22.0, gecontroleerd op 28 oktober 2026.

## Wat beschermt mining wanneer je BNT verstuurt?

<Answer>
Mining helpt het netwerk een gedeelde, geordende betaalgeschiedenis te kiezen. Elk geaccepteerd blok verwijst naar zijn voorganger en draagt proof of work bij. Nodes controleren voorgestelde blokken en transacties en vergelijken geldige takken op cumulatief werk binnen hun reorganisatieregels. Latere blokken voegen werk achter een opgenomen betaling toe en geven haar bevestigingsgeschiedenis praktische betekenis.
</Answer>

Een geldige betaling heeft autorisatie en een plek in het grootboek nodig. Je wallet levert de autorisatie om je geld uit te geven. Mining en ketenselectie helpen nodes overeenstemming te bereiken over welke geldige transacties bij de huidige geschiedenis horen. Die taken vullen elkaar aan: proof of work maakt een ongeldige betaling niet acceptabel.

De [gids over regelhandhaving](https://seineminer.com/nl/blog/who-controls-blocknet/) legt uit wie ontwikkelt, voorstelt en valideert. Hier staat de betaling zelf centraal: hoe haar opname deel wordt van de geschiedenis die je wallet en andere nodes volgen.

## Hoe leggen blokken een betaalgeschiedenis vast?

Een blokheader bevat de hash van het vorige blok en een Merkle-root afgeleid van de transacties. De root legt de inhoud cryptografisch vast; de vorige hash verbindt het blok met de eerdere geschiedenis. Een transactie wijzigen verandert de root en de header waarop wordt gemined. Afstammende blokken moeten dan via hun eigen koppelingen naar die gewijzigde geschiedenis verwijzen.

Neem een betaling in een blok, gevolgd door meerdere geaccepteerde blokken. Om dat deel te vervangen is een concurrerende tak nodig met geldige gekoppelde blokken en voldoende cumulatief werk, binnen de reorganisatieregels van de node. Een transactie in een lokaal bestand wijzigen produceert zo’n tak niet. Het werk hoort bij een specifieke reeks headers en transacties.

De [uitgebrachte blokcode](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) definieert deze headervelden, Merkle-controles en ketenselectiepaden. De [difficulty-gids](/blog/nl/mining-difficulty-explained/) legt uit hoe het netwerk de werkeis voor elk nieuw blok bepaalt.

## Waarom telt cumulatief werk zwaarder dan het aantal blokken?

Twee miners kunnen verschillende geldige blokken vinden die dezelfde ouder verlengen. Elk voorstel heeft een eigen hash; nodes kunnen ze in verschillende volgordes ontvangen. Daardoor kunnen tijdelijk concurrerende takken ontstaan. Het netwerk heeft een selectieregel nodig die het ondersteunende werk volgt, in plaats van de volgorde waarin een peer een geschiedenis aankondigde.

Core v0.22.0 berekent cumulatief werk door de difficulty van een blok bij het cumulatieve werk van de ouder op te tellen. Na validatie kan een tak met strikt meer werk een verandering van de geselecteerde keten veroorzaken. Een alternatief met gelijk werk doet dat niet via deze vergelijking. De difficulty moet zelf geldig zijn in de context van de ouder; een miner kan niet zomaar een hoger getal in de header zetten.

Vergelijk twee takken met een gezamenlijk beginstuk. Dat stuk draagt evenveel werk bij aan beide totalen. Het verschil zit in het werk na de splitsing, samen met de geldigheids- en reorganisatiecontroles. Blokken tellen zonder hun difficulty te bekijken kan dit verschil verbergen. Dit is een conceptueel voorbeeld, geen verslag van een actuele splitsing.

## Hoe helpen bevestigingen om een betaling te volgen?

Zodra een betaling in de geselecteerde keten is opgenomen, bouwen latere blokken op die geschiedenis voort. Haar diepte groeit met ieder volgend blok. Haar opname vervangen vereist een toegestane alternatieve geschiedenis van vóór het betreffende blok, terwijl de huidige tak werk blijft verzamelen. Daarom vertellen opname en daaropvolgende diepte meer dan alleen het zien van een uitgezonden transactie.

Een hoogte identificeert een positie; een hash een specifiek blok. Controleer de transactie en het blok waarin ze staat, en bevestig vervolgens dat dit blok nog in de geselecteerde keten zit. Als de gekozen geschiedenis verandert, kunnen opname en diepte ook veranderen. Bevestigingstellers kunnen het bevattende blok meetellen of alleen latere blokken tellen. Lees de conventie van de interface.

Core v0.22.0 handhaaft bovendien een reorganisatiegrens: een wissel wordt afgewezen als de gezamenlijke voorouder lager ligt dan de huidige tiphoogte min 100. Dit is een regel van de draaiende implementatie, geen universele belofte bij een weergegeven teller. De [gids over overdrachtstijd](https://seineminer.com/nl/blog/blocknet-transfer-time/) behandelt gewone besteedbaarheid in de wallet; [bevestigingen van miningbeloningen](/blog/nl/block-confirmations-mining-rewards/) behandelt coinbase-rijping en pooladministratie.

## Hoe wordt een privébetaling op herhaald uitgeven gecontroleerd?

Blocknets uitgebrachte transactieformaat gebruikt een key image dat aan het uitgavebewijs is gebonden. Validatie controleert die binding en wijst een key image af dat al is uitgegeven in de relevante ketentoestand. Zo herkennen nodes conflicterend gebruik van een output, terwijl de ringsignatuur verbergt welk toegestaan ringlid de echte input is. Privébedragen doorlopen nog steeds balans- en bereikcontroles.

Stel dat een eigenaar conflicterende transacties autoriseert die dezelfde output uitgeven. Een geldige geaccepteerde geschiedenis kan niet beide uitgaven bevatten. Ketenselectie telt omdat concurrerende geschiedenissen verschillende keuzes kunnen bevatten; transactievalidatie telt omdat elke geschiedenis de uitgaveregels moet volgen. Meer werk vervangt geen geldige handtekening en laat een ongebalanceerde transactie niet door de boekhoudcontrole komen.

Deze controles staan in de [uitgebrachte transactievalidator](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). De [Blocknet-privacygids](https://seineminer.com/nl/blog/blocknet-privacy-features/) bespreekt de bredere privacymechanismen. Betalingsveiligheid combineert geldig uitgeven, consistente geschiedenis en cryptografie die beschermt wat die geschiedenis onthult.

## Welke rol speelt poolmining bij betalingsveiligheid?

Een pool coördineert de zoektocht naar een blok en dient een opgeloste kandidaat in bij zijn upstream-node. Een share die het eenvoudigere pooldoel haalt, registreert bijgedragen werk; alleen een oplossing die het netwerkdoel haalt, kan na validatie een netwerkblok worden. De [pooluitleg](/blog/nl/what-is-a-mining-pool/) onderscheidt die twee doelen en hun functies.

Je miner kan dus bijdragen aan het verlengen van de betaalgeschiedenis terwijl de pool je werk apart bijhoudt. Een privé-poolsaldo, een sharetelling en een bevestigde walletbetaling beschrijven verschillende dingen. Begin met de [poolverbindingsgids](/blog/nl/mine-blocknet-on-bntpool/) om mee te doen en gebruik het transactierecord van je wallet om een specifieke BNT-betaling te volgen.

<CTA target="pool" title="Draag werk bij aan Blocknet" text="Verbind seine met bntpool.com en volg je miningwerk, beloningen en betalingen via de openbare hulpmiddelen van de pool." />
