---
title: 'Blocknet minen op meerdere computers voor één wallet'
description: 'Voeg op bntpool.com een tweede Blocknet-miner toe voor dezelfde wallet. Geef elke pc een naam, controleer shares en begrijp gezamenlijke beloningen en namen.'
tags: ['Mininghandleiding', 'Computers', 'Instellen']
faq:
  - q: 'Heb ik voor elke computer een wallet nodig?'
    a: 'Nee. Elke computer kan hetzelfde ontvangstadres op bntpool.com gebruiken met een eigen naam voor de installatie. De pool rekent beloningen per adres af. Miningcomputers hebben alleen dat adres of je Blocknet ID nodig, niet de herstelzin van je wallet.'
  - q: 'Wat gebeurt er bij twee miners met dezelfde naam?'
    a: 'Onder hetzelfde adres wordt hun sharegeschiedenis onder die naam samengevoegd. Dat maakt het lastiger de machine achter een meting te herkennen. Geef elke installatie een eigen naam en herstart daarmee; al vastgelegde shares behouden hun oorspronkelijke label.'
  - q: 'Wordt mijn uitbetaling gesplitst bij een extra machine?'
    a: 'Namen ordenen de getoonde miningactiviteit en maken geen aparte saldi onder hetzelfde adres. Geldig werk van je computers draagt bij aan de PPLNS-deelname van dat adres. Het saldo volgt de normale bevestigings- en uitbetalingsregels van de pool.'
sourceHash: '1e7424aee51d'
translation: draft
---

Voeg een tweede computer toe aan bntpool.com en behoud één ontvangende wallet. Geef elke installatie een naam, controleer die apart en volg daarna het gezamenlijke adres. Deze handleiding behandelt de tweede machine en dubbele namen. Stel je eerste miner eerst in met de [poolhandleiding](/blog/nl/mine-blocknet-on-bntpool/).

## Kunnen meerdere computers naar dezelfde wallet minen?

<Answer>
Ja. Start seine op elke computer met hetzelfde ontvangstadres en een eigen machinenaam. bntpool.com registreert werk en beloningen per adres; namen scheiden de activiteit in je statistieken. Controleer geaccepteerde shares van elke machine voordat je op het gezamenlijke overzicht vertrouwt.
</Answer>

*Afzonderlijke computers sturen werk via de pool naar één ontvangstadres; elke computer houdt een eigen naam.*

## Kies het ontvangstadres en de namen

Gebruik het ontvangstadres van je wallet of een Blocknet ID dat ernaar verwijst. Controleer op elke miner het opgeloste adres. Een naam vergemakkelijkt de invoer, maar de boekhouding volgt het resulterende adres. Wijzig je later de bestemming van een ID, dan kan een draaiende miner tot de herstart het eerdere adres houden. Verdiende beloningen blijven bij hun oorspronkelijke adres.

De [wallet-handleiding](https://seineminer.com/nl/blog/blocknet-wallet-for-mining/) legt uit hoe je een ontvangstadres krijgt. Miningcomputers hebben alleen die openbare bestemming nodig; bewaar herstelzin en privésleutels bij de wallet. Kies korte namen zoals `desktop-01` en `server-02`. De pool verwijdert spaties aan de randen en bewaart maximaal 64 tekens. Zet het onderscheidende deel daarom vooraan in een lange naam.

## Voeg de tweede miner met een eigen naam toe

Installeer de passende seine-release op de tweede computer en start vanuit de uitgepakte map. Het eerste PowerShell-voorbeeld stelt ontvangende naam, pool en machinenaam expliciet in. Vervang `@yourname` vooraf door je ID of volledige ontvangstadres. Behoud de aanhalingstekens en voeg eerder geteste backend- of afstelopties van die machine toe.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Op Linux of macOS gebruik je de onderstaande programmaopdracht met een andere machinenaam. Besturingssystemen mogen verschillen; het ontvangstadres moet gelijk zijn. Elk voorbeeld start echt mining. Voer het passende voorbeeld uit op de juiste computer en laat starten en afstellen afronden. Je hoeft niet beide voorbeelden op dezelfde computer uit te voeren.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Welke instellingen deel je tussen computers?

Deel ontvangstadres en poolendpoint. Houd per installatie een aparte lokale gegevensmap en naam. seine bewaart de verbinding in `seine.config.json` in de gegevensmap; standaardpad `./seine-data` hangt van de startmap af. Vanuit een andere map starten kan een andere configuratie vinden. Een vaste startmap maakt onderhoud eenvoudiger.

Kopieer je een configuratie van de eerste computer, stel dan in de startopdracht van de tweede een eigen naam in en controleer die bij het starten. Behoud die expliciete naam in latere opdrachten: een overschrijving verandert geen reeds opgeslagen naam. Pas hardwarekeuzes aan en laat elke computer lokaal afstellen. Het adresargument bepaalt de uitbetaling en het naamargument het label; voer ze apart in zoals hierboven.

## Hoe controleer je of beide computers bijdragen?

Open My Stats op bntpool.com voor het gezamenlijke adres. Zoek beide namen en een recente share van elk. Controleer ook geaccepteerde inzendingen op de miners zelf. Werk computer voor computer: zo koppel je een lokaal proces aan een poolrij en vind je fouten makkelijker dan via alleen totale hashrate.

De pool schat hashrate uit ingediende shares; een nieuwe machine heeft tijd en geaccepteerd werk nodig voor een bruikbare meting. Lokale snelheid en poolschatting gebruiken verschillende metingen en vensters. Bevestig eerst activiteit met recente shares en acceptatietellers en vergelijk daarna langere perioden. De [statistiekhandleiding](/blog/nl/track-your-miner-on-bntpool/) verklaart kaarten en rijen zonder apparaataantallen uit verbindingen af te leiden.

## Wat als machines ontbreken of een naam delen?

Gelijke namen onder hetzelfde adres groeperen vastgelegde shares. Hernoem één miner via de naamoptie en herstart hem. Nieuwe inzendingen gebruiken het nieuwe label, oude behouden hun vorige naam. De oude rij kan binnen het getoonde historische venster zichtbaar blijven. Hernoemen verplaatst geen verdiend adressaldo.

Vergelijk bij een ontbrekende machine het werkelijke opgeloste adres, endpoint en de naam met de geopende pagina. Controleer verbinding en geaccepteerde shares. CPU en GPU binnen één seine-proces hebben hiervoor geen aparte namen nodig. Volg bij afgewezen of verouderde shares de [sharestatushandleiding](/blog/nl/accepted-rejected-stale-shares/) in plaats van herhaaldelijk het walletadres te veranderen.

## Hoe werken gezamenlijke beloningen en betalingen?

Geldig werk van de computers draagt bij aan hetzelfde adres binnen het PPLNS-venster. Namen helpen bijdragen volgen en creëren geen aparte saldi. Een extra machine voegt werkelijk ingediend nuttig werk toe, geen vaste beloning per apparaat of naam. Hardware en draaitijd kunnen verschillende bijdragen opleveren; meer namen alleen leveren geen extra werk.

De [PPLNS-handleiding](/blog/nl/how-pplns-payouts-work/) beschrijft de verdeling bij een gevonden blok. Bevestigings- en uitbetalingsregels blijven gelden voor het gezamenlijke saldo. Wallets samenvoegen of saldi tussen namen overmaken is niet nodig. Controleer bestemming, eigen naam en geaccepteerde shares op elke computer voordat je hem laat draaien. Herhaal die checklist bij de volgende.

<CTA target="pool" title="Verbind nog een computer met bntpool.com" text="Gebruik één ontvangstadres, geef elke installatie een eigen naam en controleer geaccepteerde shares." />
