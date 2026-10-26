---
title: 'Je walletadres voor Blocknet-mining wijzigen'
description: 'Wijzig het ontvangstadres van je Blocknet-miners, controleer nieuwe shares op bntpool.com en volg eerdere beloningen onder het oorspronkelijke walletadres.'
tags: ['Mininggids', 'Wallet', 'Uitbetalingen']
faq:
  - q: 'Verhuist het oude saldo bij een adreswijziging?'
    a: 'De nieuwe bestemming wijst toekomstig ingediend werk aan het nieuwe adres toe. Bestaande shares, tegoeden en betalingen blijven bij het oorspronkelijke adres. Zoek beide afzonderlijk op in Mijn statistieken en behoud toegang tot de oude wallet terwijl je resterende beloningen volgt.'
  - q: 'Moet ik alle computers aanpassen?'
    a: 'Ja. Elk miningproces gebruikt zijn eigen ingestelde bestemming. Pas eerst één computer aan en controleer die, daarna de overige met hun configuratie en startargumenten. Verschillende machinenamen laten zien welke computers werk voor het nieuwe adres indienen.'
  - q: 'Verandert een Blocknet ID een actieve miner meteen?'
    a: 'seine resolveert de opgeslagen ID bij het starten en houdt het adres gedurende die uitvoering vast, ook bij opnieuw verbinden. Herstart na een gewijzigde koppeling elke miner en controleer het volledige adres. Eerder geregistreerd werk en saldo worden niet opnieuw toegewezen.'
sourceHash: 'fca912d46d55'
translation: draft
---

Wijzig de bestemming voor toekomstig werk en houd eerdere beloningen overzichtelijk. Deze gids gaat uit van seine dat al met bntpool.com verbonden is. Begin voor de eerste verbinding bij de [poolgids](/blog/nl/mine-blocknet-on-bntpool/), en voor een nieuwe wallet bij de [walletgids](https://seineminer.com/nl/blog/blocknet-wallet-for-mining/).

## Hoe wijzig je het miningadres?

<Answer>
Stop één miner, pas het ontvangstadres aan in de opgeslagen configuratie of gewone startopdracht en herstart. Controleer het volledige opgeloste adres en geaccepteerde shares, en zoek het adres op bntpool.com. Herhaal voor andere machines. Eerdere shares en beloningen blijven bij het adres waarmee ze zijn vastgelegd.
</Answer>

*Nieuw werk gaat naar de tweede wallet; eerdere beloningsgegevens blijven bij de eerste.*

## Leg beide bestemmingen vooraf vast

Kopieer het nieuwe adres rechtstreeks uit de bedoelde wallet. De miner heeft de openbare bestemming of geregistreerde Blocknet ID nodig, geen herstelzin of privésleutel. Bewaar ook het oude volledige adres: daarmee zoek je eerdere poolactiviteit op.

Open Mijn statistieken voor het oude adres en noteer openstaande beloningen, bevestigd saldo, recente betalingen en actieve machinenamen. Dit geeft een uitgangspunt; de nieuwe pagina bevat niet de oude geschiedenis. Behoud toegang tot de oorspronkelijke wallet tijdens het volgen van resterende beloningen en betalingen.

## Pas één miner en zijn startopdracht aan

Stop een miner en bekijk de gewone startmethode. Bij een bestaand bestand gaat `--address` vóór het opgeslagen adres zonder het te vervangen. Het PowerShell-voorbeeld kiest de bestemming voor deze uitvoering. Vervang `@yourname` door je nieuwe ID of volledige adres en behoud geteste pool-, gegevenspad- en hardwareargumenten.

```powershell
.\seine.exe --address "@yourname"
```

Gebruik op Linux of macOS de volgende syntaxis vanuit de gewone werkmap. Beide voorbeelden starten echt mining; kies dat van jouw systeem. De bestaande installatie levert andere verbindingsinstellingen. Behoud de machinenaam om zijn shares onder het nieuwe adres te herkennen.

```sh
./seine --address "@yourname"
```

## Laat de normale herstart het nieuwe adres gebruiken

Wijzig bij gestopte miner `address` in `seine.config.json`. Behoud andere velden en geldige JSON. Controleer de gegevensmap: `./seine-data` is relatief aan de werkmap, terwijl `--data-dir` een andere kan kiezen. Bewerk het werkelijk gebruikte bestand.

Werk oude `--address`-argumenten in snelkoppeling, script, taak of service bij of verwijder ze. Een geslaagde handmatige opdracht verandert deze andere startmethoden niet. Herstart normaal en controleer opnieuw. De [configuratiegids](https://seineminer.com/nl/blog/configure-seine-miner/) verklaart voorrang en waarom een andere werkmap een andere configuratie kan laden.

## Wat verandert met een Blocknet ID?

In seine v0.2.36 wordt `@name` bij het starten opgelost en gevalideerd. Het volledige adres blijft vast tijdens die uitvoering, inclusief nieuwe poolverbindingen. Wijzig je de naamkoppeling, herstart dan elke miner en controleer het doel. Alleen opnieuw verbinden herhaalt de oorspronkelijke naamresolutie niet.

Noteer beide volledige adressen, ook als de leesbare naam gelijk blijft. De administratie gebruikt het opgeloste adres; een nieuwe koppeling stuurt oude shares of saldo niet door. Werkt de naam niet, controleer de koppeling of gebruik bewust het volledige adres dat je in de wallet hebt gecontroleerd.

## Controleer elke computer onder het nieuwe adres

Controleer bestemming en geaccepteerde inzendingen en zoek het volledige nieuwe adres in Mijn statistieken. Vergelijk machinenaam en recente shares. Een nieuwe hashrateschatting vraagt werk en tijd: controleer eerst binnenkomende shares. De [statistiekgids](/blog/nl/track-your-miner-on-bntpool/) bespreekt tellers en ontbrekende machines.

Herhaal na de eerste bevestigde computer de wijziging op de rest. Unieke namen vereenvoudigen controle. Test ook elke normale herstart zodat een oud startscript later niet opnieuw het vorige doel gebruikt. De [gids voor meerdere computers](/blog/nl/mine-multiple-computers-one-wallet/) beschrijft hun indeling onder één ontvangstadres.

## Wat gebeurt er met oude beloningen?

Bestaande tegoeden en betalingen blijven bij het oude adres. Eerdere geldige shares kunnen nog in relevante PPLNS-vensters bijdragen; openstaande blokbeloningen volgen bevestigingsregels. Hashrate nul betekent niet dat de administratie klaar is. [PPLNS](/blog/nl/how-pplns-payouts-work/) en [bevestigingen](/blog/nl/block-confirmations-mining-rewards/) onderscheiden werk, rijpende beloningen en betalingen.

Een adreswissel voegt geen saldi samen en omzeilt geen uitbetalingsminimum. Controleer de actuele regels en beide pagina’s tot de resterende bedragen duidelijk zijn. Een machinenaam wijzigen verplaatst evenmin beloningen. Bevestig dat alle bedoelde miners nieuw werk naar het nieuwe adres sturen en bewaar de oude wallet en geschiedenis.

<CTA target="pool" title="Controleer je opstelling op bntpool.com" text="Controleer bestemming en geaccepteerd werk per machine en volg oude en nieuwe beloningen apart." />
