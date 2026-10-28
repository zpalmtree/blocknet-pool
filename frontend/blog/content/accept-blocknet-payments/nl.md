---
title: "Blocknet-betalingen voor producten en diensten"
description: "Ontvang BNT voor bestellingen, koppel betalingen in je Blocknet-wallet en controleer bevestigingen, deelbetalingen en terugbetalingen met een duidelijk proces."
tags: [Blocknet, BNT]
faq:
  - q: "Hoe weet ik dat een klant met Blocknet heeft betaald?"
    a: "Zoek de ontvangst in je eigen gesynchroniseerde wallet, vergelijk bedrag en bestelreferentie en controleer de bevestigingsstatus. Bewaar de volledige transactie-ID bij de bestelling. Een screenshot of betaallink bewijst de ontvangst niet; een explorer toont ook niet het private bedrag dat aan jouw adres is betaald."
  - q: "Wat als een klant minder BNT stuurt dan gevraagd?"
    a: "Laat de bestelling gedeeltelijk betaald staan en bereken het restant uit gecontroleerde ontvangsten. Vraag het verschil met dezelfde referentie en bewaar beide transactie-ID’s. Tel iedere ontvangst eenmaal. Spreek je annulering af, bevestig dan het terugbetalingsadres via het bestaande gesprek over de bestelling."
  - q: "Markeert een memo een bestelling automatisch als betaald?"
    a: "Nee. Het memo helpt een ontvangst te koppelen, maar de betaler kan het wijzigen of weglaten. Controleer zelf bedrag en transactiestatus. De wallet maakt verzoeken en toont ontvangsten; deze handmatige werkwijze voegt geen automatische kassa of besteladministratie toe."
sourceHash: '3309a069e5de'
translation: draft
---

Accepteer Blocknet door een duidelijk BNT-bedrag te noemen, de ontvangst in je wallet te controleren en die bij de bestelling te bewaren. Het fictieve voorbeeld vraagt **1.25 BNT** met referentie **order-1042**. We gebruiken de uitgebrachte desktopwallet v0.22.0; screenshots bevatten herkenbare demogegevens.

## Hoe accepteer je een Blocknet-betaling?

<Answer>
Spreek het BNT-bedrag en de bestelreferentie af, stuur een verzoek voor een adres dat je beheert en wacht op de ontvangst in je gesynchroniseerde wallet. Vergelijk bedrag en referentie, controleer bevestigingen, bewaar de transactie-ID en markeer de bestelling betaald volgens de bevestigingsregel die je met de klant afsprak.
</Answer>

De [desktopwallethandleiding](https://seineminer.com/nl/blog/blocknet-desktop-wallet/) helpt bij maken en openen. Bewaar de herstelback-up apart van bestellingen. Voor klantbetalingen hoef je geen miner te draaien of bij een pool te zijn.

## Leg bedrag en bestelling vast

Noteer referentie, BNT-bedrag, ontvangstadres en aankoop. Bij een prijs in een andere munt spreek je vooraf het BNT-bedrag en de geldigheid van de offerte af. Een betaallink bevat het gevraagde bedrag; hij onderhoudt geen wisselkoers en dwingt geen deadline af.

Vraag in het voorbeeld **1.25 BNT** met memo **order-1042**. Een bestelnummer volstaat: bezorgadressen en andere klantgegevens blijven in je eigen administratie. Een [versleuteld memo](https://seineminer.com/nl/blog/blocknet-encrypted-memos/) reist mee met de betaling, maar het memo in een gedeelde link is leesbaar voor iedereen die de link krijgt.

Vul bij **Receive → Payment Request** bedrag en memo in en kies **Generate Payment Link**. Controleer ontvanger en bedrag voor het delen. De [handleiding voor betaallinks](https://seineminer.com/nl/blog/blocknet-payment-links/) beschrijft formaten en ontvangers op naam. Bewaar voor een vaste factuur het volledige ontvangstadres bij het verzoek.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Uitgebrachte Blocknet-wallet met een verzoek van 1.25 BNT, memo order-1042 en een demoadres." loading="lazy" decoding="async"></a>
<figcaption>Echte Engelse interface met demogegevens. Het verzoek heeft geen geld verplaatst. Open de afbeelding om te vergroten.</figcaption>
</figure>

## Controleer de ontvangst in je eigen wallet

Vraag de klant de referentie in te vullen en de transactie-ID te bewaren. Open **History** zodra node en wallet bijgewerkt zijn en zoek de inkomende betaling. Vergelijk bedrag en memo met de bestelling. De betaler kan beide aanpassen; het verzoek bewijst niet wat is verstuurd.

Bewaar volledige ID en ontvangen bedrag. Tel niet dubbel omdat een transactie in Dashboard én History staat of de klant dezelfde ID opnieuw stuurt. Koppel concrete ontvangsten, niet de verandering van je totale saldo: daarin zitten mogelijk miningbetalingen, andere klanten en eigen transfers.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History in de Blocknet-wallet toont een demo-ontvangst van 1.25 BNT met memo order-1042." loading="lazy" decoding="async"></a>
<figcaption>De voorbeeldontvangst past bij bedrag en referentie. Controleer de transactiestatus voordat je de bestelling uitvoert.</figcaption>
</figure>

Ontbreekt de betaling, vergelijk volledig bestemmingsadres en ID met de klant en controleer synchronisatie. Een screenshot van het verzenden geeft context; je ontvangende wallet stelt het ontvangen bedrag vast.

## Wanneer markeer je de bestelling als betaald?

Onderscheid een onbevestigde ontvangst, opname in een blok en besteedbare munten. De uitgebrachte wallet wacht tien volgende blokken na het blok van een gewone betaling voordat de outputs besteedbaar zijn. De [uitleg over transfertijd](https://seineminer.com/nl/blog/blocknet-transfer-time/) behandelt tellers en variabele wachttijden.

Meld je bevestigingsregel vooraf. Een eenvoudige handmatige werkwijze wacht tot de passende ontvangst besteedbaar is voordat je levert. Alleen een zichtbaar inkomend bedrag is niet voldoende. Beloof geen vaste levertijd op basis van de afteller: blokken komen op wisselende momenten. [Hoe mining betalingen beveiligt](/blog/nl/how-mining-secures-blocknet-payments/) legt bevestigingsdiepte uit.

## Verwerk deelbetalingen, extra bedragen en ontbrekende referenties

| Wat je hebt gecontroleerd | Volgende stap |
|---|---|
| 1.00 BNT voor de bestelling van 1.25 BNT | Boek een deelbetaling en vraag nog 0.25 BNT. |
| Later nog 0.25 BNT | Bewaar beide ID’s, tel het totaal van 1.25 BNT eenmaal en controleer beide bevestigingsstanden. |
| Meer dan 1.25 BNT | Spreek terugbetaling of tegoed af en leg dit vast. |
| Juiste bedrag zonder memo | Bevestig volledige ID en bestemming met de klant voordat je koppelt. |
| Wachtende betaling | Laat de bestelling op bevestiging wachten en controleer opnieuw. |

Vraag voor een terugbetaling bevestiging van het ontvangstadres in het bestaande bestelgesprek. Leid geen retouradres af uit de openbare transactie: Blocknet-inputs bevatten geen gebruikelijk afzenderadres. Controleer bedrag, bestemming en kosten voor de nieuwe verzending en bewaar de ID bij de oorspronkelijke betaling.

## Deel alleen het benodigde betalingsbewijs

De explorer helpt de blokopname te controleren, maar onthult niet het private bedrag voor jouw adres. Bij discussie kan de afzender gericht bewijs leveren via de [handleiding voor betalingsbewijzen](https://seineminer.com/nl/blog/blocknet-payment-proof/). Controleer ontvangersmatches, bedrag en chainstatus; alleen een geldige sleutel is onvoldoende.

Bewaar bestelling, verzoek, inkomende ID’s, gecontroleerde bedragen, bevestigingsbesluit en terugbetaling samen in je privéadministratie. Deel uitsluitend de relevante ontvangst of het bewijs. Deel nooit herstelwoorden, wachtwoorden of private kijksleutels voor de hele wallet als betaalbewijs.

Mine je ook BNT, houd [pooluitbetalingen](/blog/nl/track-your-miner-on-bntpool/) apart van klantorders, zelfs binnen dezelfde wallet. Zo is duidelijk welke transfer welke factuur betaalt.


<CTA target="pool" title="Ontvang ook miningbeloningen" text="De poolhandleiding laat zien hoe je een miner aansluit en de uitbetalingen naast andere Blocknet-ontvangsten volgt." />
