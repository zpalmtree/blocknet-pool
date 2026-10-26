---
title: 'Poolkosten versus ontwikkelaarskosten van de miner'
description: 'Begrijp pool- en minerkosten bij Blocknet: waar ze gelden, hoe je ze combineert en hoe je dezelfde kosten niet tweemaal aftrekt.'
tags: ['Mininggids', 'Poolkosten', 'Beloningen']
faq:
  - q: 'Zijn dit dezelfde kosten?'
    a: 'Nee. Poolkosten horen bij beloningsverdeling, ontwikkelaarskosten bij de software. seine v0.2.36 besteedt na de startvrijstelling 2% van de daarvoor tellende miningtijd aan ontwikkeling. De afzonderlijke bntpool-kosten waren 0% op 26 oktober 2026.'
  - q: 'Hoe combineer ik ze?'
    a: 'Vermenigvuldig een bruto schatting met één min de minerfractie, daarna met één min de poolfractie. Gebruik je werkelijke versie en actuele instelling. Dit schat behouden beloningen, geen gegarandeerde betaling of geldwinst.'
  - q: 'Moet ik minerkosten van poolhashrate aftrekken?'
    a: 'Bepaal eerst wat dat cijfer inhoudt. Als het al werk voor je adres na ontwikkelaarstijd schat, tel je de kosten dubbel. De calculator kan nul gebruiken voor die reeds verwerkte aanpassing. Trek kosten niet opnieuw af van bevestigd saldo of geregistreerde betaling.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Volg de twee kostenlagen van een Blocknet-schatting. Deze gids gebruikt seine v0.2.36 en openbare bntpool-instellingen gecontroleerd op 26 oktober 2026 voor de [calculator](/blog/nl/estimate-blocknet-mining-rewards/#mining-calculator) en [My Stats](/stats).

## Wat verschilt tussen pool- en minerkosten?

<Answer>
Poolkosten horen bij de verdeling van beloningen; ontwikkelaarskosten bij het miningschema van software. Eigenaar en toepassingsfase verschillen. Controleer beide instellingen, bepaal of de invoer bruto of al aangepast is en pas elke relevante correctie eenmaal toe.
</Answer>

*Werk doorloopt een aanpassing van miningtijd en vervolgens een afzonderlijke poolbeloningsaanpassing vóór het geschatte behouden bedrag.*

## Waarop gelden poolkosten?

De pool verzamelt geldig werk en verdeelt de uitkeerbare beloning volgens zijn administratie. bntpool berekent ingestelde kosten over de geregistreerde blokbeloning vóór verdeling van het restant via PPLNS. De [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) verklaart deelname en bijschrijvingen.

Op 26 oktober 2026 publiceert `https://bntpool.com/api/info` voor `pool_fee_pct` 0,0, dus 0%. Controleer de actuele waarde via [Get Started](/start) of de API wanneer je bijwerkt. Nul beschrijft deze poolinstelling; softwarebeleid blijft een aparte invoer.

## Hoe werken seine-ontwikkelaarskosten?

v0.2.36 documenteert 2% op iedere pool en in solomodus. Het is miningtijd voor het ontwikkelaarsadres, geen latere afschrijving uit je wallet. De eerste 10 minuten zijn vrijgesteld; daarna wordt tijd bijgehouden. Zie de [configuratiegids](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md).

De planning werkt in rondes, dus een kort tijdvak hoeft niet precies 2% te zijn. Dit is het langetermijndoel na de vrijstelling, geen belofte per minuut of blok. Noteer de versie: oudere documentatie en andere software kunnen verschillen. Poolkosten vervangen die informatie niet.

## Hoe combineer je percentages?

Gebruik behouden fracties. Met minerfractie m en poolfractie p blijft `(1 - m) * (1 - p)` van bruto over. De [beloningsschatter](/blog/nl/estimate-blocknet-mining-rewards/) gebruikt dit. Percentages optellen is niet de algemene regel: de tweede aanpassing werkt op het restant.

Rekenvoorbeeld: hypothetisch 100 BNT bruto met 2% mineraanpassing en gecontroleerd 0% pool geeft 100 × 0,98 × 1,00 = 98 BNT. Dit illustreert een stabiele situatie, geen hardwaremeting, belofte of korte sessiesimulatie met startvrijstelling.

## Kies het juiste hashrate

Lokale of benchmark-H/s beschrijven machinewerk. Bepaal vóór de ontwikkelaarsfactor of dit alle miningtijd omvat of alleen werk voor je ontvangstadres. Controleer bron en tijdvak. De [hashrategids](/blog/nl/what-is-hashrate/) onderscheidt lokale, pool- en netwerkmetingen.

Poolhashrate voor je adres kan tijd voor elders gecrediteerd werk al meenemen. Vul dan nul in voor de aanvullende mineraanpassing. Dat wijzigt de berekening, niet het softwarebeleid. Poolkosten blijven relevant bij bruto geldige beloningen; controleer wat elk cijfer omvat.

## Waarom kosten niet opnieuw van saldo aftrekken?

Een bevestigd saldo is een administratief resultaat, geen onaangepaste bruto schatting. De pool heeft bijschrijvingen al toegewezen. Dezelfde kosten opnieuw van saldo of betaling aftrekken verandert het cijfer zonder nieuwe kostengebeurtenis. Gebruik het getoonde bedrag en identificeer de fase.

Openstaande beloning, bevestigd saldo en verstuurde betaling verschillen ook in timing. Blokrijping en uitbetalingsdrempel verklaren overgangen, geen extra percentages. Zie de [bevestigingsgids](/blog/nl/block-confirmations-mining-rewards/) en [betaalhistorie](/payouts). Eventuele wallettransactiekosten zijn een apart transactieveld.

## Bewaar de herkomst van invoer

Bewaar datum, versie, kosteninstellingen, hashratebron en tijdvak. De calculator combineert live pool/netwerkdata met jouw invoer; ververs netwerkdata bij een later bezoek. Werkelijke bijschrijvingen volgen ook geldige shares, gevonden blokken en PPLNS. Formule en latere boeking beantwoorden verschillende vragen.

Gebruik [My Stats](/stats) en de [trackinggids](/blog/nl/track-your-miner-on-bntpool/). Vergelijk hetzelfde adres, tijdvak en stadium vóór je kosten wijzigt. De calculator schat BNT; stroom, hardwarekosten en geldprijs vallen erbuiten. Je hoeft je walletadres niet te wijzigen.

<CTA target="pool" title="Volg beloningen op bntpool.com" text="Gebruik de calculator voor schattingen en My Stats voor geregistreerd werk, saldi en betalingen." />
