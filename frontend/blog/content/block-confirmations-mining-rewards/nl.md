---
title: 'Blokbevestigingen en onrijpe miningbeloningen uitgelegd'
description: 'Volg een Blocknet-blok tot rijpe beloning en betaling. Leer hoe bntpool de blokdiepte telt en waarom bevestigingen in blokken worden gemeten.'
tags: ['Miningbeloningen', 'Bevestigingen', 'Blocknet']
faq:
  - q: 'Hoeveel bevestigingen vraagt bntpool?'
    a: 'Bij controle op 26 oktober 2026 publiceert bntpool.com een eis van 60 blokken. Het trekt de gevonden hoogte van de huidige hoogte af en vergelijkt de hash met de node. Raadpleeg Get Started of de openbare API voor de actuele waarde.'
  - q: 'Betekenen 60 blokken precies vijf uur?'
    a: 'Nee. Zestig doelintervallen van vijf minuten geven een schatting van 300 minuten, vijf uur. Werkelijke voortgang volgt nieuwe blokken met variabele aankomsttijden. Rijping, saldodrempel en latere betaling zijn afzonderlijke stappen.'
  - q: 'Moet ik blijven minen om een beloning te laten rijpen?'
    a: 'Een gevonden blok krijgt diepte door nieuwe netwerkblokken, ook als je miner offline is. Je aandeel volgt de boekhoudregels van de pool. Stoppen versnelt of herstart de rijping niet; volg de registratie in My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Volg een Blocknet-blok door bevestiging, rijping en latere walletbetaling. Deze gids verklaart de ketendiepte achter de beloningskaarten. De [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) behandelt verdeling; de [instelgids](/blog/nl/mine-blocknet-on-bntpool/) helpt je beginnen.

## Wat zijn bevestigingen en onrijpe beloningen?

<Answer>
Bevestigingen beschrijven de plaats van een blok onder de huidige ketentop. Een onrijpe beloning komt uit een recent blok dat de vereiste diepte nog niet heeft. bntpool controleert diepte en identiteit vóór bevestiging; saldo-eligibiliteit en betaling volgen apart.
</Answer>

*Het blok krijgt diepte, bereikt rijpheid en gaat via een in aanmerking komend saldo naar een aparte betaling.*

## Hoe telt bntpool de diepte?

De pool trekt de gevonden blokhoogte van de huidige hoogte af. Op 26 oktober 2026 was de gepubliceerde eis 60 blokken. Het gevonden blok begint daarbij op nul. Elders kan het bevattende blok als eerste bevestiging tellen; vergelijk ook de telregel.

Voorbeeld: een blok op 70.000 heeft diepte nul bij top 70.000, één bij 70.001 en 60 bij 70.060. Dit zijn illustratieve hoogtes. De pool vergelijkt ook de hash met zijn node. De [implementatie](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) beschrijft beide controles.

## Waarom is vijf uur een schatting?

Blocknet streeft naar vijf minuten per netwerkblok. Zestig maal vijf geeft 300 minuten, vijf uur. Dat helpt plannen, maar de voorwaarde is een bloktelling. Volg de keten vanaf de gevonden hoogte tot de vereiste diepte, geen vaste klokdeadline.

Ieder blok op de geaccepteerde keten verhoogt de diepte; het hoeft niet van bntpool te komen. Je minersnelheid creëert geen eigen bevestigingsklok. Intervallen variëren en de duur kan afwijken. De gidsen voor [moeilijkheid](/blog/nl/mining-difficulty-explained/) en [geluk](/blog/nl/mining-pool-luck-explained/) verklaren aanpassing en rondevariatie.

## Onderscheid wachtend, bevestigd en betaald

Een nieuw geregistreerd blok wacht terwijl de diepte groeit. Je geschatte aandeel verschijnt mogelijk in [My Stats](/stats) bij geschikt werk. De beloning maakt nieuwe munten; de Blocknet-wallet vereist voor coinbase 60 blokken diepte vóór uitgeven. De overeenkomstige wachttijd van de pool volgt die rijping.

Na diepte- en identiteitscontrole kan de pool bevestigen en beloningen aan saldi toewijzen. Dat betaalt niet onmiddellijk ieder adres. Het betaalproces beoordeelt bevestigde saldi en verzendvoorwaarden apart. Blokstatus, saldo en betaalgeschiedenis beschrijven verschillende fasen.

## Waarom controleert de pool de hash?

Hoogte duidt een positie aan, hash een specifiek blok. Nodes kunnen concurrerende blokken kennen; de geaccepteerde keten bepaalt welke die positie inneemt. Daarom vraagt de pool zijn node en vergelijkt de hash. Diepte alleen bewijst niet dat het oorspronkelijke blok nog in de keten zit.

Een verdwenen blok kan verweesd worden verklaard; zijn voorlopige beloning geldt dan niet meer als beloning van de geaccepteerde keten. Dat wordt apart behandeld. Gebruik de hashlink op [Blocks](/blocks): twee regels op dezelfde hoogte hoeven niet hetzelfde voorval te zijn. De schatting blijft gekoppeld aan haar blok.

## Hoe verschilt betaling van de geminede beloning?

De beloning begint in de coinbase-output. Een poolbetaling is een latere transactie naar je ontvangstadres. Op de controledatum is het gepubliceerde minimum 0,1 BNT. Rijpheid en saldodrempel zijn verschillende voorwaarden; de [PPLNS-gids](/blog/nl/how-pplns-payouts-work/) beschrijft verdeling en betaling.

Volg na verzending de transactie en ontvangst en besteedbaarheid in je wallet. Haar bevestigingen horen bij de betaling, niet bij het oorspronkelijke poolblok. Een nieuw adres of herverbinding is niet nodig tijdens rijping. Vergelijk [betalingen](/payouts) en wallet met hetzelfde bedoelde adres.

## Hoe volg je een beloning van begin tot eind?

Zoek op [Blocks](/blocks) hoogte, hash en status. Open [My Stats](/stats) met het miningadres en bekijk geschatte en bevestigde beloningen. Volg daarna de betalingstransactie vanuit de geschiedenis. Iedere observatie hoort zo bij blok, accountsaldo of walletbetaling.

Controleer waarden op [Get Started](/start) of `https://bntpool.com/api/info` en de [poolstatus](/status). Noteer bij onderzoek adres, blok- of transactie-ID en tijd; deel nooit herstelwoorden. De [volggids](/blog/nl/track-your-miner-on-bntpool/) verklaart overige statistieken. Blokken groeien ook tijdens offline zijn; geschiktheid volgt geregistreerd werk en ketentoestand.

<CTA target="pool" title="Volg je miningbeloningen" text="Verbind met bntpool.com en volg schattingen, bevestigde saldi en betalingen via My Stats." />
