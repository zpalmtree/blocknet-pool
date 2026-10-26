---
title: 'Blocknet minen op bntpool.com'
description: 'Sluit aan op bntpool.com, de standaardpool van seine. Gebruik je adres of @name met stratum+tcp://bntpool.com:3333, geef rigs namen en bekijk je statistieken.'
tags: ['Mininggids', 'bntpool', 'Blocknet ID', 'seine']
sourceHash: '3126e8e7e396'
translation: draft
faq:
  - q: 'Wat is het Stratum-adres van bntpool.com?'
    a: 'Het adres is stratum+tcp://bntpool.com:3333, de pool die seine bij de eerste start voorstelt. Druk bij de poolvraag op Enter of geef het adres mee met --pool-url. Je Blocknet-adres of Blocknet ID-naam is je login, en de pool betaalt je beloningen aan dat adres.'
  - q: 'Kan ik op bntpool.com minen met een Blocknet ID-naam?'
    a: 'Ja. Voer @name of $name in waar seine of de pool om een adres vraagt. De pool controleert de ondertekende koppeling op blocknet.id en schrijft shares en uitbetalingen toe aan het volledige adres achter de naam. Je hebt seine v0.2.16 of nieuwer nodig. Zet de naam op de opdrachtregel tussen aanhalingstekens zodat je shell hem ongemoeid laat.'
  - q: 'Heb ik een account nodig om op bntpool.com te minen?'
    a: 'Nee. Er is geen registratie, wachtwoord of e-mail nodig. Je Blocknet-adres, of een Blocknet ID-naam die ernaar verwijst, is je login en uitbetalingsbestemming. Start seine ermee en zoek het op via My Stats om je hashrate, shares, rigs, saldo en uitbetalingen te bekijken.'
  - q: 'Kan ik met meerdere computers naar één adres minen?'
    a: 'Ja. Start seine op elke computer met hetzelfde adres of dezelfde naam en geef iedere rig een eigen naam. De pool telt shares, saldo en uitbetalingen per adres. Alle rigs dragen dus bij aan één saldo en één uitbetaling, terwijl My Stats iedere rig op een eigen regel toont.'
---

Deze gids behandelt aansluiten op bntpool.com met seine: het pooladres, inloggen met een Blocknet-adres of Blocknet ID `@name`, elke rig een naam geven en op de statistiekenpagina controleren of je shares binnenkomen. Is seine nog niet geïnstalleerd, volg dan eerst de [installatiegids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) en kom daarna terug.

## Wat is het pooladres van bntpool.com?

<Answer>
Het Stratum-adres is `stratum+tcp://bntpool.com:3333`. Seine biedt dit bij de eerste start als standaardpool aan: druk bij de poolvraag op Enter om te verbinden. Je logt in met je Blocknet-adres of een Blocknet ID `@name`. Je hoeft geen account aan te maken en beloningen gaan naar dat adres.
</Answer>

Dit zijn de poolinstellingen zoals het openbare endpoint `/api/info` ze op 25 oktober 2026 meldde:

| Instelling | Waarde |
|---|---|
| Stratum-adres | `stratum+tcp://bntpool.com:3333` |
| Login | Blocknet-adres, `@name` of `$name` |
| Uitbetalingssysteem | PPLNS met een venster van 6 uur |
| Bevestigingen voor uitbetaling | 60 blokken |
| Minimumuitbetaling | 0,1 BNT |

Dezelfde waarden staan altijd op de pagina [Get Started](/start).

## Waarom is bntpool.com de standaardpool van seine?

Bij de eerste start vraagt seine om een pool en vult het `stratum+tcp://bntpool.com:3333` als standaard in. Pool en miner sluiten op een paar praktische manieren op elkaar aan:

- **Eén protocol.** bntpool.com gebruikt het Stratum-protocol van seine, versie 2. Behalve je adres hoef je niets in te stellen.
- **Namen werken aan beide kanten.** Seine en de pool accepteren Blocknet ID-namen en controleren dezelfde ondertekende registratie op blocknet.id.
- **Versiecontrole.** Elke rig meldt zijn seine-versie bij zijn shares. De statistiekenpagina geeft oudere versies een badge «update available» met een link naar de releasepagina van seine.
- **Een installatiegids voor seine.** Get Started legt downloaden en starten uit, inclusief een opdracht om te kopiëren.

## Hoe verbind je seine met bntpool.com?

Bij de eerste start stelt seine twee vragen:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Plak je adres of typ je naam en druk op Enter om bntpool.com te accepteren. Seine bewaart beide antwoorden in `seine-data/seine.config.json`, zodat elke volgende start meteen begint met minen. Het kiest ook een rignaam en bewaart die in hetzelfde bestand; daarover hieronder meer.

Op een rig zonder scherm, in een service of script is niemand aanwezig om de vragen te beantwoorden. Geef bij de eerste start beide waarden als opties mee. Dit is dezelfde opdracht als op Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Wil je later van pool of adres veranderen, bewerk of verwijder dan `seine-data/seine.config.json`, of overschrijf de opgeslagen waarden op de opdrachtregel met `--address` en `--pool-url`.

## Kun je naar een Blocknet ID @name minen?

<Answer>
Ja. bntpool.com accepteert een korte Blocknet ID-naam als `@name` of `$name` overal waar je een adres kunt gebruiken. De pool controleert de ondertekende koppeling van blocknet.id, valideert het gekoppelde adres en schrijft shares, saldo en uitbetalingen gedurende de hele verbinding aan dat volledige adres toe.
</Answer>

Namen zijn niet hoofdlettergevoelig: `@Alice` en `@alice` zijn dezelfde login. Ondersteuning bestaat sinds seine v0.2.16 en zit in iedere huidige release. Voor het minen toont seine het gevonden adres, zodat je kunt controleren of het bij je wallet hoort. Zet de naam op de opdrachtregel tussen enkele aanhalingstekens. Zonder die tekens zou de shell `$name` als variabele proberen uit te lezen:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Als je de naam later naar een andere wallet laat wijzen, herstart dan seine. Seine zoekt een opgeslagen naam bij elke start opnieuw op en behoudt het adres gedurende die uitvoering. De pool doet hetzelfde per verbinding. Reeds verdiend saldo blijft bij het adres waarvoor het is verdiend en wordt daar uitbetaald. Een naam verplaatsen verplaatst je munten dus niet.

Je kunt een naam registreren op [blocknet.id](https://blocknet.id). De [Blocknet ID-gids](https://seineminer.com/nl/blog/blocknet-id-pay-by-name/) legt betalen op naam uit.

## Hoe geef je je rigs een naam?

Elke verbinding meldt naast je adres een rignaam. Bij de eerste start kiest seine `seine-` gevolgd door vier cijfers en bewaart dit in de configuratie. Dezelfde machine houdt zo na herstarts dezelfde naam. Stel een herkenbare naam in op de opdrachtregel:

```sh
./seine --pool-worker rig-01
```

De pool bewaart maximaal 64 tekens. Een vaste naam helpt ook na een herstart: de pool onthoudt de sharemoeilijkheid een uur lang, gekoppeld aan je adres en de rignaam. Een opnieuw verbonden rig gaat daardoor verder op de al afgestemde moeilijkheid.

## Kun je met meerdere rigs naar één adres minen?

Ja. Start seine op iedere machine met hetzelfde adres of `@name` en een andere rignaam. De pool telt shares, saldi en uitbetalingen per adres, zodat alle rigs bijdragen aan één saldo en één uitbetaling. Elke rig krijgt een eigen regel op de statistiekenpagina, zodat je eenvoudig ziet of elke machine mint.

## Hoe controleer je of je miner werkt?

Het dashboard van seine toont lokaal je hashrate en geaccepteerde shares. Voor de poolweergave open je [My Stats](/stats), voer je je adres, `@name` of `$name` in en druk je op **Lookup**. Bij een naam toont de pagina het gevonden adres naast het zoekveld. De browser onthoudt ook je laatste zoekopdracht. De [gids voor het volgen van je miner](/blog/nl/track-your-miner-on-bntpool/) bespreekt elke kaart.

Let op drie dingen:

- **Hashrate en Shares Accepted.** De hashratekaart toont wat de pool van je ziet en het aantal geaccepteerde shares loopt op terwijl je rigs werk insturen.
- **Een regel per rig.** Elke regel toont naam, hashrate, geaccepteerde en afgewezen shares en het tijdstip van de laatste share. Een recente share betekent dat de rig verbonden is en werkt.
- **Recent Shares.** De nieuwste shares staan vermeld met de rig die ze stuurde, hun moeilijkheid en status.

De pool stelt de sharemoeilijkheid per verbinding in en past die tijdens het minen aan. Zelfs een kleine CPU stuurt daardoor geregeld shares en je eerste geaccepteerde shares verschijnen binnen enkele minuten na het verbinden. Zie voor de hashrate van je hardware de [hardwaregids voor Blocknet-mining](https://seineminer.com/nl/blog/blocknet-mining-hardware-hashrate/).

Daarna volgen de beloningen. Estimated Rewards verschijnen zodra de pool een blok vindt terwijl je shares in het venster van zes uur zitten. Na 60 bevestigingen verhuist je aandeel naar Confirmed Rewards en stuurt de pool het automatisch zodra je minstens 0,1 BNT hebt. [Hoe PPLNS-uitbetalingen werken](/blog/nl/how-pplns-payouts-work/) legt de verdeling uit. Het poolbrede overzicht staat op [Blocks](/blocks) en [Payouts](/payouts).

## Wat staat er op Get Started?

De pagina [Get Started](/start) verzamelt alles op één scherm:

- **Poolinformatie.** Het Stratum-adres, met klikken om te kopiëren, het uitbetalingssysteem, minimum en aantal bevestigingen, rechtstreeks uit de pool gelezen.
- **Snel starten in drie stappen.** Download seine van [seineminer.com](https://seineminer.com/nl/#download), start het één keer en geef adres of naam en pool-URL op. Begin daarna met minen en volg hashrate en saldo op My Stats.
- **Een kopieerbare opdracht.** De volledige `./seine`-opdracht met de pool-URL al ingevuld. Vul je adres in, of `'@yourname'` tussen aanhalingstekens.
- **Instelnotities.** Waar seine de configuratie bewaart, hoe je die reset en welke opties haar overschrijven.
- **Hoe beloningen worden geteld.** Een korte uitleg van PPLNS: shares van vóór een gevonden blok verdienen aan dat blok, later werk telt voor volgende blokken. Ook staat er dat beloningen van onbevestigde blokken apart op My Stats blijven tot de bevestiging.
- **Schermafbeeldingen** van de eerste instelling en het miningdashboard van seine, zodat je weet wat je kunt verwachten.

<CTA target="pool" title="Begin met minen op bntpool.com" text="Verbind seine met bntpool.com met je adres of @name en zie je shares binnenkomen op My Stats." />
