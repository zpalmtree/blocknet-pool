---
title: Blocknet rechtstreeks naar een beurs minen
description: 'Mine Blocknet op bntpool.com rechtstreeks naar je stortingsadres bij SafeTrade of een andere beurs. Voeg je stortingsmemo toe in seine en de pool betaalt naar je account.'
tags: ['Mininggids', 'bntpool', 'SafeTrade', 'Uitbetalingen']
sourceHash: '8f73aae25c7a'
translation: draft
faq:
  - q: 'Kan ik Blocknet rechtstreeks naar een beurs minen?'
    a: 'Ja. Start seine v0.3.1 of nieuwer met het stortingsadres van je beurs en je stortingsmemo, zoals in ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com betaalt je beloningen met de memo erbij naar dat adres, zodat de beurs ze op je account bijschrijft.'
  - q: 'Waarom vraagt seine om een uitbetalingsmemo?'
    a: 'Beurzen zoals SafeTrade gebruiken één gedeeld stortingsadres voor al hun klanten en onderscheiden stortingen aan de hand van de memo. Seine kent deze gedeelde adressen, dus als je er een invult, vraagt het om je memo en slaat het die op bij het adres. Kopieer de memo precies van de stortingspagina van de beurs.'
  - q: 'Wat is de minimale uitbetaling naar een beurs op bntpool.com?'
    a: 'Uitbetalingen met een memo beginnen bij 10 BNT, zoals de pool op 4 november 2026 meldde. Je bevestigde saldo groeit op de pool tot het dat bedrag bereikt, daarna stuurt de pool het met je memo naar het beursadres. Uitbetalingen naar een eigen wallet beginnen bij 1 BNT.'
  - q: 'Hoe controleer ik mijn saldo bij minen naar een beurs?'
    a: 'Vul op de statistiekenpagina van bntpool.com het stortingsadres van de beurs in, kies Memo toevoegen en vul je memo in. Elk paar van adres en memo heeft een eigen saldo, dus je ziet alleen je eigen hashrate, rigs en uitbetalingen, niet die van andere klanten met hetzelfde beursadres.'
---

Deze gids laat zien hoe je op bntpool.com Blocknet rechtstreeks naar je beursaccount minet. Je geeft seine het stortingsadres van de beurs en je stortingsmemo, en de pool betaalt je beloningen daarheen met de memo erbij, zodat de beurs ze aan jou toekent. De gids behandelt de opdracht, de minimale uitbetaling en hoe je je saldo controleert.

## Kun je Blocknet rechtstreeks naar een beurs minen?

<Answer>
Ja. bntpool.com betaalt miningbeloningen naar een stortingsadres van een beurs met je stortingsmemo erbij. Start seine v0.3.1 of nieuwer met het beursadres en je memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Uitbetalingen met een memo beginnen bij 10 BNT, en de beurs schrijft elke uitbetaling op je account bij.
</Answer>

Minen naar een beurs scheelt een stap als je wilt handelen in wat je minet. Beloningen komen direct op je beursaccount binnen, klaar om te verkopen op de [BNT/USDT-markt](/blog/nl/sell-blocknet-bnt-safetrade/), zonder walletoverboeking ertussen.

Beurzen zoals SafeTrade geven al hun klanten hetzelfde stortingsadres en onderscheiden stortingen aan de hand van een memo, een korte code die uniek is voor je account. Daarom is de memo belangrijk: die koppelt een uitbetaling aan jou. De pool houdt voor elk paar van adres en memo een apart saldo bij, zodat je beloningen nooit vermengd raken met die van andere miners die dezelfde beurs gebruiken.

## Hoe stel je minen naar een beurs in seine in?

<Answer>
Kopieer het BNT-stortingsadres en je stortingsmemo van de stortingspagina van de beurs. Start seine met allebei: --address voor het adres en --payout-memo voor de memo. Vul je bij de eerste start een bekend beursadres in, dan vraagt seine zelf om de memo en slaat die op. Controleer de bevestiging van de pool in het logboek en mine daarna gewoon.
</Answer>

1. **Haal je stortingsgegevens op.** Open op de beurs de BNT-stortingspagina en kopieer het stortingsadres en de memo. Kopieer de memo precies: hoofdletters en voorloopnullen tellen mee.
2. **Start seine met allebei.** Op macOS of Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Op Windows, in PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Laat seine ze opslaan.** Seine slaat de memo op bij het adres, dus latere starts blijven naar je beursaccount betalen. Typ je bij de eerste start een bekend beursadres, dan vraagt seine om de memo en slaat het beide op.

De [Get Started-pagina](/start) van de pool heeft een sectie ‘Minen naar een beurs?’ die deze opdracht voor je samenstelt: plak het adres en de memo en kopieer het resultaat. Seine vereist een memo voor bekende gedeelde beursadressen, zoals dat van SafeTrade. Services, scripts en andere miners zonder toezicht moeten `--payout-memo` op de opdrachtregel meegeven, omdat er geen vraag is om te beantwoorden.

Seine controleert de memo bij de pool wanneer het inlogt. bntpool.com bevestigt hem en daarna begint het minen. Geef je rigs een naam met `--pool-worker rig-01`, net als bij een eigen adres.

## Hoe worden uitbetalingen naar een beurs gedaan?

Beloningen volgen dezelfde weg als elke bntpool.com-uitbetaling. Je shares verdienen een deel van elk blok dat de pool binnen zijn PPLNS-venster van zes uur vindt, en de beloning van elk blok wordt na 60 blokken bevestigd. Je bevestigde saldo groeit daarna op de pool.

Het `/api/info`-endpoint van de pool meldde op 4 november 2026 deze minimale uitbetalingen:

| Bestemming | Minimale uitbetaling |
|---|---|
| Beursadres met een memo | 10 BNT |
| Eigen wallet | 1 BNT |

Zodra je bevestigde saldo 10 BNT bereikt, stuurt de pool het met je memo naar het beursadres. De beurs schrijft de storting daarna bij na haar eigen aantal bevestigingen; de stortingspagina vermeldt hoeveel ze nodig heeft. [Hoe PPLNS-uitbetalingen werken](/blog/nl/how-pplns-payouts-work/) legt de kant van de pool uitgebreid uit.

Solo minen werkt ook met een memo. Zet `solo:` voor het beursadres en houd `--payout-memo`, en de pool betaalt de volledige beloning van elk blok dat je vindt naar je beursaccount. [Solo minen op bntpool.com](/blog/nl/solo-mine-blocknet-on-bntpool/) legt solobeloningen uit.

## Hoe controleer je je saldo?

Open de [statistiekenpagina](/stats), plak het stortingsadres van de beurs, kies **Memo toevoegen** en vul je memo in. De pagina toont dan je eigen hashrate, rigs, saldo en uitbetalingen voor dat paar van adres en memo. Andere klanten van dezelfde beurs hebben hun eigen paren en hun eigen saldi.

Elke uitbetaling in **Recente Uitbetalingen** linkt naar haar transactie in de explorer. Vergelijk die met de stortingsgeschiedenis op de beurs om de munten tot op je account te volgen. [Je miner volgen op bntpool.com](/blog/nl/track-your-miner-on-bntpool/) legt de rest van de statistiekenpagina uit.

## Hoe schakel je terug naar een eigen wallet?

Start seine één keer met je nieuwe adres en `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

Seine slaat het nieuwe adres op zonder memo, en nieuwe shares tellen mee voor je eigen wallet. Een saldo dat al op het beurspaar is bijgeschreven, blijft bij dat paar en wordt eraan uitbetaald zodra het het minimum bereikt; je kunt het volgen op de statistiekenpagina. [Je miningwalletadres wijzigen](/blog/nl/change-mining-wallet-address/) legt uit hoe de pool werk bijschrijft na een adreswijziging.

## Welke setups ondersteunen memo's?

Memo's vereisen de rechtstreekse verbinding van seine met de pool, wat de standaard is. De daemonmodus van seine minet op je eigen node en betaalt naar de wallet van de node, dus die heeft geen memo. De farmproxy geeft geen memo's door, dus verbind beursrigs rechtstreeks met de pool. Seine minet alleen op een pool die je memo bevestigt, zodat je werk altijd wordt uitbetaald aan het account dat je koos.

<CTA target="pool" title="Mine naar je beursaccount" text="Start seine met je stortingsadres en memo, en bntpool.com betaalt je beloningen vanaf 10 BNT rechtstreeks naar je beursaccount." />
