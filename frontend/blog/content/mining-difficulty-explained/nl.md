---
title: 'Miningmoeilijkheid: netwerk en shares'
description: 'Lees hoe miningmoeilijkheid het doel bepaalt, hoe Blocknet dit aanpast en waarom netwerk- en sharemoeilijkheid verschillende miningcijfers beïnvloeden.'
tags: ['Miningbasis', 'Moeilijkheid', 'Blocknet']
faq:
  - q: 'Verlaagt hogere moeilijkheid mijn hashrate?'
    a: 'Moeilijkheid verandert het doel waaraan een resultaat moet voldoen, niet rechtstreeks de hashes per seconde. Bij dezelfde hashrate verlengt hogere netwerkmoeilijkheid de verwachte tijd tussen blokvondsten. Controleer minerprestaties apart van veranderingen in blok- of sharefrequentie.'
  - q: 'Betaalt een moeilijkere share meer?'
    a: 'Die vertegenwoordigt meer verwacht werk, maar wordt bij dezelfde hashrate minder vaak ingediend. Vergelijk moeilijkheidsgewogen werk over dezelfde periode. Op bntpool.com hangen beloningen ook af van geaccepteerde blokken en PPLNS, niet alleen van het aantal shares.'
  - q: 'Hoe vaak past Blocknet de moeilijkheid aan?'
    a: 'Na elk blok berekent Blocknet de moeilijkheid van het volgende uit de recente ketenhistorie. De huidige regel gebruikt een lineair gewogen voortschrijdend gemiddelde over 60 intervallen en streeft naar 300 seconden per blok. Recente intervallen wegen zwaarder; afzonderlijke blokken kunnen eerder of later komen.'
sourceHash: '5c203e8c7523'
translation: draft
---

Moeilijkheid verbindt het rekenwerk van je hardware met de blokken die het netwerk accepteert. Deze gids behandelt het doel, Blocknets aanpassing en de afzonderlijke sharemoeilijkheid. Begrijp veranderingen voordat je hashrate, verwachte bloktijd of de [beloningscalculator](/blog/nl/estimate-blocknet-mining-rewards/#mining-calculator) vergelijkt.

## Wat is miningmoeilijkheid?

<Answer>
Moeilijkheid beschrijft het verwachte hashwerk voor een resultaat dat aan een doel voldoet. Hogere moeilijkheid maakt zulke resultaten zeldzamer bij dezelfde hashrate. Bij Blocknet is netwerkmoeilijkheid ongeveer het verwachte aantal hashes per blok; sharemoeilijkheid hoort bij een afzonderlijk indieningsdoel.
</Answer>

Elke hash levert een getal dat de doelgrens moet halen. Een kleiner doel accepteert minder mogelijke resultaten en heeft hogere moeilijkheid. Blocknet deelt de grootste 256-bits waarde door de moeilijkheid om het 256-bits doel te verkrijgen. Verdubbeling verdubbelt dus ongeveer het verwachte werk zonder de hashbewerking te wijzigen.

## Hoe past Blocknet netwerkmoeilijkheid aan?

Na elk blok wordt de volgende moeilijkheid berekend. De huidige mainnetregel gebruikt een lineair gewogen voortschrijdend gemiddelde, LWMA, van 60 recente intervallen. Nieuwere tellen zwaarder. Het doel is 300 seconden per blok. Eerdere moeilijkheden en gewogen tijden zorgen dat aanhoudend snelle blokken de moeilijkheid doorgaans verhogen en langzame verlagen.

De code begrenst de gebruikte intervallen en hanteert een minimum. Voor 60 intervallen zijn 61 tijdstempels nodig; het is geen aanpassing slechts elke 60 blokken. Deze regels houden het tempo nabij het doel als rekenkracht verandert. Vijf minuten is een gemiddelde; een afzonderlijk blok kan veel eerder of later verschijnen.

## Netwerkmoeilijkheid en sharemoeilijkheid

Een pool geeft een sharedoel om bijdragen tussen blokvondsten te meten. Dat is doorgaans gemakkelijker dan het netwerkdoel. Beide beschrijven verwacht werk, maar beantwoorden verschillende vragen en worden door verschillende systeemonderdelen vastgesteld:

| Moeilijkheid | Wat deze bepaalt |
|---|---|
| Netwerk | De werkdrempel voor een kandidaatblok volgens de consensusregels. |
| Share | De indieningsdrempel van de pool voor een verbinding of miningopdracht. |

bntpool.com gebruikt variabele moeilijkheid, vardiff, om inzendingen te regelen. Hogere sharemoeilijkheid geeft bij dezelfde hashrate doorgaans minder shares met meer verwacht werk per share. Ze verandert de netwerkmoeilijkheid niet. Een resultaat dat ook het netwerkdoel haalt kan kandidaatblok worden; de daemon valideert het nog. De [sharegids](/blog/nl/accepted-rejected-stale-shares/) behandelt acceptatie en afwijzing.

## Hoe verandert de verwachte bloktijd?

Deel voor Blocknet netwerkmoeilijkheid door de werkende hashrate in H/s om de gemiddelde tijd in seconden te schatten. Gebruik waarden uit dezelfde periode. Het voorbeeld houdt beide constant om de rekensom te tonen. Het zijn illustraties, geen actuele metingen of voorspelling voor een specifieke miner.

**Verwachte seconden per blok ≈ netwerkmoeilijkheid ÷ hashrate in H/s**

| Netwerkmoeilijkheid | Hashrate | Verwacht interval |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Bij 30.000 en 100 H/s is het gemiddelde 300 seconden. Bij 60.000 en dezelfde hashrate wordt het 600 seconden. Verdubbelt ook de hashrate naar 200 H/s, dan wordt het weer 300 seconden. Dit zijn gemiddelden over herhaalde pogingen. De [poolgelukgids](/blog/nl/mining-pool-luck-explained/) verklaart afwijkende rondeduren.

## Waarom veranderen beloningen bij vaste hashrate?

Hogere netwerkmoeilijkheid verlaagt bij vaste eigen hashrate de verwachte blokvondsten per tijdseenheid zolang ze aanhoudt. Als totale hashrate groeit en moeilijkheid volgt, krimpt je aandeel in het netwerkwerk. De blokbeloning verandert apart via het [beloningsschema](/blog/nl/blocknet-block-reward-schedule/). Moeilijkheid bepaalt niet het aantal BNT in de geplande blokbeloning.

Gebruik verse netwerkgegevens in de calculator voor verwachte BNT-bedragen. Controleer of je hashrate van miner of pool komt om de minervergoeding juist toe te passen, met het tarief van jouw release. Schattingen zijn gemiddelden onder aannames. Betaling hangt ook af van gevonden blokken, bevestigingen en [PPLNS](/blog/nl/how-pplns-payouts-work/); moeilijkheid alleen voorspelt het betaalmoment niet.

## Welke moeilijkheid moet je bekijken?

Begin met je vraag. Voor blokvondsten gebruik je netwerkmoeilijkheid en hashrate van de betreffende miner of pool. Voor sharefrequentie gebruik je de moeilijkheid van die opdrachten. Voor saldo of betaling bekijk je [My Stats](/stats) en [betalingshistorie](/payouts). Noteer tijdstip, bereik en eenheden bij iedere vergelijking.

Het [pooldashboard](/) en de [blokhistorie](/blocks) geven actuele context. Netwerkhashrate wordt geschat uit recent werk en verstreken bloktijd en kan dus verschillen van volgende moeilijkheid gedeeld door 300 seconden. De [hashrategids](/blog/nl/what-is-hashrate/) legt dit uit. Houd moeilijkheid, hashrate en geluk apart bij onderzoek: ze beschrijven verschillende aspecten.

<CTA target="pool" title="Verbind je miner met bntpool.com" text="Gebruik de instelpagina en volg daarna shares en activiteit op My Stats." />
