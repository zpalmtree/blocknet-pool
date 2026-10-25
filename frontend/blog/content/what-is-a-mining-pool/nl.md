---
title: 'Wat is een miningpool?'
description: 'Een miningpool bundelt de hashrate van miners en verdeelt elke blokbeloning. Lees hoe pools werken, wat PPLNS, PPS en solo betekenen en waarop je let.'
tags: ['Miningbasis', 'Miningpools', 'PPLNS', 'bntpool']
sourceHash: 7b8c04cd86ae
translation: draft
faq:
  - q: 'Verdien ik meer door bij een miningpool aan te sluiten?'
    a: 'Een pool verandert je hashrate of je aandeel in het netwerkwerk niet. Wel verandert de manier waarop je beloningen ontvangt. Alleen wacht je op hele blokken met willekeurige tussenpozen. In een pool krijg je een klein deel van elk gevonden poolblok, dus vaker betalingen die je hashrate volgen.'
  - q: 'Wat is het verschil tussen PPLNS en PPS?'
    a: 'PPLNS betaalt een deel van elk werkelijk gevonden poolblok, verdeeld volgens het werk binnen een recent venster. PPS betaalt een vast bedrag per geaccepteerde share, berekend uit zijn verwachte waarde, ongeacht of de pool een blok vindt. bntpool.com gebruikt PPLNS met een venster van zes uur.'
  - q: 'Moet ik voor poolmining een eigen node draaien?'
    a: 'Nee. De pool draait de node, bouwt bloktemplates, controleert shares en dient gevonden blokken in. Op bntpool.com heb je alleen seine en een Blocknet-adres of Blocknet ID-naam nodig. Start seine, plak je adres en accepteer de standaardpool. Je deel van elk blok wordt aan dat adres betaald.'
  - q: 'Hoe begin ik op bntpool.com?'
    a: 'Download seine voor Windows, macOS of Linux van seineminer.com. Start het en plak je Blocknet-adres of @name wanneer daarom wordt gevraagd. Accepteer bntpool.com, de standaardpool, en begin met minen. Zoek je adres op de statistiekenpagina om je hashrate, shares en saldo te bekijken.'
---

Deze gids legt uit wat een miningpool is, waarom de meeste miners eraan deelnemen en hoe een pool bepaalt wat ieder verdient. Je leert de gebruikelijke systemen PPLNS, PPS en solo kennen en weet wat je moet controleren voordat je verbindt. De Blocknet-miningpool bntpool.com dient overal als praktijkvoorbeeld.

## Wat is een miningpool?

<Answer>
Een miningpool is een server die de [hashrate](/blog/nl/what-is-hashrate/) van veel miners bundelt om samen blokken te vinden. De pool geeft iedereen werk aan hetzelfde blok, telt het ingeleverde werk en verdeelt de blokbeloning naar verhouding daarvan. Miners ontvangen kleine, frequente betalingen in plaats van zeldzame hele blokken.
</Answer>

Bij een proof-of-work-munt concurreren miners om het volgende blok. Ze hashen de header met verschillende nonces totdat een resultaat onder het netwerkdoel valt. Elke hash is een onafhankelijke poging met dezelfde kleine kans. Wie de winnende hash vindt, ontvangt de blokbeloning.

Een pool maakt daar teamwerk van. Iedereen op bntpool.com werkt aan blokken die de pool bouwt. Vindt één miner een winnende hash, dan wordt de beloning gedeeld met alle miners die werk bijdroegen.

## Waarom sluiten miners zich bij een pool aan?

<Answer>
Miners kiezen een pool voor regelmatige betalingen. Een doorsneeminer heeft alleen een klein deel van de netwerkhashrate en kan dagen tussen blokken wachten, met willekeurige tussenpozen. De gebundelde hashrate van een pool vindt vaker blokken. Iedere miner verdient aan elk blok, waardoor de opbrengst regelmatig binnenkomt en zijn hashrate volgt.
</Answer>

De berekening is op Blocknet eenvoudig te controleren. Het netwerk mikt op één blok per vijf minuten, 288 per dag. Een solominer kan dus 288 × (eigen hashrate ÷ netwerkhashrate) blokken per dag verwachten.

Neem ronde, hypothetische waarden: een rig van 50 H/s op een netwerk van 50.000 H/s bezit 0,1% van het netwerk en vindt gemiddeld 0,288 blokken per dag, ongeveer één per drieënhalve dag. Dat is alleen een gemiddelde. Blokken vinden is willekeurig; de werkelijke tussenpoos kan veel korter of langer zijn.

Zet dezelfde rig op een pool van in totaal 5.000 H/s. Die vindt ongeveer 28,8 blokken per dag, gemiddeld één per 50 minuten. De rig leverde 1% van het poolwerk en verdient 1% van elk blok. Zijn bijdrage blijft hetzelfde, maar de beloning komt vele malen per dag in kleine delen. [Pool versus solo voor Blocknet](/blog/nl/pool-vs-solo-mining-blocknet/) werkt deze vergelijking uit voor grotere opstellingen.

## Hoe werkt een miningpool?

Elke pool volgt dezelfde basiscyclus. Zo doet bntpool.com dat:

1. **De pool bouwt een bloktemplate.** bntpool.com draait een eigen Blocknet-node en haalt daar templates op. Miners hoeven zelf geen node te draaien of synchroniseren.
2. **Hij stuurt werk naar miners.** Miners verbinden via Stratum, het protocol waarmee pools werk uitdelen, op `stratum+tcp://bntpool.com:3333`. Elke verbinding krijgt een taak op basis van de huidige template.
3. **Miners hashen en sturen shares in.** Een share is een hash die aan een eenvoudiger pooldoel voldoet. Shares bewijzen hoeveel werk iedere miner doet.
4. **Een share die ook het netwerkdoel haalt, is een blok.** De pool controleert beide doelen voor elke share en dient een geldig netwerkblok bij zijn node in.
5. **De beloning wordt verdeeld en betaald.** De pool volgt zijn uitbetalingssysteem en stuurt ieders aandeel zodra het bevestigd is en het minimum haalt.

## Wat is een share in een miningpool?

Een share is een hash die een lagere lat haalt dan een echt blok. Een blok vereist een hash onder het netwerkdoel; een share alleen onder het veel eenvoudigere pooldoel. De meeste shares worden nooit blokken, maar laten geregeld zien hoeveel hashing elke miner uitvoert.

Shares wegen naar moeilijkheid. Op Blocknet betekent moeilijkheid D dat gemiddeld ongeveer D hashes nodig zijn om het doel te halen. Een share op moeilijkheid 60 vertegenwoordigt dus circa 60 hashes werk. bntpool.com past de moeilijkheid per verbinding aan, met als doel ongeveer 10 shares per vijf minuten en een beginwaarde van 60. Een snelle server krijgt moeilijkere shares dan een laptop, maar beide krijgen credit voor hun werkelijke werk. De pool onthoudt de moeilijkheid ook een uur, zodat een snelle herstart verdergaat waar de rig gebleven was.

## Gebruikelijke uitbetalingssystemen

Het uitbetalingssysteem bepaalt hoe shares opbrengsten worden. Dit zijn de meest voorkomende vormen.

| Systeem | Waarvoor je wordt betaald | Hoe het werkt |
|---|---|---|
| PPLNS (pay per last N shares) | Gevonden poolblokken | Elke beloning wordt verdeeld over shares in een recent venster, gewogen naar moeilijkheid |
| PPS (pay per share) | Elke geaccepteerde share | De pool betaalt een vast bedrag naar de verwachte waarde, ongeacht gevonden blokken |
| Solo | Blokken die je zelf vindt | Je mint op je eigen node en houdt de volledige beloning van elk eigen blok |

**PPLNS** kijkt terug vanaf ieder gevonden blok. Op bntpool.com zijn de «laatste N shares» een venster van zes uur. De pool telt de moeilijkheid van alle geaccepteerde shares in die zes uur op en geeft iedere miner hetzelfde aandeel in de beloning als in dat werk. Een share telt voor elk blok terwijl hij in het venster zit. Een doorlopende miner heeft steeds een vol venster achter zich; herstarten reset niets. [Hoe PPLNS-uitbetalingen werken op bntpool.com](/blog/nl/how-pplns-payouts-work/) legt dit stap voor stap uit.

**PPS** betaalt elke share bij binnenkomst volgens zijn verwachte waarde: sharemoeilijkheid gedeeld door netwerkmoeilijkheid, maal blokbeloning. De poolbeheerder draagt de schommelingen in blokvondsten en betaalt tussendoor uit zijn eigen saldo.

**Solomining** gebruikt geen pool. Je node bouwt templates en elk eigen blok betaalt volledig aan je wallet. Dit past bij opstellingen die zelfstandig vaak blokken vinden. Seine ondersteunt beide: standaard bntpool.com, of `./seine --mode daemon` voor je eigen gesynchroniseerde Blocknet-node.

## Waarop let je bij een miningpool?

Deze details bepalen hoe je wordt betaald en hoe eenvoudig je dat controleert:

- **Een helder uitbetalingssysteem.** De pool hoort het systeem en de instellingen, zoals het PPLNS-venster, te vermelden. bntpool.com doet dat op [Get Started](/start) en `https://bntpool.com/api/info`.
- **Bevestigingen en minimumuitbetaling.** Beloningen wachten doorgaans tot het blok diep genoeg ligt. Betalingen volgen zodra je saldo het minimum bereikt. Op bntpool.com is een beloning na 60 blokken bevestigd, gelijk aan Blocknets coinbase maturity. Saldi vanaf 0,1 BNT worden automatisch betaald.
- **Openbare, controleerbare cijfers.** Je moet elk gevonden blok en elke betaling kunnen zien. bntpool.com toont ze op [Blocks](/blocks) en [Payouts](/payouts), effort per ronde op [Luck](/luck) en de gezondheid van diensten op [Status](/status).
- **Je eigen statistieken.** Zoek je adres voor hashrate, shares en saldi. Dat kan op de [statistiekenpagina](/stats); de [gids voor het volgen van je miner](/blog/nl/track-your-miner-on-bntpool/) legt elke kaart uit.
- **Eenvoudige instelling.** Minder instellingen betekent sneller minen. bntpool.com is de standaard in seine, dat threads, kernels en geheugen zelf afstemt. Je kunt inloggen met een [Blocknet ID-naam](https://seineminer.com/nl/blog/blocknet-id-pay-by-name/) zoals `@name` in plaats van een volledig adres.

## bntpool.com in één overzicht

| Instelling | bntpool.com |
|---|---|
| Uitbetalingssysteem | PPLNS |
| Sharevenster | 6 uur |
| Bevestigingen voor uitbetaling | 60 blokken |
| Minimumuitbetaling | 0,1 BNT |
| Stratum-adres | `stratum+tcp://bntpool.com:3333` |

Het [pooldashboard](/) toont de actuele situatie: verbonden miners, pool- en netwerkhashrate, gevonden blokken en effort van de huidige ronde. Op 25 oktober 2026 om 02:25 EDT draaide de pool op circa 5.200 H/s binnen een netwerk van ongeveer 44.400 H/s, met 246 verbonden miners. Dat is circa 12% van het netwerk, goed voor naar verwachting 34 blokken per dag, ongeveer één per 42 minuten. Deze cijfers veranderen voortdurend. Kijk op het dashboard voor actuele waarden en gebruik de [gids voor beloningsschattingen](/blog/nl/estimate-blocknet-mining-rewards/) om ze naar BNT voor jouw hashrate om te rekenen.

Om deel te nemen, [download je seine](https://seineminer.com/nl/#download), start je het, plak je je Blocknet-adres of `@name` en accepteer je de standaardpool. De [mininggids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) behandelt elke stap. [Blocknet minen op bntpool.com](/blog/nl/mine-blocknet-on-bntpool/) bespreekt rignamen en het controleren van je eerste shares.

<CTA target="pool" title="Sluit aan op bntpool.com" text="Verbind seine met de pool en verdien aan elk gevonden blok, automatisch naar je adres verstuurd zodra je 0,1 BNT bereikt." />
