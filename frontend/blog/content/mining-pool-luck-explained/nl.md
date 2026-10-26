---
title: 'Geluk bij miningpools: inspanning en tijd tussen blokken'
description: 'Begrijp poolgeluk, 100% inspanning en lange rondes. Lees de rondehistorie en verwachte bloktijd van bntpool.com en onderscheid blokvondsten van uitbetalingen.'
tags: ['Miningpools', 'Poolgeluk', 'Blocknet']
faq:
  - q: 'Betekent 100% inspanning dat de pool bijna een blok vindt?'
    a: 'Nee. Het betekent dat de geregistreerde share-arbeid de hoeveelheid heeft bereikt die voor één blok wordt verwacht bij de moeilijkheid in die berekening. Het is een vergelijking met een gemiddelde, geen voltooiingspercentage. Iedere nieuwe hash moet nog steeds aan het netwerkdoel voldoen.'
  - q: 'Is een lager gelukspercentage beter op bntpool.com?'
    a: 'Lees eerst het label en de definitie. De kolom Inspanning en kaart Gemiddeld geluk op bntpool.com vergelijken werk met verwacht werk. Lagere waarden betekenen minder geregistreerd werk per blok. Andere sites gebruiken soms de omgekeerde conventie. Vergelijk overeenkomstige metingen en steekproefgroottes in plaats van alleen percentages.'
  - q: 'Verhoogt een lange ronde mijn volgende uitbetaling?'
    a: 'Een lange ronde garandeert geen hogere persoonlijke uitbetaling. Een blokvondst bepaalt wanneer een beloning beschikbaar komt; de PPLNS-administratie bepaalt je aandeel op basis van in aanmerking komend geaccepteerd werk. Daarna gelden bevestigings- en uitbetalingsregels. Bekijk je saldi bij Mijn statistieken en de volgorde in de uitbetalingsgids.'
sourceHash: dfd8c710a719
translation: draft
---

Het pooldashboard toont zowel het miningwerk als de blokken die daarmee worden gevonden. Deze gids legt uit hoe je inspanningspercentages, verwachte bloktijden en de rondehistorie op bntpool.com leest. Je leert een statistisch gemiddelde van een aftelklok onderscheiden en kunt je miningverbinding controleren zonder op het volgende poolblok te wachten.

## Wat is geluk bij een miningpool?

<Answer>
Poolgeluk beschrijft hoe werkelijke blokvondsten zich verhouden tot het gemiddelde dat op basis van het werk wordt verwacht. Blokken worden willekeurig gevonden: sommige vergen weinig werk, andere meer. Op bntpool.com vergelijkt ronde-inspanning geaccepteerde share-arbeid met de blokmoeilijkheid; lagere inspanning betekent dat de ronde haar blok met minder geregistreerd werk vond.
</Answer>

Een pool bundelt pogingen van vele miners, maar moet nog steeds een hash vinden die het netwerkdoel haalt. Geaccepteerde shares meten bijdragen met een makkelijker doel. Daarmee schat de pool hoeveel hashwerk tussen vondsten plaatsvond. De [inleiding over miningpools](/blog/nl/what-is-a-mining-pool/) legt dat verband uit; geluk beschrijft de variatie in de resulterende blokken.

## Hoe berekent de pool inspanning?

Voor een afgeronde ronde deelt bntpool.com geaccepteerde share-arbeid door de blokmoeilijkheid en vermenigvuldigt met 100. Share-arbeid is gewogen naar moeilijkheid en is dus niet simpelweg het aantal ingediende shares. Een share met een hogere toegewezen moeilijkheid staat voor meer verwacht hashwerk dan een share met een lagere moeilijkheid.

**Inspanning (%) = geaccepteerde share-arbeid ÷ blokmoeilijkheid × 100**

Stel dat een blok moeilijkheid 10.000 heeft en de pool tijdens die ronde 5.000 eenheden geaccepteerd werk registreert. De inspanning is 50%. Bij 20.000 eenheden en dezelfde blokmoeilijkheid zou dat 200% zijn. Dit zijn rekenvoorbeelden, geen metingen van een specifiek productieblok.

| Inspanning | Betekenis |
|---|---|
| 50% | De helft van het verwachte werk voor die moeilijkheid |
| 100% | De verwachte hoeveelheid werk voor één blok |
| 200% | Tweemaal het verwachte werk voor die moeilijkheid |

De inspanning van de huidige ronde gebruikt de huidige moeilijkheid, die kan veranderen terwijl de keten verdergaat. Historische rijen gebruiken de moeilijkheid van hun blok. Het getal vergelijkt dus werk; het is geen voortgangsmeter die bij 100% moet stoppen. Het verschilt ook van de rondeduur: een snellere pool kan hetzelfde werk in minder tijd doen.

## Wat betekent verwachte bloktijd?

Dat is het gemiddelde interval dat volgt uit het geschatte aandeel van de pool in de netwerkhashrate. Het dashboard van bntpool.com berekent dit als 300 seconden maal de netwerkhashrate gedeeld door de poolhashrate. Gebruik voor beide dezelfde eenheden. De [hashrategids](/blog/nl/what-is-hashrate/) legt uit waarom deze schattingen veranderen wanneer geaccepteerde shares en nieuwe blokken binnenkomen.

**Verwachte seconden = 300 × netwerk-H/s ÷ pool-H/s**

Als het netwerk op 100.000 H/s wordt geschat en de pool op 10.000 H/s, heeft de pool een tiende van het geschatte totaal. Het verwachte interval is 3.000 seconden, oftewel 50 minuten. Dat gemiddelde kan een combinatie van korte en lange rondes beschrijven; het plant het volgende blok niet precies 50 minuten vanaf nu in.

Het dashboard vergelijkt ook verstreken tijd met verwachte tijd. Dit tijdspercentage kan verschillen van de inspanning op basis van shares, omdat de invoergegevens verschillen. Controleer het label voordat je twee getallen vergelijkt. Veranderingen in poolhashrate of netwerkmoeilijkheid kunnen de huidige schatting verschuiven terwijl dezelfde ronde nog loopt.

## Is na 100% inspanning een blok verschuldigd?

<Answer>
De pool heeft geen blok tegoed zodra de inspanning 100% bereikt. Eerdere mislukte hashes vergroten de kans niet dat de volgende een ongewijzigd netwerkdoel haalt. Bij constante hashrate en moeilijkheid blijft de verwachte wachttijd vanaf nu gelijk, ook na een lange ronde. Meer miningwerk levert meer pogingen op, geen deadline.
</Answer>

Een eenvoudig kansmodel maakt dit duidelijk. Bij onafhankelijke pogingen en een constante blokvindsnelheid is de wachttijd ongeveer exponentieel verdeeld. De kans op nog geen blok na één verwacht interval is ongeveer 36,8%; na twee ongeveer 13,5%; na drie ongeveer 5,0%. Deze percentages beschrijven veel hypothetische rondes onder vaste omstandigheden, geen voorspelling voor de huidige poolronde.

| Verstreken verwachte intervallen | Geschatte kans op nog geen blok |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

De berekening is `exp(-t / T)`, waarbij `T` het gemiddelde interval is en `t` de verstreken tijd. De [NIST-referentie over de exponentiële verdeling](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) geeft deze overlevingsfunctie. Werkelijke hashrate en moeilijkheid variëren: beschouw de tabel als uitleg van toeval. Ook een reeks snelle blokken verandert de kans voor de volgende hash niet zolang het doel gelijk blijft.

## Hoe lees je de gelukshistorie van bntpool.com?

Open [Geluk](/luck) om afgeronde rondes te vergelijken. Iedere rij toont een blok, inspanning, rondeduur, status en vindtijd. Bekijk meerdere rondes samen in plaats van één uitzonderlijk korte of lange rij te kiezen. Via de bloklink bekijk je het bijbehorende ketenrecord; met de paginering onderzoek je een langere historie.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Echte gelukshistorie van bntpool.com met inspanning, rondeduur en blokstatus" width="1200" height="833" loading="lazy" />

Deze schermafbeelding is gemaakt op 26 oktober 2026. De rijen en statussen zijn een historische momentopname; gebruik de livepagina voor actuele waarden. Een status in afwachting of bevestigd beschrijft de bevestiging van het blok. Dat verandert de betekenis van de geregistreerde inspanning niet en staat los van een betaling aan jouw adres.

Op het dashboard is Gemiddeld geluk het rekenkundig gemiddelde van geregistreerde inspanningspercentages voor niet-verweesde blokken tot de huidige ketenhoogte. Ondanks de naam volgt de kaart de inspanningsconventie: lager betekent minder geregistreerd werk per blok. Gemiddelden vatten die steekproef samen. Ze beloven niet dat toekomstige rondes een bepaald eerder resultaat compenseren.

## Hoe beïnvloedt poolgeluk je beloningen?

Poolgeluk verandert wanneer blokken worden gevonden. PPLNS bepaalt hoe de beloning van een gevonden blok wordt verdeeld over in aanmerking komend geaccepteerd werk. Dat zijn verschillende stappen. Een rondegrens maakt het PPLNS-venster geen uitbetalingsregel die alleen op die ronde slaat; lees [hoe PPLNS-uitbetalingen werken](/blog/nl/how-pplns-payouts-work/) voor het daadwerkelijke venster, bevestigingen en de uitbetalingsdrempel.

Je saldo kan daarom stapsgewijs veranderen terwijl je miner geaccepteerde shares blijft indienen. De [beloningscalculator](/blog/nl/estimate-blocknet-mining-rewards/#mining-calculator) schat opbrengsten uit hashrate en netwerkgegevens. Hij helpt bij verwachtingen over langere tijd; hij voorspelt geen blok of het moment waarop een betaling je wallet bereikt.

## Wat controleer je tijdens een lange ronde?

Controleer wat je direct kunt waarnemen: je miner draait, shares blijven geaccepteerd worden en het juiste adres staat bij [Mijn statistieken](/stats). Gebruik [Status](/status) voor de openbare servicestatus. Stoppen je eigen geaccepteerde shares, volg dan de [probleemoplossing voor minerstatistieken](/blog/nl/track-your-miner-on-bntpool/#zero-hashrate); een lange poolronde en een losgekoppelde rig vragen om andere controles.

Als je verbinding goed werkt, beoordeel geluk dan over een betekenisvolle historie met dezelfde definities. Zo heeft ieder dashboardgetal een duidelijke rol: hashrate schat werk per seconde, inspanning vergelijkt werk met het verwachte werk voor een blok en saldi tonen de beloningsadministratie. Daarvoor hoef je niet te raden wanneer de volgende winnende hash komt.

<CTA target="pool" title="Begin met minen op bntpool.com" text="Verbind seine, controleer je geaccepteerde shares en volg rondes en beloningen op het pooldashboard." />
