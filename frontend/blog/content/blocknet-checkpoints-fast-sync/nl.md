---
title: 'Een Blocknet-node snel synchroniseren met bntpool-checkpoints'
description: 'Blocknet-nodes synchroniseren snel met checkpoints.dat van bntpool.com. Lees hoe checkpoints de keten vastleggen en download, controleer en laad het bestand.'
tags: ['Blocknet-node', 'Checkpoints', 'Snelle synchronisatie', 'bntpool']
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Waar haalt een Blocknet-node zijn checkpoints vandaan?'
    a: 'Van bntpool.com. Als bij de eerste start geen checkpoints.dat in de datamap staat, downloadt de daemon het van https://bntpool.com/checkpoints.dat voor snelle synchronisatie. Je kunt het ook zelf van de Checkpoints-pagina downloaden, de SHA-256 controleren en het vóór de start in de datamap zetten.'
  - q: 'Hoe vaak werkt bntpool.com checkpoints.dat bij?'
    a: 'De daemon van de pool herschrijft het bestand terwijl de keten groeit, met één vermelding per honderdste blok. Elke vermelding ligt minstens 100 blokken onder de ketentop, voorbij de maximale reorganisatiediepte van Blocknet. Checkpoints dekken dus alleen vaststaande geschiedenis. De Checkpoints-pagina toont de laatste hoogte en wijzigingstijd.'
  - q: 'Veranderen checkpoints mijn walletsaldo of geschiedenis?'
    a: 'Nee. Een node slaat nog steeds elk blok op en neemt elke output en key image op in zijn indexen. Walletscans en saldi werken dus precies zoals na volledige synchronisatie. Checkpoints laten alleen hercontrole van proof of work en handtekeningen overslaan voor geschiedenis die hun hashes al bevestigen.'
  - q: 'Kan ik een Blocknet-node zonder checkpoints synchroniseren?'
    a: 'Ja. Start de daemon met --full-sync of zet full_sync op true in de blocknet-configuratie. De node slaat de checkpointdownload over en controleert proof of work en handtekeningen voor elk blok vanaf genesis opnieuw. Dat duurt langer, daarom zijn checkpoints de standaard voor nieuwe nodes.'
---

Een nieuwe Blocknet-node moet alle blokken sinds genesis bijwerken voordat hij een wallet of solominer kan bedienen. Checkpoints versnellen dat. Deze gids legt uit wat het bestand van bntpool.com bevat, hoe de daemon de langzaamste controles overslaat terwijl hij aan de echte keten gebonden blijft en hoe je het bestand zelf downloadt, verifieert en laadt.

## Wat zijn Blocknet-checkpoints?

<Answer>
Blocknet-checkpoints zijn een lijst bekende blokhashes, één per honderdste blok, die bntpool.com als checkpoints.dat aanbiedt. Een synchroniserende node controleert of ontvangen blokken bij die hashes passen en slaat tot het laatste checkpoint het opnieuw verifiëren van proof of work en handtekeningen over. De eerste synchronisatie is daardoor veel sneller klaar.
</Answer>

Het bestand is gewone tekst met één paar `height:hash` per regel. De eerste regels zagen er op 25 oktober 2026 zo uit:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Elke hash is de blok-ID: de SHA3-256-hash van de header. De pooldaemon schrijft het bestand vanuit zijn keten en neemt alleen blokken op die minstens 100 blokken diep liggen. Honderd blokken is de harde finaliteit van Blocknet, de diepste reorganisatie die het protocol toestaat. Elk checkpoint ligt dus in vaststaande geschiedenis.

Op 25 oktober 2026 om 02:04 EDT bevatte het bestand 689 checkpoints, van hoogte 100 tot 68.900, in 48.811 bytes. Ongeveer elke 100 blokken komt er een regel bij, circa acht uur bij blokken van vijf minuten. Kijk op [Checkpoints](/checkpoints) voor de huidige hoogte.

## Hoe gebruikt een Blocknet-node checkpoints?

Een node met geladen checkpoints neemt de laatste checkpointhoogte als doel voor snelle synchronisatie. Tot die hoogte vertrouwt hij voor de twee duurste controles op de gepubliceerde checkpoints. Alle andere controles lopen normaal:

- **Proof of work rust op de checkpoints.** Elk Blocknet-blok gebruikt een Argon2id-hash over 2 GiB geheugen. Die voor elk historisch blok opnieuw berekenen kost veel werk, terwijl de checkpointhashes die geschiedenis al bevestigen.
- **RingCT-handtekeningen en range proofs ook.** Het daemoncommentaar noemt controle van output-range-proofs de grootste afzonderlijke synchronisatiekost: elke output heeft er één, inclusief elke coinbase.
- **Alle overige controles blijven actief.** Blokverbindingen, de LWMA-moeilijkheidsregel, tijdstempelregels, blokgrootte en -vorm, dubbele besteding via key images en de koppeling van elke input aan zijn handtekening worden normaal gecontroleerd. Outputs en key images gaan gewoon in de indexen, dus walletscans en saldi zijn gelijk aan die na een volledige synchronisatie.

Overslaan geldt alleen voor blokken die de huidige ketentop verlengen en alleen onder het laatste checkpoint. Daarna controleert de node elk nieuw blok volledig, inclusief proof of work.

### Hoe checkpoints de vertrouwde keten vastleggen

Tijdens synchronisatie naar het laatste checkpoint moet ieder blok op een checkpointhoogte exact de hash uit het bestand hebben. Een andere hash wordt afgewezen met een checkpoint mismatch. Omdat checkpoints elke 100 blokken voorkomen, kan een peer de node niet langs een andere geschiedenis leiden: het volgende checkpoint zou dat onthullen.

Checkpoints versnellen ook herstarts. Als er al blokken op schijf staan, zoekt de daemon het hoogste checkpoint dat bij de opgeslagen keten past. Hij controleert alleen de blokken erna opnieuw, in plaats van de volledige keten vanaf genesis te doorlopen.

## Downloadt een nieuwe node automatisch checkpoints?

<Answer>
Ja. Als bij het starten van de Blocknet-daemon geen checkpoints.dat in de datamap staat, downloadt hij het van https://bntpool.com/checkpoints.dat, laadt het en synchroniseert snel tot het laatste checkpoint. De download is een best-effort-poging: lukt die niet, dan start de node toch en verifieert hij de volledige keten.
</Answer>

Dit adres staat als standaard in Blocknet core zelf. Nieuwe nodes gebruiken dus direct het bestand van bntpool.com. De daemon haalt het alleen op als het ontbreekt, geeft het verzoek 15 seconden en begrenst de download op 32 MiB.

## Hoe gebruik je bntpool.com-checkpoints stap voor stap?

Wil je zelf downloaden en controleren, of een oude kopie vernieuwen vóór een lange hersynchronisatie, volg dan deze stappen.

### 1. Zoek je datamap

Het bestand staat in de datamap van de node, naast de ketendatabase:

- **De `blocknet`-manager:** `~/.config/bnt/data/mainnet/` op macOS en Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` op Windows, tenzij je `data_dir` hebt ingesteld.
- **Core rechtstreeks gestart:** `./blocknet-data-mainnet` in de startmap, of de map die je met `--data` opgeeft.

### 2. Download checkpoints.dat

Open [Checkpoints](/checkpoints) en druk op **Download checkpoints.dat**, of haal het via een terminal op. Dit voorbeeld schrijft naar de standaarddatamap van core:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Een download over een bestaand bestand vervangt het: zo vernieuw je een oude kopie.

### 3. Controleer de SHA-256

De Checkpoints-pagina publiceert de SHA-256 van het huidige bestand. Bereken die van jouw kopie en vergelijk:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

Gebruik op macOS `shasum -a 256`. De hash verandert telkens wanneer de pool een checkpoint toevoegt, dus vergelijk direct na de download.

### 4. Controleer een checkpoint in de explorer

Je kunt elke regel vergelijken met de openbare [Blocknet-explorer](https://explorer.blocknetcrypto.com). Open de blokpagina van een hoogte in het bestand, bijvoorbeeld `explorer.blocknetcrypto.com/block/68900`, en vergelijk de hash met de regel voor 68.900. Voor het laatste checkpoint op 25 oktober 2026 hebben we dit gedaan; de hashes kwamen overeen.

### 5. Start de node

Start of herstart de daemon. Hij leest `checkpoints.dat` bij het starten en synchroniseert snel tot het laatste checkpoint. Gebruik je de interactieve shell van core (`--cli`), dan laadt `load-checkpoints` het bestand in een draaiende node en meldt de hoogte voor snelle synchronisatie:

```text
> load-checkpoints
```

Checkpoints helpen zolang een node achterloopt. Zodra de keten voorbij het laatste checkpoint is, controleert de node elk blok al volledig. Een nieuwer bestand verandert dan niets.

## Hoe controleer je het checkpointbestand vanuit een script?

De pool publiceert bestandsgegevens als JSON op `https://bntpool.com/api/checkpoints`, zonder API-sleutel:

| Veld | Betekenis |
|---|---|
| `available` | Of de pool een bestand kan aanbieden |
| `url` | Waar je het downloadt |
| `entries` | Aantal checkpoints in het bestand |
| `latest_height`, `latest_hash` | Hoogste checkpoint en bijbehorende blokhash |
| `bytes`, `sha256` | Bestandsgrootte en SHA-256 |
| `updated_at` | Laatste wijziging als Unix-tijdstempel |

De download zelf bevat `X-Checkpoint-Height`, `X-Checkpoint-Sha256` en een `ETag` op basis van de hash. Een script kan `If-None-Match` meesturen en snel `304 Not Modified` ontvangen als niets is gewijzigd:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Antwoorden mogen 60 seconden gecachet worden, ruim voldoende voor een bestand dat ongeveer elke acht uur verandert.

<Note>
Volgens blocknetcrypto.com brengt de geplande Faraday-upgrade een lichtere proof of work, zodat nieuwe nodes de keten sneller inhalen. Tot dan zijn checkpoints de snelle manier om een node bij te werken.
</Note>

## Wat kun je met een gesynchroniseerde node?

Een gesynchroniseerde node ondersteunt de Blocknet-wallet en node-API. Seine praat ermee wanneer je solo mint met `--mode daemon`. [Pool versus solo voor Blocknet](/blog/nl/pool-vs-solo-mining-blocknet/) helpt bepalen of solo bij je hardware past. Wil je zonder eigen node beginnen, dan mint seine standaard op bntpool.com. De [installatiegids voor seine](https://seineminer.com/nl/blog/how-to-mine-blocknet/) brengt je naar je eerste shares en [minen op bntpool.com](/blog/nl/mine-blocknet-on-bntpool/) behandelt de poolkant.

<CTA target="pool" title="Mine bij de pool die Blocknet-checkpoints levert" text="Sluit binnen enkele minuten aan op bntpool.com en volg live je hashrate, shares en uitbetalingen." />
