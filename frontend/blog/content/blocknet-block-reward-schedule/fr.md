---
title: Calendrier des récompenses de bloc Blocknet
description: Les récompenses Blocknet par mois, de 723,25 BNT au lancement à 2,0 BNT permanents au mois 48, avec 288 blocs par jour et 98,6 millions de BNT avant la traîne.
tags: [Blocknet, Récompense de bloc, Émission, Minage]
faq:
  - q: 'Quelle est la récompense de bloc actuelle de Blocknet ?'
    a: 'Elle dépend du mois atteint par la chaîne, car elle diminue tous les 8 640 blocs. Le 25 octobre 2026, la chaîne était au mois 7, avec 467,67 BNT par bloc. Le mois 8 commence à la hauteur 69 120 avec 439,46 BNT. Relevez la hauteur actuelle sur le tableau de bord bntpool.com et consultez la ligne correspondante.'
  - q: 'Combien de blocs Blocknet sont minés par jour ?'
    a: 'Blocknet vise un bloc toutes les cinq minutes, soit 288 blocs par jour et 8 640 par mois de 30 jours. La difficulté est ajustée à chaque bloc à partir de la moyenne pondérée des 60 derniers temps de bloc, pour maintenir une moyenne proche de cinq minutes quand des mineurs rejoignent ou quittent le réseau.'
  - q: 'Quelle est l’offre totale de Blocknet ?'
    a: 'Environ 98,56 millions de BNT sont minés avant le début de la traîne au mois 48. Le code et le site officiel arrondissent à environ 100 millions. Ensuite, chaque bloc verse 2,0 BNT indéfiniment, soit 576 BNT par jour ou environ 210 240 BNT par an : l’offre continue donc de croître lentement.'
  - q: 'Quand commence l’émission de traîne de Blocknet ?'
    a: 'Elle commence au mois 48, à la hauteur 414 720. Cela représente 48 mois de 30 jours, soit 1 440 jours de blocs de cinq minutes après le lancement, environ quatre ans. La date exacte dépend des temps de bloc réels. À partir de cette hauteur, chaque bloc verse 2,0 BNT.'
  - q: 'Quand peut-on dépenser les BNT minés ?'
    a: 'Une récompense de bloc devient dépensable après 60 blocs, soit environ cinq heures avec les blocs de cinq minutes de Blocknet. C’est la maturité coinbase. Le pool bntpool.com attend aussi 60 blocs avant de distribuer la récompense, et ne paie donc que les récompenses de blocs arrivés à maturité.'
sourceHash: 6c38930dfa09
translation: draft
---

Suivez les BNT nouvellement émis depuis la récompense de bloc jusqu’aux coins dépensables et aux paiements ultérieurs. Cette page explique ce parcours monétaire, présente ensuite chaque mois du calendrier des récompenses et montre comment lire les chiffres en tant que mineur ou détenteur.

## Quelle est la récompense de bloc Blocknet ?

<Answer>
La récompense de bloc Blocknet a commencé à 723,25 BNT par bloc et diminue une fois par mois de 30 jours, soit 8 640 blocs, selon une courbe de décroissance régulière. Elle reste identique pour tous les blocs d’un même mois. À partir du mois 48, à la hauteur 414 720, chaque bloc verse une récompense de traîne permanente de 2,0 BNT.
</Answer>

Blocknet vise un bloc toutes les cinq minutes (300 secondes). Cela donne 288 blocs par jour et 8 640 par mois de 30 jours. La difficulté s’ajuste après chaque bloc selon une moyenne pondérée des 60 derniers temps de bloc, pour maintenir un rythme proche de cinq minutes quand le hashrate du réseau change.

La récompense d’un bloc dépend uniquement du mois correspondant à sa hauteur. Le mois est la hauteur divisée par 8 640, arrondie à l’entier inférieur : les hauteurs 0 à 8 639 appartiennent au mois 0, les hauteurs 8 640 à 17 279 au mois 1, et ainsi de suite.

## Comment une récompense de bloc devient-elle du BNT utilisable ?

Un bloc miné crée sa récompense prévue dans une sortie coinbase. Cette sortie appartient au destinataire choisi pour le bloc et passe d’abord par la maturité coinbase. Une fois dépensable, son propriétaire peut l’utiliser dans des transactions ordinaires. Si un pool a miné le bloc, sa comptabilité répartit les récompenses admissibles entre participants et ses paiements ultérieurs envoient des BNT existants à leurs portefeuilles.

Un paiement du pool ne crée donc pas une seconde récompense. Recevoir un transfert n’exige pas non plus de miner de nouveaux coins. L’émission décrit l’origine des BNT ; les transferts décrivent leurs changements de propriétaire. Le [guide des confirmations](/blog/fr/block-confirmations-mining-rewards/) suit les étapes du pool, et le [guide de l’offre Blocknet](https://seineminer.com/fr/blog/blocknet-supply/) explique le calendrier monétaire général.

## Comment la récompense est-elle calculée ?

Le daemon calcule la récompense de chaque mois avec une seule formule :

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

Le premier terme est la part de la récompense au-dessus de la traîne de 2,0 BNT. Elle est multipliée par e^-0,75 chaque année : après douze mois, elle représente environ 47 % de sa valeur initiale. Répartie par mois, cette part vaut environ 93,9 % de celle du mois précédent, d’où une baisse par petites étapes régulières.

Le résultat est arrondi à l’unité atomique inférieure (1 BNT vaut 100 000 000 unités atomiques). Le tableau affiche chaque récompense arrondie à deux décimales.

## Tableau des récompenses Blocknet par mois

| Mois | Premier bloc | Récompense par bloc (BNT) | Total miné en fin de mois (millions de BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8 640 | 679,55 | 12,12 |
| 2 | 17 280 | 638,50 | 17,64 |
| 3 | 25 920 | 599,94 | 22,82 |
| 4 | 34 560 | 563,71 | 27,69 |
| 5 | 43 200 | 529,68 | 32,27 |
| 6 | 51 840 | 497,71 | 36,57 |
| 7 | 60 480 | 467,67 | 40,61 |
| 8 | 69 120 | 439,46 | 44,40 |
| 9 | 77 760 | 412,96 | 47,97 |
| 10 | 86 400 | 388,06 | 51,33 |
| 11 | 95 040 | 364,67 | 54,48 |
| 12 | 103 680 | 342,69 | 57,44 |
| 13 | 112 320 | 322,05 | 60,22 |
| 14 | 120 960 | 302,66 | 62,83 |
| 15 | 129 600 | 284,45 | 65,29 |
| 16 | 138 240 | 267,33 | 67,60 |
| 17 | 146 880 | 251,26 | 69,77 |
| 18 | 155 520 | 236,16 | 71,81 |
| 19 | 164 160 | 221,97 | 73,73 |
| 20 | 172 800 | 208,64 | 75,53 |
| 21 | 181 440 | 196,12 | 77,23 |
| 22 | 190 080 | 184,36 | 78,82 |
| 23 | 198 720 | 173,31 | 80,32 |
| 24 | 207 360 | 162,93 | 81,73 |
| 25 | 216 000 | 153,18 | 83,05 |
| 26 | 224 640 | 144,02 | 84,29 |
| 27 | 233 280 | 135,42 | 85,46 |
| 28 | 241 920 | 127,33 | 86,56 |
| 29 | 250 560 | 119,74 | 87,60 |
| 30 | 259 200 | 112,61 | 88,57 |
| 31 | 267 840 | 105,91 | 89,49 |
| 32 | 276 480 | 99,61 | 90,35 |
| 33 | 285 120 | 93,70 | 91,16 |
| 34 | 293 760 | 88,14 | 91,92 |
| 35 | 302 400 | 82,92 | 92,63 |
| 36 | 311 040 | 78,02 | 93,31 |
| 37 | 319 680 | 73,41 | 93,94 |
| 38 | 328 320 | 69,09 | 94,54 |
| 39 | 336 960 | 65,02 | 95,10 |
| 40 | 345 600 | 61,20 | 95,63 |
| 41 | 354 240 | 57,62 | 96,13 |
| 42 | 362 880 | 54,25 | 96,60 |
| 43 | 371 520 | 51,08 | 97,04 |
| 44 | 380 160 | 48,11 | 97,45 |
| 45 | 388 800 | 45,31 | 97,85 |
| 46 | 397 440 | 42,69 | 98,21 |
| 47 | 406 080 | 40,22 | 98,56 |
| 48 et suivants | 414 720 | 2,00 | +0,017 par mois |

Le bloc genesis à la hauteur 0 ne verse aucune récompense : le mois 0 compte donc 8 639 blocs rémunérés. Tous les BNT proviennent de ces récompenses, sans premine, comme l’explique la page sur le [lancement équitable de Blocknet](/blog/fr/blocknet-fair-launch/). Le 25 octobre 2026, le pool travaillait sur le bloc 69 016, près de la fin du mois 7. Le [tableau de bord bntpool.com](/) affiche la hauteur actuelle pour trouver la ligne correspondante, et la [page des blocs](/blocks) répertorie ceux trouvés par le pool.

## Que se passe-t-il au mois 48 ?

<Answer>
Le mois 48 commence à la hauteur 414 720. Dès lors, chaque bloc Blocknet verse 2,0 BNT indéfiniment. C’est l’émission de traîne. Elle maintient une récompense pour chaque bloc tant que la chaîne fonctionne, afin de toujours rémunérer les mineurs pour la sécurisation du réseau.
</Answer>

La formule de décroissance s’applique aux mois 0 à 47 et se termine à 40,22 BNT par bloc au mois 47. Dès le mois 48, le daemon utilise directement la valeur de traîne : la récompense passe à 2,0 BNT et y reste. À 288 blocs par jour, la traîne ajoute 576 BNT par jour, soit environ 210 240 BNT par an.

Le mois 48 commence après 1 440 jours de blocs de cinq minutes depuis le lancement, environ quatre ans. La date dépend des temps de bloc réels : la hauteur est donc le repère fiable.

## Combien de BNT sont minés avant la traîne ?

La somme de tous les blocs de la hauteur 1 à 414 719 donne 98 561 905,08 BNT, environ 98,6 millions. Le code et le site officiel décrivent une émission d’environ 100 millions de coins sur quatre ans, suivie de la traîne ; la somme exacte est légèrement inférieure à ce chiffre rond.

Les totaux cumulés ci-dessous mesurent l’émission prévue des blocs acceptés. Ils ne représentent ni les soldes actuellement dépensables des portefeuilles ni l’activité de négociation. Transférer des BNT existants entre détenteurs n’augmente pas ces totaux d’émission.

La majeure partie est minée tôt. Voici le regroupement par année du calendrier :

| Mois | BNT minés | Part du total avant la traîne |
|---|---|---|
| 0-11 (année 1) | 54,48 millions | 55,3 % |
| 12-23 (année 2) | 25,84 millions | 26,2 % |
| 24-35 (année 3) | 12,32 millions | 12,5 % |
| 36-47 (année 4) | 5,93 millions | 6,0 % |

## Quand peut-on dépenser les BNT minés ?

La récompense d’un bloc devient dépensable lorsque 60 autres blocs ont été ajoutés au-dessus, environ cinq heures plus tard. Cette règle s’appelle la maturité coinbase. Le pool bntpool.com attend les mêmes 60 blocs avant de distribuer une récompense, et la [page des paiements](/payouts) répertorie les montants envoyés.

## Que signifie ce calendrier pour les mineurs ?

Les récompenses sont plus élevées pendant les premiers mois. Un bloc trouvé au mois 12 verse 342,69 BNT, environ 47 % des 723,25 BNT du mois 0, et un bloc du mois 24 verse 162,93 BNT. Chaque nouveau mois verse moins par bloc que le précédent jusqu’à la traîne.

Votre part des récompenses suit votre part du hashrate du réseau. En solo, le nombre attendu de blocs trouvés par jour est 288 × (votre hashrate ÷ hashrate du réseau), et chacun verse la récompense de son mois. Dans un pool, le même calcul s’applique au hashrate du pool, et chaque récompense est répartie entre les mineurs ayant contribué au travail. Vous gagnez donc sur chaque bloc trouvé par le pool au lieu d’attendre d’en trouver un entier vous-même. Le [tableau de bord bntpool.com](/) affiche les hashrates actuels du réseau et du pool, et la [page des statistiques](/stats) affiche celui de votre mineur une fois le minage lancé.

Pour commencer à miner :

1. Téléchargez seine sur [seineminer.com](https://seineminer.com/fr/#download). Il fonctionne sur Windows, macOS et Linux et s’optimise pour votre matériel.
2. Lancez-le et collez votre adresse Blocknet ou `@name`.
3. Acceptez le pool par défaut, `stratum+tcp://bntpool.com:3333`.

Le [guide de minage sur bntpool.com](/blog/fr/mine-blocknet-on-bntpool/) détaille les étapes côté pool, la [page Get Started](/start) donne les réglages du pool et le [guide de minage seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) présente l’installation étape par étape. Pour comparer les processeurs, consultez [le matériel et le hashrate pour miner Blocknet](https://seineminer.com/fr/blog/blocknet-mining-hardware-hashrate/).

<Note>
Les chiffres viennent du code des récompenses du daemon Blocknet, vérifié le 25 octobre 2026. Blocknet prévoit une mise à niveau réseau appelée Faraday vers le 16 février 2027, selon [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). Gardez seine à jour : il vérifie les nouvelles versions au démarrage et toutes les 12 heures.
</Note>

<CTA target="seine" title="Minez Blocknet avec seine" text="seine est gratuit pour Windows, macOS et Linux et mine sur bntpool.com par défaut." />
