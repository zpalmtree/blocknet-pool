---
title: "Calculateur de minage Blocknet"
description: "Estimez les BNT par jour et par semaine avec votre hashrate, les données réseau en direct et le calendrier des récompenses, frais affichés séparément."
tags: [Récompenses de minage, Guide de minage, Hashrate, bntpool]
faq:
  - q: 'Comment calculer les récompenses de minage Blocknet ?'
    a: 'Divisez votre hashrate par celui du réseau, multipliez par 288, le nombre de blocs produits par jour, puis par la récompense de bloc actuelle. Le résultat est votre espérance brute en BNT par jour. Relevez le hashrate du réseau sur bntpool.com et le vôtre dans seine ou My Stats.'
  - q: 'Où voir le hashrate du réseau Blocknet ?'
    a: 'Le tableau de bord bntpool.com l’affiche dans Network Hashrate, généralement en KH/s : multipliez par 1 000 pour obtenir des H/s. La même valeur est disponible en H/s dans le champ network_hashrate de bntpool.com/api/stats. Elle porte sur les dix derniers blocs et change à chaque bloc.'
  - q: 'La récompense de bloc Blocknet évolue-t-elle ?'
    a: 'Oui. Elle reste fixe pendant chaque mois de 30 jours et 8 640 blocs, puis diminue au début du mois suivant, jusqu’à une récompense permanente de 2,0 BNT par bloc au mois 48. Divisez la hauteur par 8 640, arrondissez à l’entier inférieur et recherchez ce mois dans le calendrier des récompenses.'
  - q: 'Pourquoi mes récompenses quotidiennes diffèrent-elles de l’estimation ?'
    a: 'L’estimation est une moyenne. Un jour donné, le pool peut trouver plus ou moins de blocs que prévu, et le hashrate du réseau change avec l’arrivée de mineurs. Sur une semaine ou plus de minage régulier, ces variations s’atténuent. Vous pouvez suivre les récompenses créditées sur My Stats.'
  - q: 'Miner dans un pool change-t-il la quantité de BNT attendue ?'
    a: 'Votre part attendue des récompenses du réseau reste identique, car elle dépend uniquement de votre part du hashrate du réseau. Le pool change leur rythme. Au lieu d’attendre plusieurs jours un bloc entier, vous recevez une fraction de chaque bloc trouvé par bntpool.com, en petits montants réguliers.'
sourceHash: 0e1d600e0b00
translation: draft
---

<RewardsCalculator />

Ce guide explique comment estimer les BNT que votre matériel mine par jour. Vous découvrirez la formule, où relever chaque valeur sur le [tableau de bord bntpool.com](/) et un exemple daté utilisant les données réelles du pool et les meilleurs hashrates mesurés avec seine.

## Combien de BNT votre matériel minera-t-il par jour ?

<Answer>
Les BNT attendus par jour correspondent à votre hashrate divisé par celui du réseau, multiplié par 288 blocs par jour, puis par la récompense actuelle. Votre hashrate vient de seine ou My Stats, celui du réseau du tableau de bord bntpool.com, et la récompense de la hauteur actuelle et du calendrier Blocknet.
</Answer>

Sous forme de formule :

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Chaque élément a une raison simple :

- **288 blocs par jour.** Blocknet vise un bloc toutes les cinq minutes. Les 86 400 secondes d’une journée divisées par 300 secondes donnent 288. La difficulté s’ajuste après chaque bloc pour maintenir ce rythme quand des mineurs arrivent.
- **Votre part du hashrate est votre part des blocs.** La difficulté Blocknet correspond approximativement au nombre de hachages attendu pour trouver un bloc. Chaque hachage a la même chance : une machine effectuant 1 % des hachages du réseau peut donc attendre 1 % des blocs sur la durée.
- **La récompense dépend de la hauteur.** Tous les blocs d’un même mois de 30 jours et 8 640 blocs versent la même récompense. Elle est donc une constante connue pour chaque journée de ce mois.

Le résultat est votre récompense brute attendue, moyennée dans le temps.

## Où trouver chaque valeur ?

### Votre hashrate

seine affiche votre hashrate dans son tableau de bord dès le démarrage. Laissez-lui quelques minutes : il optimise le nombre de threads et le kernel CPU sur du travail réel, et le chiffre se stabilise après avoir trouvé les meilleurs réglages.

Une fois sur le pool, la carte **Hashrate** de [My Stats](/stats) affiche votre hashrate vu par le pool, moyenné sur la dernière heure. Pour une estimation, utilisez la valeur stable de seine ou le graphique sur 24 heures du pool.

### Le hashrate du réseau

La carte **Network Hashrate** affiche celui de tout le réseau. Il est en KH/s : 62,33 KH/s signifie 62 330 H/s. Convertissez en H/s avant la division, sinon le résultat sera faux d’un facteur mille.

Le pool calcule cette valeur à partir du travail et des horodatages des dix derniers blocs. Elle change donc d’un bloc à l’autre ; [qu’est-ce que le hashrate ?](/blog/fr/what-is-hashrate/) explique le calcul et ses variations. Pour une valeur plus stable, relevez-la plusieurs fois dans la journée et prenez la moyenne. Vous pouvez aussi la lire en H/s via l’API :

```sh
curl -s https://bntpool.com/api/stats
```

La section `chain` contient `network_hashrate` en H/s et `current_job_height`, le bloc sur lequel le pool travaille.

### La récompense de bloc

La carte **Current Block** affiche la hauteur en cours. Divisez-la par 8 640 et arrondissez à l’entier inférieur pour obtenir le mois, puis recherchez-le dans le [calendrier des récompenses Blocknet](/blog/fr/blocknet-block-reward-schedule/). La [page des blocs](/blocks) permet une vérification rapide : la colonne Reward indique la récompense entière de chaque bloc trouvé par le pool.

## Exemple du 25 octobre 2026

<Note>
Cet exemple utilise les valeurs de https://bntpool.com/api/stats relevées le 25 octobre 2026 à 02:03 EDT. Le hashrate du réseau et la récompense changent : remplacez-les par les valeurs du jour avant de vous appuyer sur le résultat.
</Note>

| Donnée | Valeur au moment du relevé |
|---|---|
| Bloc actuel | 69 020 (mois 7, car 69 020 ÷ 8 640 = 7,99) |
| Récompense de bloc | 467,67 BNT |
| Hashrate du réseau | 62 329 H/s (affiché 62,33 KH/s) |
| Hashrate bntpool.com | 5 240 H/s |

Prenons le serveur AMD EPYC Zen 5 des benchmarks seine, à 49,4 H/s :

1. Part du réseau : 49,4 ÷ 62 329 = 0,0793 %.
2. Blocs attendus par jour : 0,000793 × 288 = 0,2283.
3. BNT attendus par jour : 0,2283 × 467,67 = environ 106,8 BNT.

Le mois 8 commence à la hauteur 69 120, environ 100 blocs après le relevé, et verse 439,46 BNT par bloc. À hashrate réseau identique, l’estimation devient 0,2283 × 439,46 = environ 100,3 BNT par jour. C’est pourquoi il est utile de vérifier la récompense au début de chaque mois.

Voici le même calcul, au hashrate réseau du relevé, pour le meilleur résultat mesuré avec seine par génération de matériel :

| Matériel | Threads | Hashrate | Part du réseau | BNT/jour à 467,67 | BNT/jour à 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793 % | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573 % | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | 0,0496 % | 66,8 | 62,7 |
| Serveur Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345 % | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210 % | 28,3 | 26,6 |

La valeur de votre propre machine est celle qui compte. Faites tourner seine, prenez son hashrate stable et utilisez-le à la place du tableau. Le guide [matériel et hashrate pour miner Blocknet](https://seineminer.com/fr/blog/blocknet-mining-hardware-hashrate/) explique le rôle de la bande passante mémoire et de la RAM. Tous les résultats sont dans la [page des performances](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md) de seine.

## Comment le minage sur bntpool.com change-t-il l’estimation ?

Votre part attendue reste identique. C’est la façon de recevoir la récompense qui change. bntpool.com utilise PPLNS : chaque récompense est répartie entre les shares envoyés dans les six heures précédant le bloc, proportionnellement à leur difficulté. [Le fonctionnement des paiements PPLNS](/blog/fr/how-pplns-payouts-work/) détaille cette répartition.

Le même calcul côté pool donne la même réponse. Lors du relevé, les 5 240 H/s du pool représentaient 8,4 % du réseau, soit 288 × 5 240 ÷ 62 329 = environ 24,2 blocs attendus par jour, environ un par heure. Le tableau de bord affiche **Expected Block Time** dans Round Progress. Un serveur à 49,4 H/s représente 0,94 % du pool, et 0,94 % de 24,2 blocs à 467,67 BNT donne encore environ 106,8 BNT par jour. Le hashrate du pool s’annule dans le calcul.

La différence est le rythme. En solo, ce serveur Zen 5 attendrait un bloc entier environ tous les 4,4 jours ; [minage en pool ou en solo sur Blocknet](/blog/fr/pool-vs-solo-mining-blocknet/) compare les deux. Sur bntpool.com, il reçoit une fraction de chacun des quelque 24 blocs quotidiens du pool et son solde augmente par petites étapes tout au long de la journée. Les frais du pool et les frais de développement de seine sont déduits du montant brut calculé.

Deux détails façonnent votre première journée :

- **La fenêtre se remplit sur six heures.** La part d’un nouveau mineur augmente à mesure que ses shares remplissent la fenêtre PPLNS, puis reste complète tant qu’il continue.
- **Les récompenses mûrissent avant paiement.** Elles passent dans Confirmed Rewards après 60 confirmations, environ cinq heures, puis le pool envoie automatiquement le solde dès 0,1 BNT.

## Comment comparer l’estimation aux résultats réels ?

L’estimation est une moyenne. Chaque journée varie autour d’elle selon la chance du pool. Sur de nombreuses périodes, l’effort moyen se rapproche de 100 % : une semaine de minage régulier est donc une bonne durée de comparaison. Relevez la somme de Confirmed Rewards et Paid Balance sur [My Stats](/stats) au début et à la fin d’une semaine. L’augmentation correspond aux crédits issus des blocs arrivés à maturité cette semaine. Comparez-la à l’estimation des mêmes jours en utilisant leurs récompenses et hashrates réseau.

Refaites le calcul dès qu’une valeur change :

- **À chaque nouveau mois**, lorsque la récompense baisse. Les limites tombent tous les 8 640 blocs, environ tous les 30 jours.
- **Quand le hashrate du réseau change**, avec l’arrivée de mineurs.
- **Quand votre hashrate change**, après l’ajout d’une machine, la réservation de huge pages sur Linux ou l’ajout d’un GPU NVIDIA, que seine utilise avec le CPU.

<Note>
Les hashrates décrivent la preuve de travail Argon2id actuelle. Blocknet prévoit Faraday vers le 16 février 2027, selon [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), avec un changement prévu de preuve de travail. Gardez seine à jour : il vérifie les nouvelles versions au démarrage et toutes les 12 heures.
</Note>

<CTA target="seine" title="Mesurez votre propre hashrate" text="Téléchargez seine, laissez-le s’optimiser quelques minutes et utilisez son hashrate dans la formule avec les valeurs du jour." />
