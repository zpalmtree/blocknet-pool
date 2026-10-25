---
title: Qu’est-ce que le hashrate en minage ?
description: Le hashrate mesure les hachages calculés par seconde. Découvrez ses unités, les différences entre mineur, pool et réseau, et pourquoi ces valeurs varient.
tags: [Hashrate, Bases du minage, Blocknet, bntpool]
faq:
  - q: 'Quel hashrate un ordinateur peut-il atteindre sur Blocknet ?'
    a: 'Les meilleurs résultats seine par génération sont 49,4 H/s sur un serveur AMD EPYC Zen 5, 35,7 H/s sur AMD EPYC Zen 4, 30,9 H/s sur un portable Apple M4 Max et 21,5 H/s sur un serveur Arm AWS Graviton4. Votre résultat dépend surtout de la bande passante mémoire et du nombre de threads de 2 GB que contient votre RAM.'
  - q: 'Pourquoi Blocknet utilise-t-il H/s plutôt que MH/s ou TH/s ?'
    a: 'Chaque hachage est un calcul Argon2id écrivant environ 2 GiB de mémoire puis la relisant. Même un gros serveur ne termine donc que des dizaines de hachages par seconde. Les preuves de travail à hachage rapide, comme SHA-256, atteignent des valeurs bien plus grandes et utilisent de plus grandes unités. Comparez uniquement un même algorithme.'
  - q: 'Pourquoi le pool affiche-t-il un hashrate différent du mineur ?'
    a: 'Votre mineur compte tous les hachages terminés. Le pool ne voit que les shares soumis et estime le hashrate selon leur difficulté sur la dernière heure. Ils arrivent aléatoirement : la valeur varie davantage et se stabilise avec le temps. Comparez le graphique 24 heures de My Stats à la moyenne du mineur.'
  - q: 'Un hashrate supérieur donne-t-il plus de récompenses ?'
    a: 'Oui, proportionnellement. Chaque hachage a la même chance de trouver un bloc : votre part attendue des blocs égale votre part du hashrate réseau. Doubler votre hashrate double vos récompenses attendues, tandis qu’une hausse du réseau réduit votre part. Sur bntpool.com, elle arrive comme fraction de chaque bloc trouvé.'
sourceHash: 0bfdcdca348e
translation: draft
---

Le hashrate est le premier chiffre affiché par les mineurs, pools et explorateurs. Ce guide explique ce qu’il mesure, ses unités à partir de H/s, le rapport entre votre hashrate, celui du pool et celui du réseau, et leurs variations minute par minute, avec Blocknet et bntpool.com comme exemples.

## Qu’est-ce que le hashrate ?

<Answer>
Le hashrate est le nombre de hachages calculés par seconde. En preuve de travail, chaque hachage est un essai pour résoudre un bloc, avec la même faible chance de réussite. Un hashrate plus élevé donne plus d’essais par seconde : votre part attendue des blocs augmente proportionnellement à votre part du hashrate total.
</Answer>

Une chaîne à preuve de travail demande un en-tête de bloc dont le hachage est inférieur à une cible. Aucun raccourci : le mineur change le nonce dans l’en-tête, hache, compare à la cible et recommence. Le hashrate compte les essais terminés chaque seconde.

Sur Blocknet, un essai est un hachage Argon2id. Le nonce sert de mot de passe, le reste de l’en-tête de sel, et la fonction parcourt 2 GiB de mémoire avant de produire un résultat de 32 octets. seine et le pool comptent les hachages terminés : 1 H/s signifie donc un calcul Argon2id complet par seconde.

## Que signifient H/s, kH/s et MH/s ?

Les unités suivent les préfixes métriques, chaque niveau valant mille fois le précédent :

| Unité | Hachages par seconde | Usage typique |
|---|---|---|
| H/s | 1 | Un ordinateur sur Blocknet |
| kH/s (KH/s) | 1 000 | Totaux pool et réseau Blocknet |
| MH/s | 1 000 000 | Algorithmes de preuve de travail plus rapides |
| GH/s | 1 000 000 000 | Hachages rapides comme SHA-256 |
| TH/s et plus | 1 000 000 000 000 et plus | Totaux réseau de hachages rapides |

Le tableau de bord bntpool.com écrit les milliers KH/s et change d’unité chaque fois qu’une valeur dépasse 1 000. Ainsi, 44,44 KH/s signifie 44 440 H/s. Avant de diviser deux hashrates, convertissez-les tous deux en H/s.

## Pourquoi les hashrates Blocknet sont-ils si petits ?

La preuve de travail Blocknet est gourmande en mémoire. Chaque hachage écrit environ 2 GiB puis en relit environ 2 GiB, bien plus que la capacité des caches CPU. La vitesse mémoire donne donc le rythme. Chaque thread a besoin de ses propres 2 GiB : la RAM détermine le nombre de threads.

Voici les meilleurs résultats seine par génération :

| Matériel | Threads | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Serveur Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Même le serveur Zen 5 à 32 threads termine environ 1,5 hachage par thread et par seconde. Les hachages rapides donnent des nombres beaucoup plus grands : le wiki Bitcoin note que les mineurs double SHA-256 étaient déjà mesurés en milliards de hachages par seconde en 2014. Ces chiffres représentent des travaux différents par hachage. Un hashrate n’a donc de sens que comparé au même algorithme. [La preuve de travail Argon2id](https://seineminer.com/fr/blog/argon2id-proof-of-work-explained/) détaille l’algorithme, et le [guide matériel Blocknet](https://seineminer.com/fr/blog/blocknet-mining-hardware-hashrate/) explique le rôle de la bande passante mémoire et de la RAM.

## Votre hashrate, celui du pool et du réseau

Trois hashrates comptent pour un mineur en [pool](/blog/fr/what-is-a-mining-pool/), chacun mesuré différemment.

### Votre hashrate

seine compte chaque hachage terminé et affiche **Current**, le débit actuel, et **Average**, la moyenne de la session. Pour une mesure stable, utilisez la moyenne après quelques minutes d’optimisation des threads et du kernel CPU sur le travail réel.

Le pool ne voit pas vos hachages, seulement vos shares : des hachages passant sous une cible plus facile propre à votre connexion. bntpool.com estime votre hashrate d’après la difficulté totale acceptée sur la dernière heure et l’affiche dans [My Stats](/stats). [Suivre votre mineur sur bntpool.com](/blog/fr/track-your-miner-on-bntpool/) présente la page.

### Hashrate du pool

**Pool Hashrate** additionne le travail des shares de tous les mineurs connectés sur la même fenêtre d’une heure. Cette valeur indique la part du travail réseau fournie par bntpool.com et la fréquence attendue des blocs, affichée dans **Expected Block Time**.

### Hashrate du réseau

Personne ne peut compter tous les hachages réseau. Le hashrate est donc déduit des blocs. La difficulté est le nombre attendu de hachages pour un bloc et Blocknet vise 300 secondes par bloc.

## Comment le hashrate réseau est-il calculé ?

<Answer>
Il est estimé d’après la difficulté et les temps de bloc. Sur Blocknet, la difficulté correspond approximativement aux hachages attendus par bloc. Avec une cible de 300 secondes, le hashrate vaut donc environ difficulté divisée par 300. bntpool.com utilise le travail et les horodatages réels des dix derniers blocs.
</Answer>

Voici le calcul avec les valeurs relevées le 25 octobre 2026 à 02:26 EDT. L’[explorateur Blocknet](https://explorer.blocknetcrypto.com) indiquait une prochaine difficulté de 16 943 147, soit environ 16,9 millions de hachages attendus par bloc. Divisés par 300 secondes, ils donnent environ 56 477 H/s.

Au même instant, bntpool.com affichait 44,44 KH/s, soit 44 440 H/s. Ce sont deux estimations de la même chose sous des angles différents. La difficulté donne le débit qui produirait des blocs exactement toutes les cinq minutes. Le tableau de bord divise le travail réel des dix derniers blocs par leur durée réelle : si ces blocs prennent plus longtemps, la valeur est plus basse. Blocknet ajuste la difficulté après chaque bloc, ce qui rapproche les deux dans le temps.

## Pourquoi le hashrate varie-t-il ?

Chaque valeur a ses raisons :

- **Votre machine.** Au début, seine compare threads et kernels en minant et retient les plus rapides : le débit monte pendant l’optimisation. Ensuite, d’autres usages de mémoire ou de CPU peuvent prendre une part du temps de calcul.
- **La vision du pool.** Les shares sont aléatoires. Le pool vise environ 10 shares toutes les cinq minutes, soit environ 120 par heure. Une période chanceuse ou calme fait varier l’estimation. Un nouveau rig est moyenné sur ses minutes de minage jusqu’à remplir l’heure.
- **Le réseau.** Les temps de bloc sont eux aussi aléatoires et dix blocs forment un petit échantillon. Le 25 octobre 2026, le tableau affichait environ 50 057 H/s à 00:26 EDT, 62 329 H/s à 02:03 et 44 440 H/s à 02:26. Les arrivées et départs de mineurs ajoutent des changements réels à ce bruit.

Pour décider, utilisez les moyennes : celle de la session seine, le graphique 24 heures de My Stats et plusieurs relevés réseau dans une journée.

## Que signifie votre hashrate pour les récompenses ?

Chaque hachage ayant la même chance, votre part du hashrate réseau est votre part attendue des blocs. Blocknet produit 288 blocs par jour : un mineur à 0,1 % du réseau peut attendre l’équivalent d’environ 0,29 bloc par jour sur la durée. [Estimer vos récompenses Blocknet](/blog/fr/estimate-blocknet-mining-rewards/) convertit cela en BNT avec les valeurs du [tableau de bord](/).

Sur bntpool.com, cette part arrive comme fraction de chaque bloc trouvé, créditée selon les shares acceptés. Les récompenses s’accumulent donc par petites étapes jour et nuit. Pour commencer, le [guide d’installation seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) mène du téléchargement aux premiers shares en quelques minutes.

<Note>
Ces hashrates décrivent la preuve de travail Argon2id actuelle. Blocknet prévoit Faraday vers le 16 février 2027, selon [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), avec un changement prévu de preuve de travail. Gardez seine à jour : il vérifie les nouvelles versions au démarrage et toutes les 12 heures.
</Note>

<CTA target="seine" title="Découvrez votre propre hashrate" text="Téléchargez seine, laissez-le s’optimiser quelques minutes et suivez votre hashrate dans son tableau de bord et My Stats." />
