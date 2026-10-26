---
title: 'Difficulté de minage : réseau et shares'
description: 'Comprenez la cible de difficulté, son ajustement dans Blocknet et pourquoi les difficultés du réseau et des shares influencent des statistiques différentes.'
tags: ['Bases du minage', 'Difficulté', 'Blocknet']
faq:
  - q: 'Une difficulté plus élevée réduit-elle mon hashrate ?'
    a: 'La difficulté change la cible à satisfaire, pas directement le nombre de hashes calculés chaque seconde. À hashrate identique, une difficulté réseau supérieure allonge l''intervalle moyen attendu entre blocs. Analysez les performances du mineur séparément de la fréquence des blocs ou des shares.'
  - q: 'Une share plus difficile rapporte-t-elle davantage ?'
    a: 'Elle représente davantage de travail attendu, mais à hashrate identique vous en soumettez moins. Comparez le travail pondéré par difficulté sur une même période. Sur bntpool.com, les récompenses dépendent aussi des blocs acceptés et du calcul PPLNS, plutôt que du nombre brut de shares.'
  - q: 'À quelle fréquence Blocknet ajuste-t-il la difficulté ?'
    a: 'Après chaque bloc, Blocknet calcule la difficulté du suivant à partir de l''historique récent. La règle actuelle utilise une moyenne mobile pondérée linéairement sur 60 intervalles et vise 300 secondes par bloc. Les intervalles récents pèsent davantage ; chaque bloc peut néanmoins arriver plus tôt ou plus tard.'
sourceHash: '5c203e8c7523'
translation: draft
---

La difficulté relie le travail de votre matériel aux blocs acceptés par le réseau. Ce guide explique la cible, son ajustement dans Blocknet et la difficulté distincte des shares. Interprétez ses changements avant de comparer hashrate, temps attendu par bloc ou [calculateur de récompenses](/blog/fr/estimate-blocknet-mining-rewards/#mining-calculator).

## Qu'est-ce que la difficulté de minage ?

<Answer>
La difficulté mesure le travail de hachage attendu pour trouver un résultat satisfaisant une cible. Plus elle augmente, moins ces résultats sont fréquents à hashrate identique. Pour Blocknet, la difficulté réseau correspond approximativement au nombre moyen de hashes par bloc ; celle des shares concerne une cible de soumission distincte.
</Answer>

Chaque hash produit un nombre qui doit satisfaire un seuil. Une cible plus petite accepte une fraction moindre des résultats possibles : sa difficulté est supérieure. Blocknet calcule la cible de 256 bits en divisant la plus grande valeur de 256 bits par la difficulté. Doubler la difficulté double donc approximativement le travail attendu sans modifier l'opération de hachage.

## Comment Blocknet ajuste-t-il la difficulté réseau ?

Blocknet détermine la difficulté suivante après chaque bloc. La règle actuelle du réseau principal utilise une moyenne mobile pondérée linéairement, LWMA, sur 60 intervalles récents, avec davantage de poids pour les plus récents. La cible est de 300 secondes par bloc. Difficultés passées et durées pondérées font généralement monter la difficulté lors d'arrivées durablement rapides et la font baisser lors d'arrivées lentes.

Le calcul borne les intervalles utilisés et impose une difficulté minimale. Les 60 intervalles nécessitent 61 horodatages ; cela ne signifie pas un ajustement tous les 60 blocs. Ces règles rapprochent le rythme de la chaîne de sa cible lorsque la puissance évolue. Les cinq minutes sont une moyenne : un bloc individuel peut arriver bien avant ou après.

## Difficulté réseau et difficulté des shares

Le pool attribue une cible de share pour observer le travail contribué entre découvertes de blocs. Elle est généralement plus facile à satisfaire que celle du réseau. Les deux valeurs décrivent du travail attendu, mais répondent à des questions différentes et sont déterminées par des parties distinctes du système :

| Difficulté | Ce qu'elle contrôle |
|---|---|
| Réseau | Le seuil de travail d'un bloc candidat, déterminé par les règles de consensus. |
| Share | Le seuil de soumission attribué par le pool à une connexion ou tâche de minage. |

bntpool.com utilise une difficulté variable, vardiff, pour ajuster la fréquence des soumissions. À hashrate identique, une difficulté supérieure produit généralement moins de shares, chacune représentant davantage de travail attendu. Elle ne modifie pas la difficulté réseau. Un résultat satisfaisant aussi la cible réseau peut devenir un bloc candidat ; le daemon doit encore le valider. Le [guide des shares](/blog/fr/accepted-rejected-stale-shares/) explique acceptations et rejets.

## Quel effet sur le temps attendu par bloc ?

Pour Blocknet, divisez la difficulté réseau par le hashrate réalisant le travail pour estimer l'intervalle moyen en secondes. Utilisez des H/s et des valeurs de la même période. L'exemple conserve difficulté et hashrate constants pour illustrer l'arithmétique. Ces chiffres ne sont ni des mesures actuelles du réseau ni une prévision pour un mineur particulier.

**Secondes attendues par bloc ≈ difficulté réseau ÷ hashrate en H/s**

| Difficulté réseau | Hashrate | Intervalle attendu |
|---|---|---|
| 30 000 | 100 H/s | 300 s |
| 60 000 | 100 H/s | 600 s |
| 60 000 | 200 H/s | 300 s |

À une difficulté de 30 000 et 100 H/s, la moyenne est de 300 secondes. À 60 000 avec le même hashrate, elle devient 600 secondes. Si le hashrate double aussi, à 200 H/s, elle revient à 300 secondes. Il s'agit de moyennes sur des essais répétés. Le [guide de la chance du pool](/blog/fr/mining-pool-luck-explained/) explique les écarts d'une ronde réelle.

## Pourquoi les récompenses changent-elles à hashrate constant ?

À hashrate personnel constant, une difficulté réseau supérieure réduit le nombre attendu de blocs par unité de temps tant qu'elle demeure ainsi. Si le hashrate total augmente et la difficulté suit, votre part du travail réseau diminue. La récompense par bloc est une autre variable : son [calendrier](/blog/fr/blocknet-block-reward-schedule/) la fait évoluer indépendamment. La difficulté ne fixe pas les BNT de la récompense programmée.

Utilisez des données récentes dans le calculateur pour explorer les BNT attendus. Vérifiez si votre hashrate vient du mineur ou du pool afin de traiter correctement les frais du mineur, avec le taux de votre version. Les estimations sont des moyennes sous certaines hypothèses. Le paiement dépend aussi des blocs trouvés, confirmations et [règles PPLNS](/blog/fr/how-pplns-payouts-work/) ; la difficulté seule n'annonce pas sa date.

## Quel chiffre de difficulté consulter ?

Partez de votre question : pour découvrir des blocs, utilisez difficulté réseau et hashrate du mineur ou pool concerné. Pour la fréquence des shares, utilisez la difficulté de leurs tâches. Pour un solde ou paiement, consultez [My Stats](/stats) et l'[historique des paiements](/payouts). Gardez l'horodatage, le périmètre et les unités avec chaque valeur comparée.

Le [tableau de bord](/) et l'[historique des blocs](/blocks) donnent le contexte actuel. Le hashrate réseau est estimé à partir du travail récent et du temps écoulé ; il peut différer de la difficulté suivante divisée par 300 secondes. Le [guide du hashrate](/blog/fr/what-is-hashrate/) explique cette distinction. Séparez difficulté, hashrate et chance pour analyser un changement : chacun décrit une autre dimension.

<CTA target="pool" title="Connecter son mineur à bntpool.com" text="Suivez la page de configuration, puis vos shares et votre activité dans My Stats." />
