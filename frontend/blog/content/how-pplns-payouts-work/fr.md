---
title: Les paiements PPLNS sur bntpool.com
description: bntpool.com rémunère les mineurs Blocknet par PPLNS sur six heures. Découvrez la répartition, les 60 confirmations et le minimum de paiement de 0,1 BNT.
tags: [Paiements, PPLNS, bntpool, Guide de minage]
faq:
  - q: 'Quelle est la durée de la fenêtre PPLNS sur bntpool.com ?'
    a: 'Six heures. Quand le pool trouve un bloc, il répartit sa récompense entre tous ceux qui ont envoyé des shares pendant les six heures précédentes, proportionnellement à leur difficulté. La valeur actuelle figure sur la page Get Started du pool.'
  - q: 'Quel est le paiement minimum sur bntpool.com ?'
    a: 'Le minimum est de 0,1 BNT. Dès que votre solde confirmé atteint ce montant, le pool l’envoie automatiquement à votre adresse Blocknet, sans demande ni réclamation. Un solde inférieur reste sur votre compte et augmente avec chaque bloc confirmé auquel vous avez contribué, jusqu’au seuil.'
  - q: 'Pourquoi Estimated Rewards diffère-t-il de Confirmed Rewards ?'
    a: 'Estimated Rewards représente votre part des blocs trouvés qui attendent encore leurs 60 confirmations : le chiffre peut donc évoluer. Confirmed Rewards provient de blocs enfouis à 60 blocs et attend le paiement. À chaque maturation, le montant passe de la première carte à la seconde.'
  - q: 'Mes récompenses cessent-elles dès que j’arrête de miner ?'
    a: 'Non. Chaque share reste dans la fenêtre PPLNS pendant six heures après son envoi. Les blocs trouvés pendant ce délai vous créditent encore. Les récompenses déjà confirmées restent dans votre solde et sont payées dès qu’il atteint le minimum de 0,1 BNT.'
  - q: 'Que signifie un effort supérieur à 100 % sur la page de chance ?'
    a: 'L’effort compare le travail du pool pendant une période à la difficulté du bloc, soit le travail attendu. Au-dessus de 100 %, la période a pris plus de temps que la moyenne ; en dessous, elle a été plus rapide. Les deux sont normaux et, sur de nombreuses périodes, l’effort moyen se rapproche de 100 %.'
sourceHash: 'e00112e9e82e'
translation: draft
---

Ce guide explique comment bntpool.com transforme les shares de votre mineur en paiements Blocknet. Vous saurez comment une récompense est répartie, pourquoi elle attend environ cinq heures avant paiement, quand votre solde est envoyé et comment lire les cartes de la [page des statistiques](/stats) et la [page de chance](/luck).

Si vous découvrez les pools, [qu’est-ce qu’un pool de minage ?](/blog/fr/what-is-a-mining-pool/) présente les bases. Pour démarrer, le [guide de minage seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) vous mène du téléchargement aux premiers shares en quelques minutes.

## Qu’est-ce que PPLNS ?

<Answer>
PPLNS signifie « pay per last N shares », paiement selon les N derniers shares. Quand bntpool.com trouve un bloc, il répartit la récompense entre les shares envoyés pendant les six heures précédentes, proportionnellement à leur difficulté. Plus vous fournissez de travail régulier dans cette fenêtre, plus votre part de chaque bloc est grande.
</Answer>

Sur bntpool.com, les « N derniers » correspondent à une durée : six heures. La [page Get Started](/start) affiche le système de paiement, la fenêtre, le minimum et les confirmations. Ces mêmes valeurs sont publiées à `https://bntpool.com/api/info`.

| Réglage | Valeur sur bntpool.com |
|---|---|
| Système de paiement | PPLNS |
| Fenêtre des shares | 6 heures |
| Confirmations avant paiement | 60 blocs |
| Paiement minimum | 0,1 BNT |

Les shares sont comptés selon leur difficulté. Le pool attribue une difficulté à chaque connexion et l’ajuste pour que chaque mineur envoie un flux régulier. Une machine rapide reçoit des shares plus difficiles : chacun pèse donc davantage. Votre part dépend du travail total fourni, une mesure équitable pour un portable comme pour un serveur.

## Comment bntpool.com répartit-il une récompense de bloc ?

À chaque bloc trouvé, les étapes sont les mêmes :

1. **Le bloc est enregistré.** Il apparaît sur la [page des blocs](/blocks) avec un statut en attente.
2. **La fenêtre est mesurée.** Le pool prend les six heures se terminant au bloc et additionne la difficulté des shares acceptés pour chaque adresse.
3. **La récompense est divisée.** Chaque adresse reçoit la même fraction de récompense que sa fraction du travail total de la fenêtre.
4. **Le bloc mûrit.** Votre part reste une estimation jusqu’à ce que le bloc soit enfoui à 60 blocs, puis rejoint le solde confirmé.
5. **Le solde est payé.** Dès que le solde confirmé atteint 0,1 BNT, le pool l’envoie à votre adresse.

## Un exemple de calcul

<Note>
Cet exemple utilise des nombres ronds fictifs pour illustrer le calcul. Les totaux réels dépendent du hashrate du pool, du vôtre et de la [récompense de bloc](/blog/fr/blocknet-block-reward-schedule/) à ce moment.
</Note>

Supposons que, pendant les six heures précédant un bloc, tous les mineurs aient soumis des shares totalisant une difficulté de 1 000 000, dont 20 000 pour vous. Votre fraction est 20 000 ÷ 1 000 000 = 2 %. Si le pool répartit 400 BNT, votre crédit vaut 400 × 0,02 = 8 BNT.

Un deuxième mineur au même hashrate vous rejoint 30 minutes avant le bloc. Trente minutes étant un douzième de six heures, il totalise environ 1 667 de difficulté et reçoit environ 0,67 BNT. Son travail continue toutefois à compter : chaque share reste dans la fenêtre pendant les six heures suivantes et gagne une part de chaque bloc trouvé durant ce délai.

| Mineur (exemple) | Travail dans la fenêtre | Fraction | Crédit sur 400 BNT |
|---|---|---|---|
| Vous, pendant les six heures | 20 000 | 2 % | 8 BNT |
| Même hashrate, arrivé 30 minutes avant | 1 667 | environ 0,17 % | environ 0,67 BNT |
| Petit mineur, pendant les six heures | 100 | 0,01 % | 0,04 BNT |

Le petit mineur de la dernière ligne gagne 0,04 BNT par bloc. Son solde confirmé dépasse donc le minimum de 0,1 BNT après trois blocs de ce type, puis il est payé.

## Pourquoi PPLNS favorise-t-il le minage régulier ?

Chaque share compte pour tous les blocs trouvés pendant les six heures suivantes. Un mineur fonctionnant en continu garde toujours une fenêtre pleine de travail : chaque nouveau bloc lui attribue sa part complète. Ses revenus suivent ainsi son hashrate de près d’un bloc à l’autre.

PPLNS tolère aussi les interruptions. Un redémarrage, une mise à jour de pilote ou une courte coupure ne remet pas votre position à zéro : les shares déjà envoyés restent six heures dans la fenêtre. Le travail ajouté après un bloc compte pour les suivants, donc tout hashrate supplémentaire est utile.

Pour les résultats les plus réguliers, laissez votre mineur tourner et s’optimiser. seine ajuste threads et kernel CPU sans se reconnecter au pool, maintenant un flux continu de shares. Vous pouvez [télécharger seine](https://seineminer.com/fr/#download) pour Windows, macOS ou Linux.

## Pourquoi attendre 60 confirmations pour une récompense ?

Blocknet impose 60 blocs avant de pouvoir dépenser les coins nouvellement minés. Le daemon appelle cette règle la maturité coinbase, environ cinq heures au rythme de cinq minutes (60 × 5 minutes = 300 minutes). bntpool.com utilise les mêmes 60 blocs avant de confirmer une récompense : chaque paiement provient de coins réellement dépensables.

Pendant la maturation, le pool vérifie que le bloc reste dans la chaîne principale. À 60 blocs de profondeur, il le marque confirmé et ajoute la récompense aux soldes confirmés. Dans le rare cas où le réseau retient un autre bloc à cette hauteur, le bloc devient orphelin et ses récompenses estimées sont retirées.

## Quand arrivent les paiements bntpool.com ?

<Answer>
Après 60 confirmations, environ cinq heures avec les blocs de cinq minutes de Blocknet, votre part rejoint Confirmed Rewards sur la page de statistiques. Dès que le solde confirmé atteint 0,1 BNT, bntpool.com l’envoie automatiquement à votre adresse. Rien à demander ni à réclamer.
</Answer>

Pour un nouveau mineur, la première estimation apparaît quand le pool trouve un bloc avec vos shares dans la fenêtre. Le premier paiement suit la maturation du bloc et l’atteinte du minimum. Les envois sont répertoriés sur la [page des paiements](/payouts) et dans votre historique personnel.

## Comment sont affichées les récompenses provisoires ?

Recherchez votre adresse ou `@name` sur la [page des statistiques](/stats). Trois cartes apparaissent :

- **Estimated Rewards :** votre part des blocs non encore arrivés à maturité. Elle reste provisoire jusqu’à confirmation ou classement comme orphelin.
- **Confirmed Rewards :** votre solde arrivé à maturité, en attente de paiement.
- **Paid Balance :** tout ce qui a déjà été envoyé à votre adresse.

Le pool sépare les récompenses provisoires pour que le solde confirmé ne contienne que des montants définitifs. Sous les cartes, chaque bloc non confirmé est listé avec votre part estimée, pour suivre sa progression. Le [guide de suivi de votre mineur](/blog/fr/track-your-miner-on-bntpool/) présente le reste de la page.

## Que montrent la chance et l’effort ?

L’effort mesure le travail fourni pour trouver un bloc : le travail accepté pendant une période divisé par la difficulté du bloc, exprimé en pourcentage. La difficulté Blocknet équivaut au nombre attendu de hachages par bloc : 100 % signifie exactement le travail moyen. En dessous, le pool a trouvé plus vite ; au-dessus, il lui a fallu plus longtemps.

La [page de chance](/luck) répertorie chaque période avec hauteur, effort, durée, statut et date de découverte. Le [tableau de bord du pool](/) affiche l’effort de la période en cours et la chance moyenne. Les périodes passent au-dessus et au-dessous de 100 %, normalement pour tout pool de preuve de travail. Sur de nombreuses périodes, l’effort moyen se rapproche de 100 %.

La chance influence la fréquence des blocs. Votre fraction de chacun dépend uniquement de votre part du travail dans la fenêtre, et un minage régulier la maintient complète, que la période soit courte ou longue.

<CTA target="pool" title="Rejoignez le pool" text="Minez régulièrement sur bntpool.com : votre part de chaque bloc est payée automatiquement dès 0,1 BNT." />
