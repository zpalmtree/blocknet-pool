---
title: Qu’est-ce qu’un pool de minage ?
description: Un pool réunit le hashrate de nombreux mineurs et répartit les récompenses. Découvrez son fonctionnement, PPLNS, PPS, le solo et les critères de choix.
tags: [Bases du minage, Pools de minage, PPLNS, bntpool]
faq:
  - q: 'Rejoindre un pool augmente-t-il ce que je mine ?'
    a: 'Le pool ne change ni votre hashrate ni votre part du travail du réseau. Il change la façon de recevoir les récompenses. Seul, vous attendez des blocs entiers à intervalles aléatoires. En pool, vous gagnez une petite fraction de chaque bloc trouvé : les paiements sont fréquents et suivent votre hashrate.'
  - q: 'Quelle différence entre PPLNS et PPS ?'
    a: 'PPLNS verse une fraction de chaque bloc réellement trouvé, selon le travail soumis dans une fenêtre récente. PPS verse un montant fixe pour chaque share accepté, calculé d’après sa valeur attendue, que le pool trouve un bloc ou non. bntpool.com utilise PPLNS sur six heures.'
  - q: 'Faut-il gérer son propre nœud pour miner dans un pool ?'
    a: 'Non. Le pool gère le nœud, prépare les modèles, vérifie les shares et soumet les blocs. Sur bntpool.com, il suffit de seine et d’une adresse Blocknet ou d’un nom Blocknet ID. Lancez seine, collez votre adresse et acceptez le pool par défaut : votre part des blocs y est payée.'
  - q: 'Comment commencer sur bntpool.com ?'
    a: 'Téléchargez seine pour Windows, macOS ou Linux sur seineminer.com, lancez-le et collez votre adresse Blocknet ou @name. bntpool.com est le pool par défaut : acceptez-le pour commencer. Recherchez votre adresse dans les statistiques du pool pour voir hashrate, shares et solde.'
sourceHash: 7b8c04cd86ae
translation: draft
---

Ce guide explique ce qu’est un pool de minage, pourquoi la plupart des mineurs en rejoignent un et comment il calcule les gains. Vous découvrirez PPLNS, PPS et le solo, puis les points à vérifier avant de connecter votre mineur. bntpool.com, le pool Blocknet, sert d’exemple tout au long du guide.

## Qu’est-ce qu’un pool de minage ?

<Answer>
Un pool est un serveur qui réunit le [hashrate](/blog/fr/what-is-hashrate/) de nombreux mineurs pour trouver des blocs ensemble. Il leur attribue du travail sur le même bloc, mesure le travail soumis par chacun et répartit chaque récompense proportionnellement. Les mineurs reçoivent de petits paiements fréquents plutôt que de rares blocs entiers.
</Answer>

Dans une monnaie à preuve de travail, les mineurs cherchent le prochain bloc en hachant son en-tête avec différents nonces jusqu’à obtenir un résultat inférieur à la cible réseau. Chaque hachage est un essai indépendant avec la même faible chance de succès. Celui qui trouve le hachage gagnant reçoit la récompense.

Le pool transforme cette compétition en travail collectif. Tous les mineurs de bntpool.com travaillent sur les blocs qu’il prépare. Dès que l’un trouve un hachage gagnant, la récompense est répartie entre les mineurs dont le travail a contribué à la recherche.

## Pourquoi rejoindre un pool ?

<Answer>
Pour être payé régulièrement. Seul, un mineur ordinaire possède une petite part du hashrate réseau et peut attendre plusieurs jours entre ses blocs, à intervalles aléatoires. En pool, le hashrate réuni trouve souvent des blocs et chacun en reçoit une part : les revenus arrivent régulièrement et suivent son hashrate.
</Answer>

Le calcul est simple sur Blocknet. Le réseau vise un bloc toutes les cinq minutes, 288 par jour. Un mineur seul peut donc attendre 288 × (son hashrate ÷ hashrate réseau) blocs par jour.

Prenons des nombres ronds hypothétiques : un rig à 50 H/s sur un réseau à 50 000 H/s détient 0,1 % du réseau et trouve en moyenne 0,288 bloc par jour, un tous les trois jours et demi environ. Ce n’est qu’une moyenne. La découverte étant aléatoire, l’écart réel peut être bien plus court ou plus long.

Sur un pool totalisant 5 000 H/s, environ 28,8 blocs sont trouvés par jour, un toutes les 50 minutes en moyenne. Le rig fournit 1 % du travail et gagne 1 % de chaque bloc. Son travail ne change pas ; la récompense arrive simplement en petites portions plusieurs fois par jour. [Pool ou solo sur Blocknet](/blog/fr/pool-vs-solo-mining-blocknet/) étend la comparaison aux grandes installations.

## Comment fonctionne un pool ?

Tous suivent la même boucle de base. Sur bntpool.com :

1. **Le pool prépare un modèle de bloc.** Il exploite son propre nœud Blocknet et lui demande un modèle : les mineurs n’ont aucun nœud à lancer ni synchroniser.
2. **Il envoie du travail aux mineurs.** Ils se connectent par Stratum, le protocole de distribution du travail, à `stratum+tcp://bntpool.com:3333`. Chaque connexion reçoit une tâche issue du modèle actuel.
3. **Les mineurs hachent et soumettent des shares.** Un share est un hachage respectant une cible plus facile fixée par le pool. Il prouve le travail effectué.
4. **Un share respectant aussi la cible réseau est un bloc.** Le pool vérifie chaque share contre les deux cibles. S’il convient au réseau, il soumet le bloc au nœud.
5. **La récompense est répartie et payée.** Le pool la divise selon son système et envoie la part de chaque mineur une fois confirmée et au-dessus du minimum.

## Qu’est-ce qu’un share ?

Un share est un hachage satisfaisant une exigence moindre qu’un véritable bloc. Un bloc exige un hachage sous la cible réseau ; un share doit seulement passer sous la cible beaucoup plus facile du pool. La plupart ne deviennent jamais des blocs, mais montrent régulièrement combien chaque mineur hache.

Les shares sont pondérés par difficulté. Sur Blocknet, une difficulté D signifie environ D hachages attendus avant une réussite. Un share de difficulté 60 représente donc environ 60 hachages de travail. bntpool.com ajuste la difficulté par connexion pour viser environ 10 shares toutes les cinq minutes, avec une difficulté initiale de 60. Un serveur rapide reçoit des shares plus difficiles qu’un portable ; chacun est crédité selon son travail réel. Le pool mémorise aussi la difficulté de chaque rig pendant une heure, pour reprendre après un bref redémarrage.

## Les principaux systèmes de paiement

Le système de paiement définit comment transformer les shares en gains. Voici les plus courants.

| Système | Ce qui est rémunéré | Fonctionnement |
|---|---|---|
| PPLNS (pay per last N shares) | Blocs trouvés par le pool | Chaque récompense est répartie selon la difficulté des shares d’une fenêtre récente |
| PPS (pay per share) | Chaque share accepté | Montant fixe par share selon sa valeur attendue, que le pool trouve un bloc ou non |
| Solo | Blocs que vous trouvez | Vous utilisez votre nœud et gardez toute la récompense |

**PPLNS** regarde en arrière à partir de chaque bloc trouvé. Sur bntpool.com, les « N derniers shares » correspondent à six heures : le pool additionne leur difficulté et attribue à chacun la même fraction de récompense que de travail. Chaque share compte pour tous les blocs trouvés durant sa présence. Un mineur régulier conserve donc une fenêtre pleine et un redémarrage ne remet rien à zéro. [Les paiements PPLNS sur bntpool.com](/blog/fr/how-pplns-payouts-work/) détaillent la répartition.

**PPS** paie chaque share à son arrivée selon sa valeur attendue : difficulté du share divisée par difficulté réseau, multipliée par récompense de bloc. L’opérateur assume les variations des découvertes et paie les mineurs depuis son propre solde entre les blocs.

**Le minage solo** se passe de pool. Votre nœud prépare les modèles et chaque bloc trouvé verse toute sa récompense à votre portefeuille. Il convient aux installations assez grandes pour trouver souvent des blocs seules. seine prend en charge les deux : bntpool.com par défaut, ou `./seine --mode daemon` pour votre nœud synchronisé.

## Que vérifier dans un pool ?

Ces critères déterminent votre rémunération et la facilité de la contrôler :

- **Un système clair.** Le pool doit annoncer son système et ses paramètres, notamment la fenêtre PPLNS. bntpool.com les indique sur [Get Started](/start) et à `https://bntpool.com/api/info`.
- **Confirmations et minimum.** Les récompenses attendent généralement une profondeur suffisante, puis le paiement part au seuil. Sur bntpool.com, la confirmation demande 60 blocs, comme la maturité coinbase Blocknet, et les soldes de 0,1 BNT ou plus sont payés automatiquement.
- **Des chiffres publics et vérifiables.** Chaque bloc trouvé et paiement doit être visible. bntpool.com les affiche dans [blocs](/blocks) et [paiements](/payouts), l’effort par période sur [chance](/luck), et la santé des services sur [statut](/status).
- **Vos statistiques.** Recherchez votre adresse pour voir hashrate, shares et soldes. C’est la [page des statistiques](/stats) ; le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/) explique chaque carte.
- **Une installation simple.** Moins de réglages signifie un démarrage plus rapide. bntpool.com est le pool par défaut de seine, qui optimise seul threads, kernels et mémoire. Vous pouvez utiliser un [nom Blocknet ID](https://seineminer.com/fr/blog/blocknet-id-pay-by-name/) comme `@name` au lieu de l’adresse complète.

## bntpool.com en bref

| Réglage | bntpool.com |
|---|---|
| Système de paiement | PPLNS |
| Fenêtre des shares | 6 heures |
| Confirmations avant paiement | 60 blocs |
| Paiement minimum | 0,1 BNT |
| Adresse Stratum | `stratum+tcp://bntpool.com:3333` |

Le [tableau de bord du pool](/) présente en direct mineurs connectés, hashrates du pool et du réseau, blocs et effort courant. Le 25 octobre 2026 à 02:25 EDT, le pool totalisait environ 5 200 H/s sur environ 44 400 H/s de réseau, avec 246 mineurs connectés. Cela représente environ 12 %, assez pour attendre environ 34 blocs par jour, un toutes les 42 minutes. Ces chiffres évoluent constamment : consultez les valeurs actuelles et le [guide d’estimation des récompenses](/blog/fr/estimate-blocknet-mining-rewards/) pour les convertir en BNT selon votre hashrate.

Pour rejoindre le pool, [téléchargez seine](https://seineminer.com/fr/#download), lancez-le, collez votre adresse ou `@name` et acceptez le pool par défaut. Le [guide seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) décrit les étapes, et [miner sur bntpool.com](/blog/fr/mine-blocknet-on-bntpool/) couvre noms de rigs et premiers shares.

<CTA target="pool" title="Rejoignez bntpool.com" text="Connectez seine au pool et gagnez une part de chaque bloc trouvé, envoyée automatiquement à votre adresse dès 0,1 BNT." />
