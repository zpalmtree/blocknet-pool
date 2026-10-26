---
title: 'Frais de pool et frais du développeur du mineur'
description: 'Comprenez les frais du pool et du mineur sur Blocknet : où ils s''appliquent, comment les combiner et comment éviter de déduire deux fois les mêmes frais.'
tags: ['Guide de minage', 'Frais de pool', 'Récompenses']
faq:
  - q: 'S''agit-il des mêmes frais ?'
    a: 'Non. Les frais du pool relèvent de la distribution, ceux du développeur du logiciel. seine v0.2.36 consacre 2% du temps soumis aux frais au développement après la période initiale. Le taux distinct publié par bntpool.com était de 0% le 26 octobre 2026.'
  - q: 'Comment les combiner ?'
    a: 'Multipliez l''estimation brute par un moins la fraction du mineur, puis par un moins celle du pool. Utilisez votre version réelle et le réglage courant. Cela estime les récompenses conservées, sans garantir un paiement ni calculer un bénéfice monétaire.'
  - q: 'Faut-il déduire les frais du mineur du hashrate du pool ?'
    a: 'Identifiez d''abord sa signification. S''il estime déjà le travail crédité à votre adresse après le temps développeur, une nouvelle déduction ferait doublon. Le calculateur permet zéro pour cet ajustement déjà inclus. Ne déduisez à nouveau aucun frais d''un solde confirmé ou paiement enregistré.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Repérez les deux niveaux de frais d'une estimation Blocknet et leurs étapes. Ce guide utilise seine v0.2.36 et les réglages publics de bntpool vérifiés le 26 octobre 2026. Il aide à saisir le [calculateur](/blog/fr/estimate-blocknet-mining-rewards/#mining-calculator) et à lire [My Stats](/stats).

## Quelle différence entre frais de pool et de mineur ?

<Answer>
Les frais du pool concernent la distribution ; ceux du développeur concernent le calendrier de minage du logiciel. Responsables et étapes diffèrent. Vérifiez les deux réglages, déterminez si l'entrée est brute ou déjà ajustée et appliquez chaque correction pertinente une seule fois.
</Answer>

*Le travail passe par un ajustement du temps du mineur puis de la récompense du pool avant le montant conservé estimé.*

## À quoi s'appliquent les frais du pool ?

Le pool regroupe le travail admissible et partage la récompense distribuable selon ses règles. bntpool calcule les frais configurés sur la récompense enregistrée du bloc avant de répartir le reste en PPLNS. Le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) détaille participation et crédits.

Le 26 octobre 2026, `https://bntpool.com/api/info` publie `pool_fee_pct` à 0,0, soit 0%. Consultez la valeur actuelle sur [Get Started](/start) ou cette API à chaque estimation. Zéro décrit ce réglage du pool ; la politique du logiciel reste une entrée distincte.

## Comment fonctionnent les frais de seine ?

v0.2.36 documente 2% sur chaque pool et en solo. Il s'agit de temps de minage pour l'adresse du développeur, pas d'un retrait ultérieur de votre portefeuille. Les 10 premières minutes sont sans frais ; le suivi compte le temps suivant. Le [guide de configuration](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) décrit cette politique.

La planification fonctionne par tours : une courte fenêtre ne donne pas nécessairement exactement 2%. C'est l'objectif à long terme après la période initiale, pas une promesse par minute ou bloc. Notez la version ; une ancienne documentation ou un autre logiciel peut différer. Les frais du pool ne remplacent pas cette information.

## Comment combiner les pourcentages ?

Utilisez les fractions conservées. Avec m pour le mineur et p pour le pool, exprimés en fractions, il reste `(1 - m) * (1 - p)` du brut. C'est la formule de l'[estimateur](/blog/fr/estimate-blocknet-mining-rewards/). Additionner les taux n'est pas la règle générale : le second agit sur le reste.

Exemple uniquement arithmétique : 100 BNT bruts hypothétiques, ajustement mineur de 2% et pool vérifié à 0% donnent 100 × 0,98 × 1,00 = 98 BNT. Cela illustre le régime stable, pas une mesure matérielle, une récompense promise ou une courte session avec période initiale.

## Choisissez la bonne entrée de hashrate

Un taux local ou de benchmark décrit le travail de la machine. Avant le facteur développeur, déterminez s'il couvre tout le temps ou seulement le travail pour votre adresse. Vérifiez origine et fenêtre. Le [guide du hashrate](/blog/fr/what-is-hashrate/) explique les observations locales, du pool et du réseau.

Le taux du pool pour votre adresse peut déjà refléter les périodes créditées ailleurs. Si cet ajustement est inclus, saisissez zéro pour les frais supplémentaires du mineur. Cela change le calcul, pas la politique réelle. Les frais du pool restent applicables à une estimation brute admissible ; vérifiez le contenu de chaque chiffre.

## Pourquoi ne pas déduire les frais du solde à nouveau ?

Un solde confirmé est un résultat comptable, pas une estimation brute intacte. Le pool a déjà attribué les crédits. Déduire encore les mêmes frais du solde ou d'un paiement modifie le chiffre sans nouvel événement de frais. Utilisez le montant affiché pour rapprocher les comptes et identifiez l'étape.

Récompense en attente, solde confirmé et paiement envoyé diffèrent aussi dans le temps. Maturité et seuil de paiement expliquent les transitions, sans frais en pourcentage supplémentaires. Consultez le [guide des confirmations](/blog/fr/block-confirmations-mining-rewards/) et l'[historique](/payouts). Les frais éventuels d'une transaction du portefeuille sont un champ distinct.

## Gardez les entrées traçables

Conservez date, version, frais, source du hashrate et fenêtre. Le calculateur combine données pool/réseau en direct et vos entrées. Actualisez les données en revenant au résultat. Les crédits réels suivent aussi shares admissibles, blocs trouvés et PPLNS ; formule et écriture ultérieure répondent à des questions différentes.

Utilisez [My Stats](/stats) et le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/) pour les données enregistrées. Comparez même adresse, période et étape avant de modifier les frais. Le calculateur estime des BNT ; électricité, matériel et prix monétaire restent hors calcul. Aucun changement d'adresse n'est nécessaire.

<CTA target="pool" title="Suivez vos récompenses sur bntpool.com" text="Utilisez le calculateur pour les estimations et My Stats pour le travail, les soldes et les paiements enregistrés." />
