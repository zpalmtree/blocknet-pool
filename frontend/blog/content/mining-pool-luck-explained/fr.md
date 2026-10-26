---
title: 'Chance d’un pool de minage : effort et temps entre les blocs'
description: 'Comprenez la chance du pool, l’effort à 100 % et les longues rondes. Consultez l’historique de bntpool.com et distinguez découverte de blocs et paiements.'
tags: ['Pools de minage', 'Chance du pool', 'Blocknet']
faq:
  - q: 'Un effort de 100 % signifie-t-il que le pool va bientôt trouver un bloc ?'
    a: 'Non. Cela signifie que le travail enregistré via les shares a atteint la quantité attendue pour un bloc à la difficulté utilisée dans ce calcul. C’est une comparaison avec une moyenne, pas un pourcentage d’achèvement. Chaque nouveau hash doit toujours satisfaire la cible du réseau.'
  - q: 'Un pourcentage de chance plus faible est-il préférable sur bntpool.com ?'
    a: 'Lisez d’abord le libellé et sa définition. La colonne Effort et la carte Chance moyenne de bntpool.com expriment le travail par rapport au travail attendu : une valeur inférieure signifie moins de travail enregistré par bloc. D’autres sites utilisent parfois la convention inverse. Comparez des métriques et tailles d’échantillon équivalentes, pas seulement des pourcentages.'
  - q: 'Une longue ronde augmentera-t-elle mon prochain paiement ?'
    a: 'Une longue ronde ne garantit pas un paiement personnel supérieur. La découverte d’un bloc détermine quand une récompense devient disponible ; la comptabilité PPLNS du pool détermine votre part selon le travail accepté admissible. Les règles de confirmation et de paiement s’appliquent ensuite. Consultez vos soldes dans Mes statistiques et les étapes dans le guide des paiements.'
sourceHash: 'f7051fa67591'
translation: draft
---

Le tableau de bord du pool affiche le travail de minage et les blocs qu’il permet de trouver. Ce guide explique les pourcentages d’effort, le temps attendu entre les blocs et l’historique des rondes sur bntpool.com. Vous pourrez distinguer une moyenne statistique d’un compte à rebours, puis vérifier votre connexion de minage sans attendre le prochain bloc du pool.

## Qu’est-ce que la chance d’un pool de minage ?

<Answer>
La chance d’un pool compare les découvertes réelles de blocs à la moyenne attendue pour son travail. Les blocs arrivent au hasard : certains demandent peu de travail, d’autres davantage. Sur bntpool.com, l’effort d’une ronde compare le travail des shares acceptées à la difficulté du bloc ; un effort inférieur signifie que la ronde a trouvé son bloc avec moins de travail enregistré.
</Answer>

Un pool réunit les tentatives de nombreux mineurs, mais doit toujours trouver un hash satisfaisant la cible du réseau. Les shares acceptées mesurent les contributions avec une cible plus facile. Elles permettent d’estimer le travail de hachage effectué entre deux découvertes. L’[introduction aux pools](/blog/fr/what-is-a-mining-pool/) explique cette relation ; la chance décrit la variation des blocs obtenus.

## Comment le pool calcule-t-il l’effort ?

Pour une ronde terminée, bntpool.com divise le travail des shares acceptées par la difficulté du bloc, puis multiplie par 100. Le travail est pondéré par la difficulté : ce n’est donc pas simplement le nombre de shares soumises. Une share à difficulté attribuée plus élevée représente davantage de travail de hachage attendu qu’une share à difficulté moindre.

**Effort (%) = travail des shares acceptées ÷ difficulté du bloc × 100**

Supposons qu’un bloc ait une difficulté de 10 000 et que le pool enregistre 5 000 unités de travail accepté durant sa ronde. Son effort est de 50 %. Avec 20 000 unités à difficulté identique, il serait de 200 %. Ce sont des exemples arithmétiques, pas des mesures d’un bloc particulier en production.

| Effort | Interprétation |
|---|---|
| 50% | La moitié du travail attendu pour cette difficulté |
| 100% | La quantité de travail attendue pour un bloc |
| 200% | Deux fois le travail attendu pour cette difficulté |

L’effort de la ronde actuelle utilise la difficulté actuelle, qui peut changer à mesure que la chaîne avance. Les lignes historiques utilisent la difficulté de leur bloc. Il s’agit donc d’une comparaison du travail, pas d’une jauge de progression devant s’arrêter à 100 %. La durée de la ronde est aussi distincte : un pool plus rapide peut effectuer le même travail en moins de temps.

## Que signifie le temps attendu entre les blocs ?

C’est l’intervalle moyen suggéré par la part estimée du pool dans le taux de hachage du réseau. Le tableau de bord de bntpool.com le calcule en multipliant 300 secondes par le hashrate du réseau, puis en divisant par celui du pool. Utilisez la même unité pour les deux. Le [guide du hashrate](/blog/fr/what-is-hashrate/) explique pourquoi ces estimations changent avec l’arrivée de shares acceptées et de nouveaux blocs.

**Secondes attendues = 300 × H/s du réseau ÷ H/s du pool**

Si le réseau est estimé à 100 000 H/s et le pool à 10 000 H/s, le pool représente un dixième du total estimé. Son intervalle attendu est de 3 000 secondes, soit 50 minutes. Cette moyenne peut décrire un mélange de rondes courtes et longues ; elle ne programme pas le prochain bloc exactement dans 50 minutes.

Le tableau de bord compare aussi le temps écoulé au temps attendu. Ce pourcentage temporel et l’effort basé sur les shares peuvent différer, car leurs données d’entrée sont différentes. Vérifiez les libellés avant de comparer deux chiffres. Des variations du hashrate du pool ou de la difficulté du réseau peuvent modifier l’estimation pendant une même ronde.

## Un bloc est-il dû après 100 % d’effort ?

<Answer>
Un pool n’a pas droit à un bloc lorsque l’effort atteint 100 %. Les hashes infructueux précédents n’augmentent pas la probabilité que le suivant satisfasse une cible réseau inchangée. À hashrate et difficulté constants, l’attente moyenne à partir de maintenant reste identique, même après une longue ronde. Davantage de travail crée plus de tentatives, pas une échéance.
</Answer>

Un modèle de probabilité simple permet de le comprendre. Avec des tentatives indépendantes et un taux constant de découverte des blocs, l’attente suit approximativement une loi exponentielle. La probabilité de n’avoir aucun bloc après un intervalle attendu est d’environ 36,8 % ; après deux, 13,5 % ; après trois, 5,0 %. Ces chiffres décrivent de nombreuses rondes hypothétiques dans des conditions fixes, pas une prévision de la ronde actuelle.

| Intervalles attendus écoulés | Probabilité approximative de n’avoir encore aucun bloc |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Le calcul est `exp(-t / T)`, où `T` est l’intervalle moyen et `t` le temps écoulé. La [référence du NIST sur la loi exponentielle](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) donne cette fonction de survie. Le hashrate et la difficulté réels varient : la table explique donc le hasard. Une succession de blocs rapides ne change pas non plus la probabilité du hash suivant lorsque la cible reste identique.

## Comment lire l’historique de chance de bntpool.com ?

Ouvrez [Chance](/luck) pour comparer les rondes terminées. Chaque ligne affiche un bloc, son effort, la durée de la ronde, son statut et l’heure de découverte. Examinez plusieurs rondes plutôt que d’en choisir une exceptionnellement courte ou longue. Le lien du bloc permet d’inspecter son enregistrement dans la chaîne ; la pagination donne accès à un historique plus large.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Véritable page d’historique de chance de bntpool.com avec effort, durée des rondes et statut des blocs" width="1200" height="833" loading="lazy" />

Cette capture date du 26 octobre 2026. Les lignes et statuts sont un instantané historique ; consultez la page en direct pour les valeurs actuelles. Un statut en attente ou confirmé décrit l’état de confirmation du bloc. Il ne change pas la signification de l’effort enregistré et reste distinct d’un paiement à votre adresse.

Sur le tableau de bord, Chance moyenne est la moyenne arithmétique des pourcentages d’effort enregistrés pour les blocs non orphelins jusqu’à la hauteur actuelle de la chaîne. Malgré son nom, la carte suit la convention de l’effort : une valeur inférieure signifie moins de travail enregistré par bloc. Les moyennes résument cet échantillon ; elles ne promettent aucune compensation future d’un résultat antérieur particulier.

## Comment la chance du pool affecte-t-elle vos récompenses ?

La chance du pool fait varier le moment où les blocs sont trouvés. PPLNS détermine comment répartir la récompense d’un bloc trouvé entre le travail accepté admissible. Ce sont deux étapes distinctes. Une limite de ronde ne transforme pas la fenêtre PPLNS en règle de paiement limitée à cette ronde ; lisez [le fonctionnement des paiements PPLNS](/blog/fr/how-pplns-payouts-work/) pour connaître la fenêtre, les confirmations et le seuil réels.

Votre solde peut donc évoluer par paliers alors que votre mineur continue de soumettre des shares acceptées. Le [calculateur de récompenses](/blog/fr/estimate-blocknet-mining-rewards/#mining-calculator) estime la production selon le hashrate et les données réseau. Il aide à établir des attentes dans la durée ; il ne prédit ni le bloc ni l’instant où un paiement arrivera dans votre portefeuille.

## Que vérifier pendant une longue ronde ?

Vérifiez les éléments directement observables : votre mineur fonctionne, les shares acceptées continuent et la bonne adresse apparaît dans [Mes statistiques](/stats). Consultez [État](/status) pour la santé publique du service. Si vos shares acceptées cessent, suivez le [guide de dépannage des statistiques](/blog/fr/track-your-miner-on-bntpool/#zero-hashrate) ; une longue ronde et une machine déconnectée demandent des vérifications différentes.

Une fois votre connexion saine, interprétez la chance sur un historique pertinent en conservant les mêmes définitions. Chaque chiffre prend alors un sens clair : le hashrate estime le travail par seconde, l’effort compare le travail au travail attendu pour un bloc et vos soldes montrent la comptabilité des récompenses. Aucun ne nécessite de deviner l’arrivée du prochain hash gagnant.

<CTA target="pool" title="Commencez à miner sur bntpool.com" text="Connectez seine, vérifiez vos shares acceptées et suivez les rondes et récompenses sur le tableau de bord du pool." />
