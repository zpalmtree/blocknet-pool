---
title: 'Shares acceptés, rejetés et périmés : comprendre'
description: 'Comprenez les shares acceptés, rejetés et périmés sur bntpool.com, leurs messages d’erreur et l’effet de la difficulté sur vos statistiques de minage.'
tags: ['Guide de minage', 'Shares', 'Dépannage']
faq:
  - q: 'Un share accepté signifie-t-il que j’ai trouvé un bloc ?'
    a: 'Cela signifie que le pool a accepté un résultat selon ses règles de traitement des shares. Un bloc doit atteindre une cible réseau distincte et être accepté par le daemon. Les récompenses suivent la découverte, la validation et le paiement ; l’accusé de réception d’un share n’annonce pas un paiement.'
  - q: 'Pourquoi rejeter un share périmé ?'
    a: 'La réponse stale job indique une affectation devenue inéligible, par exemple expirée ou liée à un ancien modèle au-delà de son délai de grâce. Vérifiez l’arrivée de nouveaux jobs et de shares acceptés. Des réponses répétées demandent de vérifier la connexion et la version du mineur.'
  - q: 'Une difficulté supérieure réduit-elle ma récompense ?'
    a: 'La difficulté change le travail représenté par chaque share éligible et sa fréquence attendue. Comparez le travail pondéré par difficulté sur des périodes équivalentes plutôt que les nombres bruts de shares. La récompense dépend aussi de la fenêtre PPLNS, du travail éligible, des blocs trouvés et des règles de validation.'
sourceHash: 5be830208e6c
translation: draft
---

Le résultat d’un share dans le journal permet de décider si vous devez continuer, vérifier la connexion ou examiner des rejets répétés. Voici les réponses de bntpool.com et la difficulté derrière ses compteurs. Commencez par le [guide de connexion](/blog/fr/mine-blocknet-on-bntpool/) si nécessaire, ou par le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/#zero-hashrate) si une adresse ne montre aucune activité.

## Qu’est-ce qu’un share accepté ?

<Answer>
Un share est un résultat soumis selon une cible attribuée par le pool. Une réponse acceptée confirme son entrée dans le traitement des shares : du travail parvient au pool. La découverte de blocs, la vérification des shares et l’éligibilité au paiement constituent des étapes distinctes. L’acceptation seule n’est donc pas un paiement.
</Answer>

Le mineur calcule de nombreux hashes entre deux shares. La plupart n’atteignent pas la cible et ne sont pas soumis. Le compteur d’acceptations représente des résultats de soumission, pas tous les hashes calculés par le CPU ou GPU. Le [guide du hashrate](/blog/fr/what-is-hashrate/) distingue comptage local et estimation du pool à partir du travail reçu.

## Quelle différence entre shares et blocs ?

Le pool attribue des jobs et une difficulté à votre connexion. Atteindre la cible de share démontre du travail à cette difficulté. Un candidat bloc doit aussi atteindre la cible réseau et passer les contrôles du daemon. Trouver un share n’implique pas l’ajout d’un bloc à la chaîne.

bntpool.com enregistre des shares vérifiés ou provisoires pendant la validation. Une soumission acceptée peut être provisoire ; la pondération des paiements applique des règles de validation et d’éligibilité. L’acceptation renseigne sur la connexion. Consultez [Mes statistiques](/stats) pour le travail et les récompenses enregistrés, puis le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) pour la suite après un bloc.

## Que signifient les shares rejetés et périmés ?

Un rejet signifie qu’un contrôle n’a pas été satisfait. Son motif est plus utile que le seul compteur. Un job périmé est une catégorie de rejet : l’affectation n’est plus éligible. Lisez le message exact et rapprochez-le des événements simultanés, comme une reconnexion ou un nouveau job.

| Message | Signification et vérification suivante |
|---|---|
| `stale job` | Affectation absente, expirée, remplacée au-delà du délai de grâce, ou liée à un ancien modèle indisponible. Vérifiez la reprise des nouveaux jobs et shares acceptés. |
| `duplicate share` | La même combinaison modèle et nonce a déjà été soumise. Cherchez des soumissions répétées ; renvoyer le même travail n’ajoute pas une contribution. |
| `low difficulty share` | Le hash soumis n’atteint pas la cible attribuée. Si cela se répète, vérifiez la version du mineur et le traitement des jobs et difficultés. |
| `nonce out of assigned range` | Le nonce est hors de la plage attribuée au job. Vérifiez la version du mineur et tout proxy ou intégration personnalisée. |

Un share périmé et un bloc orphelin concernent des étapes différentes : le travail d’une affectation pour le premier, la place d’un bloc dans la chaîne pour le second. Distinguez le journal des shares de la [liste des blocs](/blocks). Un message stale job seul ne signifie pas que le pool a trouvé un bloc orphelin.

## Pourquoi la difficulté des shares change-t-elle ?

<Answer>
La difficulté variable, ou vardiff, adapte la cible d’une connexion à son activité observée. Une difficulté supérieure donne généralement moins de shares, chacun représentant davantage de travail ; une difficulté inférieure produit l’inverse. Comparez le travail éligible pondéré sur la même période plutôt que le nombre de shares seul.
</Answer>

Exemple arithmétique : dix shares éligibles de difficulté 100 totalisent autant qu’un share de difficulté 1 000, soit 1 000. Cela illustre la pondération, sans promettre de récompense ni d’horaire d’arrivée. Les intervalles réels varient et les règles de validation déterminent le travail comptabilisé.

La difficulté de l’affectation compte lors de la soumission. Ne comparez pas un ancien résultat uniquement à la dernière difficulté affichée. Le pool conserve les affectations pour contrôler les envois. Un ajustement normal n’est pas une erreur ; imposer des shares plus faciles ne crée aucun travail de calcul supplémentaire.

## Comment interpréter un taux de rejet ?

Comparez acceptations et rejets sur la même période et le même périmètre. Le mineur peut compter une session tandis que le pool couvre un autre intervalle ou plusieurs machines. Redémarrer le mineur change la fenêtre locale, sans effacer l’historique de votre adresse au pool.

Par exemple, 99 soumissions acceptées et une rejetée donnent 1% de rejet en nombre. Ce n’est pas automatiquement le pourcentage de récompense perdu : les difficultés peuvent varier et la comptabilité applique des règles d’éligibilité. Examinez les raisons, leur chronologie et la poursuite du travail accepté avant de modifier des réglages.

## Que faire si les rejets se répètent ?

Commencez par une machine touchée. Notez la version, le message exact, le début du problème et l’arrivée éventuelle de nouveaux jobs. Consultez [État](/status), puis recherchez des interruptions de connexion. Mettez à jour depuis les [versions officielles de seine](https://github.com/zpalmtree/seine/releases) et comparez après une seule modification à la fois.

Un proxy ou une intégration personnalisée doit préserver le job, la plage de nonce et la difficulté attribués. Si le problème persiste, fournissez un court extrait de journal et les détails de la machine au canal d’assistance du projet. Retirez secrets et informations privées ; une phrase de récupération n’est jamais nécessaire pour diagnostiquer des shares.

## Comment confirmer la reprise du minage ?

Observez de nouveaux jobs suivis de shares acceptés, puis ouvrez [Mes statistiques](/stats) avec l’adresse de réception du mineur. Vérifiez l’activité récente de la machine. Laissez l’estimation du hashrate accumuler du travail avant de comparer les moyennes. Suivez ensuite récompenses estimées, solde et paiements comme étapes distinctes dans le guide PPLNS.

<CTA target="pool" title="Se connecter à bntpool.com" text="Suivez le guide de connexion, confirmez les shares acceptés et retrouvez votre activité avec votre adresse de réception." />
