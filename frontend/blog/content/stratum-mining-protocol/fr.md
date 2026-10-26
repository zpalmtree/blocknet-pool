---
title: 'Stratum : comment les mineurs parlent au pool'
description: 'Suivez la conversation Stratum entre seine et bntpool.com : connexion, tâches, cibles, envois de shares et réponses, pour comprendre l''activité du mineur.'
tags: ['Bases du minage', 'Stratum', 'Minage en pool']
faq:
  - q: 'Quel est le rôle de Stratum dans un pool ?'
    a: 'Stratum transporte la conversation entre mineur et pool : identification de la session, distribution du travail, envoi des shares et réponses. Le mineur calcule les hashes localement. Le pool relie ce travail à son nœud et tient la comptabilité nécessaire à son mode de paiement.'
  - q: 'Une adresse Stratum est-elle une adresse web ?'
    a: 'Ces adresses servent des connexions distinctes. Le site bntpool.com utilise HTTPS pour ses pages et son API ; son point de minage documenté est stratum+tcp://bntpool.com:3333. Saisissez celui-ci dans les paramètres du pool du mineur. Ouvrir le site ne crée pas à lui seul une connexion de minage.'
  - q: 'Une share Stratum acceptée signifie-t-elle un paiement ?'
    a: 'Non. La réponse indique le résultat d''un envoi dans la session. Vérification, bloc valide pour le réseau, admissibilité aux récompenses et paiement terminé sont des étapes distinctes. Suivez l''activité acceptée dans My Stats et consultez le guide des paiements pour les confirmations, PPLNS et le seuil.'
sourceHash: '1dba87acd7c0'
translation: draft
---

Suivez une tâche de minage de bntpool.com vers seine, puis une share vers le pool. Cette conversation éclaire les messages de connexion, les nouvelles tâches et les réponses. Ce guide décrit l'implémentation de bntpool.com ; pour configurer l'adresse de connexion et de paiement, utilisez le [guide du pool](/blog/fr/mine-blocknet-on-bntpool/).

## Qu'est-ce que Stratum dans le minage ?

<Answer>
Stratum est la couche de communication utilisée pour recevoir du travail du pool et envoyer des résultats. Avec seine et bntpool.com, une connexion TCP persistante transporte des messages JSON de connexion, de tâches et de shares. Votre machine calcule les hashes ; le pool obtient les modèles de blocs de son nœud et traite le travail envoyé.
</Answer>

*Un mineur envoie sa connexion et ses shares au pool, reçoit des tâches et réponses, tandis que le pool échange modèles et candidats de blocs avec son nœud.*

## Le point de connexion désigne le service de minage

Le point documenté est `stratum+tcp://bntpool.com:3333`. Le schéma indique le transport, le nom d'hôte le service et le port son écouteur de minage. La connexion du navigateur à `https://bntpool.com` a une autre fonction. Copiez l'adresse complète depuis [Get Started](/start) dans les paramètres du pool pour ouvrir la connexion prévue.

La connexion persistante permet aux deux côtés d'envoyer des messages dès que du travail est disponible. bntpool utilise du JSON délimité par des sauts de ligne et des identifiants reliant réponses et requêtes. Ces détails décrivent son format : le mineur doit le prendre en charge, avec ses capacités et l'algorithme Blocknet. Le [code du protocole](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) décrit les champs actuels.

## Que se passe-t-il à l'ouverture de session ?

Le client envoie `login` avec l'adresse de paiement, le nom de la machine, la version du protocole et les capacités prises en charge. Le pool vérifie puis renvoie ses informations de protocole et capacités requises. Cela établit les modalités de l'échange. La connexion TCP est une première étape ; un login accepté confirme le début de la conversation applicative.

Le nom de machine distingue son activité sous une adresse. Utilisez des noms différents pour plusieurs machines, avec l'adresse de paiement souhaitée. Le login demande l'adresse de réception, pas la phrase de récupération ni la clé privée de dépense. Une fois la session acceptée et du travail disponible, le pool envoie `job` pour démarrer le hachage.

## Que contient une tâche de minage ?

Une tâche comprend les identifiants de l'affectation et du modèle, les données d'en-tête, la hauteur, les cibles de share et de réseau, ainsi qu'une plage de nonces. Le nonce est la valeur modifiée pendant le hachage. La plage définit l'espace de recherche de la session. L'identifiant relie un envoi ultérieur au travail et aux conditions initialement attribués.

La cible de share définit les résultats à envoyer comme preuve de travail. La cible réseau détermine les candidats de blocs. La plupart des shares servent à la comptabilité sans devenir des blocs. Le [guide de difficulté](/blog/fr/mining-difficulty-explained/) explique la taille des cibles et le travail attendu ; le protocole fournit ces cibles dans l'affectation.

## Les tâches peuvent changer sans fermer la connexion

Une nouvelle tâche n'indique pas à elle seule une déconnexion. Le pool peut distribuer une mise à jour après changement de modèle ou une affectation avec une nouvelle difficulté de share. Sa difficulté variable adapte le seuil d'envoi au travail observé. Le mineur suit cette affectation tandis que la connexion continue à transporter les messages dans les deux sens.

Distinguez les identifiants de tâche et de modèle dans les journaux techniques. Plusieurs affectations peuvent utiliser le même modèle avec des détails de session différents. La hauteur ne décrit pas tous les changements. En pratique, vérifiez que les tâches arrivent, que le hachage progresse et que les envois reçoivent une réponse. Le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/) montre cette activité.

## Que contient l'envoi d'une share ?

Lorsqu'il trouve un résultat admissible, seine envoie `submit` avec la tâche, le nonce et le hash déclaré. Le pool le traite dans le contexte de l'affectation et répond avec l'identifiant de requête. Le hash déclaré appartient au message ; vérification et validation déterminent la suite. Un candidat de bloc est transmis au daemon pour validation.

La réponse peut indiquer acceptation, état de vérification et difficulté. Ces champs restent distincts : chaque réponse réussie n'est pas une découverte de bloc. Comptabilité et paiements interviennent au-delà de cet échange. Le [guide des shares](/blog/fr/accepted-rejected-stale-shares/) explique les résultats d'envoi, et le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) relie le travail admissible aux récompenses et paiements.

## Comment utiliser cette séquence pour vérifier un mineur ?

Lisez les étapes dans l'ordre : point atteint, login accepté, tâche reçue, hachage actif, envois répondus. Après une perte de connexion, le client reconnecte et établit une session avant de recevoir du travail. Utilisez le message exact et son heure pour situer l'étape. Un site accessible ne prouve pas que le port de minage l'est depuis cette machine.

Ouvrez [My Stats](/stats) pour l'adresse souhaitée et le [statut du pool](/status) pour le service. Après démarrage ou reconnexion, laissez les estimations de hashrate accumuler des shares. Distinguez protocole et calendrier des paiements : login prépare la session, les tâches fournissent le travail, les shares rapportent les résultats et la comptabilité détermine les étapes suivantes.

<CTA target="pool" title="Connectez seine à bntpool.com" text="Utilisez la page de configuration du pool, puis suivez les tâches, shares et votre machine dans My Stats." />
