---
title: 'Paiement envoyé, mais absent du portefeuille ?'
description: 'Suivez un paiement Blocknet : transaction, adresse de réception et synchronisation du portefeuille, puis rassemblez les éléments pour retrouver l’entrée.'
tags: ['Paiements', 'Portefeuille', 'Dépannage']
faq:
  - q: 'Un paiement en file signifie-t-il que le portefeuille l’a reçu ?'
    a: 'La file d’attente est un état de traitement côté pool. Vérifiez si l’entrée comporte un identifiant de transaction et son statut actuel. L’enregistrement du paiement, l’arrivée de la transaction sur la chaîne et la reconnaissance de sa sortie par votre portefeuille sont des étapes distinctes. Suivez la transaction précise dès qu’elle existe.'
  - q: 'L’explorateur peut-il montrer le solde de mon portefeuille privé ?'
    a: 'L’explorateur permet de vérifier l’état public d’une transaction sur la chaîne. Blocknet utilise des sorties privées : une recherche d’adresse ordinaire n’affiche donc pas le solde de votre portefeuille et ne prouve pas qu’une sortie vous appartient. Votre portefeuille scanne la chaîne pour reconnaître ses propres sorties.'
  - q: 'Que fournir pour un paiement manquant ?'
    a: 'Fournissez l’adresse de réception complète utilisée sur le pool, l’identifiant de transaction, l’heure approximative, le statut du pool et l’avancement de la synchronisation. Précisez si la transaction figure dans l’historique. Partagez les données de compte dans le contexte d’assistance approprié, jamais la phrase de récupération, les clés privées ou le mot de passe.'
sourceHash: '915075cf4e2b'
translation: draft
---

Suivez un paiement Blocknet depuis le pool jusqu’au portefeuille destinataire. Ce guide commence lorsqu’une entrée de paiement est identifiable sur bntpool.com. Si seules des récompenses estimées ou confirmées attendent encore leur paiement, consultez d’abord le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) : c’est une étape antérieure avec ses règles de comptabilisation et de paiement.

## Comment retrouver un paiement absent du portefeuille ?

<Answer>
Repérez le paiement pour l’adresse complète, notez son identifiant de transaction et son statut actuel, puis vérifiez son état sur la chaîne. Ouvrez le portefeuille de cette destination et laissez synchronisation et scan rattraper leur retard. Comparez l’historique avant de déterminer si le paiement ou l’affichage local mérite une investigation.
</Answer>

*Un enregistrement de paiement rejoint une transaction sur la chaîne, puis le portefeuille qui scanne ses propres sorties entrantes.*

## Partez d’un paiement précis

Ouvrez My Stats et recherchez l’adresse complète utilisée lors du travail ayant produit ces récompenses. Notez montant, heure, statut et identifiant de transaction s’il existe. Partez de cette entrée plutôt que du total payé depuis le début, qui peut regrouper plusieurs paiements. Le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/) situe ces champs.

La page publique Payouts décrit des lots pouvant contenir plusieurs destinataires et liens de transactions. Gardez l’entrée propre à votre adresse avec la transaction examinée. Le total du lot n’est pas le montant attribué à votre portefeuille. Si plusieurs identifiants sont listés, développez la liste et gardez les identifiants pertinents plutôt qu’un libellé abrégé.

## Le paiement est-il en file, non confirmé ou confirmé ?

Lisez le statut actuel avant de supposer qu’un paiement listé a franchi toutes les étapes. Une entrée en file est encore traitée côté pool ; une transaction non confirmée attend une confirmation sur la chaîne. Actualisez l’entrée et suivez son lien lorsqu’il existe. La [page de statut](/status) apporte le contexte opérationnel si plusieurs indices évoquent une interruption.

Distinguez la maturité de la récompense minée de la confirmation de la transaction de paiement. Le bloc qui a produit la récompense et la transaction ultérieure ont des identifiants et rôles différents. Le [guide des confirmations](/blog/fr/block-confirmations-mining-rewards/) l’explique. Un identifiant donne un élément concret à examiner, mais ne prouve pas que votre portefeuille local a déjà scanné et reconnu le paiement.

## Vérifiez la transaction sur la chaîne

Ouvrez le lien d’explorateur joint au paiement et comparez l’identifiant complet à vos notes. Lisez l’état sur la chaîne et les informations du bloc si disponibles. Si l’explorateur ne trouve pas encore la transaction, comparez l’entrée actualisée et le statut du service avant de conclure. Un résultat temporaire ne permet pas, seul, d’identifier la cause.

Avec la confidentialité de Blocknet, une recherche d’adresse ordinaire dans l’explorateur n’affiche pas de solde public du destinataire. L’état de transaction et les vérifications de propriété du portefeuille répondent à des questions différentes. Votre portefeuille reconnaît ses sorties entrantes en scannant la chaîne. Le [guide des adresses furtives](https://seineminer.com/fr/blog/stealth-addresses-explained/) décrit ce mécanisme ; une liste publique ne reproduit pas l’historique privé du portefeuille.

## Consultez-vous le bon portefeuille destinataire ?

Comparez l’adresse recherchée sur le pool à l’adresse de réception du portefeuille ouvert. Comparez des valeurs complètes : des préfixes abrégés similaires ne suffisent pas. Avec un Blocknet ID, gardez l’adresse complète utilisée pour le travail initial. Le nom peut désormais pointer ailleurs après une modification, tandis qu’un ancien paiement reste associé à sa destination initiale.

Vérifiez tout ancien portefeuille utilisé avant le changement des réglages de minage. Modifier l’adresse du mineur ou l’association d’un nom ne déplace pas un paiement déjà attribué. Le [guide de changement d’adresse](/blog/fr/change-mining-wallet-address/) explique le suivi des deux historiques. Conservez le portefeuille initial et sa sauvegarde ; distinguez l’ouverture d’un portefeuille existant de la création d’un nouveau avec une autre adresse.

## Laissez le portefeuille se synchroniser et scanner

Vérifiez que le nœud avance vers la chaîne actuelle et que le scan du portefeuille a atteint le bloc de la transaction. Synchronisation du nœud et scan sont deux progressions distinctes. Un portefeuille récemment ouvert ou restauré peut demander du temps pour découvrir ses sorties alors que l’explorateur montre déjà la transaction dans un bloc.

Gardez le portefeuille connecté et suivez sa procédure documentée de synchronisation. Examinez l’historique entrant et le solde principal, car des dépenses ultérieures peuvent modifier ce total. Si une entrée existe, vérifiez sa confirmation ou disponibilité. Commencez par ces observations avant une récupération ou un scan complet et préservez les sauvegardes avant maintenance. Le [guide du portefeuille](https://seineminer.com/fr/blog/blocknet-wallet-for-mining/) couvre accès et sauvegardes.

## Quels éléments aident à résoudre une différence persistante ?

Notez adresse complète du pool, identifiant de transaction, heure approximative et statut actuel du pool. Ajoutez version du portefeuille, avancement du nœud et du scan, et présence de l’entrée dans l’historique. Précisez vos vérifications. Cela distingue un problème d’état de transaction, une mauvaise adresse et un portefeuille encore en cours de rattrapage.

Utilisez l’assistance officielle du projet et partagez les données de compte uniquement dans le contexte adapté. L’investigation ne nécessite ni phrase de récupération, ni clés privées, ni mot de passe. Examinez les fichiers de diagnostic avant publication. Terminez en reliant le même paiement, la même transaction et le portefeuille destinataire ; un nouveau hashrate ou redémarrage du mineur ne vérifie pas la réception d’un ancien paiement.

<CTA target="pool" title="Suivez vos paiements sur bntpool.com" text="Utilisez votre adresse de réception complète pour retrouver le paiement et comparer sa transaction à l’historique du portefeuille synchronisé." />
