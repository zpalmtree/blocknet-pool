---
title: "Accepter des paiements Blocknet pour vos ventes"
description: "Acceptez des BNT pour vos commandes, retrouvez les paiements dans votre portefeuille Blocknet et gérez confirmations, paiements partiels et remboursements."
tags: [Blocknet, BNT]
faq:
  - q: "Comment savoir si un client a payé en Blocknet ?"
    a: "Retrouvez le paiement entrant dans votre portefeuille synchronisé, comparez son montant et sa référence à la commande, puis vérifiez ses confirmations. Conservez l’identifiant complet de transaction avec la commande. Une capture du client ou un lien de paiement ne prouve pas la réception ; l’explorateur ne montre pas le montant privé reçu par votre adresse."
  - q: "Que faire si un client envoie moins de BNT que demandé ?"
    a: "Gardez la commande partiellement payée et calculez le reste à partir des paiements vérifiés. Demandez la différence avec la même référence, puis conservez les deux identifiants. Ne comptez chaque réception qu’une fois. Si vous convenez d’une annulation, confirmez l’adresse de remboursement dans la conversation existante avec le client."
  - q: "Un mémo marque-t-il automatiquement une commande comme payée ?"
    a: "Non. Il aide à rattacher un paiement à une commande, mais le payeur peut le modifier ou l’omettre. Vérifiez le montant reçu et l’état de la transaction. Le portefeuille crée des demandes et affiche les réceptions ; ce processus manuel n’ajoute pas de caisse automatique ni de système de gestion des commandes."
sourceHash: '3309a069e5de'
translation: draft
---

Acceptez un paiement Blocknet en donnant un montant précis en BNT au client, en vérifiant sa réception dans votre portefeuille et en l’associant à la commande. Cet exemple fictif porte sur **1.25 BNT**, avec la référence **order-1042**. Il utilise le portefeuille de bureau v0.22.0 ; les captures présentent des données de démonstration signalées.

## Comment accepter un paiement Blocknet ?

<Answer>
Convenez du montant en BNT et de la référence, envoyez une demande vers une adresse que vous contrôlez, puis attendez la réception dans votre portefeuille synchronisé. Comparez montant et référence, vérifiez les confirmations, puis enregistrez l’identifiant de transaction et marquez la commande payée selon la règle convenue avec le client.
</Answer>

Le [guide du portefeuille de bureau](https://seineminer.com/fr/blog/blocknet-desktop-wallet/) explique sa création et son ouverture. Gardez sa sauvegarde de récupération à part des commandes. Recevoir le paiement d’un client ne demande ni mineur ni inscription à un pool.

## Fixer le montant et identifier la commande

Notez la référence, le montant en BNT, l’adresse de réception et l’achat. Si le prix est exprimé dans une autre monnaie, convenez du montant en BNT et de la durée de validité du devis avant le paiement. Le lien transporte un montant demandé ; il ne maintient pas un cours de change et ne fait pas respecter une échéance.

Pour l’exemple, demandez **1.25 BNT** avec le mémo **order-1042**. Un numéro de commande suffit : gardez l’adresse de livraison et les autres données client dans votre système privé. Un [mémo chiffré](https://seineminer.com/fr/blog/blocknet-encrypted-memos/) accompagne le paiement, mais le mémo écrit dans un lien partagé est lisible par toute personne qui reçoit ce lien.

Dans **Receive → Payment Request**, saisissez montant et mémo, puis cliquez sur **Generate Payment Link**. Vérifiez destinataire et montant avant de partager. Le [guide des liens de paiement](https://seineminer.com/fr/blog/blocknet-payment-links/) détaille les formats et les destinataires par nom. Pour une facture fixe, conservez l’adresse complète avec la demande.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Portefeuille Blocknet publié affichant une demande de 1.25 BNT, le mémo order-1042 et une adresse de démonstration." loading="lazy" decoding="async"></a>
<figcaption>Interface anglaise réelle avec données de démonstration. Générer la demande ne déplace aucun fonds. Ouvrez l’image pour l’agrandir.</figcaption>
</figure>

## Vérifier la réception dans votre portefeuille

Demandez au client d’inclure la référence et de garder l’identifiant de transaction. Une fois le nœud et le portefeuille à jour, ouvrez **History** et trouvez le paiement entrant. Comparez montant et mémo à la commande. Le payeur peut changer ces champs : la demande ne prouve pas ce qui a été envoyé.

Enregistrez l’identifiant complet et le montant reçu. Ne comptez pas deux fois la même transaction parce qu’elle apparaît dans Dashboard et History, ou parce que le client renvoie son identifiant. Rapprochez des réceptions précises, pas la variation du solde global, qui peut comprendre paiements de minage, autres clients et vos propres transferts.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History du portefeuille Blocknet montrant 1.25 BNT reçus avec le mémo order-1042 et des données de démonstration." loading="lazy" decoding="async"></a>
<figcaption>Le paiement d’exemple correspond au montant et à la référence demandés. Vérifiez son état avant d’exécuter la commande.</figcaption>
</figure>

Si le paiement manque, comparez l’adresse complète et l’identifiant avec le client, puis vérifiez la synchronisation. Sa capture d’envoi donne du contexte ; votre portefeuille de réception établit le montant reçu.

## Quand marquer la commande comme payée ?

Distinguez paiement entrant non confirmé, transaction incluse dans un bloc et fonds disponibles. Le portefeuille publié attend dix blocs après le bloc contenant un paiement ordinaire avant de considérer ses sorties comme dépensables. Le [guide des délais de transfert](https://seineminer.com/fr/blog/blocknet-transfer-time/) explique les compteurs et les délais variables.

Annoncez votre règle de confirmation avant le paiement. Une procédure manuelle simple attend que la réception correspondante soit dépensable avant de livrer. Voir un montant entrant ne suffit pas à terminer cette vérification. Ne promettez pas d’heure fixe à partir du compte à rebours : l’arrivée des blocs varie. [Le rôle du minage dans la sécurité des paiements](/blog/fr/how-mining-secures-blocknet-payments/) explique l’importance de la profondeur de confirmation.

## Gérer les paiements partiels, excédentaires ou non attribués

| Réception vérifiée | Suite à donner |
|---|---|
| 1.00 BNT pour une commande de 1.25 BNT | Enregistrez un paiement partiel et demandez les 0.25 BNT restants. |
| Une réception ultérieure de 0.25 BNT | Gardez les deux identifiants, comptez 1.25 BNT une seule fois et vérifiez les confirmations des deux paiements. |
| Plus de 1.25 BNT | Convenez d’un remboursement ou d’un avoir et notez la décision. |
| Bon montant, sans mémo | Confirmez identifiant complet et destination avec le client avant l’attribution. |
| Paiement en attente | Gardez la commande en attente de confirmation et revérifiez son état. |

Pour rembourser, demandez au client de confirmer une adresse dans la conversation existante de la commande. Ne déduisez pas d’adresse de retour de la transaction publique : les entrées Blocknet ne fournissent pas une adresse classique d’expéditeur. Vérifiez montant, destination et frais avant d’approuver le nouvel envoi, puis gardez son identifiant avec le paiement initial.

## Partager uniquement les preuves nécessaires

L’explorateur aide à vérifier l’inclusion d’une transaction, mais ne révèle pas le montant privé reçu à votre adresse. En cas de désaccord, l’expéditeur peut fournir une preuve propre à la transaction avec le [guide des preuves de paiement](https://seineminer.com/fr/blog/blocknet-payment-proof/). Vérifiez correspondance du destinataire, montant et état sur la chaîne ; une clé valide seule ne suffit pas.

Conservez ensemble et en privé commande, demande, identifiants entrants, montants vérifiés, décision de confirmation et remboursement éventuel. Partagez uniquement le reçu ou la preuve utile. Ne communiquez jamais mots de récupération, mot de passe ou clé privée de visualisation de tout le portefeuille comme preuve de paiement.

Si vous minez aussi des BNT, séparez les [relevés du pool](/blog/fr/track-your-miner-on-bntpool/) des commandes client, même avec le même portefeuille. Vous saurez ainsi quel transfert règle quelle facture.


<CTA target="pool" title="Recevoir aussi les récompenses de minage" text="Le guide du pool explique comment connecter un mineur et suivre ses paiements avec vos autres réceptions de Blocknet." />
