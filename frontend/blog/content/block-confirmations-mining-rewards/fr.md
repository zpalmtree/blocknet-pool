---
title: 'Confirmations de blocs et récompenses de minage immatures'
description: 'Suivez un bloc Blocknet jusqu''à la maturité et au paiement. Comprenez le calcul de profondeur de bntpool et pourquoi les confirmations comptent des blocs.'
tags: ['Récompenses de minage', 'Confirmations', 'Blocknet']
faq:
  - q: 'Combien de confirmations bntpool exige-t-il ?'
    a: 'À la vérification du 26 octobre 2026, bntpool.com publie 60 blocs. Le contrôle soustrait la hauteur trouvée de la hauteur courante et compare le hash avec le nœud. Consultez Get Started ou l''API publique pour le réglage actuel.'
  - q: '60 blocs signifient-ils exactement cinq heures ?'
    a: 'Non. Soixante intervalles cibles de cinq minutes donnent une estimation de 300 minutes, soit cinq heures. Le progrès réel dépend des blocs ajoutés, à intervalles variables. Maturité, seuil de paiement et transaction ultérieure sont des étapes distinctes.'
  - q: 'Faut-il continuer à miner pour qu''une récompense mûrisse ?'
    a: 'Un bloc trouvé gagne de la profondeur avec les blocs du réseau, même si votre mineur est déconnecté. Votre droit dépend des règles comptables du pool. Arrêter n''accélère ni ne réinitialise sa maturité ; suivez la récompense dans My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Suivez un bloc Blocknet jusqu'à sa confirmation, sa maturité puis un paiement au portefeuille. Ce guide explique la profondeur derrière les cartes de bntpool.com. Le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) traite du partage ; le [guide de connexion](/blog/fr/mine-blocknet-on-bntpool/) explique comment commencer.

## Que sont les confirmations et récompenses immatures ?

<Answer>
Les confirmations décrivent la position d'un bloc sous le sommet actuel. Une récompense immature vient d'un bloc récent encore sous la profondeur requise. bntpool vérifie profondeur et identité avant confirmation ; l'éligibilité du solde et le paiement suivent séparément.
</Answer>

*Le bloc gagne de la profondeur, atteint la maturité, puis le solde éligible mène à un paiement distinct.*

## Comment bntpool compte-t-il la profondeur ?

Le pool soustrait la hauteur du bloc trouvé de la hauteur actuelle. Au 26 octobre 2026, l'exigence publiée était de 60 blocs. Le bloc trouvé commence à zéro. Ailleurs, le bloc contenant peut compter comme première confirmation : comparez la convention et le chiffre.

Exemple : un bloc à 70 000 a une profondeur de zéro au sommet 70 000, un à 70 001 et 60 à 70 060. Ce sont des hauteurs illustratives. Le pool vérifie aussi le hash avec son nœud. L'[implémentation](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) décrit ces contrôles.

## Pourquoi cinq heures est-il une estimation ?

Blocknet vise cinq minutes entre blocs. Soixante fois cinq donne 300 minutes, cinq heures. C'est une aide à la planification ; la condition reste un nombre de blocs. Observez l'avancée de la chaîne depuis la hauteur trouvée, plutôt qu'une échéance fixe.

Tout bloc de la chaîne acceptée fait avancer la profondeur, pas uniquement ceux de bntpool. Votre vitesse ne crée pas une horloge personnelle. Les arrivées varient, donc la durée peut différer de l'estimation. Les guides de [difficulté](/blog/fr/mining-difficulty-explained/) et de [chance](/blog/fr/mining-pool-luck-explained/) expliquent ajustement et variation des rondes.

## Distinguez en attente, confirmé et payé

Un bloc enregistré reste en attente pendant sa maturation. Votre part estimée peut apparaître dans [My Stats](/stats) avec du travail éligible. La récompense crée des pièces ; la règle coinbase du portefeuille Blocknet exige 60 blocs de profondeur avant dépense. L'attente du pool suit ce processus.

Après contrôle de profondeur et d'identité, le pool peut confirmer et distribuer la récompense aux soldes éligibles. Cela n'envoie pas instantanément un paiement à chaque adresse. Le paiement évalue séparément soldes confirmés et conditions d'envoi. Statut du bloc, solde et historique décrivent des étapes différentes.

## Pourquoi vérifier le hash du bloc ?

La hauteur désigne une position, le hash un bloc précis. Les nœuds peuvent connaître des blocs concurrents ; la chaîne acceptée décide lequel occupe la position. Le pool interroge donc le nœud et compare le hash. La profondeur seule ne garantit pas que le bloc initial reste dans la chaîne.

Un bloc sorti de cette chaîne peut être marqué orphelin ; sa récompense provisoire n'est plus éligible comme récompense de la chaîne acceptée. Ce statut est traité séparément. Utilisez le lien du hash dans [Blocks](/blocks) : deux entrées de même hauteur ne désignent pas nécessairement le même événement. L'estimation reste liée au bloc.

## En quoi le paiement diffère-t-il de la récompense ?

La récompense minée appartient d'abord à la sortie coinbase. Le paiement du pool est une transaction ultérieure vers votre adresse. À la date vérifiée, le minimum publié est 0,1 BNT. Maturité et seuil de solde sont deux conditions ; le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) détaille répartition et paiement.

Après envoi, suivez cette transaction et l'état de réception et de disponibilité dans votre portefeuille. Ses confirmations concernent le paiement, pas le bloc initial. Inutile de changer d'adresse ou de reconnecter le mineur pendant la maturation. Comparez l'[historique des paiements](/payouts) et le portefeuille avec la même adresse.

## Comment suivre une récompense de bout en bout ?

Dans [Blocks](/blocks), identifiez hauteur, hash et statut. Ouvrez [My Stats](/stats) avec l'adresse du mineur et consultez estimations et soldes confirmés. Lorsqu'un paiement existe, suivez sa transaction depuis l'historique. Chaque observation correspond ainsi au bloc, au compte ou au portefeuille.

Vérifiez les valeurs dans [Get Started](/start) ou `https://bntpool.com/api/info` et le [statut du pool](/status). Pour une investigation, notez adresse, identifiant de bloc ou transaction et heure ; ne partagez jamais les mots de récupération. Le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/) complète l'explication. Les blocs gagnent de la profondeur hors ligne ; l'éligibilité suit travail enregistré et chaîne.

<CTA target="pool" title="Suivez vos récompenses" text="Connectez-vous à bntpool.com et suivez estimations, soldes confirmés et paiements dans My Stats." />
