---
title: 'Peut-on miner Blocknet à temps partiel ?'
description: 'Minez Blocknet par sessions : suivez les shares dans la fenêtre PPLNS de six heures, reprenez avec la même adresse et consultez les soldes impayés.'
tags: ['Guide de minage', 'PPLNS', 'Horaires']
faq:
  - q: 'Faut-il miner six heures avant que les shares comptent ?'
    a: 'Non. Les shares éligibles peuvent participer à la fenêtre d’un bloc sans attendre six heures. Votre part dépend du travail pondéré par difficulté face au total du pool dans cette fenêtre, et un bloc doit être trouvé pour attribuer sa récompense.'
  - q: 'Le mineur doit-il rester connecté pour recevoir un paiement existant ?'
    a: 'Une connexion active n’est pas une condition de paiement. Les soldes confirmés sont traités selon les règles du pool, notamment le seuil minimum actuel. Se déconnecter n’efface pas le solde et ne demande pas de paiement immédiat.'
  - q: 'La reconnexion remet-elle à zéro l’âge des anciennes shares ?'
    a: 'Non. Elles conservent leur horodatage. Le travail nouveau entre dans les fenêtres correspondantes après votre retour ; la reconnexion ne renouvelle pas les anciennes shares et ne crée pas de travail pendant l’absence. Vérifiez de nouvelles shares acceptées au redémarrage.'
sourceHash: '0a11c393062e'
translation: draft
---

Vous pouvez faire tourner un mineur Blocknet à certaines heures puis revenir. Distinguez connexion actuelle, shares déjà envoyées et récompenses déjà attribuées. Ce guide suit une courte session pour vous aider à arrêter, relancer et vérifier votre compte bntpool.com avec des attentes claires.

## Une courte session peut-elle générer des récompenses ?

<Answer>
Oui. Le travail éligible d’une courte session peut participer aux blocs dont les fenêtres PPLNS incluent ces shares. Il n’est pas nécessaire de terminer six heures. Les récompenses dépendent des blocs trouvés et de votre part pondérée par difficulté. L’arrêt n’ajoute aucun travail ; les shares vieillissent normalement et les soldes suivent les règles de paiement.
</Answer>

*Une frise présente une session, une absence et un retour, avec des repères distincts pour l’éligibilité des shares et le solde existant.*

## Distinguez les différents délais

bntpool publie actuellement une fenêtre PPLNS de six heures, 60 blocs avant éligibilité au paiement et un minimum de 0,1 BNT sur la [page de démarrage](/start). Il s’agit du travail considéré, de la maturation du bloc et d’un seuil de solde. La fenêtre de six heures n’est pas une minuterie promettant un paiement.

Le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) détaille la formule. Vos shares éligibles sont pondérées par difficulté face à tout le travail éligible de la fenêtre. Les minutes de connexion ne fixent pas un pourcentage de la prochaine récompense. Matériel, participation du pool et résultats des shares peuvent modifier les poids à durée égale.

## Suivez une share pendant une pause

Exemple en heure locale : vous minez de 18:00 à 20:00, avec une share éligible enregistrée à 19:00. Un bloc dont la fenêtre se termine à 22:00 regarde six heures en arrière, jusqu’à 16:00. Cette share est donc incluse même si vous êtes déconnecté. C’est un exemple d’éligibilité, pas une prédiction de découverte d’un bloc à cette heure.

Une fenêtre se terminant à 02:00 le lendemain commence à 20:00 : la share de 19:00 est exclue. Revenir à 08:00 ne change pas son horodatage. Le nouveau travail contribue ensuite aux fenêtres suivantes. Le [guide de la chance](/blog/fr/mining-pool-luck-explained/) explique la variabilité des blocs ; ne planifiez pas en supposant qu’un bloc est dû.

## Arrêtez en gardant un repère

Avant une pause, notez adresse de réception, nom de machine, heure approximative et derniers messages de shares acceptées. Vérifiez le compte avec le [guide de suivi](/blog/fr/track-your-miner-on-bntpool/). Cela donne un point de comparaison et aide à distinguer une machine arrêtée d’une autre adresse ou d’un affichage encore retardé.

Quittez seine normalement avec `q` dans le tableau de bord ou `Ctrl+C` dans le terminal. Gardez la commande et la configuration pour le retour. Le [guide d’usage du PC](https://seineminer.com/fr/blog/mine-while-using-pc/) explique comment partager le temps avec d’autres tâches ; arrêter proprement ne demande ni suppression des réglages, ni changement d’adresse, ni fermeture de compte.

## Que devient le solde impayé pendant votre absence ?

Lorsqu’une share sort de la fenêtre d’un bloc ultérieur, cela n’efface pas une récompense déjà attribuée à un bloc antérieur. Les estimations suivent toujours l’état et la maturation du bloc ; les récompenses confirmées impayées restent au compte jusqu’au traitement. Le [guide des confirmations](/blog/fr/block-confirmations-mining-rewards/) distingue crédit provisoire et solde arrivé à maturité.

Un solde confirmé inférieur à 0,1 BNT reste sous le seuil automatique actuel jusqu’à ce que d’autres crédits atteignent le minimum. Reconnecter n’augmente pas ce solde et ne force pas un paiement. Un solde éligible ne nécessite pas non plus un mineur en ligne pendant le traitement. Consultez l’[historique des paiements](/payouts) pour distinguer solde impayé et transaction envoyée.

## Revenez au même compte et vérifiez du travail frais

Relancez la commande enregistrée et vérifiez pool, adresse et nom de machine avant de comparer les statistiques. L’adresse identifie le compte suivi. Pour choisir volontairement une autre destination, consultez le [guide de changement d’adresse](/blog/fr/change-mining-wallet-address/) : le solde de l’ancien compte n’est pas transféré silencieusement au nouveau.

Recherchez un job actuel et de nouvelles shares acceptées, puis laissez la fenêtre de mesure du pool s’actualiser. La moyenne d’une nouvelle session peut différer de l’estimation glissante du pool. Le [guide du hashrate](/blog/fr/what-is-hashrate/) explique ces fenêtres. Vérifiez travail réel et bon compte avant d’interpréter une différence passagère comme des récompenses manquantes.

## Planifiez selon votre routine

Choisissez les heures selon l’usage du PC et le budget électrique. Le [guide de l’électricité](https://seineminer.com/fr/blog/mining-electricity-use/) calcule la consommation du planning réel, avec plusieurs tarifs si nécessaire. Une session plus courte réduit la durée de fonctionnement, mais son énergie exacte dépend toujours du matériel et de la charge mesurés.

Notez débuts, arrêts, travail accepté et soldes sur plusieurs sessions. Le [calculateur de récompenses](/blog/fr/estimate-blocknet-mining-rewards/) donne une espérance sous ses hypothèses, pas une promesse pour une soirée. Évaluez une période représentative et séparez connexion, éligibilité des shares, maturation des blocs et paiements envoyés.

<CTA target="pool" title="Suivez vos sessions sur bntpool.com" text="Reprenez avec la même adresse, vérifiez de nouvelles shares et suivez les étapes normales du paiement." />
