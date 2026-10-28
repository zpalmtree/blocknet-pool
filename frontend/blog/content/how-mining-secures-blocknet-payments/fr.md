---
sourceHash: 77522359a409
translation: draft
title: Comment le minage sécurise les paiements Blocknet
description: Découvrez comment Blocknet choisit l’historique des paiements par preuve de travail, le rôle des confirmations et les contrôles des nœuds sur les blocs.
tags: [Blocknet, Sécurité des paiements, Preuve de travail]
faq:
  - q: Plus de puissance de minage peut-elle autoriser un paiement depuis mon portefeuille ?
    a: La puissance de minage aide à produire des blocs concurrents. Dépenser des fonds exige toujours une autorisation valide, et les nœuds vérifient les preuves et l’équilibre des transactions avant d’accepter un bloc. Une branche plus lourde doit respecter ces règles. La preuve de travail ne remplace pas les clés privées nécessaires pour dépenser vos sorties.
  - q: Blocknet suit-il la chaîne qui compte le plus de blocs ?
    a: Core v0.22.0 calcule le travail cumulé en additionnant la difficulté de chaque bloc. Une branche valide avec davantage de travail peut devenir la chaîne retenue, sous réserve des règles de réorganisation. Le nombre de blocs ne suffit pas, car leurs difficultés peuvent différer. Le travail et la validité comptent tous deux.
  - q: Les confirmations de paiement et les shares du pool sont-ils identiques ?
    a: Un share enregistre du travail de minage soumis au pool. Une confirmation concerne l’inclusion d’une transaction dans la chaîne retenue et les blocs construits ensuite. Les shares servent à répartir les récompenses ; les blocs réseau font avancer l’historique. Augmenter votre nombre de shares n’ajoute pas directement de confirmations à un paiement précis.
---

Suivez un paiement en BNT depuis son inclusion dans un bloc jusqu’à un historique soutenu par du travail de minage ultérieur. Ce guide explique pourquoi le réseau doit choisir entre des historiques concurrents, ce qu’indiquent les confirmations et comment ce choix accompagne la validation des transactions privées. L’implémentation décrite est core v0.22.0, vérifiée le 28 octobre 2026.

## Que protège le minage lorsque vous envoyez des BNT ?

<Answer>
Le minage aide le réseau à choisir un historique de paiements partagé et ordonné. Chaque bloc accepté se relie au précédent et apporte une preuve de travail. Les nœuds vérifient les blocs et transactions proposés, puis comparent les branches valides par travail cumulé selon leurs règles de réorganisation. Les blocs suivants ajoutent du travail derrière un paiement inclus, donnant un sens concret aux confirmations.
</Answer>

Un paiement valide nécessite une autorisation et une place dans le registre. Votre portefeuille fournit l’autorisation de dépenser vos fonds. Le minage et la sélection de chaîne aident les nœuds à s’accorder sur les transactions valides appartenant à l’historique courant. Ces fonctions se complètent : la preuve de travail d’un bloc ne rend pas acceptable un paiement invalide.

Le [guide d’application des règles](https://seineminer.com/fr/blog/who-controls-blocknet/) explique qui développe, propose et valide. Ici, nous suivons le paiement lui-même : comment son inclusion rejoint l’historique suivi par votre portefeuille et les autres nœuds.

## Comment les blocs engagent-ils un historique de paiements ?

L’en-tête d’un bloc contient le hash du bloc précédent et une racine de Merkle issue de ses transactions. La racine engage cryptographiquement le contenu du bloc, tandis que le hash précédent le rattache à l’historique. Modifier une transaction change la racine et l’en-tête sur lequel porte le minage. Les blocs descendants doivent alors référencer cet historique modifié par leurs propres liens.

Prenez un paiement inclus dans un bloc, puis plusieurs blocs acceptés après lui. Remplacer cette partie de l’historique exige une branche concurrente de blocs valides et liés, avec assez de travail cumulé, dans les limites des règles de réorganisation du nœud. Modifier une transaction dans un fichier local ne produit pas cette branche. Le travail correspond à une séquence précise d’en-têtes et de transactions.

Le [code de blocs publié](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) définit ces champs, les contrôles de Merkle et les parcours de sélection de chaîne. Le [guide de difficulté](/blog/fr/mining-difficulty-explained/) explique comment le réseau fixe le travail requis pour chaque nouveau bloc.

## Pourquoi le travail cumulé compte-t-il plus que le nombre de blocs ?

Deux mineurs peuvent trouver des blocs valides différents prolongeant le même parent. Chaque proposition possède son hash, et les nœuds peuvent les recevoir dans des ordres différents. Cela peut brièvement créer des branches concurrentes. Le réseau a besoin d’une règle fondée sur le travail soutenant un historique, plutôt que sur l’ordre d’annonce par un pair.

Core v0.22.0 calcule le travail cumulé en ajoutant la difficulté du bloc au travail cumulé de son parent. Après validation, une branche dont le travail est strictement supérieur peut déclencher un changement de chaîne retenue. Une alternative de travail égal ne le déclenche pas par cette comparaison. La difficulté doit elle-même être valide dans le contexte du parent ; un mineur ne peut pas simplement inscrire un nombre supérieur dans l’en-tête.

Comparez deux branches partageant un début commun. Cette partie apporte le même travail aux deux totaux. La différence déterminante est le travail accumulé après leur divergence, avec les contrôles de validité et de réorganisation. Compter les blocs sans tenir compte de leur difficulté peut masquer cette différence. Il s’agit d’une comparaison conceptuelle, pas du récit d’une bifurcation en cours.

## Comment les confirmations aident-elles à suivre un paiement ?

Une fois le paiement inclus dans la chaîne retenue, les blocs suivants prolongent cet historique. Sa profondeur augmente à leur arrivée. Remplacer son inclusion nécessiterait un historique alternatif admissible remontant avant le bloc concerné, pendant que la branche actuelle continue d’accumuler du travail. Vérifier l’inclusion et la profondeur est donc plus informatif que constater la diffusion d’une transaction.

Une hauteur désigne une position ; un hash désigne un bloc précis. Consultez la transaction et le bloc qui la contient, puis vérifiez que ce bloc reste dans la chaîne retenue. Si l’historique choisi change, l’inclusion et la profondeur peuvent aussi changer. Certains compteurs incluent le bloc contenant la transaction, d’autres seulement les suivants : consultez la convention de l’interface.

Core v0.22.0 impose également une limite de réorganisation : il rejette un changement dont l’ancêtre commun se situe sous la hauteur actuelle de la pointe moins 100. C’est une règle de l’implémentation exécutée, pas une promesse universelle attachée à un compteur. Le [guide des délais de transfert](https://seineminer.com/fr/blog/blocknet-transfer-time/) traite de la disponibilité ordinaire des fonds ; les [confirmations des récompenses](/blog/fr/block-confirmations-mining-rewards/) couvrent la maturité coinbase et la comptabilité du pool.

## Comment contrôler la double dépense d’un paiement privé ?

Le format de transaction publié de Blocknet utilise une image de clé liée à la preuve de dépense. La validation vérifie ce lien et rejette une image déjà dépensée dans l’état pertinent de la chaîne. Les nœuds détectent ainsi des utilisations incompatibles d’une sortie, tandis que la signature en anneau masque quel membre admissible est l’entrée réelle. Les montants privés subissent toujours des contrôles d’équilibre et de plage.

Supposons qu’un propriétaire autorise des transactions incompatibles dépensant la même sortie. Un historique valide accepté ne peut contenir ces deux dépenses. La sélection de chaîne compte, car des historiques concurrents peuvent retenir des choix différents ; la validation compte, car chacun doit respecter les règles de dépense. Plus de travail ne remplace pas une signature valide et ne fait pas passer le contrôle comptable à une transaction déséquilibrée.

Ces contrôles figurent dans le [validateur de transactions publié](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). Le [guide de confidentialité de Blocknet](https://seineminer.com/fr/blog/blocknet-privacy-features/) présente les autres mécanismes. La sécurité des paiements combine dépenses valides, historique cohérent et cryptographie protégeant les informations que cet historique révèle.

## Quel rôle joue le minage en pool dans la sécurité des paiements ?

Un pool coordonne la recherche d’un bloc et soumet le candidat résolu à son nœud amont. Un share satisfaisant la cible plus facile du pool enregistre du travail apporté ; seule une solution satisfaisant la cible réseau peut devenir un bloc après validation. L’[explication des pools](/blog/fr/what-is-a-mining-pool/) distingue ces deux cibles et leurs rôles.

Votre mineur peut donc contribuer à prolonger l’historique des paiements tandis que le pool comptabilise votre travail séparément. Un solde privé du pool, un nombre de shares et un paiement confirmé dans un portefeuille décrivent des choses différentes. Consultez le [guide de connexion au pool](/blog/fr/mine-blocknet-on-bntpool/) pour participer, et le registre de votre portefeuille pour suivre un paiement BNT précis.

<CTA target="pool" title="Contribuez au travail de Blocknet" text="Connectez seine à bntpool.com et suivez votre travail de minage, vos récompenses et vos paiements avec les outils publics du pool." />
