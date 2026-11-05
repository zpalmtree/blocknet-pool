---
title: Miner Blocknet en solo sur bntpool.com
description: 'Minez Blocknet en solo sans faire tourner de nœud. Placez solo: devant votre adresse dans seine et bntpool.com vous verse la récompense entière de chaque bloc trouvé.'
tags: [Guide de minage, Minage en solo, bntpool, seine]
faq:
  - q: 'Comment miner Blocknet en solo sur bntpool.com ?'
    a: 'Mettez seine à jour en v0.3.1 ou plus récente et placez solo: devant votre adresse de paiement, par exemple ./seine --address solo:YOUR_ADDRESS. seine se connecte à bntpool.com comme d’habitude, et le pool vous verse la récompense entière de chaque bloc trouvé par votre rig via ses paiements habituels. Vous n’avez pas besoin de faire tourner un nœud.'
  - q: 'Faut-il mon propre nœud Blocknet pour miner en solo ?'
    a: 'Non. Avec le préfixe solo:, bntpool.com exploite le nœud, prépare les modèles de blocs et soumet les blocs que vous trouvez. Votre rig ne fait tourner que seine. Le mode daemon de seine est l’autre façon de miner en solo, sur votre propre nœud synchronisé, et convient aux mineurs qui en exploitent déjà un.'
  - q: 'Peut-on miner en solo avec un nom Blocknet ID ?'
    a: 'Oui. Utilisez solo:@name ou solo:$name à la place de solo:ADDRESS, entre guillemets sur la ligne de commande pour que votre shell le transmette tel quel. Le pool résout le nom vers son adresse complète, crédite vos blocs solo et les paie à cette adresse comme tout autre paiement.'
  - q: 'Comment revenir aux récompenses du pool ?'
    a: 'Retirez le préfixe solo: de votre adresse et redémarrez seine. Votre rig partage de nouveau les récompenses PPLNS de chaque bloc trouvé par le pool. Chaque rig choisit son mode à la connexion : vous pouvez donc faire tourner certains rigs en solo et d’autres en récompenses partagées avec la même adresse.'
sourceHash: '623b1e0b356a'
translation: draft
---

Ce guide vous montre comment miner Blocknet en solo sur bntpool.com : ajoutez un préfixe à votre adresse dans seine, et le pool vous verse la récompense entière de chaque bloc trouvé par vos rigs. Vous n’installez ni ne synchronisez aucun nœud. Vous allez le configurer, voir comment les blocs solo sont payés et savoir où les suivre sur le pool.

## Qu’est-ce que le minage solo sur bntpool.com ?

<Answer>
Le minage solo sur bntpool.com vous paie uniquement pour les blocs trouvés par vos propres rigs, et chacun verse sa récompense de bloc entière à votre adresse. Le pool continue d’exploiter le nœud, de préparer les modèles de blocs et de soumettre vos blocs : vous obtenez des récompenses solo avec la même configuration que le minage en pool. Il s’active en plaçant solo: devant votre adresse.
</Answer>

En minage en pool classique, bntpool.com répartit chaque bloc entre les mineurs dont les shares se trouvent dans sa fenêtre PPLNS de six heures : vous gagnez ainsi une part de chaque bloc trouvé par le pool. Un rig solo garde à la place la récompense entière de ses propres blocs et ne gagne rien sur les blocs trouvés par les autres. Les deux utilisent le même serveur, la même adresse et les mêmes paiements. Seul l’identifiant de connexion change.

Le minage solo via le pool vous apporte des récompenses solo sans le travail d’un nœud :

- **Aucun nœud à exploiter.** Le nœud du pool suit la chaîne, fournit des modèles à jour et soumet vos blocs résolus.
- **Des paiements automatiques.** Vos blocs sont payés à votre adresse ou à votre nom Blocknet ID comme tout autre paiement bntpool.com.
- **Des statistiques en direct.** Vos rigs, votre hashrate et vos blocs apparaissent sur la [page des statistiques](/stats) du pool.
- **Un préfixe pour changer.** Retirez `solo:` et redémarrez seine pour revenir aux récompenses partagées.

Le mode daemon de seine est l’autre façon de miner en solo, sur votre propre nœud Blocknet synchronisé. [Pool ou solo pour miner Blocknet](/blog/fr/pool-vs-solo-mining-blocknet/) compare les trois options et montre à quelle fréquence votre hashrate trouve un bloc.

## Comment commencer à miner en solo avec seine ?

<Answer>
Installez seine v0.3.1 ou plus récente, puis lancez-le avec solo: devant votre adresse Blocknet, par exemple ./seine --address solo:YOUR_ADDRESS sous macOS ou Linux. seine se connecte à bntpool.com, son pool par défaut, et le tableau de bord affiche votre compte comme Solo. Vous pouvez aussi saisir solo: suivi de votre adresse quand seine demande une adresse de paiement au premier lancement.
</Answer>

1. **Mettez seine à jour.** Les connexions solo nécessitent seine v0.3.1 ou plus récente. [Téléchargez seine](https://seineminer.com/fr/#download), ou laissez les mises à jour automatiques l’installer.
2. **Lancez seine avec le préfixe.** Sous macOS ou Linux :

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Sous Windows, dans PowerShell :

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine mine sur bntpool.com par défaut. La page [Get Started](/start) du pool affiche la même commande avec l’adresse du pool écrite en entier.
3. **Vérifiez le tableau de bord.** Le panneau du solde affiche votre compte comme **Solo**, et le journal montre la confirmation du pool que vous êtes payé pour les blocs trouvés par cette adresse.

Le préfixe fonctionne partout où seine accepte une adresse : l’invite du premier lancement, la configuration enregistrée, l’API de contrôle locale et les rigs du proxy de ferme. seine l’enregistre avec votre adresse, donc les lancements suivants continuent de miner en solo.

## Peut-on miner en solo avec un nom Blocknet ID ?

Oui. Un nom Blocknet ID fonctionne comme une adresse complète. Écrivez `solo:@name` ou `solo:$name`, entre guillemets pour que votre shell le transmette tel quel :

```sh
./seine --address 'solo:@yourname'
```

Le pool résout le nom sur blocknet.id et paie vos blocs solo à l’adresse correspondante. Pour nommer un rig, ajoutez son nom à une adresse complète après un point, comme dans `solo:YOUR_ADDRESS.rig1`, ou passez `--pool-worker rig1` avec n’importe quelle adresse ou nom. Le préfixe accepte majuscules et minuscules : `SOLO:` fonctionne aussi.

## Comment les blocs solo sont-ils payés ?

Quand votre rig trouve un bloc, le pool l’enregistre comme bloc solo. Une fois à 60 blocs de profondeur, soit environ cinq heures, le pool crédite votre solde de sa récompense de bloc entière, moins les frais du pool. Les frais sont de 0 % au 4 novembre 2026. Votre solde est ensuite payé à votre adresse via les paiements habituels du pool.

Les paiements suivent les mêmes règles que le minage en pool. Chaque bloc attend 60 confirmations pour qu’une courte réorganisation de la chaîne ne puisse pas annuler une récompense déjà payée ; [confirmations de blocs et récompenses de minage](/blog/fr/block-confirmations-mining-rewards/) explique pourquoi. Le point d’accès `/api/info` du pool indiquait un paiement minimum de 1 BNT le 4 novembre 2026, et une récompense de bloc Blocknet dépasse largement ce montant : un bloc solo confirmé est donc payé selon le calendrier de paiement habituel du pool. La [page des paiements](/payouts) liste chaque paiement envoyé par le pool.

Si vous minez vers une adresse de dépôt d’exchange avec un mémo de paiement, le solo fonctionne de la même façon. Le mémo et son propre paiement minimum s’appliquent à vos blocs solo comme aux récompenses partagées.

Le travail de votre rig compte toujours dans le hashrate, l’effort de round et la chance du pool, et un bloc solo clôt le round du pool comme n’importe quel autre bloc. Les shares solo n’entrent jamais dans la fenêtre PPLNS : les autres mineurs ne gagnent rien sur vos blocs, et vous ne gagnez rien sur les leurs.

## À quelle fréquence un rig solo trouve-t-il un bloc ?

Blocknet vise un bloc toutes les cinq minutes, soit 288 blocs par jour. Un rig solo en trouve en moyenne 288 × (votre hashrate ÷ hashrate du réseau) par jour. Relevez le hashrate du réseau sur la [page d’accueil de bntpool.com](/) et le vôtre sur le tableau de bord de seine, puis divisez une journée par le résultat pour obtenir le temps moyen entre vos blocs.

Les récompenses solo arrivent sous forme de blocs entiers : elles conviennent donc aux mineurs dont le hashrate trouve souvent des blocs, comme les grandes fermes, les parcs de serveurs et tous ceux qui détiennent une grande part du réseau. Les rigs plus modestes gagnent en moyenne autant avec les récompenses PPLNS partagées, versées en parts régulières sur chaque bloc trouvé par le pool. [Pool ou solo pour miner Blocknet](/blog/fr/pool-vs-solo-mining-blocknet/) détaille des exemples pour trois hashrates.

## Où voir vos rigs et blocs solo ?

Recherchez votre adresse sur la [page des statistiques](/stats). Vous pouvez la coller avec ou sans le préfixe `solo:`. La page signale alors votre travail solo :

- **Workers** liste chaque rig avec une colonne Type qui indique **Solo** pour les rigs ayant envoyé des shares solo dans la fenêtre de hashrate.
- **Estimated rewards** signale vos blocs solo pendant qu’ils attendent leurs confirmations.
- **Blocks** et **Luck** affichent une colonne Type, Pool ou Solo, avant le statut de chaque bloc. La [page des blocs](/blocks) liste tous les blocs trouvés par le pool, du plus récent au plus ancien.

[Suivre votre mineur sur bntpool.com](/blog/fr/track-your-miner-on-bntpool/) présente le reste de la page des statistiques.

## Peut-on combiner rigs solo et rigs en pool ?

Oui. Chaque rig choisit son mode à la connexion : une même adresse peut donc faire tourner des rigs solo et des rigs en pool en même temps. Une ferme peut miner en solo sur ses plus gros serveurs et garder ses ordinateurs de bureau en récompenses partagées, le tout payé sur un seul solde. Le pool sépare les deux types de travail : un share trouvé sur une connexion solo ne compte que pour les blocs solo de cette adresse.

Pour remettre un rig en pool, retirez `solo:` de son adresse, dans votre commande ou la configuration enregistrée, puis redémarrez seine. Les shares déjà envoyés en mode solo restent solo, et les nouveaux shares rejoignent immédiatement la fenêtre PPLNS.

<CTA target="pool" title="Minez en solo sur bntpool.com" text="Ajoutez solo: à votre adresse dans seine et gardez la récompense entière de chaque bloc trouvé par vos rigs, sans nœud à exploiter." />
