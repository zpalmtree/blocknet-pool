---
title: Miner Blocknet en pool ou en solo
description: Choisissez le pool ou le solo pour Blocknet. Calculez vos blocs attendus, découvrez les paiements réguliers de bntpool.com et configurez le minage solo.
tags: [Guide de minage, Minage en solo, bntpool, PPLNS]
faq:
  - q: 'À quelle fréquence trouverais-je un bloc Blocknet en solo ?'
    a: 'En moyenne, 288 × (votre hashrate ÷ hashrate du réseau) blocs par jour, car Blocknet vise un bloc toutes les cinq minutes. Avec 50 H/s sur un réseau de 50 000 H/s, cela donne 0,288 bloc par jour, environ un tous les trois jours et demi. Consultez le hashrate réseau en direct sur bntpool.com.'
  - q: 'Faut-il un nœud Blocknet pour miner sur bntpool.com ?'
    a: 'Non. Le pool exploite son propre nœud, prépare les modèles de blocs et soumet les blocs trouvés. Il vous suffit de seine et d’une adresse Blocknet ou d’un nom Blocknet ID. Lancez seine, collez votre adresse et acceptez le pool par défaut : votre part de chaque bloc y sera payée.'
  - q: 'seine peut-il miner Blocknet en solo ?'
    a: 'Oui. Lancez ./seine --mode daemon pour miner directement avec votre propre nœud Blocknet synchronisé, en utilisant son cookie API pour la connexion. Chaque bloc trouvé verse sa récompense entière au portefeuille du nœud. Le guide solo du dépôt seine traite des installations personnalisées et des mots de passe de portefeuille.'
  - q: 'Peut-on passer du pool au solo plus tard ?'
    a: 'Oui. seine utilise le même téléchargement pour les deux. Lancez-le normalement pour bntpool.com ou ajoutez --mode daemon pour votre nœud, puis redémarrez pour changer. Les shares déjà envoyés au pool restent dans sa fenêtre PPLNS de six heures et continuent à rapporter durant ce délai.'
sourceHash: 171d240d6c4b
translation: draft
---

Ce guide vous aide à choisir entre bntpool.com et le minage solo sur votre propre nœud. Vous calculerez combien de blocs votre hashrate trouve seul, verrez comment le pool transforme ce travail en paiements réguliers et apprendrez à configurer les deux avec seine.

## Faut-il miner Blocknet en pool ou en solo ?

<Answer>
Le calcul est identique : votre part du hashrate réseau détermine votre part des blocs. Sur bntpool.com, vous gagnez une fraction de chaque bloc trouvé par le pool, avec des paiements réguliers au fil des découvertes. En solo, vous gardez la récompense entière de chaque bloc trouvé, ce qui convient aux grandes installations exploitant leur propre nœud.
</Answer>

Pour la plupart des mineurs, bntpool.com est le plus simple pour commencer. C’est le pool par défaut de seine : seule votre adresse est à renseigner, sans nœud à exploiter ni à synchroniser. Le solo est également pleinement pris en charge grâce au mode daemon de seine. La suite vous aide à choisir selon votre matériel.

## Combien de blocs trouveriez-vous en solo ?

<Answer>
Blocknet vise un bloc toutes les cinq minutes, soit 288 par jour. Seul, vous pouvez en attendre 288 × (votre hashrate ÷ hashrate du réseau) par jour. Divisez une journée par ce nombre pour obtenir l’intervalle moyen entre vos blocs. Consultez le hashrate réseau en direct sur bntpool.com.
</Answer>

Cette formule fonctionne parce que Blocknet ajuste la difficulté pour que le réseau trouve un bloc toutes les 300 secondes en moyenne. La difficulté correspond approximativement au nombre de hachages attendus avant une réussite. Chaque hachage ayant la même chance, la fraction des hachages du réseau produite par votre machine correspond à la fraction des blocs attendue.

Deux valeurs sont nécessaires :

- **Votre hashrate.** seine l’affiche pendant le minage, et la [page des statistiques](/stats) du pool le montre après votre arrivée.
- **Le hashrate réseau.** Le [tableau de bord bntpool.com](/) l’affiche dans Network Hashrate, près de Pool Hashrate. Les deux changent : vérifiez-les au moment du calcul.

Chaque bloc trouvé verse la récompense de son mois. Le [calendrier des récompenses](/blog/fr/blocknet-block-reward-schedule/) les répertorie.

## Un exemple de calcul

<Note>
Les valeurs suivantes sont des nombres ronds hypothétiques pour illustrer le calcul, pas des données en direct. Utilisez le hashrate réseau actuel pour votre estimation.
</Note>

Supposons un réseau de 50 000 H/s. Un rig à 50 H/s, environ le résultat de seine sur un serveur AMD EPYC Zen 5, représente 0,1 % du réseau. Voici ce que ce rig et deux installations plus grandes trouveraient seuls :

| Votre hashrate (exemple) | Part du réseau | Blocs attendus par jour | Intervalle moyen |
|---|---|---|---|
| 50 H/s | 0,1 % | 0,288 | environ 83 heures (3,5 jours) |
| 500 H/s | 1 % | 2,88 | environ 8,3 heures |
| 5 000 H/s | 10 % | 28,8 | environ 50 minutes |

Plaçons maintenant le rig de 50 H/s sur un pool totalisant 5 000 H/s. Le pool trouve environ 28,8 blocs par jour, un toutes les 50 minutes en moyenne. Le rig produit 1 % du travail du pool et gagne 1 % de chacun de ces blocs. Cela représente toujours 0,288 bloc par jour. Seul le rythme change : une petite portion environ toutes les 50 minutes, plutôt qu’un bloc entier tous les quelques jours.

Le tableau explique aussi pourquoi le solo convient aux grandes installations. À 5 000 H/s, un mineur solo trouve aussi souvent des blocs qu’un pool de cette taille : ses revenus sont réguliers par eux-mêmes.

Pour convertir les blocs quotidiens en BNT avec votre matériel et un exemple daté fondé sur les données réelles du pool, consultez [comment estimer vos récompenses Blocknet](/blog/fr/estimate-blocknet-mining-rewards/).

## Pourquoi un pool verse-t-il plus régulièrement ?

bntpool.com réunit le hashrate de tous ses mineurs et trouve donc des blocs bien plus souvent qu’un rig isolé. PPLNS partage chaque récompense entre les mineurs ayant contribué : le pool examine les six heures précédant le bloc et répartit selon la difficulté de leurs shares.

Cela a plusieurs effets pratiques :

- **Des gains sur chaque bloc.** Tant que vos shares sont dans la fenêtre de six heures, vous recevez une part de chaque bloc trouvé. Suivez-les sur la [page des blocs](/blocks).
- **Des paiements automatiques.** À 60 blocs de profondeur, votre part rejoint le solde confirmé, envoyé dès 0,1 BNT. Les envois figurent sur la [page des paiements](/payouts).
- **Une continuité après redémarrage.** Vos shares restent six heures dans la fenêtre : un redémarrage ou une mise à jour de pilote ne remet pas votre position à zéro.
- **Seulement le mineur à exécuter.** Le pool gère le nœud, les modèles et la soumission des blocs. Plusieurs rigs peuvent miner vers une même adresse ou un `@name` Blocknet ID.

Le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) détaille la répartition avec un exemple. [Get Started](/start) affiche les réglages de paiement actuels.

## Pourquoi miner Blocknet en solo ?

Le solo vous donne le contrôle de tout le processus. Votre nœud prépare chaque modèle et vérifie lui-même chaque bloc. Chaque bloc que vous trouvez verse sa récompense entière directement à son portefeuille.

C’est surtout adapté lorsque votre hashrate trouve souvent des blocs. Dans l’exemple, une installation à 10 % du réseau trouve un bloc environ toutes les 50 minutes : ses paiements sont aussi réguliers que ceux d’un pool. Si vous exploitez déjà un nœud Blocknet pour votre portefeuille ou d’autres services, il suffit d’y ajouter le mineur.

seine est également conçu pour ce mode. Il mine sur les modèles du nœud. Si celui-ci a oublié un modèle, après un redémarrage par exemple, seine soumet de nouveau le bloc résolu complet pour que le bloc trouvé atteigne malgré tout la chaîne.

## Comment configurer le solo avec seine ?

1. **Installez et synchronisez un nœud Blocknet.** Avec le gestionnaire `blocknet`, exécutez :

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Le cookie API est écrit dans `~/.config/bnt/data/mainnet/api.cookie`. Pour accélérer la première synchronisation, bntpool.com publie `checkpoints.dat` sur sa [page checkpoints](/checkpoints). [Synchroniser un nœud avec des checkpoints](/blog/fr/blocknet-checkpoints-fast-sync/) explique son usage.
2. **Attendez la fin de la synchronisation.** Les messages `[sync] progress` atteignent 100 %. Le nœud ne fournit des modèles de blocs qu’une fois à jour.
3. **Lancez seine en mode daemon.**

   ```sh
   ./seine --mode daemon
   ```

   Pour une installation personnalisée, indiquez vous-même l’API et le cookie :

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

Le [guide solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) couvre mots de passe et configurations personnalisées, et le [guide de minage seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) couvre téléchargement et premiers shares. [Téléchargez seine](https://seineminer.com/fr/#download) pour Windows, macOS ou Linux.

## Que choisir ?

Appliquez la formule à votre hashrate et à celui du réseau sur le [tableau de bord](/), puis regardez l’intervalle moyen entre blocs.

- **Commencez sur bntpool.com** si vous minez avec un ordinateur de bureau, un portable ou quelques serveurs, ou souhaitez des paiements réguliers sans gérer de nœud. Lancez seine, collez votre adresse ou `@name`, puis acceptez le pool par défaut.
- **Minez en solo** si votre installation représente une grande part du réseau, trouve donc souvent des blocs seule, et si vous gérez déjà ou souhaitez gérer votre nœud synchronisé.

seine permet de revoir facilement ce choix. Les deux modes utilisent le même téléchargement : une option et un redémarrage suffisent quand votre matériel évolue. Si un nœud local tourne pendant votre minage en pool, seine affiche aussi son solde près de celui du pool.

<Note>
Blocknet prévoit Faraday vers le 16 février 2027, selon [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), avec un changement prévu de preuve de travail. Gardez seine à jour : il vérifie les nouvelles versions au démarrage et toutes les 12 heures.
</Note>

<CTA target="pool" title="Commencez à miner sur bntpool.com" text="Lancez seine, collez votre adresse et gagnez une part de chaque bloc du pool, payée automatiquement dès 0,1 BNT." />
