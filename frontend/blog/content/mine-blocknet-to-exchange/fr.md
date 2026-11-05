---
title: Miner Blocknet directement vers une plateforme d’échange
description: 'Minez Blocknet directement vers votre adresse de dépôt SafeTrade ou d’une autre plateforme sur bntpool.com. Ajoutez votre mémo de dépôt dans seine et le pool paie votre compte.'
tags: [Guide de minage, bntpool, SafeTrade, Paiements]
faq:
  - q: 'Peut-on miner Blocknet directement vers une plateforme d’échange ?'
    a: 'Oui. Lancez seine v0.3.1 ou plus récent avec l’adresse de dépôt de la plateforme et votre mémo de dépôt, comme dans ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com verse vos récompenses à cette adresse avec le mémo joint, et la plateforme les crédite sur votre compte.'
  - q: 'Pourquoi seine demande-t-il un mémo de paiement ?'
    a: 'Des plateformes comme SafeTrade utilisent une seule adresse de dépôt partagée par tous leurs clients et distinguent les dépôts grâce au mémo. seine connaît ces adresses partagées : quand vous en saisissez une, il vous demande votre mémo et l’enregistre avec l’adresse. Copiez le mémo exactement depuis la page de dépôt de la plateforme.'
  - q: 'Quel est le paiement minimum vers une plateforme sur bntpool.com ?'
    a: 'Les paiements avec mémo commencent à 10 BNT, selon le pool au 4 novembre 2026. Votre solde confirmé s’accumule sur le pool jusqu’à ce montant, puis le pool l’envoie à l’adresse de la plateforme avec votre mémo. Les paiements vers un portefeuille personnel commencent à 1 BNT.'
  - q: 'Comment vérifier mon solde de minage vers une plateforme ?'
    a: 'Sur la page des statistiques de bntpool.com, saisissez l’adresse de dépôt de la plateforme, choisissez Ajouter un mémo et saisissez votre mémo. Chaque paire adresse et mémo a son propre solde : vous ne voyez donc que votre hashrate, vos rigs et vos paiements, pas ceux des autres clients qui utilisent la même adresse.'
sourceHash: '8f73aae25c7a'
translation: draft
---

Ce guide vous montre comment miner Blocknet directement vers votre compte sur une plateforme d’échange avec bntpool.com. Vous donnez à seine l’adresse de dépôt de la plateforme et votre mémo de dépôt, et le pool y verse vos récompenses avec le mémo joint, pour que la plateforme vous les crédite. Il couvre la commande, le paiement minimum et la consultation de votre solde.

## Peut-on miner Blocknet directement vers une plateforme d’échange ?

<Answer>
Oui. bntpool.com verse les récompenses de minage à une adresse de dépôt de plateforme d’échange avec votre mémo de dépôt joint. Lancez seine v0.3.1 ou plus récent avec l’adresse de la plateforme et votre mémo : ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Les paiements avec mémo commencent à 10 BNT, et la plateforme crédite chacun sur votre compte.
</Answer>

Miner vers une plateforme vous fait gagner une étape si vous comptez échanger ce que vous minez. Les récompenses arrivent directement sur votre compte, prêtes à être vendues sur le [marché BNT/USDT](/blog/fr/sell-blocknet-bnt-safetrade/), sans transfert depuis un portefeuille.

Des plateformes comme SafeTrade donnent la même adresse de dépôt à tous leurs clients et distinguent les dépôts par un mémo, un code court propre à votre compte. C’est pourquoi le mémo compte : c’est lui qui relie un paiement à vous. Le pool tient un solde séparé pour chaque paire adresse et mémo, donc vos récompenses ne se mélangent jamais à celles des autres mineurs qui utilisent la même plateforme.

## Comment configurer le minage vers une plateforme dans seine ?

<Answer>
Copiez l’adresse de dépôt BNT et votre mémo de dépôt depuis la page de dépôt de la plateforme. Lancez seine avec les deux : --address pour l’adresse et --payout-memo pour le mémo. Si vous saisissez une adresse de plateforme connue à l’invite du premier lancement, seine demande le mémo de lui-même et l’enregistre. Vérifiez la confirmation du pool dans le journal, puis minez comme d’habitude.
</Answer>

1. **Récupérez vos informations de dépôt.** Sur la plateforme, ouvrez la page de dépôt BNT et copiez l’adresse de dépôt et le mémo. Copiez le mémo exactement : les majuscules et les zéros initiaux comptent.
2. **Lancez seine avec les deux.** Sous macOS ou Linux :

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Sous Windows, dans PowerShell :

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Laissez seine l’enregistrer.** seine enregistre le mémo avec l’adresse, donc les lancements suivants continuent de payer votre compte sur la plateforme. Si vous tapez une adresse de plateforme connue à l’invite du premier lancement, seine demande le mémo et enregistre les deux.

La page [Get Started](/start) du pool comporte une section « Miner vers une plateforme ? » qui construit cette commande pour vous : collez l’adresse et le mémo, puis copiez le résultat. seine exige un mémo pour les adresses partagées des plateformes connues, comme celles de SafeTrade. Les services, scripts et autres mineurs sans surveillance doivent passer `--payout-memo` en ligne de commande, car aucune invite ne peut recevoir de réponse.

seine vérifie le mémo auprès du pool à la connexion. bntpool.com le confirme, puis le minage démarre. Pour nommer vos rigs, passez `--pool-worker rig-01` comme avec une adresse personnelle.

## Comment les paiements vers une plateforme sont-ils versés ?

Les récompenses suivent le même chemin que tout paiement bntpool.com. Vos shares gagnent une part de chaque bloc trouvé par le pool dans sa fenêtre PPLNS de six heures, et la récompense de chaque bloc est confirmée après 60 blocs. Votre solde confirmé s’accumule ensuite sur le pool.

Le point d’accès `/api/info` du pool indiquait ces paiements minimums le 4 novembre 2026 :

| Destination | Paiement minimum |
|---|---|
| Adresse de plateforme avec mémo | 10 BNT |
| Portefeuille personnel | 1 BNT |

Dès que votre solde confirmé atteint 10 BNT, le pool l’envoie à l’adresse de la plateforme avec votre mémo. La plateforme crédite ensuite le dépôt après son propre nombre de confirmations ; sa page de dépôt indique combien il en faut. [Fonctionnement des paiements PPLNS](/blog/fr/how-pplns-payouts-work/) détaille le côté pool.

Le minage solo fonctionne aussi avec un mémo. Placez `solo:` devant l’adresse de la plateforme et gardez `--payout-memo` : le pool verse alors sur votre compte la récompense entière de chaque bloc que vous trouvez. [Le minage solo sur bntpool.com](/blog/fr/solo-mine-blocknet-on-bntpool/) explique les récompenses solo.

## Comment vérifier votre solde ?

Ouvrez la [page des statistiques](/stats), collez l’adresse de dépôt de la plateforme, choisissez **Ajouter un mémo** et saisissez votre mémo. La page affiche alors votre hashrate, vos rigs, votre solde et vos paiements pour cette paire adresse et mémo. Les autres clients de la même plateforme ont leurs propres paires et leurs propres soldes.

Chaque paiement de **Recent Payouts** renvoie vers sa transaction dans l’explorateur. Comparez-le à l’historique des dépôts de la plateforme pour suivre les pièces jusqu’à votre compte. [Suivre votre mineur sur bntpool.com](/blog/fr/track-your-miner-on-bntpool/) présente le reste de la page des statistiques.

## Comment revenir à un portefeuille personnel ?

Lancez seine une fois avec votre nouvelle adresse et `--no-payout-memo` :

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine enregistre la nouvelle adresse sans mémo, et les nouveaux shares comptent pour votre portefeuille personnel. Un solde déjà crédité à la paire de la plateforme reste attaché à cette paire et lui est versé dès qu’il atteint le minimum ; vous pouvez le suivre sur la page des statistiques. [Changer votre adresse de minage](/blog/fr/change-mining-wallet-address/) explique comment le pool crédite le travail après un changement d’adresse.

## Quelles configurations prennent en charge les mémos ?

Les mémos nécessitent la connexion directe de seine au pool, qui est le réglage par défaut. Le mode daemon de seine mine sur votre propre nœud et paie le portefeuille du nœud : il n’a donc pas de mémo. Le proxy de ferme ne transmet pas les mémos ; connectez donc les rigs destinés à la plateforme directement au pool. seine ne mine que sur un pool qui confirme votre mémo, donc votre travail est toujours payé sur le compte que vous avez choisi.

<CTA target="pool" title="Minez vers votre compte sur la plateforme" text="Lancez seine avec votre adresse de dépôt et votre mémo, et bntpool.com verse vos récompenses directement sur votre compte dès 10 BNT." />
