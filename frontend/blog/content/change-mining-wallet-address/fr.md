---
title: 'Changer l’adresse de minage Blocknet'
description: 'Changez l’adresse de réception de vos mineurs Blocknet, vérifiez les nouvelles shares sur bntpool.com et suivez les récompenses liées à l’ancienne adresse.'
tags: ['Guide de minage', 'Portefeuille', 'Paiements']
faq:
  - q: 'Changer d’adresse transfère-t-il l’ancien solde ?'
    a: 'Changer la destination du mineur dirige le travail futur vers la nouvelle adresse. Les shares, crédits et paiements existants restent associés à l’adresse d’origine. Consultez séparément les deux adresses dans Mes statistiques et gardez accès à l’ancien portefeuille pendant le suivi des récompenses restantes.'
  - q: 'Faut-il changer l’adresse sur chaque ordinateur ?'
    a: 'Oui. Chaque processus utilise sa propre destination configurée. Modifiez et vérifiez d’abord un ordinateur, puis les autres, y compris leurs fichiers et arguments de lancement. Des noms de machines distincts permettent de reconnaître celles qui envoient désormais du travail à la nouvelle adresse.'
  - q: 'Modifier mon Blocknet ID actualise-t-il un mineur actif ?'
    a: 'seine résout l’ID enregistré au démarrage et conserve l’adresse pendant l’exécution, reconnexions comprises. Après modification de l’association, redémarrez chaque mineur et contrôlez l’adresse complète résolue. Le changement ne réattribue ni le travail ni les soldes enregistrés pour l’ancienne adresse.'
sourceHash: 'fca912d46d55'
translation: draft
---

Changez la destination du travail futur tout en gardant une trace claire des récompenses précédentes. Ce guide concerne une installation seine déjà connectée à bntpool.com. Pour commencer, consultez le [guide de connexion](/blog/fr/mine-blocknet-on-bntpool/) ; pour un nouveau portefeuille, le [guide du portefeuille](https://seineminer.com/fr/blog/blocknet-wallet-for-mining/).

## Comment changer l’adresse de minage ?

<Answer>
Arrêtez un mineur, modifiez sa destination enregistrée ou sa commande habituelle, puis redémarrez-le. Vérifiez l’adresse complète résolue et les shares acceptées, puis recherchez cette adresse sur bntpool.com. Répétez sur les autres machines. Les shares et récompenses antérieures restent liées à l’adresse utilisée lors de leur enregistrement.
</Answer>

*Le nouveau travail va au second portefeuille ; les anciennes récompenses restent liées au premier.*

## Notez les deux destinations avant la transition

Copiez la nouvelle adresse depuis le portefeuille prévu. Le mineur utilise une destination publique ou un Blocknet ID enregistré, jamais la phrase de récupération ni la clé privée. Gardez aussi l’ancienne adresse complète : elle reste la clé de recherche de l’activité passée après le changement.

Ouvrez Mes statistiques pour l’ancienne adresse et notez récompenses en attente, solde confirmé, paiements récents et noms des machines actives. Ce relevé sert de repère ; la nouvelle page ne contient pas l’ancien historique. Conservez l’accès au portefeuille d’origine pendant le suivi des récompenses et paiements restants.

## Modifiez un mineur et vérifiez sa commande

Arrêtez un mineur et examinez son lancement habituel. Avec un fichier existant, `--address` prime sur l’adresse enregistrée sans la remplacer. L’exemple PowerShell choisit la destination de cette exécution. Remplacez `@yourname` par votre nouvel ID ou adresse complète et conservez vos arguments éprouvés de pool, données et matériel.

```powershell
.\seine.exe --address "@yourname"
```

Sous Linux ou macOS, utilisez la syntaxe suivante depuis votre répertoire habituel. Les deux exemples démarrent réellement le minage : choisissez celui de votre système. L’installation existante fournit les autres réglages. Gardez le même nom de machine pour reconnaître ses shares sous la nouvelle adresse.

```sh
./seine --address "@yourname"
```

## Conservez la nouvelle adresse au prochain démarrage

Pour enregistrer le changement, modifiez `address` dans `seine.config.json`, mineur arrêté. Préservez les autres champs et la syntaxe JSON. Vérifiez le répertoire : `./seine-data` dépend du répertoire de travail et `--data-dir` peut en choisir un autre. Modifiez le fichier réellement utilisé par votre lanceur.

Actualisez ou retirez tout ancien `--address` du raccourci, script, tâche ou service. Une commande manuelle réussie ne change pas ces lanceurs. Redémarrez normalement et vérifiez encore. Le [guide de configuration de seine](https://seineminer.com/fr/blog/configure-seine-miner/) explique la priorité des réglages et pourquoi un autre répertoire de travail peut charger un autre fichier.

## Que change l’utilisation d’un Blocknet ID ?

Avec seine v0.2.36, `@name` est résolu et validé au démarrage. L’adresse complète reste fixe pendant cette exécution, même après reconnexion au pool. Si vous modifiez l’association du nom, redémarrez chaque mineur et contrôlez la destination. Une simple reconnexion ne répète pas cette résolution initiale.

Conservez les deux adresses complètes, même si le nom lisible reste identique. La comptabilité suit l’adresse résolue : modifier l’association ne redirige pas les anciennes shares ou soldes. Si la résolution échoue, vérifiez l’association ou choisissez explicitement l’adresse complète contrôlée dans votre portefeuille.

## Vérifiez chaque ordinateur sous la nouvelle adresse

Contrôlez la destination et les soumissions acceptées, puis recherchez la nouvelle adresse complète dans Mes statistiques. Repérez le nom de machine et les shares récentes. Une estimation fraîche du hashrate demande du travail et du temps ; vérifiez d’abord les arrivées. Le [guide des statistiques](/blog/fr/track-your-miner-on-bntpool/) explique les compteurs et les machines absentes.

Après le premier ordinateur, répétez sur les autres. Un nom distinct par machine simplifie le contrôle. Testez aussi chaque démarrage habituel pour éviter qu’un ancien lanceur réutilise plus tard la destination précédente. Le [guide multi-ordinateurs](/blog/fr/mine-multiple-computers-one-wallet/) explique l’organisation des machines sous une même adresse.

## Que deviennent les anciennes récompenses ?

Les crédits et paiements restent à l’ancienne adresse. Ses shares admissibles peuvent encore contribuer aux fenêtres PPLNS concernées ; les récompenses en attente suivent les confirmations. Un hashrate à zéro ne signifie pas que la comptabilité est terminée. Les guides [PPLNS](/blog/fr/how-pplns-payouts-work/) et [confirmations](/blog/fr/block-confirmations-mining-rewards/) distinguent travail enregistré, maturation et paiement.

Changer la destination ne fusionne pas les soldes et ne contourne pas le minimum de paiement. Consultez les règles actuelles et suivez les deux pages jusqu’à comprendre les montants restants. Renommer une machine ne transfère pas non plus les récompenses. Vérifiez finalement que chaque mineur prévu envoie le nouveau travail à la nouvelle adresse, tout en conservant l’ancien portefeuille et son historique.

<CTA target="pool" title="Vérifier votre configuration sur bntpool.com" text="Confirmez destination et travail accepté sur chaque machine, puis suivez séparément anciennes et nouvelles récompenses." />
