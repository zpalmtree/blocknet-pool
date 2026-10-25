---
title: Miner Blocknet sur bntpool.com
description: Rejoignez bntpool.com, le pool Blocknet par défaut de seine. Connectez votre adresse ou @name, nommez vos rigs et suivez vos statistiques.
tags: [Guide de minage, bntpool, Blocknet ID, seine]
faq:
  - q: 'Quelle est l’adresse Stratum de bntpool.com ?'
    a: 'C’est stratum+tcp://bntpool.com:3333, le pool proposé au premier lancement de seine. Appuyez sur Entrée à l’invite du pool ou indiquez-le avec --pool-url. Votre adresse Blocknet ou votre nom Blocknet ID sert d’identifiant, et le pool y verse vos récompenses.'
  - q: 'Peut-on miner sur bntpool.com avec un nom Blocknet ID ?'
    a: 'Oui. Saisissez @name ou $name partout où seine ou le pool demande une adresse. Le pool vérifie la correspondance signée sur blocknet.id, puis crédite shares et paiements à l’adresse complète. Il faut seine v0.2.16 ou plus récent. En ligne de commande, mettez le nom entre apostrophes pour que le shell ne l’interprète pas.'
  - q: 'Faut-il un compte pour miner sur bntpool.com ?'
    a: 'Non. Aucune inscription, aucun mot de passe ni e-mail. Votre adresse Blocknet, ou un nom Blocknet ID qui y mène, sert d’identifiant et de destination de paiement. Lancez seine avec cette valeur, puis recherchez-la sur My Stats pour voir hashrate, shares, rigs, solde et paiements.'
  - q: 'Peut-on miner vers une seule adresse depuis plusieurs ordinateurs ?'
    a: 'Oui. Lancez seine sur chaque ordinateur avec la même adresse ou le même nom et donnez un nom distinct à chaque rig. Le pool compte les shares, soldes et paiements par adresse. Tous les rigs alimentent donc un seul solde et un seul paiement, avec une ligne par rig sur My Stats.'
sourceHash: f6080e03ffd0
translation: draft
---

Ce guide présente la connexion à bntpool.com avec seine : adresse du pool, identification par adresse Blocknet ou `@name` Blocknet ID, nommage des rigs et vérification de l’arrivée des shares. Si seine n’est pas encore installé, suivez d’abord le [guide d’installation](https://seineminer.com/fr/blog/how-to-mine-blocknet/), puis revenez ici.

## Quelle est l’adresse du pool bntpool.com ?

<Answer>
L’adresse Stratum est `stratum+tcp://bntpool.com:3333`. seine la propose au premier lancement : appuyer sur Entrée à l’invite suffit pour vous connecter. Vous utilisez votre adresse Blocknet ou votre `@name` Blocknet ID. Aucun compte à créer, et les récompenses vont à cette adresse.
</Answer>

Voici les réglages indiqués par l’endpoint public `/api/info` le 25 octobre 2026 :

| Réglage | Valeur |
|---|---|
| Adresse Stratum | `stratum+tcp://bntpool.com:3333` |
| Identifiant | Adresse Blocknet, `@name` ou `$name` |
| Système de paiement | PPLNS sur une fenêtre de 6 heures |
| Confirmations avant paiement | 60 blocs |
| Paiement minimum | 0,1 BNT |

Vous retrouvez ces valeurs à tout moment sur [Get Started](/start).

## Pourquoi bntpool.com est-il le pool par défaut de seine ?

Au premier lancement, seine demande un pool et propose `stratum+tcp://bntpool.com:3333`. Le pool et le mineur s’accordent sur plusieurs points pratiques :

- **Un même protocole.** bntpool.com utilise le protocole Stratum de seine, version 2. Rien à configurer au-delà de votre adresse.
- **Les noms des deux côtés.** seine et le pool acceptent les noms Blocknet ID et vérifient le même enregistrement signé sur blocknet.id.
- **Vérification des versions.** Chaque rig transmet sa version de seine avec ses shares. La page de statistiques affiche un badge « update available » pour une ancienne version, avec un lien vers les versions de seine.
- **Un guide seine.** Get Started explique le téléchargement et le lancement, avec une commande à copier.

## Comment connecter seine à bntpool.com ?

Au premier démarrage, seine pose deux questions :

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Collez votre adresse ou saisissez votre nom, puis appuyez sur Entrée pour accepter bntpool.com. seine enregistre les réponses dans `seine-data/seine.config.json` : les lancements suivants commencent directement à miner. Il choisit aussi un nom de rig et l’enregistre dans ce fichier, comme expliqué plus bas.

Sur un rig sans écran, dans un service ou un script, personne ne peut répondre. Fournissez les deux valeurs par des options au premier démarrage, avec la commande de Get Started :

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Pour changer ensuite de pool ou d’adresse, modifiez ou supprimez `seine-data/seine.config.json`, ou remplacez les valeurs enregistrées avec `--address` et `--pool-url`.

## Peut-on miner vers un @name Blocknet ID ?

<Answer>
Oui. bntpool.com accepte un nom court Blocknet ID écrit `@name` ou `$name` partout où une adresse est acceptée. Le pool vérifie la correspondance signée de blocknet.id, valide l’adresse associée et lui attribue shares, solde et paiements pendant toute la connexion.
</Answer>

Les noms ne distinguent pas majuscules et minuscules : `@Alice` et `@alice` sont le même identifiant. Leur prise en charge date de seine v0.2.16 et figure dans toutes les versions actuelles. Avant le minage, seine affiche l’adresse résolue pour vous permettre de la comparer au portefeuille. En ligne de commande, entourez le nom d’apostrophes. Sans elles, le shell tenterait d’interpréter `$name` comme une variable :

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Si vous faites ensuite pointer le nom vers un autre portefeuille, redémarrez seine. Il résout le nom enregistré à chaque démarrage et conserve l’adresse pendant toute l’exécution ; le pool fait de même pour chaque connexion. Un solde déjà gagné reste attaché à son adresse d’origine et y est payé. Déplacer un nom ne déplace donc jamais vos coins.

Vous pouvez enregistrer un nom sur [blocknet.id](https://blocknet.id). Le [guide Blocknet ID](https://seineminer.com/fr/blog/blocknet-id-pay-by-name/) explique le paiement par nom.

## Comment nommer vos rigs ?

Chaque connexion fournit un nom de rig avec votre adresse. Au premier lancement, seine en choisit un, `seine-` suivi de quatre chiffres, et l’enregistre dans sa configuration : la machine garde le même nom après redémarrage. Pour un nom reconnaissable, indiquez-le en ligne de commande :

```sh
./seine --pool-worker rig-01
```

Le pool conserve jusqu’à 64 caractères. Un nom stable aide aussi après un redémarrage : le pool mémorise pendant une heure la difficulté des shares de chaque rig, selon votre adresse et son nom. Le rig reconnecté reprend donc la difficulté déjà trouvée.

## Peut-on miner vers une adresse depuis plusieurs rigs ?

Oui. Lancez seine sur chaque machine avec la même adresse ou `@name` et un nom de rig différent. Le pool compte shares, soldes et paiements par adresse : tous vos rigs alimentent un solde et un paiement uniques. Chaque rig dispose de sa ligne dans les statistiques, pour voir facilement que toutes les machines minent.

## Comment vérifier que votre mineur fonctionne ?

Le tableau de bord seine affiche localement hashrate et shares acceptés. Pour voir le côté pool, ouvrez [My Stats](/stats), saisissez votre adresse, `@name` ou `$name`, puis cliquez sur **Lookup**. Avec un nom, l’adresse résolue apparaît près du champ de recherche. La page mémorise aussi la dernière recherche dans ce navigateur. Le [guide de suivi du mineur](/blog/fr/track-your-miner-on-bntpool/) explique toutes ses cartes.

Vérifiez trois éléments :

- **Hashrate et Shares Accepted.** La carte donne le hashrate vu par le pool et le compteur augmente avec le travail envoyé par les rigs.
- **Une ligne par rig.** Chaque ligne donne nom, hashrate, shares acceptés et refusés, et heure du dernier share. Un share récent indique un rig connecté et actif.
- **Recent Shares.** Les derniers shares affichent leur rig, leur difficulté et leur statut.

Le pool ajuste séparément la difficulté de chaque connexion pendant le minage. Même un petit CPU envoie donc régulièrement des shares et les premiers acceptés apparaissent dans les minutes suivant la connexion. Le [guide matériel Blocknet](https://seineminer.com/fr/blog/blocknet-mining-hardware-hashrate/) présente les hashrates attendus.

Viennent ensuite les récompenses. Estimated Rewards apparaît lorsqu’un bloc est trouvé avec vos shares dans la fenêtre de six heures. Après 60 confirmations, votre part passe dans Confirmed Rewards et est envoyée automatiquement dès 0,1 BNT. [Les paiements PPLNS](/blog/fr/how-pplns-payouts-work/) expliquent la répartition. L’historique global figure dans [Blocks](/blocks) et [Payouts](/payouts).

## Que contient la page Get Started ?

La page [Get Started](/start) rassemble tout sur un écran :

- **Informations du pool.** Adresse Stratum à cliquer pour copier, système de paiement, minimum et confirmations nécessaires, lus en direct.
- **Démarrage en trois étapes.** Téléchargez seine sur [seineminer.com](https://seineminer.com/fr/#download), lancez-le et indiquez adresse ou nom et URL du pool, puis minez et suivez hashrate et solde sur My Stats.
- **Commande à copier.** La commande `./seine` complète avec l’URL du pool. Remplacez l’adresse par la vôtre ou par `'@yourname'` entre apostrophes.
- **Notes de configuration.** Emplacement du fichier, réinitialisation et options prioritaires sur les valeurs enregistrées.
- **Comptage des récompenses.** Une brève explication PPLNS : les shares précédant un bloc gagnent sa récompense, le travail ajouté ensuite compte pour les blocs suivants. Les récompenses non confirmées sont affichées séparément jusqu’à confirmation.
- **Captures d’écran** du premier lancement et du tableau de bord seine, pour savoir à quoi vous attendre.

<CTA target="pool" title="Commencez à miner sur bntpool.com" text="Connectez seine à bntpool.com avec votre adresse ou @name et suivez l’arrivée de vos shares sur My Stats." />
