---
title: 'Miner Blocknet sur plusieurs PC vers un portefeuille'
description: 'Ajoutez un second mineur Blocknet au même portefeuille sur bntpool.com. Nommez chaque PC, vérifiez ses parts et comprenez les récompenses et noms en double.'
tags: ['Guide de minage', 'Machines', 'Configuration']
faq:
  - q: 'Faut-il un portefeuille par ordinateur ?'
    a: 'Non. Chaque ordinateur peut utiliser la même adresse de réception sur bntpool.com avec un nom distinct pour identifier son installation. Le pool comptabilise les récompenses par adresse. Les machines de minage n''ont besoin que de cette adresse ou de votre Blocknet ID, pas de la phrase de récupération.'
  - q: 'Que se passe-t-il si deux mineurs ont le même nom ?'
    a: 'Sous une même adresse, leur historique de parts est regroupé sous ce nom. Il devient plus difficile d''identifier la machine correspondant à une mesure. Donnez un nom distinct à chaque installation et redémarrez-la avec celui-ci ; les parts déjà enregistrées conservent leur ancienne étiquette.'
  - q: 'Ajouter une machine divise-t-il mon paiement ?'
    a: 'Les noms organisent l''affichage de l''activité et ne créent pas de soldes séparés sous la même adresse. Le travail valide de vos ordinateurs contribue à la participation PPLNS de cette adresse. Son solde suit les règles habituelles de confirmation et de paiement du pool.'
sourceHash: '1cfcb79502c0'
translation: draft
---

Ajoutez un second ordinateur à bntpool.com en conservant un portefeuille de réception. Nommez chaque installation, vérifiez-la séparément, puis surveillez l'adresse commune. Ce guide explique l'ajout d'une machine et les noms en double. Pour votre premier mineur, commencez par le [guide de configuration du pool](/blog/fr/mine-blocknet-on-bntpool/).

## Plusieurs ordinateurs peuvent-ils miner vers le même portefeuille ?

<Answer>
Oui. Lancez seine sur chaque ordinateur avec la même adresse de réception et un nom de machine distinct. bntpool.com enregistre travail et récompenses par adresse ; les noms séparent l'activité dans les statistiques. Vérifiez les parts acceptées de chaque machine avant de vous fier à la vue commune.
</Answer>

*Des ordinateurs distincts envoient du travail via le pool vers une adresse de réception ; chacun conserve son propre nom.*

## Choisissez l'adresse et les noms

Utilisez l'adresse de réception du portefeuille ou un Blocknet ID qui pointe vers elle. Vérifiez l'adresse résolue sur chaque mineur. Le nom simplifie la saisie, mais la comptabilité suit l'adresse obtenue. Si vous changez la destination de l'ID, une exécution existante peut conserver l'ancienne résolution jusqu'au redémarrage ; les récompenses déjà acquises restent à leur adresse d'origine.

Le [guide du portefeuille de minage](https://seineminer.com/fr/blog/blocknet-wallet-for-mining/) explique comment obtenir une adresse. Les machines ont seulement besoin de cette destination publique ; gardez phrase de récupération et clés privées avec le portefeuille. Choisissez des noms courts comme `desktop-01` et `server-02`. Le pool retire les espaces extérieurs et conserve jusqu'à 64 caractères : placez la partie distinctive au début d'un long nom.

## Ajoutez le second mineur avec son nom

Installez la version adaptée de seine sur le second ordinateur et lancez-la depuis le dossier extrait. Le premier exemple PowerShell indique explicitement nom destinataire, pool et machine. Remplacez `@yourname` par votre ID ou adresse complète avant l'exécution. Gardez les guillemets et ajoutez les options de backend ou de réglage déjà testées pour cette machine.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Sous Linux ou macOS, la commande ci-dessous utilise l'exécutable correspondant et un autre nom de machine. Les systèmes peuvent différer, mais l'adresse de réception doit correspondre. Chaque exemple lance un vrai minage. Exécutez celui qui convient sur l'ordinateur concerné et laissez le démarrage et les réglages se terminer. Les deux exemples ne sont pas à lancer sur la même machine.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Quels réglages sont communs aux ordinateurs ?

Partagez l'adresse de réception et le serveur du pool. Gardez un dossier de données local et un nom distincts par installation. seine enregistre la connexion dans `seine.config.json` dans son répertoire de données ; le chemin par défaut `./seine-data` dépend du dossier de lancement. Un autre dossier peut retrouver une autre configuration. Un emplacement stable simplifie la maintenance.

Si vous copiez la configuration du premier PC, fixez un nom distinct dans la commande du second et vérifiez-le au démarrage. Conservez cette indication dans les prochaines commandes : une substitution en ligne de commande ne réécrit pas un nom déjà enregistré. Adaptez les choix matériels à chaque PC et laissez-le se régler localement. L'argument d'adresse fixe la destination du paiement ; celui du nom fixe l'étiquette. Saisissez-les séparément comme ci-dessus.

## Comment vérifier la contribution des deux PC ?

Ouvrez My Stats sur bntpool.com pour l'adresse commune. Cherchez les deux noms et une part récente de chacun. Vérifiez aussi les soumissions acceptées dans chaque mineur. Procédez machine par machine : vous reliez ainsi le processus local à une ligne du pool et repérez mieux les erreurs qu'avec le seul hashrate total.

Le pool estime le hashrate à partir des parts soumises. Un nouveau mineur a besoin de temps et de travail accepté pour une mesure informative. Taux local et estimation du pool correspondent à des mesures et périodes différentes. Confirmez d'abord l'activité avec les parts récentes et les compteurs, puis comparez des périodes plus longues. Le [guide des statistiques](/blog/fr/track-your-miner-on-bntpool/) explique cartes et lignes sans déduire le nombre d'appareils du nombre de connexions.

## Que faire si une machine manque ou partage un nom ?

Deux PC avec le même nom sous la même adresse voient leurs parts regroupées. Renommez-en un avec son option de nom puis redémarrez-le. Les nouvelles soumissions utilisent la nouvelle étiquette ; les anciennes gardent la précédente. L'ancienne ligne peut rester visible pendant sa période d'historique. Renommer ne déplace pas le solde gagné par l'adresse.

Pour une machine absente, comparez adresse réellement résolue, serveur et nom avec la page ouverte. Vérifiez ensuite sa connexion et ses parts acceptées. Un CPU et un GPU dans le même processus seine n'ont pas besoin de noms séparés ici. Pour des parts rejetées ou périmées, suivez le [guide des états des parts](/blog/fr/accepted-rejected-stale-shares/) plutôt que de changer sans cesse l'adresse du portefeuille.

## Comment fonctionnent récompenses et paiements communs ?

Le travail valide des ordinateurs contribue à la participation d'une même adresse dans la fenêtre PPLNS. Les noms facilitent le suivi, sans créer de soldes distincts. Ajouter une machine apporte le travail utile réellement soumis, pas une récompense fixe par appareil ou nom. Matériel et durée d'activité peuvent produire des contributions différentes ; davantage de noms ne crée pas de travail supplémentaire.

Le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) explique la répartition à la découverte d'un bloc. Les règles de confirmation et de paiement s'appliquent toujours au solde commun. Inutile de fusionner des portefeuilles ou de transférer des soldes entre noms après l'ajout. Vérifiez destination, nom distinct et parts acceptées sur chaque PC avant de le laisser tourner, puis réutilisez cette liste pour le suivant.

<CTA target="pool" title="Connecter un autre ordinateur à bntpool.com" text="Utilisez une adresse de réception, nommez chaque installation distinctement et vérifiez ses parts acceptées." />
