---
title: Synchroniser vite un nœud Blocknet avec les checkpoints bntpool.com
description: Les nœuds Blocknet se synchronisent vite avec checkpoints.dat de bntpool.com. Découvrez comment vérifier, télécharger et charger ces points de contrôle.
tags: [Nœud Blocknet, Checkpoints, Synchronisation rapide, bntpool]
faq:
  - q: 'Où un nœud Blocknet obtient-il ses checkpoints ?'
    a: 'Sur bntpool.com. Au premier démarrage, si le répertoire de données ne contient pas checkpoints.dat, le daemon Blocknet le télécharge depuis https://bntpool.com/checkpoints.dat et l’utilise pour la synchronisation rapide. Vous pouvez aussi le télécharger depuis la page Checkpoints, vérifier son SHA-256 et le placer dans le répertoire de données avant de lancer le nœud.'
  - q: 'À quelle fréquence bntpool.com actualise-t-il checkpoints.dat ?'
    a: 'Le daemon du pool réécrit le fichier à mesure que la chaîne grandit, avec une entrée tous les 100 blocs. Chaque entrée se situe au moins 100 blocs sous la tête de chaîne, au-delà de la profondeur maximale de réorganisation de Blocknet : les checkpoints ne couvrent que l’historique définitif. La page Checkpoints affiche la dernière hauteur et la dernière modification du fichier.'
  - q: 'Les checkpoints changent-ils mon solde ou mon historique ?'
    a: 'Non. Un nœud synchronisé avec des checkpoints stocke toujours chaque bloc et inscrit chaque sortie et image de clé dans ses index. Le scan du portefeuille et les soldes fonctionnent donc exactement comme après une synchronisation complète. Les checkpoints évitent seulement de revérifier la preuve de travail et les signatures de l’historique déjà garanti par leurs hachages.'
  - q: 'Peut-on synchroniser un nœud Blocknet sans checkpoints ?'
    a: 'Oui. Lancez le daemon avec --full-sync ou définissez full_sync à true dans la configuration blocknet. Le nœud ne télécharge pas les checkpoints et revérifie la preuve de travail et les signatures de chaque bloc depuis le genesis. Cela prend plus de temps, raison pour laquelle les nouveaux nœuds utilisent les checkpoints par défaut.'
sourceHash: 7a3491743f7d
translation: draft
---

Un nouveau nœud Blocknet doit rattraper tous les blocs depuis le genesis avant de servir un portefeuille ou un mineur solo. Les checkpoints accélèrent ce rattrapage. Ce guide explique le contenu du fichier de bntpool.com, comment le daemon évite les vérifications les plus lentes tout en restant attaché à la vraie chaîne, et comment télécharger, vérifier et charger le fichier vous-même.

## Que sont les checkpoints Blocknet ?

<Answer>
Les checkpoints Blocknet sont une liste de hachages de blocs connus, un tous les 100 blocs, fournie par bntpool.com dans checkpoints.dat. Un nœud en synchronisation vérifie que les blocs reçus correspondent à ces hachages et évite de revérifier la preuve de travail et les signatures jusqu’au dernier checkpoint. La première synchronisation se termine ainsi beaucoup plus vite.
</Answer>

Le fichier est du texte brut, avec une paire `height:hash` par ligne. Le 25 octobre 2026, ses premières lignes étaient :

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Chaque hachage est l’identifiant du bloc, le SHA3-256 de son en-tête. Le daemon du pool écrit le fichier depuis sa chaîne et n’inclut que des blocs enfouis à au moins 100 blocs. Cent blocs correspondent à la finalité stricte de Blocknet, la profondeur maximale de réorganisation autorisée : chaque checkpoint appartient donc à l’historique définitif.

Le 25 octobre 2026 à 02:04 EDT, le fichier contenait 689 checkpoints, de la hauteur 100 à 68 900, sur 48 811 octets. Il gagne une ligne environ tous les 100 blocs, soit environ huit heures avec des blocs de cinq minutes. Consultez la [page Checkpoints](/checkpoints) pour la hauteur actuelle.

## Comment un nœud Blocknet utilise-t-il les checkpoints ?

Un nœud ayant chargé les checkpoints prend la dernière hauteur comme cible de synchronisation rapide. Jusqu’à cette hauteur, il s’appuie sur les checkpoints publiés pour les deux contrôles les plus coûteux et effectue tous les autres normalement :

- **La preuve de travail repose sur les checkpoints.** Celle de chaque bloc Blocknet est un hachage Argon2id sur 2 GiB de mémoire. La recalculer pour chaque bloc historique coûte cher, et les hachages des checkpoints garantissent déjà cet historique.
- **Les signatures RingCT et les preuves d’intervalle aussi.** Les commentaires du daemon désignent la vérification des preuves d’intervalle des sorties comme le premier coût d’une synchronisation, car chaque sortie en porte une, y compris la coinbase de chaque bloc.
- **Tous les autres contrôles restent actifs.** Liaison des blocs, règle de difficulté LWMA, règles d’horodatage, taille et structure des blocs, contrôle des doubles dépenses par image de clé et liaison entre chaque entrée et sa signature fonctionnent normalement. Sorties et images de clé rejoignent les index comme d’habitude : scan du portefeuille et soldes restent identiques à une synchronisation complète.

L’allègement ne concerne que les blocs prolongeant la tête actuelle du nœud, et seulement sous le dernier checkpoint. Au-delà, le nœud vérifie chaque nouveau bloc intégralement, preuve de travail comprise.

### Comment les checkpoints fixent la chaîne de confiance

Pendant le rattrapage vers le dernier checkpoint, tout bloc à une hauteur de checkpoint doit avoir exactement le hachage du fichier. Un hachage différent provoque une erreur de non-correspondance. Avec un checkpoint tous les 100 blocs, un pair ne peut pas entraîner le nœud vers un autre historique : la prochaine hauteur de checkpoint révélerait l’écart.

Les checkpoints accélèrent aussi chaque redémarrage. Lorsque le daemon retrouve des blocs sur disque, il cherche le checkpoint le plus haut correspondant à sa chaîne stockée et ne revérifie que les blocs suivants, au lieu de parcourir toute la chaîne depuis le genesis.

## Un nouveau nœud télécharge-t-il les checkpoints automatiquement ?

<Answer>
Oui. Quand le daemon Blocknet démarre sans checkpoints.dat dans son répertoire de données, il télécharge le fichier depuis https://bntpool.com/checkpoints.dat, le charge et se synchronise rapidement jusqu’au dernier checkpoint. Le téléchargement est tenté sans obligation de réussite : s’il échoue, le nœud démarre quand même et vérifie toute la chaîne.
</Answer>

Cette adresse est la valeur par défaut inscrite dans le core Blocknet lui-même : chaque nouveau nœud utilise donc directement le fichier publié par bntpool.com. Le daemon ne le récupère que s’il n’existe pas, accorde 15 secondes à la requête et limite le téléchargement à 32 MiB.

## Comment utiliser les checkpoints bntpool.com étape par étape ?

Si vous préférez récupérer et vérifier le fichier vous-même, ou actualiser une ancienne copie avant une longue resynchronisation, voici la procédure manuelle.

### 1. Trouvez le répertoire de données

Le fichier se place dans le répertoire de données du nœud, près de la base de données de la chaîne :

- **Le gestionnaire `blocknet` :** `~/.config/bnt/data/mainnet/` sur macOS et Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` sur Windows, sauf si vous avez défini `data_dir` dans sa configuration.
- **Le core lancé directement :** `./blocknet-data-mainnet` dans le dossier de lancement, ou le chemin fourni avec `--data`.

### 2. Téléchargez checkpoints.dat

Ouvrez [Checkpoints](/checkpoints) et cliquez sur **Download checkpoints.dat**, ou récupérez-le dans un terminal. Cet exemple écrit dans le répertoire par défaut du core :

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Télécharger par-dessus un fichier existant le remplace : c’est ainsi que l’on actualise une ancienne copie.

### 3. Vérifiez le SHA-256

La page Checkpoints publie le SHA-256 du fichier actuel. Calculez celui de votre copie et comparez :

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

Sur macOS, utilisez plutôt `shasum -a 256`. Le hachage change à chaque ajout de checkpoint : comparez avec la page juste après le téléchargement.

### 4. Vérifiez un checkpoint sur l’explorateur

Vous pouvez confirmer n’importe quelle ligne dans l’[explorateur public Blocknet](https://explorer.blocknetcrypto.com). Ouvrez un bloc à une hauteur du fichier, par exemple `explorer.blocknetcrypto.com/block/68900`, et comparez son hachage avec la ligne 68 900. Nous l’avons fait pour le dernier checkpoint le 25 octobre 2026 : les hachages correspondaient.

### 5. Lancez le nœud

Lancez ou redémarrez le daemon. Il lit `checkpoints.dat` au démarrage et se synchronise rapidement jusqu’au dernier checkpoint. Si vous utilisez le shell interactif du core (`--cli`), la commande `load-checkpoints` charge le fichier dans un nœud en marche et indique la hauteur de synchronisation rapide :

```text
> load-checkpoints
```

Les checkpoints aident un nœud en retard. Une fois la chaîne au-delà du dernier checkpoint, le nœud vérifie déjà tous les blocs intégralement et un fichier plus récent ne change rien.

## Comment vérifier le fichier de checkpoints depuis un script ?

Le pool publie les détails du fichier en JSON à `https://bntpool.com/api/checkpoints`, sans clé API :

| Champ | Signification |
|---|---|
| `available` | Le pool dispose-t-il d’un fichier à fournir ? |
| `url` | Adresse de téléchargement |
| `entries` | Nombre de checkpoints dans le fichier |
| `latest_height`, `latest_hash` | Dernier checkpoint et hachage de son bloc |
| `bytes`, `sha256` | Taille du fichier et SHA-256 |
| `updated_at` | Dernière modification, sous forme d’horodatage Unix |

Le téléchargement contient aussi les en-têtes `X-Checkpoint-Height`, `X-Checkpoint-Sha256` et un `ETag` construit à partir du hachage. Un script peut donc envoyer `If-None-Match` et obtenir rapidement `304 Not Modified` si rien n’a changé :

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Les réponses peuvent rester en cache 60 secondes, ce qui convient largement à un fichier modifié environ toutes les huit heures.

<Note>
blocknetcrypto.com indique que la mise à niveau Faraday prévue apportera une preuve de travail plus légère pour accélérer le rattrapage des nouveaux nœuds. En attendant, les checkpoints permettent de mettre un nœud à jour rapidement.
</Note>

## Que faire avec un nœud synchronisé ?

Un nœud synchronisé sert le portefeuille et l’API Blocknet. C’est à lui que seine se connecte en solo avec `--mode daemon`. [Minage en pool ou en solo sur Blocknet](/blog/fr/pool-vs-solo-mining-blocknet/) explique comment déterminer si le solo convient à votre matériel. Si vous préférez miner sans gérer de nœud, seine utilise bntpool.com par défaut : le [guide d’installation de seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) mène aux premiers shares, et [miner sur bntpool.com](/blog/fr/mine-blocknet-on-bntpool/) couvre le côté pool.

<CTA target="pool" title="Minez sur le pool qui fournit les checkpoints Blocknet" text="Rejoignez bntpool.com en quelques minutes et suivez votre hashrate, vos shares et vos paiements en direct." />
