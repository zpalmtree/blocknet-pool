---
title: Suivre votre mineur sur bntpool.com
description: Recherchez votre adresse ou @name pour suivre hashrate, shares, solde et paiements sur bntpool.com, puis explorez blocs, chance, disponibilité et API.
tags: [bntpool, Statistiques de minage, API du pool, Blocknet]
faq:
  - q: 'Pourquoi le hashrate bntpool.com diffère-t-il de celui de seine ?'
    a: 'seine compte tous les hachages calculés, tandis que le pool estime votre hashrate à partir de la difficulté des shares acceptés, sur la dernière heure. Les shares arrivent aléatoirement : la valeur du pool varie davantage et se stabilise avec le temps. Comparez le graphique sur 24 heures de My Stats à la moyenne de seine.'
  - q: 'Peut-on rechercher ses statistiques avec un @name ?'
    a: 'Oui. Saisissez votre nom court Blocknet ID, comme @name ou $name, dans My Stats. La page le résout via blocknet.id, affiche vos statistiques et remplace le champ de recherche par l’adresse complète. Utilisez cette adresse pour appeler l’API depuis un script.'
  - q: 'L’API bntpool.com nécessite-t-elle une clé ?'
    a: 'Non. Les endpoints de statistiques, blocs, paiements, chance, statut et mineurs fournissent du JSON public via HTTPS. Un script curl ou un tableau de bord domestique peut donc les lire sans inscription. Les montants sont en unités atomiques : divisez par 100 000 000 pour obtenir des BNT et interrogez toutes les 30 à 60 secondes.'
  - q: 'Pourquoi My Stats affiche-t-il zéro dans Estimated Rewards ?'
    a: 'Les estimations apparaissent lorsque le pool trouve un bloc avec vos shares dans la fenêtre de paiement. Pour un nouveau mineur, cela peut prendre quelques périodes de minage. Shares acceptés et hashrate apparaissent d’abord, puis les récompenses estimées, qui deviennent Confirmed Rewards à maturité et sont payées automatiquement.'
sourceHash: '5367192a57e2'
translation: draft
---

Une fois seine connecté à bntpool.com, le pool vous montre tout ce qu’il voit : hashrate, shares acceptés, gains et paiements envoyés à votre adresse. Ce guide présente My Stats, les pages publiques du pool et l’API utilisable dans vos scripts et tableaux de bord domestiques.

Si vous n’avez pas commencé, le [guide d’installation de seine](https://seineminer.com/fr/blog/how-to-mine-blocknet/) vous mène aux premiers shares en quelques minutes.

## Comment consulter les statistiques de votre mineur ?

<Answer>
Ouvrez My Stats sur bntpool.com/stats, collez votre adresse Blocknet ou @name et cliquez sur Lookup. La page affiche hashrate, shares acceptés et refusés, soldes estimé, confirmé et payé, graphique de hashrate, une ligne par rig et paiements récents. Elle s’actualise tant que l’onglet reste ouvert.
</Answer>

Dans [My Stats](/stats), saisissez l’adresse ou le [nom court Blocknet ID](https://seineminer.com/fr/blog/blocknet-id-pay-by-name/) fourni à seine. Un nom comme `@name` ou `$name` est résolu via blocknet.id et le champ affiche ensuite l’adresse complète. La dernière recherche est mémorisée dans ce navigateur : vos chiffres sont déjà là à la prochaine visite.

## Que montre My Stats ?

La page regroupe cartes et tableaux répondant chacun à une question sur votre mineur.

### Solde

- **Estimated Rewards :** votre part des blocs encore en maturation.
- **Confirmed Rewards :** solde arrivé à maturité, en attente du prochain paiement.
- **Paid Balance :** tous les montants déjà envoyés à votre adresse.

Quand des récompenses sont en cours, **Estimated From Recent Blocks** liste chaque bloc avec sa hauteur, votre crédit estimé, les confirmations restantes et sa date de découverte. Chaque hauteur mène au bloc dans l’[explorateur Blocknet](https://explorer.blocknetcrypto.com), pour vérifier vous-même. [Les paiements PPLNS](/blog/fr/how-pplns-payouts-work/) expliquent la répartition et le calendrier des paiements.

### Minage et shares

Les cartes **Mining** montrent votre **Hashrate**, le nombre de **Blocks Found** résolus par vos rigs pour le pool et votre date de début (**Mining Since**). Suivent **Shares Accepted**, **Shares Rejected**, **Reject Rate** et **Avg Difficulty**. Un rig sain envoie régulièrement des shares acceptés avec un taux de rejet proche de zéro.

**Hashrate History** trace le hashrate sur 1h, 24h, 7d ou 30d. Les périodes longues permettent de voir au mieux si un rig a fonctionné toute la nuit.

### Une ligne par rig

Si vous avez plusieurs machines, donnez à chacune un nom distinct : le pool les liste séparément avec hashrate, shares acceptés et refusés et heure du dernier share. seine choisit au premier lancement `seine-` suivi de quatre chiffres. Vous pouvez définir le vôtre :

```sh
./seine --pool-worker rig-01
```

Un rig utilisant une ancienne version reçoit un badge **update available** lié à la [page des versions seine](https://github.com/zpalmtree/seine/releases), pour repérer une machine oubliée lors des mises à jour.

### Paiements et shares récents

**Recent Payouts** liste chaque paiement avec montant, lien vers la transaction et statut en file d’attente, non confirmé ou confirmé. **Recent Shares** montre les 20 derniers shares avec tâche, rig, difficulté et statut : le moyen le plus rapide de confirmer qu’une machine fraîchement lancée atteint le pool.

## Pourquoi le hashrate du pool diffère-t-il de celui de seine ?

seine compte tous les hachages calculés. Le pool ne voit que les shares soumis et estime le hashrate à partir de leur difficulté totale acceptée sur la dernière heure. Chaque share est un événement aléatoire. bntpool.com ajuste la difficulté de chaque connexion pour viser environ 10 shares toutes les 5 minutes, donc une courte fenêtre comporte du bruit.

Deux habitudes permettent de comparer correctement :

- Comparez les moyennes : le graphique 24h de My Stats et la moyenne seine sur la même durée.
- Laissez du temps à un nouveau rig. Au début, le pool moyenne sur les minutes déjà minées : la première valeur dépend de quelques shares. Après une heure, la fenêtre complète s’applique.

Recent Shares confirme immédiatement leur arrivée, avant même la stabilisation du hashrate.

<span id="zero-hashrate"></span>

## Pourquoi seine mine-t-il alors que My Stats affiche un hashrate nul ?

Vérifiez d’abord l’adresse de paiement, la connexion au pool et les shares acceptés. Le hashrate local indique que la machine calcule des hashes ; l’estimation du pool nécessite des shares acceptés pour l’adresse recherchée.

1. **Vérifiez l’adresse.** Recherchez la même adresse de réception ou le même `@name` enregistré que seine utilise. Si vous avez modifié l’adresse d’un nom court, redémarrez seine pour le résoudre à nouveau, puis consultez cette adresse dans My Stats.
2. **Vérifiez le pool.** Assurez-vous que seine utilise `stratum+tcp://bntpool.com:3333`. Une machine qui mine ailleurs ou en solo n’ajoute aucun share ici. Le [guide de configuration du pool](/blog/fr/mine-blocknet-on-bntpool/) détaille la connexion.
3. **Consultez Recent Shares.** Attendez des shares acceptés, puis le rafraîchissement de la page. Les shares arrivent à intervalles variables ; un hashrate local actif ne garantit pas une estimation immédiate du pool. Si des shares sont acceptés, comparez les moyennes dans la durée.
4. **Lisez le message de connexion.** Utilisez le tableau tant que le mineur n’envoie pas de shares acceptés. Consultez [Status](/status) pour les incidents en cours.

| Message dans seine | Vérification suivante |
|---|---|
| `failed to connect to pool at` | Vérifiez le nom d’hôte et le port, votre connexion et l’autorisation des connexions TCP sortantes vers le port 3333. |
| `login rejected:` | Lisez le motif qui suit ; vérifiez l’adresse de réception ou le nom court et utilisez la version actuelle de seine. |
| `stale job` | Le share concerne une ancienne tâche. Surveillez les nouvelles tâches et les shares acceptés ; si cela continue, vérifiez la connexion et la version du mineur. |
| `submit interrupted during reconnect` | Seine se reconnecte. Attendez l’acceptation de la connexion et de nouveaux shares acceptés après le rétablissement de la connexion. |

Un montant nul dans **Estimated Rewards** est un autre sujet : des shares peuvent être acceptés avant qu’un bloc du pool crédite des récompenses. Le [guide PPLNS](/blog/fr/how-pplns-payouts-work/) explique cet enchaînement.

## Quelles pages publiques montrent l’ensemble du pool ?

Ces pages ne nécessitent aucune recherche et montrent ouvertement l’activité du pool. Le [tableau de bord](/) rassemble mineurs connectés, hashrates du pool et du réseau, blocs trouvés et chance moyenne.

### Blocs

[Blocks](/blocks) liste tous les blocs trouvés, du plus récent au plus ancien : hauteur liée à l’explorateur, récompense, effort, durée, statut et date. Filtrez par Confirmed, Pending ou Orphaned pour connaître leur état.

### Paiements

[Payouts](/payouts) liste chaque lot avec montant total, nombre de mineurs payés, liens de transaction et statut. Un lot apparaît non confirmé dès sa diffusion et devient confirmé peu après sa première confirmation de bloc. Vous pouvez ainsi suivre votre paiement côté pool et dans l’explorateur.

### Chance

[Luck](/luck) montre l’effort de chaque période : le travail des shares acceptés divisé par la difficulté du bloc. 100 % correspond au travail attendu, une valeur inférieure à une découverte plus rapide. Chaque période peut varier fortement dans les deux sens : surveillez donc **Average Luck** sur le tableau de bord. Son tableau **Pool Luck History** mène ici via **View All**.

### Statut

[Status](/status) est la page de santé du pool. Elle indique si le pool, HTTP public, l’API, Stratum, la base et le daemon sont en ligne, la fraîcheur du modèle courant, la hauteur, la disponibilité sur 10 minutes, 6 heures, 24 heures et 7 jours, et les incidents. Deux mesures de disponibilité sont réalisées : un moniteur sur le serveur du pool et une sonde externe sur Cloudflare.

## Comment utiliser l’API bntpool.com ?

<Answer>
L’API fournit du JSON public via HTTPS, sans clé. Appelez https://bntpool.com/api/miner/ suivi de votre adresse pour hashrate, rigs, shares et paiements, ajoutez /balance pour les soldes et utilisez /api/stats pour les hashrates du pool et du réseau. Les montants sont en unités atomiques : divisez par 100 000 000 pour obtenir des BNT.
</Answer>

Toutes les pages utilisent ces endpoints : tout ce que vous voyez peut être récupéré vous-même.

| Endpoint | Résultat |
|---|---|
| `/api/stats` | Mineurs connectés, hashrate du pool, blocs trouvés, hashrate réseau, hauteur actuelle |
| `/api/stats/history?range=24h` | Historique du hashrate du pool (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Période actuelle, effort moyen, périodes récentes |
| `/api/info` | Réglages : port Stratum, système et fenêtre de paiement, minimum |
| `/api/blocks?status=confirmed` | Blocs trouvés, filtrables par statut |
| `/api/payouts/recent` | Lots de paiements avec hachages des transactions |
| `/api/luck` | Effort et durée de chaque période |
| `/api/status` | Santé des services, fenêtres de disponibilité, incidents |
| `/api/miner/{address}` | Votre hashrate, totaux, rigs, derniers shares, paiements |
| `/api/miner/{address}/balance` | Soldes estimé, confirmé et payé |
| `/api/miner/{address}/hashrate?range=24h` | Votre historique de hashrate |

Les listes acceptent `limit` (25 par défaut, jusqu’à 200) et `offset`, et renvoient `items` et `total`. Les endpoints mineur exigent une adresse complète : après une recherche @name, copiez-la depuis le champ de My Stats.

Pour consulter votre mineur dans un terminal :

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Avec `jq`, extrayez par exemple hashrate et shares acceptés, ou le total payé en BNT :

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Afficher votre mineur dans un tableau de bord domestique

Le même endpoint peut servir de capteur. Dans Home Assistant, l’intégration RESTful le lit avec quelques lignes dans `configuration.yaml` :

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Une interrogation toutes les 30 à 60 secondes suffit : le hashrate est une moyenne horaire et My Stats actualise les détails toutes les 30 secondes. L’API publique limite le débit par client. Si un script reçoit HTTP 429, il doit ralentir et attendre le délai de l’en-tête `Retry-After` avant de réessayer.

## Et le tableau de bord de seine ?

Le pool affiche ce qu’il vous a crédité. Pour voir la machine en direct, le tableau de bord terminal seine montre hashrate et shares immédiatement. seine peut aussi exposer une API locale avec REST, flux d’événements serveur et métriques Prometheus à `/metrics`, sur `127.0.0.1:9977` par défaut. Le [guide de l’API de contrôle](https://github.com/zpalmtree/seine/blob/master/docs/API.md) présente options et endpoints. Beaucoup de mineurs utilisent les deux : les métriques seine par machine, et l’API du pool pour soldes et paiements.

Vous découvrez seine ? [Téléchargez-le](https://seineminer.com/fr/#download), choisissez le pool par défaut et votre adresse apparaîtra sur My Stats dans les minutes suivant le premier share accepté.

<CTA target="pool" title="Commencez à miner et suivez tout en direct" text="Rejoignez bntpool.com en quelques minutes, puis suivez votre hashrate, vos shares et votre solde sur My Stats." />
