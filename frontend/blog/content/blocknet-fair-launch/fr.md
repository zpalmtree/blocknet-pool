---
title: 'Le lancement équitable de Blocknet : chaque coin est miné'
description: Blocknet est né sans premine, fonds de développement ni trésorerie. Vérifiez le genesis vide et la règle coinbase à une sortie, puis minez vos BNT.
tags: [Blocknet, Lancement équitable, Bloc genesis, Minage, bntpool]
faq:
  - q: 'Blocknet a-t-il eu un premine ?'
    a: 'Non. Le bloc genesis de Blocknet, créé le 15 février 2026, ne contient aucune transaction : aucun coin n’existait avant le minage. Les premiers coins proviennent de la récompense du bloc 1, et chaque BNT depuis a été versé à la personne ayant miné son bloc d’origine.'
  - q: 'Blocknet possède-t-il un fonds de développement ou une trésorerie ?'
    a: 'Non. Le consensus autorise exactement une transaction coinbase par bloc, avec exactement une sortie valant la récompense de bloc. Aucune deuxième sortie n’est réservée à un développeur, un fondateur ou une trésorerie. Chaque nœud rejette un bloc qui tente d’en ajouter une.'
  - q: 'Quand le bloc genesis de Blocknet a-t-il été créé ?'
    a: 'Son horodatage est le 15 février 2026 à 20:58:38 EST, soit le 16 février à 01:58:38 UTC. Il porte le message « Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything », dont le SHA3-256 constitue le hachage du bloc précédent.'
  - q: 'Comment vérifier moi-même le lancement équitable de Blocknet ?'
    a: 'Ouvrez le bloc 0 sur l’explorateur Blocknet : il comporte zéro transaction et une racine de Merkle entièrement nulle. Consultez ensuite le code ouvert du daemon : le genesis est codé avec une liste de transactions vide et le contrôle coinbase accepte une sortie égale à la récompense du bloc.'
  - q: 'Comment commencer à miner Blocknet ?'
    a: 'Téléchargez seine pour Windows, macOS ou Linux, lancez-le et collez votre adresse Blocknet ou votre nom court Blocknet ID lorsqu’il le demande. Appuyez sur Entrée pour accepter bntpool.com, le pool par défaut, et seine commence immédiatement. Vous pouvez ensuite rechercher votre adresse sur la page de statistiques bntpool.com.'
sourceHash: 814839500c3d
translation: draft
---

Blocknet a démarré sans aucune allocation à qui que ce soit. Cette page présente le contenu du bloc genesis, les règles de consensus qui excluent premine, fonds de développement et trésorerie, comment les vérifier sur l’explorateur et dans le code, et comment miner vos propres BNT sur bntpool.com.

## Blocknet a-t-il eu un premine ?

<Answer>
Non. Le bloc genesis de Blocknet, créé le 15 février 2026, ne contient aucune transaction : aucun coin n’existait avant le minage. Depuis le bloc 1, chaque bloc verse sa récompense dans une seule sortie coinbase, égale au montant prévu. Chaque BNT en circulation a donc été miné par quelqu’un exécutant la preuve de travail.
</Answer>

Aucune allocation n’est réservée aux fondateurs, développeurs, investisseurs ou à une fondation, ni dans le genesis ni dans les blocs suivants. L’offre provient uniquement des récompenses de bloc. Les règles qui l’imposent font partie du consensus et chaque nœud les vérifie pour chaque bloc.

## Que contient le bloc genesis de Blocknet ?

Le bloc genesis est codé en dur dans le daemon : chaque nœud part donc du même bloc 0. Ses champs sont fixes :

| Champ | Valeur |
|---|---|
| Hauteur | 0 |
| Horodatage | 15 février 2026, 20:58:38 EST (16 février à 01:58:38 UTC) |
| Message | « Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything » |
| Hachage précédent | SHA3-256 du message : `a94a7f2b…d909df` |
| Difficulté | 4, le minimum du réseau |
| Nonce | 0 |
| Transactions | aucune |
| Racine de Merkle | uniquement des zéros |

Le message est le titre d’un article Wired daté du 15 février 2026, cité comme source dans le code. Puisque le hachage précédent dérive du message, ce titre fait partie de l’identité du bloc 0 et de tout ce qui est construit dessus.

Le commentaire de la fonction genesis est explicite : « Le genesis ne contient aucune transaction : ni coinbase, ni coins brûlés. Le premier vrai bloc à la hauteur 1 aura la première coinbase. » La validation confirme cela. Un nœud rejette le bloc 0 s’il n’a pas exactement zéro transaction et une racine de Merkle entièrement nulle. Même un nœud modifié ne pourrait donc glisser des coins dans le genesis sans être rejeté par tous les autres.

## Comment le consensus exclut-il fonds de développement et trésorerie ?

<Answer>
Chaque bloc Blocknet doit commencer par exactement une transaction coinbase, sans entrées, avec des frais nuls et exactement une sortie égale à la récompense prévue pour cette hauteur. Il n’y a aucun emplacement pour un deuxième paiement : une sortie destinée à un fonds de développement, un fondateur ou une trésorerie ne peut donc pas exister sur la chaîne.
</Answer>

Plusieurs contrôles du daemon agissent ensemble :

- **Une coinbase par bloc.** La première transaction doit être une coinbase et aucune autre transaction du bloc ne peut l’être.
- **Une sortie, aucune entrée.** Une coinbase avec zéro ou deux sorties, ou avec une entrée, est invalide.
- **La récompense exacte.** Blocknet masque les montants avec des engagements de Pedersen, mais la coinbase est un cas particulier. Son facteur d’aveuglement dérive de données publiques, la clé publique de la transaction et la hauteur du bloc. Chaque nœud peut donc reconstruire l’engagement de la récompense prévue et comparer. Si la sortie engage un autre montant, le bloc est rejeté.
- **Un mémo vide.** Le mémo coinbase doit être vide : les récompenses ne peuvent pas non plus transporter de données supplémentaires.

Les transactions ordinaires ne peuvent pas créer de coins. Chacune doit s’équilibrer : entrées égales aux sorties plus les frais, contrôle effectué sur les engagements eux-mêmes. Chaque sortie porte aussi une preuve d’intervalle Bulletproofs confirmant un montant dans un intervalle valide, jamais négatif. Les montants restent ainsi privés tout en respectant exactement le calendrier d’émission.

## Comment vérifier vous-même le lancement équitable de Blocknet ?

Vous n’avez pas à croire qui que ce soit sur parole. La chaîne et le code sont publics.

**Sur l’explorateur.** Ouvrez le [bloc 0 sur l’explorateur Blocknet](https://explorer.blocknetcrypto.com/block/0). Il affiche 0 transaction, une racine de Merkle de 64 zéros, un nonce de 0 et un hachage précédent commençant par `a94a7f2b`. La ligne de récompense affiche le montant prévu à cette hauteur, mais sans transaction, aucune sortie ne le reçoit. Ouvrez ensuite le [bloc 1](https://explorer.blocknetcrypto.com/block/1) : sa seule transaction est une coinbase sans entrées et avec une sortie, les premiers coins de la chaîne.

Vous pouvez confirmer que le hachage précédent est bien celui du message genesis. Avec OpenSSL 1.1.1 ou plus récent, exécutez :

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Le résultat se termine par `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, la valeur affichée pour le bloc 0.

**Dans le code.** Le daemon est ouvert sur [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). Dans [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go), recherchez ces fonctions :

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` crée le bloc 0 avec une liste de transactions vide, `validateGenesisBlock` rejette tout bloc 0 contenant des transactions, et `validateCoinbaseConsensus` impose la sortie unique valant la récompense. Dans `transaction.go`, `validateCoinbase` et `verifyCommitmentBalance` contrôlent la structure coinbase et l’équilibre des transactions ordinaires.

## Pourquoi un lancement équitable compte-t-il pour les mineurs ?

Pour les mineurs, il signifie que les récompenses de bloc expliquent entièrement l’origine des coins.

- **Tous les nouveaux coins vont aux mineurs.** Sans premine ni sortie de trésorerie, 100 % de l’émission rémunère les personnes qui trouvent les blocs. Le bloc 1 a versé 723,25093035 BNT dans son unique sortie coinbase, et chaque bloc suivant a versé sa récompense entière de la même façon.
- **Les mêmes règles pour tous.** Depuis le bloc 1, exécuter la preuve de travail est le seul moyen de gagner de nouveaux BNT. Le [calendrier des récompenses](/blog/fr/blocknet-block-reward-schedule/) est défini dans le code et s’applique autant au premier mineur qu’à celui qui commence aujourd’hui.
- **Une preuve de travail adaptée aux ordinateurs ordinaires.** Blocknet utilise Argon2id avec 2 GiB de mémoire par hachage, conçu pour résister aux ASIC. Le minage reste donc ouvert aux ordinateurs de bureau, portables et serveurs déjà possédés. L’[explication d’Argon2id sur seineminer.com](https://seineminer.com/fr/blog/argon2id-proof-of-work-explained/) détaille pourquoi.
- **Une émission facile à auditer.** Chaque coin remonte à une sortie coinbase vérifiée par chaque nœud. Le total émis à toute hauteur découle donc directement du calendrier des récompenses.

Si vous découvrez la monnaie, [Qu’est-ce que Blocknet ?](https://seineminer.com/fr/blog/what-is-blocknet/) présente la confidentialité, les paiements par nom et les portefeuilles.

## Comment miner Blocknet vous-même ?

Participer à un lancement équitable, c’est miner. Le plus rapide est d’utiliser seine sur bntpool.com :

1. **Obtenez une adresse.** Installez le [portefeuille en ligne de commande](https://github.com/blocknetprivacy/blocknet/releases/latest) ou le [portefeuille de bureau](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) officiel et copiez votre adresse, ou utilisez un nom court Blocknet ID comme `@name`.
2. **Téléchargez seine.** Prenez l’archive Windows, macOS ou Linux sur [seineminer.com](https://seineminer.com/fr/#download) et décompressez-la. Rien à installer.
3. **Lancez-le et acceptez les valeurs par défaut.** Collez votre adresse ou `@name` et appuyez sur Entrée pour accepter `stratum+tcp://bntpool.com:3333`. seine commence aussitôt et optimise threads et kernels pendant le minage, sur CPU et sur cartes NVIDIA sous Windows et Linux.
4. **Suivez votre mineur.** Recherchez votre adresse sur la [page des statistiques](/stats) pour voir hashrate, shares et solde, et suivez les découvertes sur la [page des blocs](/blocks).

Le [guide de minage sur bntpool.com](/blog/fr/mine-blocknet-on-bntpool/) couvre la connexion par adresse ou nom et le suivi du mineur. La [page Get Started](/start) présente les réglages de paiement du pool, et le [guide pour miner Blocknet](https://seineminer.com/fr/blog/how-to-mine-blocknet/) explique l’installation de seine sur chaque système.

<CTA target="seine" title="Minez votre part" text="Chaque BNT est miné. Téléchargez seine et commencez sur bntpool.com en quelques minutes." />
