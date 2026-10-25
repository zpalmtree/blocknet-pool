---
title: 'Blocknetの公平な開始：プレマインなし、全コインを採掘'
seoTitle: 'Blocknetの公平な開始とプレマインなしの仕組み'
description: 'Blocknetにはプレマイン、開発基金、運営準備金がありません。空のジェネシスブロックと単一出力のcoinbase規則を確認し、自分で検証してBNTを採掘する方法を紹介します。'
tags: ['Blocknet', '公平な開始', 'ジェネシスブロック', 'マイニング', 'bntpool']
sourceHash: 814839500c3d
translation: draft
faq:
  - q: 'Blocknetにはプレマインがありましたか？'
    a: 'ありません。2026年2月15日に作成されたジェネシスブロックには取引が一件もなく、採掘開始前のコインは存在しません。最初のコインはブロック1の報酬で、それ以降のすべてのBNTも、それを生んだブロックの採掘者へ支払われています。'
  - q: 'Blocknetに開発基金や運営準備金はありますか？'
    a: 'ありません。合意規則は各ブロックにcoinbase取引を一件だけ認め、その出力もブロック報酬と同額の一つだけに限定します。開発者、創設者、準備金向けの二つ目の出力はなく、追加しようとするブロックはネットワークの全ノードに拒否されます。'
  - q: 'Blocknetのジェネシスブロックはいつ作られましたか？'
    a: 'タイムスタンプは2026年2月15日20:58:38 EST、UTCでは2月16日01:58:38です。「Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything」というメッセージを持ち、前ブロックのハッシュにはこのメッセージのSHA3-256が使われています。'
  - q: 'Blocknetの公平な開始を自分で確認するには？'
    a: 'Blocknetエクスプローラーでブロック0を開くと、取引数が0でMerkle rootがすべて0だと分かります。公開daemonコードも確認してください。ジェネシスは空の取引リストで固定され、coinbase検証は報酬額と等しい単一出力だけを認めています。'
  - q: 'Blocknetの採掘を始めるには？'
    a: 'Windows、macOS、Linux用のseineをダウンロードして起動し、質問にBlocknetアドレスかBlocknet ID短縮名を入力します。Enterで標準プールbntpool.comを選ぶとすぐ採掘が始まります。その後、bntpool.comの統計ページで自分のアドレスを検索できます。'
---

Blocknetは誰にも事前配分せずに始まりました。このページではジェネシスの内容、プレマインや開発基金、準備金を排除する合意規則、エクスプローラーとコードでの確認方法、bntpool.comで自分のBNTを採掘する方法を説明します。

## Blocknetにはプレマインがありましたか？

<Answer>
ありません。2026年2月15日のジェネシスブロックは取引を含まないため、採掘前のコインは存在しません。ブロック1から各ブロックは決められた額と等しい一つのcoinbase出力で報酬を払い、流通するすべてのBNTはproof of workを実行した誰かが採掘したものです。
</Answer>

ジェネシスにも後続ブロックにも、創設者、開発者、投資家、財団への配分はありません。供給源はブロック報酬だけです。これを守る規則は合意規則に含まれ、全ノードが各ブロックを検証します。

## ジェネシスブロックには何が入っていますか？

ジェネシスはdaemon内で固定され、全ノードが同じブロック0から始めます。値は次のとおりです。

| 項目 | 値 |
|---|---|
| 高さ | 0 |
| 時刻 | 2026年2月15日20:58:38 EST（2月16日01:58:38 UTC） |
| メッセージ | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| 前ハッシュ | メッセージのSHA3-256：`a94a7f2b…d909df` |
| 難易度 | 4、ネットワークの最小値 |
| Nonce | 0 |
| 取引 | なし |
| Merkle root | すべて0 |

メッセージは2026年2月15日付Wired記事の見出しで、コードはその記事を出典に挙げています。前ハッシュがこの文字列から導かれるため、見出しはブロック0とその上に積み上がるすべての識別に固定されています。

daemonのジェネシス関数のコメントには、ジェネシスに取引、coinbase、焼却コインはなく、高さ1の最初の実ブロックが最初のcoinbaseを持つと明記されています。検証もそのとおりです。取引数が厳密に0でMerkle rootがすべて0でなければブロック0を拒否します。改造ノードでも、他の全ノードに拒否されずにジェネシスへコインを差し込むことはできません。

## 合意規則は開発基金や準備金をどう排除しますか？

<Answer>
各Blocknetブロックの先頭にはcoinbase取引が一つ必要です。入力なし、手数料0、出力はその高さのブロック報酬と同額の一つだけでなければなりません。二つ目の支払いを入れる場所がないため、開発基金、創設者報酬、準備金向けの出力はチェーン上に作れません。
</Answer>

daemonの複数の検証が組み合わさっています。

- **1ブロックにcoinbaseは一つ。** 最初の取引がcoinbaseで、それ以外の取引はcoinbaseではいけません。
- **出力一つ、入力なし。** 出力が0個や2個、または入力があるcoinbaseは無効です。
- **報酬額が正確。** BlocknetはPedersen commitmentsで金額を隠しますが、coinbaseは特別です。ブラインディング係数は取引公開鍵とブロック高という公開情報から導かれ、各ノードは予定報酬のcommitmentを再構築して照合できます。別の金額ならブロックを拒否します。
- **空のmemo。** Coinbaseのmemoは空でなければならず、報酬に追加データを載せることもできません。

通常取引はコインを作れません。入力が出力と手数料の和に等しいことをcommitments上で検証します。各出力にはBulletproofs range proofがあり、金額が有効範囲に入り負数でないと示します。金額を非公開に保ちながら発行は正確に予定どおり進みます。

## 公平な開始を自分で確認するには？

誰かの説明を信じるだけでなく、公開チェーンとコードで確認できます。

**エクスプローラーで。** [Blocknetのブロック0](https://explorer.blocknetcrypto.com/block/0)を開くと、取引0件、64個の0からなるMerkle root、nonce 0、`a94a7f2b`で始まる前ハッシュが表示されます。報酬欄はその高さの予定額ですが、取引がないので受け取る出力はありません。[ブロック1](https://explorer.blocknetcrypto.com/block/1)を開くと、唯一の取引は入力なし・出力一つのcoinbaseで、これがチェーン最初のコインです。

前ハッシュが本当にジェネシスメッセージのハッシュかも確認できます。OpenSSL 1.1.1以降で次を実行します。

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

結果の末尾は`a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`で、ブロック0の表示と一致します。

**コードで。** daemonのソースは[github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core)で公開されています。[`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go)で次の関数を探します。

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock`は空の取引リストでブロック0を作り、`validateGenesisBlock`は取引入りのブロック0を拒否します。`validateCoinbaseConsensus`は報酬額の単一出力を強制します。`transaction.go`の`validateCoinbase`と`verifyCommitmentBalance`はcoinbaseの形と通常取引の収支を検証します。

## 公平な開始はマイナーにとってなぜ重要ですか？

マイナーにとって、公平な開始はコインの由来がブロック報酬だけで説明できることを意味します。

- **新規コインはすべてマイナーへ。** プレマインも準備金出力もないので、発行量の100%がブロック発見者へ支払われます。ブロック1は一つのcoinbase出力で723.25093035 BNTを払い、以後も同じ方法で報酬全額を払います。
- **全員に同じ規則。** ブロック1以降、新しいBNTを得る方法はproof of workだけです。[報酬スケジュール](/blog/ja/blocknet-block-reward-schedule/)はコードで決まり、最初のマイナーにも今日始める人にも同じです。
- **一般的なPCに合うproof of work。** Blocknetは1ハッシュ2 GiBを使うArgon2idで、ASICに対抗するよう設計されています。手持ちのデスクトップ、ノートPC、サーバーで採掘に参加できます。[seineminer.comのArgon2id解説](https://seineminer.com/ja/blog/argon2id-proof-of-work-explained/)が理由を説明します。
- **発行量を監査しやすい。** すべてのコインは全ノードが検証したcoinbase出力に由来し、任意の高さまでの総発行量は報酬表から直接求められます。

コイン全体について初めて読む方は[Blocknetとは？](https://seineminer.com/ja/blog/what-is-blocknet/)でプライバシー、名前での支払い、ウォレットを確認できます。

## 自分でBlocknetを採掘するには？

公平な開始への参加とは採掘することです。手早い方法はbntpool.comでseineを使うことです。

1. **アドレスを取得。** 公式の[コマンドラインウォレット](https://github.com/blocknetprivacy/blocknet/releases/latest)か[デスクトップウォレット](https://github.com/blocknetprivacy/blocknet-gui/releases/latest)を導入し、アドレスをコピーします。`@name`のようなBlocknet IDも使えます。
2. **seineを入手。** [seineminer.com](https://seineminer.com/ja/#download)からOS用アーカイブをダウンロードして展開します。インストール操作は不要です。
3. **起動して標準値を選ぶ。** アドレスか`@name`を貼り付け、Enterで`stratum+tcp://bntpool.com:3333`を選択します。seineはすぐ計算を始め、CPUと、Windows・LinuxではNVIDIA GPUで、スレッドやカーネルを調整します。
4. **マイナーを確認。** [統計ページ](/stats)で速度、シェア、残高を検索し、[ブロックページ](/blocks)でプールの発見を追います。

[bntpool.comの採掘ガイド](/blog/ja/mine-blocknet-on-bntpool/)はアドレスや名前でのログインと稼働確認、[Get Started](/start)は支払い設定、[Blocknetの採掘ガイド](https://seineminer.com/ja/blog/how-to-mine-blocknet/)は各OSでのseine導入を説明します。

<CTA target="seine" title="自分の取り分を採掘する" text="すべてのBNTは採掘で生まれます。seineを入手し、数分でbntpool.comに参加しましょう。" />
