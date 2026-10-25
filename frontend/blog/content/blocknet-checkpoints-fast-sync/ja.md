---
title: 'bntpool.comのチェックポイントでBlocknetを高速同期'
seoTitle: 'チェックポイントでBlocknetを高速同期'
description: 'Blocknetノードはbntpool.comのcheckpoints.datで高速同期できます。チェーンを固定する仕組みと、ファイルのダウンロード、検証、読み込み手順を解説します。'
tags: ['Blocknetノード', 'チェックポイント', '高速同期', 'bntpool']
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Blocknetノードはチェックポイントをどこから取得しますか？'
    a: 'bntpool.comです。初回起動でデータディレクトリにcheckpoints.datがない場合、daemonがhttps://bntpool.com/checkpoints.datから取得して高速同期に使います。Checkpointsページから手動で取得し、SHA-256を確認して起動前にデータディレクトリへ置くこともできます。'
  - q: 'bntpool.comはcheckpoints.datをどのくらい更新しますか？'
    a: 'プールのdaemonがチェーンの成長に合わせて書き直し、100ブロックごとに一件を記録します。各項目は先端より最低100ブロック深く、Blocknetの最大再編成深度を超えた確定履歴だけを扱います。Checkpointsページで最新高と更新時刻を確認できます。'
  - q: 'チェックポイントはウォレット残高や履歴を変えますか？'
    a: '変えません。同期中もすべてのブロックを保存し、各出力とkey imageを索引へ記録するので、ウォレット走査と残高は完全同期後と同じです。省くのは、チェックポイントのハッシュが裏付ける履歴に対するproof of workと署名の再検証だけです。'
  - q: 'チェックポイントなしでBlocknetを同期できますか？'
    a: 'はい。daemonに--full-syncを付けるか、blocknet設定のfull_syncをtrueにすると、ダウンロードせずジェネシスから全ブロックのproof of workと署名を再検証します。より時間がかかるため、新しいノードではチェックポイントが標準です。'
---

新しいBlocknetノードは、ウォレットやソロマイナーに応答する前にジェネシス以降の全ブロックへ追いつく必要があります。チェックポイントはその処理を速めます。このガイドではbntpool.comのファイルの内容、実際のチェーンに固定されたまま重い検証を省く仕組み、手動で取得・検証・読み込みする方法を説明します。

## Blocknetのチェックポイントとは？

<Answer>
既知のブロックハッシュを100ブロックごとに一つ並べた一覧で、bntpool.comがcheckpoints.datとして配布します。同期ノードは受信ブロックのハッシュ一致を確認し、最後のチェックポイントまでproof of workと署名の再検証を省略するため、初回同期が大幅に速くなります。
</Answer>

ファイルは各行に`height:hash`を一組置くプレーンテキストです。2026年10月25日の先頭部分は次のとおりでした。

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

各ハッシュはブロックID、すなわちヘッダーのSHA3-256です。プール自身のdaemonが自分のチェーンから作り、最低100ブロック深いものだけを含めます。100ブロックはBlocknetのハードファイナリティ、プロトコルが許す最深の再編成なので、各項目は確定した履歴内にあります。

2026年10月25日02:04 EDTには、高さ100から68,900まで689件、48,811バイトでした。約100ブロックに一行、5分間隔なら約8時間に一行増えます。現在高は[Checkpointsページ](/checkpoints)で確認してください。

## ノードはチェックポイントをどう使いますか？

読み込んだノードは最後の高さを高速同期の目標にします。それまでは二つの最も重い検証を公開チェックポイントに委ね、その他は通常どおり実行します。

- **Proof of workをチェックポイントで裏付ける。** 各ブロックのproof of workは2 GiBのメモリを使うArgon2idです。全履歴で再計算するコストは大きく、チェックポイントのハッシュがすでにその履歴を裏付けています。
- **RingCT署名とrange proofも同様。** daemonのコメントは出力range proof検証を同期最大の単一コストと説明しています。coinbaseを含むすべての出力がproofを持つためです。
- **他の検証はすべて実行。** ブロックの連結、LWMA難易度、時刻、サイズと構造、key imageによる二重使用、各入力と署名の結合を通常どおり確認します。出力とkey imageは索引に保存され、ウォレット走査と残高は完全同期と同じです。

省略されるのは現在の先端を延長するブロックで、最後のチェックポイントより下だけです。その高さを過ぎると、proof of workも含め新しいブロックをすべて完全検証します。

### 信頼するチェーンを固定する仕組み

最後のチェックポイントへ同期している間、その高さにあるブロックのハッシュはファイルと厳密に一致しなければなりません。違えばcheckpoint mismatchで拒否します。100ブロックごとに検証点があるため、peerが別の履歴へ誘導しても次のチェックポイントで露見します。

再起動も速くなります。ディスクに既存ブロックがあるとdaemonは保存チェーンと一致する最高のチェックポイントを探し、ジェネシスから全件をたどらず、その後だけを再検証します。

## 新しいノードは自動でダウンロードしますか？

<Answer>
はい。起動時にデータディレクトリにcheckpoints.datがなければ、https://bntpool.com/checkpoints.datから取得し、読み込んで最後の点まで高速同期します。取得はベストエフォートです。失敗してもノードは起動し、チェーンを完全検証します。
</Answer>

このURLはBlocknet core自体の標準値です。新しいノードは最初からbntpool.comのファイルで高速同期します。ファイルがない場合だけ取得し、要求の制限は15秒、ダウンロード上限は32 MiBです。

## 手動で使う手順は？

自分で取得して確認したい場合や、長い再同期の前に古いコピーを更新したい場合は、次の手順を使います。

### 1. データディレクトリを探す

ファイルはチェーンDBの隣に置きます。

- **`blocknet`マネージャー:** macOSとLinuxは`~/.config/bnt/data/mainnet/`、Windowsは`C:\Users\<you>\.config\bnt\data\mainnet\`です。設定の`data_dir`を指定した場合はその場所です。
- **coreを直接起動:** 起動したフォルダーの`./blocknet-data-mainnet`、または`--data`で指定した場所です。

### 2. checkpoints.datを取得する

[Checkpoints](/checkpoints)で**Download checkpoints.dat**を押すか、端末で取得します。これはcoreの標準データディレクトリへ保存する例です。

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

既存ファイルの上に取得すると置き換えられ、古いコピーを更新できます。

### 3. SHA-256を確認する

Checkpointsページは現在のファイルのSHA-256を公開しています。手元のハッシュを計算して比べます。

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

macOSでは`shasum -a 256`を使います。プールが項目を追加するたびにハッシュが変わるので、取得直後にページと比べてください。

### 4. エクスプローラーで一件確認する

どの行も公開[Blocknetエクスプローラー](https://explorer.blocknetcrypto.com)で確認できます。例えば`explorer.blocknetcrypto.com/block/68900`を開き、高さ68,900の行とハッシュを比べます。2026年10月25日に最後の項目で実施し、一致を確認しました。

### 5. ノードを起動する

daemonを起動または再起動すると`checkpoints.dat`を読み、最後の点まで高速同期します。対話シェル（`--cli`）を使う場合、`load-checkpoints`で稼働中ノードに読み込み、高速同期の高さを確認できます。

```text
> load-checkpoints
```

チェックポイントが役立つのはノードが遅れている間です。最後の高さを過ぎればすでに全ブロックを完全検証しており、新しいファイルにしても動作は変わりません。

## スクリプトからファイルを確認するには？

プールは`https://bntpool.com/api/checkpoints`で詳細をJSON公開しており、APIキーは不要です。

| フィールド | 意味 |
|---|---|
| `available` | 配布可能なファイルがあるか |
| `url` | ダウンロード先 |
| `entries` | ファイル内の件数 |
| `latest_height`, `latest_hash` | 最高のチェックポイントとそのブロックハッシュ |
| `bytes`, `sha256` | サイズとSHA-256 |
| `updated_at` | 最終更新時刻のUnixタイムスタンプ |

ダウンロードには`X-Checkpoint-Height`、`X-Checkpoint-Sha256`、ハッシュ由来の`ETag`も付くので、スクリプトは`If-None-Match`を送り、変更がなければすぐ`304 Not Modified`を受け取れます。

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

応答は60秒キャッシュされる場合があります。約8時間ごとの変更には十分な頻度です。

<Note>
blocknetcrypto.comによると、計画中のFaraday更新は軽いproof of workを導入し、新しいノードが速く追いつけるようにします。それまではチェックポイントが迅速な同期手段です。
</Note>

## 同期済みノードで何ができますか？

BlocknetウォレットとノードAPIを支え、`--mode daemon`でソロ採掘するseineの接続先になります。[Blocknetのプールとソロの比較](/blog/ja/pool-vs-solo-mining-blocknet/)でハードウェアに合うか判断できます。ノードを運用せず始めるなら、seineは標準でbntpool.comへ接続します。[seineの導入ガイド](https://seineminer.com/ja/blog/how-to-mine-blocknet/)で最初のシェアまで進み、[bntpool.comのガイド](/blog/ja/mine-blocknet-on-bntpool/)でプール側を確認できます。

<CTA target="pool" title="Blocknetのチェックポイントを配布するプールで採掘" text="数分でbntpool.comに参加し、ハッシュレート、シェア、支払いをリアルタイムで追いましょう。" />
