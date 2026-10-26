---
title: 'bntpool.comでBlocknetをマイニングする'
seoTitle: 'bntpool.comでBlocknetを採掘'
description: 'seineの標準プール、bntpool.comへの接続方法。アドレスや@nameでログインし、リグに名前を付け、ハッシュレートやシェアの到着を確認します。'
tags: ['マイニングガイド', 'bntpool', 'Blocknet ID', 'seine']
sourceHash: '84ce1ed92e20'
translation: draft
faq:
  - q: 'bntpool.comのStratumアドレスは？'
    a: 'stratum+tcp://bntpool.com:3333です。seineが初回起動時に提案するプールなので、プールの質問でEnterを押すか、--pool-urlで指定します。BlocknetアドレスまたはBlocknet ID名がログイン情報になり、そのアドレスへ報酬が支払われます。'
  - q: 'Blocknet ID名でbntpool.comを利用できますか？'
    a: 'はい。seineやプールでアドレスを求められた場所に@nameまたは$nameを入力します。プールはblocknet.idの署名付き対応情報を確認し、名前に対応する完全なアドレスへシェアと支払いを計上します。seine v0.2.16以降が必要です。コマンドラインではシェルによる解釈を防ぐため名前を引用符で囲んでください。'
  - q: 'bntpool.comで採掘するにはアカウントが必要ですか？'
    a: '不要です。登録、パスワード、メールアドレスは使いません。Blocknetアドレス、またはそれを指すBlocknet ID名がログイン情報と支払い先になります。指定してseineを起動し、My Statsで検索するとハッシュレート、シェア、リグ、残高、支払いを確認できます。'
  - q: '複数のコンピューターから同じアドレスへ採掘できますか？'
    a: 'はい。各コンピューターで同じアドレスか名前を指定し、別々のリグ名でseineを実行します。プールはシェア、残高、支払いをアドレス単位で集計するため、全リグが一つの残高と支払いにまとまります。My Statsではリグごとに別の行が表示されます。'
---

このガイドではseineでbntpool.comに参加する手順を説明します。プールアドレス、BlocknetアドレスやBlocknet IDの`@name`によるログイン、各リグの命名、統計ページでシェアの到着を確認する方法を扱います。まだseineを導入していない場合は、先に[seineの導入ガイド](https://seineminer.com/ja/blog/how-to-mine-blocknet/)を読み、ここへ戻ってください。

## bntpool.comのプールアドレスは？

<Answer>
Stratumアドレスは`stratum+tcp://bntpool.com:3333`です。seineの初回起動時に標準プールとして表示されるので、プールの質問でEnterを押せば接続できます。BlocknetアドレスかBlocknet IDの`@name`でログインします。アカウント作成は不要で、報酬はそのアドレスに届きます。
</Answer>

公開エンドポイント`/api/info`が2026年10月25日に示した設定は次のとおりです。

| 設定 | 値 |
|---|---|
| Stratumアドレス | `stratum+tcp://bntpool.com:3333` |
| ログイン | Blocknetアドレス、`@name`、`$name` |
| 支払い方式 | 6時間のウィンドウによるPPLNS |
| 支払いまでの承認数 | 60ブロック |
| 最低支払額 | 0.1 BNT |

同じ値はプールの[Get Started](/start)でいつでも確認できます。

## なぜbntpool.comがseineの標準プールなのですか？

seineを初めて実行するとプールを尋ねられ、`stratum+tcp://bntpool.com:3333`が標準値として入っています。プールとマイナーは次の点で連携しています。

- **同じプロトコル。** bntpool.comはseineが使用するStratumバージョン2に対応しているため、アドレス以外の設定は必要ありません。
- **両側で名前に対応。** seineもプールもBlocknet ID名を受け付け、blocknet.idの同じ署名付きレコードで検証します。
- **バージョン確認。** 各リグはシェアとともにseineのバージョンを報告します。古い版のリグには統計ページで「update available」が付き、seineのリリースページにリンクします。
- **seineの設定ガイド。** Get Startedにはダウンロードと起動の説明に加え、コピーできるコマンドがあります。

## seineをbntpool.comへ接続するには？

初回起動時に二つの質問が表示されます。

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

アドレスを貼り付けるか名前を入力し、Enterでbntpool.comを選択します。seineは両方の回答を`seine-data/seine.config.json`へ保存するので、次回からすぐに採掘を開始します。リグ名も選んで同じファイルに保存します。詳しくは後述します。

画面のないリグ、サービス、スクリプトでは質問に答える人がいません。初回起動で両方をオプションとして渡してください。Get Startedにあるコマンドと同じです。

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

後でプールやアドレスを変えるには、`seine-data/seine.config.json`を編集または削除するか、コマンドラインの`--address`と`--pool-url`で保存値を上書きします。

## Blocknet IDの@nameへ採掘できますか？

<Answer>
はい。bntpool.comはアドレスを受け付ける場所で`@name`または`$name`形式のBlocknet ID短縮名を使えます。プールはblocknet.idの署名付き対応情報と宛先アドレスを検証し、その接続中のシェア、残高、支払いをすべて完全なアドレスへ計上します。
</Answer>

名前は大文字と小文字を区別しないため、`@Alice`と`@alice`は同じログインです。名前対応はseine v0.2.16で追加され、現在のリリースにはすべて含まれます。採掘前に解決先アドレスが表示されるので、ウォレットと一致することを確認できます。コマンドラインでは名前をシングルクォートで囲んでください。囲まないとシェルが`$name`を変数として展開しようとします。

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

後で名前を別のウォレットへ向けたら、seineを再起動してください。保存された名前は起動ごとに解決され、その実行中は同じアドレスを使います。プールも接続単位で同じ動作をします。すでに得た残高は元のアドレスに残り、そこへ支払われます。名前を移してもコインは移りません。

名前は[blocknet.id](https://blocknet.id)で登録できます。[Blocknet IDガイド](https://seineminer.com/ja/blog/blocknet-id-pay-by-name/)では名前による支払いを説明しています。

## リグに名前を付けるには？

各接続はアドレスと一緒にリグ名を送ります。初回起動でseineは`seine-`に4桁の数字を付けた名前を選び、設定ファイルへ保存します。同じマシンは再起動後も同じ名前を使います。分かりやすい名前にするにはコマンドラインで指定します。

```sh
./seine --pool-worker rig-01
```

プールが保持する名前は最大64文字です。固定名は再起動にも役立ちます。プールはアドレスとリグ名をキーにシェア難易度を1時間記憶するため、再接続したリグは調整済みの難易度で続けられます。

## 複数のリグから一つのアドレスへ採掘できますか？

はい。各マシンで同じアドレスか`@name`を使い、異なるリグ名でseineを起動します。シェア、残高、支払いはアドレス単位なので、すべて一つの残高と支払いになります。統計ページには各リグが別行で表示され、全マシンの稼働を確認できます。

## マイナーの動作を確認するには？

seineのダッシュボードにはそのマシンのハッシュレートと受理済みシェアが表示されます。プール側の状況を見るには[My Stats](/stats)でアドレス、`@name`、`$name`を入力し、**Lookup**を押します。名前の場合は解決先アドレスが検索欄の横に表示されます。ブラウザーには最後の検索も保存されます。[マイナーの追跡ガイド](/blog/ja/track-your-miner-on-bntpool/)で各カードを説明しています。

次の三つを確認します。

- **HashrateとShares Accepted。** プールが観測する速度が表示され、リグが仕事を送ると受理数が増えます。
- **各リグの行。** 名前、ハッシュレート、受理・拒否シェア数、最後のシェア時刻を表示します。直近のシェアがあれば接続して動作しています。
- **Recent Shares。** 最新のシェアを送信元リグ、難易度、状態とともに表示します。

プールは接続ごとにシェア難易度を調整するので、小さなCPUでも継続的に送信でき、接続から数分で最初の受理済みシェアが現れます。ハードウェアの目安は[Blocknetマイニングのハードウェアガイド](https://seineminer.com/ja/blog/blocknet-mining-hardware-hashrate/)をご覧ください。

次に報酬が発生します。シェアが6時間のウィンドウ内にある間にプールがブロックを見つけるとEstimated Rewardsが表示されます。60承認後にConfirmed Rewardsへ移り、0.1 BNT以上になると自動送金されます。[PPLNSの支払い方式](/blog/ja/how-pplns-payouts-work/)で分配を説明しています。プール全体の履歴は[Blocks](/blocks)と[Payouts](/payouts)にあります。

## Get Startedには何がありますか？

[Get Started](/start)には次の情報が一画面にまとまっています。

- **プール情報。** クリックでコピーできるStratumアドレス、支払い方式、最低額、必要承認数をプールから直接取得して表示します。
- **3段階のクイックスタート。** [seineminer.com](https://seineminer.com/ja/#download)からダウンロードし、初回にアドレスか名前とプールURLを入力して採掘開始。My Statsで速度と残高を追います。
- **コピー用コマンド。** プールURL入りの完全な`./seine`コマンドです。自分のアドレスか、引用符付きの`'@yourname'`へ差し替えます。
- **設定メモ。** 設定ファイルの保存先、リセット方法、上書きオプションを説明します。
- **報酬の計算。** PPLNSの短い説明です。発見前のシェアがそのブロックの報酬を得て、発見後の仕事は次のブロックに計上されます。未承認ブロックの報酬は承認までMy Statsに別表示されることも説明しています。
- **スクリーンショット。** seineの初回設定と採掘ダッシュボードを、始める前に確認できます。

<CTA target="pool" title="bntpool.comで採掘を始める" text="アドレスか@nameでseineをbntpool.comへ接続し、My Statsでシェアの到着を確認しましょう。" />
