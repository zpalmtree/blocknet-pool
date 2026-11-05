---
title: '把 Blocknet 直接挖到交易所'
description: '在 bntpool.com 上把 Blocknet 直接挖到 SafeTrade 或其他交易所的儲值地址。在 seine 中新增儲值備註，礦池就會支付到你的帳戶。'
tags: [挖礦指南, bntpool, SafeTrade, 支付]
sourceHash: 8f73aae25c7a
translation: draft
faq:
  - q: '可以把 Blocknet 直接挖到交易所嗎？'
    a: '可以。用交易所儲值地址與你的儲值備註啟動 seine v0.3.1 或更新版本，例如 ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO。bntpool.com 會把收益連同備註一起支付到這個地址，交易所就會把它記入你的帳戶。'
  - q: '為什麼 seine 會詢問支付備註？'
    a: 'SafeTrade 等交易所讓所有客戶共用一個儲值地址，並用備註區分每筆儲值。seine 認得這些共用地址，所以你輸入其中一個時，它會詢問你的備註，並與地址一起儲存。請從交易所的儲值頁面準確複製備註。'
  - q: '在 bntpool.com 上支付到交易所的最低支付額是多少？'
    a: '依據礦池在 2026年11月4日回報的資料，帶備註的支付從 10 BNT 起付。你的已確認餘額會在礦池中累積，達到該數額後，礦池把它連同備註傳送到交易所地址。支付到個人錢包則從 1 BNT 起付。'
  - q: '如何查看挖到交易所的餘額？'
    a: '在 bntpool.com 統計頁面輸入交易所儲值地址，選擇新增備註並輸入你的備註。每個地址與備註的組合都有自己的餘額，所以你只會看到自己的算力、礦機與支付，而不會看到使用同一交易所地址的其他客戶的資料。'
---

本指南說明如何在 bntpool.com 上把 Blocknet 直接挖到你的交易所帳戶。你把交易所的儲值地址與你的儲值備註告訴 seine，礦池就會把收益連同備註支付到那裡，交易所再把它記入你的帳戶。本文涵蓋命令、最低支付額以及如何查看餘額。

## 可以把 Blocknet 直接挖到交易所嗎？

<Answer>
可以。bntpool.com 會把挖礦收益連同你的儲值備註一起支付到交易所儲值地址。用交易所地址與你的備註啟動 seine v0.3.1 或更新版本：./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO。帶備註的支付從 10 BNT 起付，交易所會把每一筆都記入你的帳戶。
</Answer>

如果你打算交易挖到的幣，直接挖到交易所可以省去一步。收益直接進入你的交易所帳戶，可以立即在 [BNT/USDT 市場](/blog/zh-hant/sell-blocknet-bnt-safetrade/)出售，中間無需從錢包轉帳。

SafeTrade 等交易所給所有客戶同一個儲值地址，並用備註（你帳戶專屬的一段短代碼）區分每筆儲值。這就是備註重要的原因：它把一筆支付與你連結起來。礦池為每個地址與備註的組合分別記帳，所以你的收益永遠不會與使用同一交易所的其他礦工混在一起。

## 如何在 seine 中設定挖到交易所？

<Answer>
從交易所的儲值頁面複製 BNT 儲值地址與你的儲值備註。用這兩項啟動 seine：--address 填地址，--payout-memo 填備註。在首次執行的提示中輸入已知的交易所地址時，seine 會自動詢問備註並儲存。在日誌中確認礦池已經確認，然後照常挖礦。
</Answer>

1. **取得儲值資訊。** 在交易所開啟 BNT 儲值頁面，複製儲值地址與備註。請準確複製備註：大寫字母與開頭的零都有區別。
2. **用這兩項啟動 seine。** 在 macOS 或 Linux 上：

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   在 Windows 上，於 PowerShell 中執行：

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **讓 seine 儲存。** seine 會把備註與地址一起儲存，所以之後的執行會繼續支付到你的交易所帳戶。如果你在首次執行的提示中輸入已知的交易所地址，seine 會詢問備註並把兩者都儲存下來。

礦池的 [Get Started 頁面](/start)有一個「挖礦到交易所？」部分，可以幫你產生這條命令：貼上地址與備註，然後複製結果。對於 SafeTrade 這類已知的共用交易所地址，seine 要求填寫備註。服務、指令碼與其他無人值守的礦機必須在命令列中傳入 `--payout-memo`，因為沒有可以回答的提示。

seine 登入時會向礦池核對備註。bntpool.com 確認後就開始挖礦。要為礦機命名，像使用個人地址時一樣傳入 `--pool-worker rig-01`。

## 支付到交易所的款項如何發放？

收益與 bntpool.com 的其他支付走同樣的流程。你的份額在六小時 PPLNS 區間內分得礦池找到的每個區塊的一部分，每個區塊的獎勵在 60 個區塊後確認。之後你的已確認餘額會在礦池中累積。

礦池的 `/api/info` 介面在 2026年11月4日回報的最低支付額如下：

| 支付目標 | 最低支付額 |
|---|---|
| 帶備註的交易所地址 | 10 BNT |
| 個人錢包 | 1 BNT |

已確認餘額達到 10 BNT 後，礦池會把它連同備註傳送到交易所地址。交易所隨後依自己的確認數入帳，所需確認數列在交易所的儲值頁面上。[PPLNS 收益如何支付](/blog/zh-hant/how-pplns-payouts-work/)詳細說明了礦池這一側的流程。

單獨挖礦也可以使用備註。在交易所地址前加上 `solo:`，並保留 `--payout-memo`，礦池就會把你找到的每個區塊的全部獎勵支付到你的交易所帳戶。[在 bntpool.com 上單獨挖 Blocknet（solo）](/blog/zh-hant/solo-mine-blocknet-on-bntpool/)說明了單獨挖礦的收益。

## 如何查看餘額？

開啟[統計頁面](/stats)，貼上交易所儲值地址，選擇 **新增備註** 並輸入你的備註。頁面隨後會顯示該地址與備註組合下你自己的算力、礦機、餘額與支付。同一交易所的其他客戶有各自的組合與餘額。

**Recent Payouts** 中的每一筆支付都連結到區塊瀏覽器上的交易。把它與交易所的儲值紀錄對照，就能追蹤這些幣進入你的帳戶。[在 bntpool.com 上查看你的礦機](/blog/zh-hant/track-your-miner-on-bntpool/)說明了統計頁面的其餘部分。

## 如何切換回個人錢包？

用你的新地址與 `--no-payout-memo` 啟動一次 seine：

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine 會儲存不帶備註的新地址，新的份額會計入你的個人錢包。已經記入交易所組合的餘額會留在該組合中，達到最低支付額後支付給它；你可以在統計頁面上追蹤。[變更挖礦錢包地址](/blog/zh-hant/change-mining-wallet-address/)說明了變更地址後礦池如何記帳。

## 哪些設定支援備註？

備註需要 seine 直接連線到礦池，這也是預設方式。seine 的 daemon 模式對接你自己的節點挖礦並支付到節點的錢包，因此沒有備註。礦場代理不傳遞備註，所以請讓挖到交易所的礦機直接連線到礦池。seine 只在確認了你的備註的礦池上挖礦，所以你的工作總是支付到你選擇的帳戶。

<CTA target="pool" title="挖礦到你的交易所帳戶" text="用你的儲值地址與備註啟動 seine，bntpool.com 會從 10 BNT 起把收益直接支付到你的交易所帳戶。" />
