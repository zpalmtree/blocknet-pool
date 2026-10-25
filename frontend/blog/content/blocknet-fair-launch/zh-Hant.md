---
title: Blocknet 的公平啟動：沒有預挖，每一枚幣都是挖出來的
description: Blocknet 啟動時沒有預挖、開發基金或國庫。了解空的創世區塊與單一輸出 coinbase 規則，親自查證，並開始挖 BNT。
tags: [Blocknet, 公平啟動, 創世區塊, 挖礦, bntpool]
faq:
  - q: 'Blocknet 有預挖嗎？'
    a: '沒有。Blocknet 的創世區塊建立於 2026 年 2 月 15 日，裡面沒有任何交易，因此在挖礦開始之前不存在任何幣。第一批幣來自區塊 1 的區塊獎勵，此後的每一枚 BNT 都支付給了挖出對應區塊的人。'
  - q: 'Blocknet 有開發基金或國庫嗎？'
    a: '沒有。共識規則規定每個區塊只能有一筆 coinbase 交易，而這筆交易必須恰好有一個輸出，金額恰好等於區塊獎勵。沒有可以留給開發者、創辦人或國庫的第二個輸出，試圖加入這種輸出的區塊會被網路上的每個節點拒絕。'
  - q: 'Blocknet 的創世區塊是什麼時候建立的？'
    a: '創世區塊的時間戳記是 2026 年 2 月 15 日 20:58:38（美國東部標準時間 EST），即 UTC 時間 2 月 16 日 01:58:38。它帶有訊息 "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything"，它的前一區塊雜湊就是這則訊息的 SHA3-256 雜湊。'
  - q: '如何親自查證 Blocknet 的公平啟動？'
    a: '在 Blocknet 區塊瀏覽器上開啟區塊 0，你會看到交易數為零、Merkle 根全為零。接著閱讀開源的節點程式碼：創世區塊以空交易清單寫死在程式碼中，coinbase 檢查只接受一個金額等於區塊獎勵的輸出。'
  - q: '如何開始挖 Blocknet？'
    a: '下載適用於 Windows、macOS 或 Linux 的 seine 挖礦軟體，執行它，並在提示時貼上你的 Blocknet 地址或 Blocknet ID 短名稱。按 Enter 接受預設礦池 bntpool.com，seine 會立即開始挖礦。之後你可以在 bntpool.com 的統計頁面查詢自己的地址。'
sourceHash: 814839500c3d
translation: reviewed
---

Blocknet 啟動時沒有替任何人預留任何幣。本頁介紹創世區塊裡有什麼、哪些共識規則讓預挖、開發基金或國庫無法存在、如何在區塊瀏覽器與原始碼中查證這兩點，以及如何在 bntpool.com 上親自挖 BNT。

## Blocknet 有預挖嗎？

<Answer>
沒有。Blocknet 的創世區塊建立於 2026 年 2 月 15 日，裡面沒有任何交易，因此在挖礦開始之前不存在任何幣。從區塊 1 起，每個區塊都以單一 coinbase 輸出支付獎勵，金額恰好等於排定數額，所以流通中的每一枚 BNT 都是有人執行工作量證明挖出來的。
</Answer>

無論是在創世區塊還是之後的任何區塊中，都沒有分配給創辦人、開發者、投資人或基金會的幣。幣的供給只來自區塊獎勵，執行這一點的規則屬於共識的一部分，因此網路上的每個節點都會在每個區塊上檢查它們。

## Blocknet 的創世區塊裡有什麼？

創世區塊寫死在節點程式中，因此每個節點都從同一個區塊 0 開始。它的欄位是固定的：

| 欄位 | 值 |
|---|---|
| 高度 | 0 |
| 時間戳記 | 2026 年 2 月 15 日 20:58:38 EST（UTC 時間 2 月 16 日 01:58:38） |
| 訊息 | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| 前一區塊雜湊 | 這則訊息的 SHA3-256：`a94a7f2b…d909df` |
| 難度 | 4，即網路最低難度 |
| Nonce | 0 |
| 交易 | 無 |
| Merkle 根 | 全為零 |

這則訊息是 Wired 一篇日期為 2026 年 2 月 15 日的報導標題，程式碼中註明了這篇文章是出處。由於前一區塊雜湊由這則訊息計算而來，這個標題就固定在區塊 0 以及其後整條鏈的身分之中。

節點程式中創世函式上方的註解寫得很直白："Genesis has no transactions - no coinbase, no burned coins. First real block at height 1 will have the first coinbase."（創世區塊沒有交易：沒有 coinbase，也沒有銷毀的幣。高度 1 的第一個真正區塊才會有第一筆 coinbase。）驗證規則也印證了這段註解。節點在檢查區塊 0 時，只要它不是恰好零筆交易、Merkle 根不是全零，就會拒絕它，所以即使有人修改了節點，也無法把幣塞進創世區塊而不被其他所有節點拒絕。

## 共識如何杜絕開發基金或國庫？

<Answer>
每個 Blocknet 區塊都必須以恰好一筆 coinbase 交易開頭，這筆 coinbase 必須沒有輸入、手續費為零，並且恰好有一個輸出，金額等於該高度的區塊獎勵。沒有地方可以放第二筆付款，所以開發基金、創辦人獎勵或國庫輸出不可能存在於鏈上。
</Answer>

節點程式中的幾項檢查共同保證了這一點：

- **每個區塊一筆 coinbase。** 第一筆交易必須是 coinbase，區塊中的其他交易都不能是 coinbase。
- **一個輸出，沒有輸入。** 有零個或兩個輸出、或者帶有輸入的 coinbase 都是無效的。
- **精確的獎勵。** Blocknet 用 Pedersen 承諾隱藏金額，但 coinbase 是特例。它的盲化因子由公開資料（交易公鑰與區塊高度）推導而來，因此每個節點都能為排定的獎勵重建承諾並加以比對。如果輸出承諾的是其他任何金額，區塊就會被拒絕。
- **空的備註。** coinbase 的備註必須為空，因此區塊獎勵也不能攜帶額外資料。

一般交易無法創造幣。每筆交易都必須平衡：輸入等於輸出加手續費，這一點直接在承諾上檢查；每個輸出還附帶 Bulletproofs 範圍證明，證明其金額在有效範圍內、不可能為負。金額正是這樣保持私密，而發行量仍然嚴格遵循排程。

## 如何親自查證 Blocknet 的公平啟動？

你不必相信任何人的說法。鏈與程式碼都是公開的。

**在區塊瀏覽器上。** 開啟 [Blocknet 區塊瀏覽器上的區塊 0](https://explorer.blocknetcrypto.com/block/0)。頁面顯示交易數為 0、Merkle 根是 64 個零、nonce 為 0，前一區塊雜湊以 `a94a7f2b` 開頭。區塊獎勵一欄顯示的是排程中該高度對應的數額，但區塊裡沒有交易，也就沒有接收它的輸出。接著開啟[區塊 1](https://explorer.blocknetcrypto.com/block/1)：它唯一的交易是一筆沒有輸入、只有一個輸出的 coinbase，這就是鏈上的第一批幣。

你可以確認前一區塊雜湊確實是創世訊息的雜湊。在 OpenSSL 1.1.1 或更新版本下執行：

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

輸出的結尾是 `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`，與區塊瀏覽器為區塊 0 顯示的值相同。

**在程式碼中。** 節點程式在 [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core) 開源。在 [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go) 中尋找以下函式：

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` 以空交易清單建立區塊 0，`validateGenesisBlock` 拒絕任何帶有交易的區塊 0，`validateCoinbaseConsensus` 則強制要求唯一一個金額等於區塊獎勵的輸出。在 `transaction.go` 中，`validateCoinbase` 與 `verifyCommitmentBalance` 分別負責 coinbase 的結構檢查與一般交易的平衡檢查。

## 公平啟動對礦工為什麼重要？

對礦工來說，公平啟動代表區塊獎勵就是幣的全部來源。

- **所有新幣都歸礦工。** 沒有預挖，也沒有國庫輸出，100% 的發行量都支付給找到區塊的人。區塊 1 在它唯一的 coinbase 輸出中支付了 723.25093035 BNT，此後的每個區塊都以同樣的方式支付全部獎勵。
- **所有人遵循同樣的規則。** 從區塊 1 起，取得新 BNT 的唯一方式就是執行工作量證明。[獎勵排程](/blog/zh-hant/blocknet-block-reward-schedule/)寫在程式碼中，對第一位礦工與今天才開始的人同樣適用。
- **工作量證明適合一般電腦。** Blocknet 使用每次雜湊需要 2 GiB 記憶體的 Argon2id，其設計目的是抵抗 ASIC。這讓挖礦對人們已有的桌上型電腦、筆電與伺服器保持開放。[seineminer.com 上的 Argon2id 詳解](https://seineminer.com/zh-hant/blog/argon2id-proof-of-work-explained/)說明了原因。
- **發行量易於稽核。** 每一枚幣都可以追溯到一個經過每個節點檢查的 coinbase 輸出，因此任何高度的累計發行總量都可以直接由獎勵排程得出。

如果你剛開始認識這枚幣，[Blocknet 是什麼？](https://seineminer.com/zh-hant/blog/what-is-blocknet/)介紹了隱私、以名稱付款與錢包。

## 如何親自挖 Blocknet？

參與公平啟動就是去挖礦。最快的方式是在 bntpool.com 上使用 seine 挖礦軟體：

1. **準備地址。** 安裝官方 Blocknet [命令列錢包](https://github.com/blocknetprivacy/blocknet/releases/latest)或[桌面錢包](https://github.com/blocknetprivacy/blocknet-gui/releases/latest)並複製你的地址，或者使用 `@name` 這樣的 Blocknet ID 短名稱。
2. **下載 seine。** 從 [seineminer.com](https://seineminer.com/zh-hant/#download) 取得適用於 Windows、macOS 或 Linux 的壓縮檔並解壓縮。無需安裝。
3. **執行並接受預設設定。** 啟動 seine，貼上你的地址或 `@name`，按 Enter 接受預設礦池 `stratum+tcp://bntpool.com:3333`。seine 會立即開始計算雜湊，並在執行中調整執行緒數與運算核心程式；在 Windows 與 Linux 上，它會同時使用 CPU 與 NVIDIA 顯示卡。
4. **查看你的礦機。** 在[統計頁面](/stats)查詢你的地址，查看算力、份額與餘額，並在[區塊頁面](/blocks)關注礦池找到的區塊。

[在 bntpool.com 挖 Blocknet](/blog/zh-hant/mine-blocknet-on-bntpool/)一文介紹了如何用地址或名稱登入並查看礦機，[入門頁面](/start)列出了礦池的支付設定，[如何挖 Blocknet 指南](https://seineminer.com/zh-hant/blog/how-to-mine-blocknet/)則一步步介紹了在各個系統上安裝 seine 的方法。

<CTA target="seine" title="挖到屬於你的那份" text="每一枚 BNT 都是挖出來的。下載 seine，幾分鐘內即可在 bntpool.com 上開始挖礦。" />
