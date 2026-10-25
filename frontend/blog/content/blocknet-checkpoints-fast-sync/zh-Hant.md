---
title: 用 bntpool.com 檢查點快速同步 Blocknet 節點
description: Blocknet 節點透過 bntpool.com 的 checkpoints.dat 快速同步。了解檢查點如何鎖定鏈，再用幾個步驟下載、驗證並載入該檔案。
tags: [Blocknet 節點, 檢查點, 快速同步, bntpool]
faq:
  - q: 'Blocknet 節點從哪裡取得檢查點？'
    a: '從 bntpool.com 取得。首次啟動時，如果資料目錄中沒有 checkpoints.dat，Blocknet 背景程式會從 https://bntpool.com/checkpoints.dat 下載它並用於快速同步。你也可以在 Checkpoints 頁面自行下載該檔案，核對 SHA-256 後放入資料目錄，再啟動節點。'
  - q: 'bntpool.com 多久更新一次 checkpoints.dat？'
    a: '礦池的背景程式會隨著鏈的增長重寫該檔案，每第 100 個區塊一筆記錄。每筆記錄都至少比鏈頂低 100 個區塊，超過 Blocknet 的最大重組深度，所以檢查點只涵蓋已經確定的歷史。Checkpoints 頁面會顯示最新高度與檔案最近一次更新的時間。'
  - q: '檢查點會改變我的錢包餘額或交易紀錄嗎？'
    a: '不會。使用檢查點同步的節點仍會保存每個區塊，並把每個輸出與金鑰映像記錄到索引中，所以錢包掃描與餘額和完整同步後完全一樣。檢查點只是讓節點不必重新檢查那些已由檢查點雜湊擔保的歷史區塊的工作量證明與簽章。'
  - q: '可以不用檢查點同步 Blocknet 節點嗎？'
    a: '可以。用 --full-sync 參數啟動背景程式，或在 blocknet 設定中把 full_sync 設為 true，節點就會略過檢查點下載，從創世區塊開始為每個區塊重新驗證工作量證明與簽章。這比檢查點同步耗時更長，所以新節點預設使用檢查點。'
sourceHash: 7a3491743f7d
translation: reviewed
---

一個新的 Blocknet 節點必須追上自創世區塊以來的每個區塊，才能為錢包或單獨挖礦提供服務。檢查點能讓這個追趕過程變快。本指南介紹 bntpool.com 的檢查點檔案包含什麼、Blocknet 背景程式如何用它略過最慢的檢查同時始終鎖定在真實的鏈上，以及如何自己下載、驗證並載入這個檔案。

## 什麼是 Blocknet 檢查點？

<Answer>
Blocknet 檢查點是一份已知區塊雜湊的清單，每第 100 個區塊一筆，由 bntpool.com 以 checkpoints.dat 的形式提供。正在同步的節點會核對收到的區塊是否與這些雜湊一致，並在最後一個檢查點之前略過對工作量證明與簽章的重複驗證，因此首次同步會快得多。
</Answer>

該檔案是純文字，每行一組 `height:hash`。2026-10-25 時檔案的前幾行如下：

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

每個雜湊都是區塊的 ID，即區塊標頭的 SHA3-256 雜湊。礦池自己的背景程式根據它的鏈寫出這個檔案，並且只收錄深度至少為 100 個區塊的區塊。100 個區塊是 Blocknet 的硬最終性，也就是協定允許的最深重組，所以每個檢查點都位於已經確定的歷史中。

2026-10-25 02:04（美國東部夏令時間）時，該檔案包含 689 個檢查點，從高度 100 到高度 68,900，大小為 48,811 位元組。它大約每 100 個區塊增加一行，以五分鐘出塊約每八小時一次，目前高度請查看 [Checkpoints 頁面](/checkpoints)。

## Blocknet 節點如何使用檢查點？

載入了檢查點的節點會把最後一個檢查點的高度當作快速同步的目標。在鏈到達該高度之前，它依靠已發布的檢查點完成兩項開銷最大的檢查，其他檢查照常進行：

- **工作量證明依靠檢查點。** 每個 Blocknet 區塊的工作量證明都是一次佔用 2 GiB 記憶體的 Argon2id 雜湊。為每個歷史區塊重新計算一次代價很高，而檢查點雜湊已經為這段歷史作了擔保。
- **RingCT 簽章與範圍證明同樣依靠檢查點。** 背景程式自己的程式碼註解指出，輸出範圍證明的驗證是同步中最大的一項開銷，因為每個輸出都帶有一個範圍證明，包括每個區塊的 coinbase。
- **其他檢查全部照常進行。** 區塊連結關係、LWMA 難度規則、時間戳記規則、區塊大小與結構、金鑰映像的雙花檢查，以及每個輸入與其簽章之間的綁定檢查都照常執行。輸出與金鑰映像照常寫入節點索引，所以錢包掃描與餘額和完整同步後相同。

略過只適用於延伸節點目前鏈頂的區塊，並且只在最後一個檢查點之下。節點越過該高度後，會完整驗證每個新區塊，包括工作量證明。

### 檢查點如何鎖定可信的鏈

當節點向最後一個檢查點同步時，任何位於檢查點高度的區塊都必須與檔案中的雜湊完全一致。雜湊不同的區塊會以檢查點不符的錯誤被拒絕。由於檢查點每 100 個區塊一個，對等節點無法把快速同步中的節點引向另一段歷史：下一個檢查點高度就會揭露它。

檢查點還能加快每次重新啟動。背景程式啟動時如果磁碟上已有區塊，它會找到與已儲存的鏈相符的最高檢查點，只重新檢查其後的區塊，而不必從創世區塊開始走過整條鏈。

## 新節點會自動下載檢查點嗎？

<Answer>
會。Blocknet 背景程式啟動時，如果資料目錄中沒有 checkpoints.dat，它會從 https://bntpool.com/checkpoints.dat 下載該檔案，載入後快速同步到最後一個檢查點。下載是盡力而為的：如果無法完成，節點照樣啟動，並完整驗證整條鏈。
</Answer>

這個位址是寫在 Blocknet 核心程式中的預設值，所以開箱即用時，每個新節點都會用 bntpool.com 發布的檔案快速同步。背景程式只在檔案不存在時下載，請求時限為 15 秒，下載上限為 32 MiB。

## 如何一步步使用 bntpool.com 檢查點？

如果你想自己取得並核對檔案，或者想在長時間重新同步之前更新舊的副本，可以依以下步驟手動完成。

### 1. 找到資料目錄

檢查點檔案位於節點的資料目錄中，與鏈資料庫放在一起：

- **`blocknet` 管理程式：** macOS 與 Linux 上為 `~/.config/bnt/data/mainnet/`，Windows 上為 `C:\Users\<you>\.config\bnt\data\mainnet\`，除非你在其設定中設定了 `data_dir`。
- **直接執行核心程式：** 啟動目錄下的 `./blocknet-data-mainnet`，或你透過 `--data` 指定的目錄。

### 2. 下載 checkpoints.dat

開啟 [Checkpoints](/checkpoints) 並點選 **Download checkpoints.dat**，或在終端機中取得。下面的範例寫入核心程式的預設資料目錄：

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

下載到已有檔案的位置會覆蓋它，這就是更新舊副本的方法。

### 3. 核對 SHA-256

Checkpoints 頁面公布了目前檔案的 SHA-256。計算你的副本的雜湊並比較：

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

在 macOS 上請改用 `shasum -a 256`。每當礦池增加一個檢查點，雜湊就會變化，所以請在下載後立即與頁面比較。

### 4. 在區塊瀏覽器上抽查檢查點

你可以在公開的 [Blocknet 區塊瀏覽器](https://explorer.blocknetcrypto.com)上核對任意一行。開啟檔案中某個高度的區塊頁面，例如 `explorer.blocknetcrypto.com/block/68900`，把它的雜湊與 68,900 那一行比較。我們在 2026-10-25 對最後一個檢查點做了這項核對，雜湊一致。

### 5. 啟動節點

啟動或重新啟動背景程式。它會在啟動時讀取 `checkpoints.dat`，並快速同步到最後一個檢查點。如果你以互動式命令列（`--cli`）執行核心程式，`load-checkpoints` 指令會把檔案載入正在執行的節點，並顯示快速同步的目標高度：

```text
> load-checkpoints
```

檢查點在節點落後時發揮作用。一旦你的鏈越過最後一個檢查點的高度，節點就已經在完整驗證每個區塊，新的檔案不會帶來任何變化。

## 如何用腳本檢查檢查點檔案？

礦池在 `https://bntpool.com/api/checkpoints` 以 JSON 公布檔案資訊，無需 API 金鑰：

| 欄位 | 含義 |
|---|---|
| `available` | 礦池是否有可提供的檔案 |
| `url` | 下載位址 |
| `entries` | 檔案中的檢查點數量 |
| `latest_height`、`latest_hash` | 最高的檢查點及其區塊雜湊 |
| `bytes`、`sha256` | 檔案大小與 SHA-256 |
| `updated_at` | 檔案最近一次更新的時間（Unix 時間戳記） |

下載回應本身帶有 `X-Checkpoint-Height` 與 `X-Checkpoint-Sha256` 標頭，以及根據雜湊產生的 `ETag`，所以腳本可以送出 `If-None-Match`，在沒有變化時快速得到 `304 Not Modified`：

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

回應最多可快取 60 秒，對於約每八小時變化一次的檔案來說已經足夠。

<Note>
blocknetcrypto.com 表示，計畫中的 Faraday 升級將帶來更輕量的工作量證明，讓新節點更快追上鏈。在此之前，檢查點是讓節點快速同步到最新的方法。
</Note>

## 同步好的節點可以做什麼？

同步好的節點為 Blocknet 錢包與節點 API 提供支援，也是你用 `--mode daemon` 單獨挖礦時 seine 連線的對象。[Blocknet 礦池挖礦與單獨挖礦](/blog/zh-hant/pool-vs-solo-mining-blocknet/)介紹如何判斷單獨挖礦是否適合你的硬體。如果你想不執行節點就開始挖礦，seine 預設就在 bntpool.com 挖礦：[seine 安裝指南](https://seineminer.com/zh-hant/blog/how-to-mine-blocknet/)帶你取得第一批份額，[在 bntpool.com 挖礦](/blog/zh-hant/mine-blocknet-on-bntpool/)介紹礦池這一側。

<CTA target="pool" title="在提供 Blocknet 檢查點的礦池挖礦" text="幾分鐘內加入 bntpool.com，即時查看你的算力、份額與付款。" />
