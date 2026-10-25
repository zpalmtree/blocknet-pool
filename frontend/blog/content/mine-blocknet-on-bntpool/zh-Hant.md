---
title: 在 bntpool.com 挖 Blocknet
description: 加入 seine 的預設 Blocknet 礦池 bntpool.com：用你的地址或 @name 連線到 stratum+tcp://bntpool.com:3333，為礦機命名，並查看你的挖礦統計。
tags: [挖礦指南, bntpool, Blocknet ID, seine]
faq:
  - q: 'bntpool.com 的 Stratum 地址是什麼？'
    a: '地址是 stratum+tcp://bntpool.com:3333，也就是 seine 首次執行時建議的礦池。在礦池提示處按 Enter 即可接受，也可以用 --pool-url 參數傳入。你的 Blocknet 地址或 Blocknet ID 名稱就是登入名稱，礦池會把收益支付到該地址。'
  - q: '可以用 Blocknet ID 名稱在 bntpool.com 挖礦嗎？'
    a: '可以。在 seine 或礦池要求輸入地址的地方填入 @name 或 $name 即可。礦池會在 blocknet.id 核對簽章對應，然後把份額與收益記入該名稱對應的完整地址。需要 seine v0.2.16 或更新版本；在命令列中請為名稱加上引號，避免被 shell 改寫。'
  - q: '在 bntpool.com 挖礦需要註冊帳號嗎？'
    a: '不需要。沒有註冊、密碼或電子郵件。你的 Blocknet 地址，或指向它的 Blocknet ID 名稱，既是登入名稱也是收款地址。用它啟動 seine，然後在 My Stats 頁面查詢，就能看到你的算力、份額、礦機、餘額與付款紀錄。'
  - q: '可以用多台電腦挖到同一個地址嗎？'
    a: '可以。在每台電腦上用相同的地址或名稱執行 seine，並為每台設定不同的礦機名稱。礦池依地址統計份額、餘額與付款，所以所有礦機都計入同一個餘額、合併為同一筆付款，而 My Stats 頁面會把每台礦機分別列成一列。'
sourceHash: f6080e03ffd0
translation: reviewed
---

本指南說明如何用 seine 加入 bntpool.com 礦池：礦池地址、用 Blocknet 地址或 Blocknet ID `@name` 登入、為每台礦機命名，以及在統計頁面上確認你的份額已經送達。如果還沒有安裝 seine，請先依照 [seine 安裝指南](https://seineminer.com/zh-hant/blog/how-to-mine-blocknet/)操作，再回到這裡。

## bntpool.com 的礦池地址是什麼？

<Answer>
bntpool.com 的 Stratum 地址是 `stratum+tcp://bntpool.com:3333`。seine 首次執行時會把它當作預設礦池，所以在礦池提示處按 Enter 就能連線。你用自己的 Blocknet 地址或 Blocknet ID `@name` 登入。不必建立帳號，收益會發送到該地址。
</Answer>

以下是礦池的設定，來自其公開的 `/api/info` 端點（2026 年 10 月 25 日）：

| 設定 | 值 |
|---|---|
| Stratum 地址 | `stratum+tcp://bntpool.com:3333` |
| 登入 | Blocknet 地址、`@name` 或 `$name` |
| 支付方式 | PPLNS，6 小時區間 |
| 付款前所需確認數 | 60 個區塊 |
| 最低付款額 | 0.1 BNT |

你隨時可以在礦池的 [Get Started](/start)（開始挖礦）頁面查看這些值。

## 為什麼 bntpool.com 是 seine 的預設礦池？

第一次執行 seine 時，它會詢問礦池，並把 `stratum+tcp://bntpool.com:3333` 填為預設值。礦池與挖礦軟體在幾個實際面向上相互配合：

- **同一套協定。** bntpool.com 執行的是 seine 所使用的 Stratum 協定（第 2 版），因此除了你的地址之外，seine 連線時不需要任何設定。
- **兩端都支援名稱。** seine 與礦池都接受 Blocknet ID 名稱，並依據 blocknet.id 上同一份簽章紀錄進行核對。
- **版本檢查。** 每台礦機提交份額時會回報自己的 seine 版本。如果某台礦機執行的是較舊的 seine，統計頁面會在它旁邊標上「update available」（有可用更新）標記，並連結到 seine 的版本發布頁面。
- **seine 設定指南。** 礦池的 Get Started 頁面會逐步說明如何下載與執行 seine，並提供一行可以直接複製的指令。

## 如何把 seine 連線到 bntpool.com？

第一次啟動 seine 時，它會問兩個問題：

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

貼上你的地址或輸入你的名稱，然後按 Enter 接受 bntpool.com。seine 會把這兩個答案儲存到 `seine-data/seine.config.json`，之後每次啟動都會直接開始挖礦。它也會自動選一個礦機名稱並儲存在同一個檔案裡（下文詳述）。

在沒有螢幕的礦機上、以服務方式執行或在腳本中，沒有人能回答這些提示。請在首次啟動時用參數傳入這兩個值，這也正是 Get Started 頁面提供的指令：

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

之後要更換礦池或地址，可以編輯或刪除 `seine-data/seine.config.json`，或在命令列上用 `--address` 與 `--pool-url` 覆寫已儲存的值。

## 可以挖到 Blocknet ID @name 嗎？

<Answer>
可以。凡是 bntpool.com 接受地址的地方，都可以使用 Blocknet ID 短名稱，寫作 `@name` 或 `$name`。礦池會核對來自 blocknet.id 的簽章對應，驗證該名稱指向的地址，並在整個連線期間把份額、餘額與付款都記入這個完整地址。
</Answer>

名稱不分大小寫，所以 `@Alice` 與 `@alice` 是同一個登入名稱。seine 從 v0.2.16 起支援名稱，任何目前的版本都已具備。開始挖礦前，seine 會顯示名稱解析出的地址，方便你確認它與自己的錢包一致。在命令列中，請用單引號把名稱括起來，否則 shell 會把 `$name` 當作變數展開：

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

如果之後你把名稱指向另一個錢包，請重新啟動 seine。seine 每次啟動時都會重新解析已儲存的名稱，並在整個執行期間使用該地址；礦池對每個連線也是如此。已經賺到的餘額會留在賺得它的那個地址，並支付到那裡，所以修改名稱不會移動你的幣。

你可以在 [blocknet.id](https://blocknet.id) 註冊名稱。[Blocknet ID 指南](https://seineminer.com/zh-hant/blog/blocknet-id-pay-by-name/)說明了以名稱付款的運作方式。

## 如何為礦機命名？

每個連線在登入時都會附帶一個礦機名稱與你的地址。首次執行時，seine 會替你選一個名稱，也就是 `seine-` 加上四位數字，並儲存在設定檔中，所以同一台機器重新啟動後名稱不變。想用一個容易辨認的名稱，可以在命令列上設定：

```sh
./seine --pool-worker rig-01
```

礦池最多保留名稱的 64 個字元。固定的名稱在重新啟動後也有好處：礦池會依你的地址與礦機名稱，把每台礦機的份額難度記住一小時，所以重新連線的礦機會從它已經穩定下來的難度繼續。

## 可以用多台礦機挖到同一個地址嗎？

可以。在每台機器上用相同的地址或 `@name`、不同的礦機名稱啟動 seine。礦池依地址統計份額、餘額與付款，所以你所有的礦機都會計入同一個餘額、合併為同一筆付款。在統計頁面上，每台礦機各占一列，一眼就能看出每台機器都在挖礦。

## 如何確認礦機正常運作？

seine 的儀表板會在本機顯示你的算力與已接受的份額。要查看礦池這一端的狀況，請開啟 [My Stats](/stats)（我的統計），輸入你的地址、`@name` 或 `$name`，然後按下 **Lookup**（查詢）。如果輸入的是名稱，頁面會在搜尋框旁顯示它解析出的地址。頁面也會在該瀏覽器中記住你上一次的查詢，[查看礦機指南](/blog/zh-hant/track-your-miner-on-bntpool/)介紹了頁面上的每一張卡片。

請留意三點：

- **算力與 Shares Accepted（已接受份額）。** 算力卡片顯示礦池看到的你的算力，隨著礦機提交工作，已接受份額的數量會持續增加。
- **每台礦機一列。** 每列顯示礦機名稱、算力、已接受與被拒絕的份額，以及最後一次提交份額的時間。如果最後一次份額就在剛才，代表這台礦機已連線並在運作。
- **Recent Shares（最近份額）。** 最新的份額會連同提交它的礦機、難度與狀態一起列出。

礦池會為每個連線分別設定份額難度，並在挖礦過程中持續調整，所以即使是小型 CPU 也能穩定提交份額，你的第一批被接受的份額會在連線後幾分鐘內出現。至於你的硬體能有多少算力，請參閱 [Blocknet 挖礦硬體指南](https://seineminer.com/zh-hant/blog/blocknet-mining-hardware-hashrate/)。

接下來是收益。當礦池找到區塊、而你的份額位於六小時區間內時，Estimated Rewards（預估收益）就會出現。該區塊獲得 60 個確認後，你的那一份會轉入 Confirmed Rewards（已確認收益），當你累積到至少 0.1 BNT 時，礦池會自動發送。[PPLNS 收益如何支付](/blog/zh-hant/how-pplns-payouts-work/)一文說明了每個區塊的獎勵如何分配。全礦池的歷史紀錄見 [Blocks](/blocks)（區塊）與 [Payouts](/payouts)（付款）頁面。

## Get Started 頁面包含哪些內容？

礦池的 [Get Started](/start) 頁面把所有內容放在同一頁：

- **礦池資訊。** Stratum 地址（點一下即可複製）、支付方式、最低付款額，以及付款前所需的區塊確認數，皆即時讀取自礦池。
- **三步驟快速開始。** 從 [seineminer.com](https://seineminer.com/zh-hant/#download) 下載 seine，執行一次並輸入你的地址或名稱以及礦池 URL，然後開始挖礦，在 My Stats 上關注你的算力與餘額。
- **可複製的指令。** 已填好礦池 URL 的完整 `./seine` 指令。換上你的地址，或加上引號的 `'@yourname'` 即可。
- **設定說明。** seine 把設定儲存在哪裡、如何重設，以及用來覆寫設定的參數。
- **收益如何計算。** 對 PPLNS 的簡短說明：區塊被找到之前你提交的份額分得該區塊的獎勵，之後增加的工作則計入後續的區塊。頁面也說明，未確認區塊帶來的收益會在 My Stats 上分開顯示，直到這些區塊獲得確認。
- **截圖。** 展示 seine 的首次執行設定畫面與挖礦儀表板，讓你在開始前就知道會看到什麼。

<CTA target="pool" title="在 bntpool.com 開始挖礦" text="用你的地址或 @name 將 seine 連接到 bntpool.com，然後在「我的統計」裡查看你的份額。" />
