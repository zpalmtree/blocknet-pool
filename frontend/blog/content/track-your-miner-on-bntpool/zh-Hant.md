---
title: 在 bntpool.com 上查看你的礦機
description: 在 bntpool.com 輸入地址或 @name，查看算力、份額、餘額與支付紀錄；再查看區塊、幸運值與上線狀態，或用公開 API 撰寫腳本。
tags: [bntpool, 挖礦統計, 礦池 API, Blocknet]
faq:
  - q: '為什麼 bntpool.com 上的算力和 seine 顯示的不一樣？'
    a: 'seine 計算的是你的電腦算出的每一個雜湊，而礦池只能依據它接受的份額難度來估算你的算力，並取最近一小時的平均值。份額到達的時間是隨機的，所以礦池的數字波動較大，需要一段時間才會穩定。要公平比較，請用 My Stats 上的 24 小時圖表對照 seine 的平均算力。'
  - q: '可以用 @name 查詢我在 bntpool.com 的統計嗎？'
    a: '可以。在 My Stats 的搜尋框輸入你的 Blocknet ID 短名稱，例如 @name 或 $name。頁面會透過 blocknet.id 解析它，顯示你的統計，並把解析出的完整地址填入搜尋框。在腳本中呼叫礦池 API 時，請使用這個完整地址。'
  - q: 'bntpool.com 的 API 需要 API 金鑰嗎？'
    a: '不需要。礦池統計、區塊、支付、幸運值、狀態以及個別礦工的端點都是透過 HTTPS 提供的公開 JSON，curl 指令或家庭儀表板不必註冊就能讀取。金額以最小單位表示，除以 100,000,000 即為 BNT，建議每 30 到 60 秒輪詢一次。'
  - q: '為什麼 My Stats 顯示的 Estimated Rewards 是零？'
    a: '礦池在你的份額位於支付區間內時找到區塊，Estimated Rewards（預估收益）才會出現。對新礦工來說，這可能需要挖幾輪。你被接受的份額與算力會最先顯示，接著是預估收益，它們成熟後轉入 Confirmed Rewards（已確認收益），並自動支付。'
sourceHash: '8f19279f18f8'
translation: reviewed
---

seine 在 bntpool.com 上開始挖礦後，礦池會把它看到的一切呈現給你：你的算力、每一個被接受的份額、你已賺取的金額，以及發送到你地址的每一筆支付。本指南介紹 My Stats 頁面、呈現整個礦池的公開頁面，以及可供腳本與家庭儀表板使用的公開 API。

如果你還沒開始挖礦，[seine 安裝指南](https://seineminer.com/zh-hant/blog/how-to-mine-blocknet/)可以讓你在幾分鐘內取得第一批份額。

## 如何在 bntpool.com 上查看礦機統計？

<Answer>
開啟 bntpool.com/stats 上的 My Stats，貼上你的 Blocknet 地址或 @name，然後按下 Lookup。頁面會顯示你的算力、被接受與被拒絕的份額、預估、已確認與已支付的餘額、算力圖表、每台礦機一列的清單，以及你最近的支付。只要分頁開著，它就會自動更新。
</Answer>

前往 [My Stats](/stats)，輸入你提供給 seine 的同一個地址或 [Blocknet ID 短名稱](https://seineminer.com/zh-hant/blog/blocknet-id-pay-by-name/)。像 `@name` 或 `$name` 這樣的名稱會透過 blocknet.id 解析，接著搜尋框會顯示它對應的完整地址。頁面會在該瀏覽器中記住你上一次的查詢，下次開啟時你的資料已經在那裡了。

## My Stats 頁面顯示什麼？

頁面分成數組卡片與表格，每一組回答一個關於你礦機的問題。

### Balance（餘額）

- **Estimated Rewards：** 礦池已找到、但仍在成熟中的區塊裡屬於你的部分。
- **Confirmed Rewards：** 已成熟、等待下一次支付的餘額。
- **Paid Balance：** 已經發送到你地址的全部金額。

當你有尚在途中的獎勵時，名為 **Estimated From Recent Blocks** 的表格會列出每個區塊的高度、你的預估收益、剩餘確認數與找到的時間。每個高度都連結到 [Blocknet 區塊瀏覽器](https://explorer.blocknetcrypto.com)上的對應區塊，你可以自行核對。[PPLNS 收益如何支付](/blog/zh-hant/how-pplns-payouts-work/)一文說明了每個區塊的獎勵如何分配，以及餘額何時支付。

### Mining（挖礦）與份額

**Mining** 卡片顯示你目前的 **Hashrate**（算力）、你自己的礦機為礦池找到的區塊數 **Blocks Found**，以及你開始挖礦的日期 **Mining Since**。下方是 **Shares Accepted**（已接受份額）、**Shares Rejected**（被拒份額）、**Reject Rate**（拒絕率）與 **Avg Difficulty**（平均難度）。運作良好的礦機會持續產生被接受的份額，拒絕率接近零。

**Hashrate History** 以 1h、24h、7d 或 30d 為範圍繪製你的算力。較長的範圍最適合查看一台礦機是否整夜都在運作。

### 每台礦機一列

如果你執行多台電腦，替每台取一個名字，礦池就會分別列出它們，顯示算力、被接受與被拒絕的份額，以及最後一個份額的時間。seine 第一次執行時會自動選一個名稱，也就是 `seine-` 加上四位數字，你也可以在命令列中自行指定：

```sh
./seine --pool-worker rig-01
```

執行舊版 seine 的礦機會顯示 **update available** 標記，並連結到 [seine 發布頁面](https://github.com/zpalmtree/seine/releases)，方便你找出忘了升級的電腦。

### 支付與最近的份額

**Recent Payouts** 列出每一筆支付到你地址的款項，包括金額、區塊瀏覽器上的交易連結，以及 queued（排隊中）、unconfirmed（未確認）或 confirmed（已確認）狀態。**Recent Shares** 顯示你最近 20 個份額的工作、礦機、難度與狀態，這是確認剛啟動的電腦已經連上礦池的最快方法。

## 為什麼礦池的算力和 seine 的不一樣？

seine 計算的是你的電腦算出的每一個雜湊。礦池只能看到你提交的份額，因此它依據最近一小時內你被接受份額的總難度來估算算力。每個份額都是隨機事件，而 bntpool.com 為每個連線設定的難度目標大約是每 5 分鐘 10 個份額，所以短時間區間會有一些波動。

兩個習慣可以讓數字對得上：

- 比較平均值：用 My Stats 上的 24h 圖表對照 seine 在同一時段的平均算力。
- 給新礦機一些時間。對剛啟動的礦機，礦池依它已經挖礦的分鐘數取平均，所以第一個讀數只根據少量份額。一小時後就會採用完整的一小時區間。

即使算力數字還沒穩定，Recent Shares 表格也能立即告訴你份額正在到達。

<span id="zero-hashrate"></span>

## 為什麼 seine 正在挖礦，My Stats 卻顯示零算力？

先檢查收款地址、礦池連線和已接受的份額。本機算力表示機器正在計算雜湊；礦池估算需要您所查詢地址的已接受份額。

1. **核對地址。** 查詢與 seine 使用的相同收款地址或已註冊的 `@name`。如果您更改了短名稱對應的地址，請重新啟動 seine 以重新解析名稱，再到 My Stats 查詢解析出的地址。
2. **核對礦池。** 確認 seine 使用 `stratum+tcp://bntpool.com:3333`。在其他礦池或單挖模式下執行的機器不會向這裡提交份額。[礦池設定指南](/blog/zh-hant/mine-blocknet-on-bntpool/)介紹了連線步驟。
3. **檢查 Recent Shares。** 等待份額被接受，再等待頁面更新。份額抵達的間隔並不固定；本機顯示算力並不保證礦池立即提供估算。如果已接受的份額持續抵達，請比較較長時間內的平均值。
4. **閱讀連線訊息。** 如果礦工尚未產生已接受的份額，請參考下表。透過 [Status](/status) 查看目前的故障。

| seine 中的訊息 | 下一步檢查 |
|---|---|
| `failed to connect to pool at` | 核對主機名稱和連接埠、網路連線，以及是否允許向連接埠 3333 建立對外 TCP 連線。 |
| `login rejected:` | 閱讀後續原因；檢查收款地址或短名稱，並使用目前版本的 seine。 |
| `stale job` | 該份額對應舊工作。觀察是否收到新工作以及份額是否被接受；若持續出現，請檢查連線和礦工版本。 |
| `submit interrupted during reconnect` | Seine 正在重新連線。連線恢復後，觀察登入是否被接受，以及新份額是否被接受。 |

**Estimated Rewards** 為零是另一個問題：礦池區塊獎勵入帳前，份額就可能已被接受。[PPLNS 指南](/blog/zh-hant/how-pplns-payouts-work/)介紹了這個流程。

## 哪些公開頁面呈現整個礦池？

這些頁面不需查詢，公開呈現礦池所做的一切。[儀表板](/)匯集了主要數字：連線礦工數、礦池與全網算力、已找到的區塊數與平均幸運值。

### Blocks（區塊）

[Blocks](/blocks) 由新到舊列出 bntpool.com 找到的每一個區塊：高度（連結到區塊瀏覽器）、獎勵、Effort（努力值）、本輪用時、狀態與找到的時間。可以依 Confirmed、Pending 或 Orphaned 篩選，查看每個區塊的狀態。

### Payouts（支付）

[Payouts](/payouts) 列出每一批支付的總金額、獲得支付的礦工數、交易連結與狀態。一批支付廣播後立即顯示為 unconfirmed，在第一個區塊確認後不久變為 confirmed，所以你可以從礦池這一端和區塊瀏覽器上追蹤自己的款項。

### Luck（幸運值）

[Luck](/luck) 顯示每一輪的 Effort。Effort 是本輪礦池接受的份額工作量除以該區塊的難度：100% 表示區塊正好用了預期的工作量，更低則表示礦池更早找到它。單輪數值上下波動很大，所以應留意儀表板上的 **Average Luck**（平均幸運值）。儀表板的 **Pool Luck History** 表格透過 **View All** 連結到這裡。

### Status（狀態）

[Status](/status) 是礦池自己的健康頁面。它顯示礦池、公開 HTTP、API、Stratum、資料庫與節點是否上線，目前工作範本的新鮮程度，鏈高度，最近 10 分鐘、6 小時、24 小時與 7 天的上線率，以及事件紀錄。上線率有兩種量測方式：礦池伺服器上的監控程式，以及在 Cloudflare 上執行的外部探測。

## 如何使用 bntpool.com 的 API？

<Answer>
bntpool.com 的 API 是透過 HTTPS 提供的公開 JSON，不需要金鑰。呼叫 https://bntpool.com/api/miner/ 加上你的地址，可取得你的算力、礦機、份額與支付；加上 /balance 取得餘額；用 /api/stats 取得礦池與全網算力。金額以最小單位表示：除以 100,000,000 即為 BNT。
</Answer>

上面每個頁面都讀取這些端點，所以你在網站上看到的任何內容都可以自己取得：

| 端點 | 回傳內容 |
|---|---|
| `/api/stats` | 連線礦工數、礦池算力、已找到區塊數、全網算力、目前高度 |
| `/api/stats/history?range=24h` | 礦池算力隨時間的變化（`1h`、`24h`、`7d`、`30d`） |
| `/api/stats/insights` | 目前輪次、平均努力值、最近的輪次 |
| `/api/info` | 礦池設定：Stratum 連接埠、支付方式與區間、最低支付額 |
| `/api/blocks?status=confirmed` | 已找到的區塊，可依狀態篩選 |
| `/api/payouts/recent` | 支付批次與交易雜湊 |
| `/api/luck` | 每一輪的努力值與用時 |
| `/api/status` | 服務健康狀況、上線率區間、事件 |
| `/api/miner/{address}` | 你的算力、總計、礦機、最新份額、支付 |
| `/api/miner/{address}/balance` | 預估、已確認與已支付餘額 |
| `/api/miner/{address}/hashrate?range=24h` | 你的算力歷史 |

清單端點接受 `limit`（預設 25，最多 200）與 `offset` 參數，回傳 `items` 與 `total`。礦工端點需要完整地址，所以在 My Stats 上用 @name 查詢後，請從搜尋框複製完整地址。

在終端機中讀取你的礦機資料：

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

搭配 `jq` 可以取出單一數值，例如你的算力與被接受的份額，或以 BNT 表示的已支付總額：

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### 在家庭儀表板上顯示你的礦機

同一個端點可以當作家庭儀表板中的感測器。在 Home Assistant 中，RESTful 整合只需在 `configuration.yaml` 寫幾行就能讀取它：

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

每 30 到 60 秒輪詢一次就夠了：算力數字是一小時的平均值，而 My Stats 本身每 30 秒更新一次你的礦工詳細資料。公開 API 依用戶端限制請求頻率。如果腳本收到 HTTP 429 回應，應該放慢請求，等待 `Retry-After` 標頭指定的時間後再重試。

## seine 自己的儀表板呢？

礦池顯示的是它記入你名下的資料。若要即時查看電腦本身，seine 的終端機儀表板會即時顯示算力與份額；seine 也可以開啟本機控制 API，提供 REST、伺服器推送事件串流以及位於 `/metrics` 的 Prometheus 指標，預設繫結到 `127.0.0.1:9977`。[控制 API 指南](https://github.com/zpalmtree/seine/blob/master/docs/API.md)介紹了相關參數與端點。許多礦工兩者都用：用 seine 的指標查看每台電腦，用礦池 API 查看餘額與支付。

第一次使用 seine？[下載它](https://seineminer.com/zh-hant/#download)，使用預設礦池，在你的第一個份額被接受後幾分鐘內，你的地址就會出現在 My Stats 上。

<CTA target="pool" title="開始挖礦並即時查看" text="幾分鐘內加入 bntpool.com，然後在「我的統計」裡查看你的算力、份額與餘額。" />
