---
title: 在 bntpool.com 上查看你的矿机
description: 在 bntpool.com 输入地址或 @name，查看算力、份额、余额和支付记录；再查看区块、幸运值和在线状态，或用公开 API 编写脚本。
tags: [bntpool, 挖矿统计, 矿池 API, Blocknet]
faq:
  - q: '为什么 bntpool.com 上的算力和 seine 显示的不一样？'
    a: 'seine 统计的是你的机器算出的每一个哈希，而矿池只能根据它接受的份额难度来估算你的算力，并取最近一小时的平均值。份额到达的时间是随机的，所以矿池的数字波动更大，需要一段时间才能稳定。要公平比较，请用 My Stats 上的 24 小时图表对比 seine 的平均算力。'
  - q: '可以用 @name 查询我在 bntpool.com 的统计吗？'
    a: '可以。在 My Stats 的搜索框中输入你的 Blocknet ID 短名称，例如 @name 或 $name。页面会通过 blocknet.id 解析它，显示你的统计，并把解析出的完整地址填入搜索框。在脚本中调用矿池 API 时，请使用这个完整地址。'
  - q: 'bntpool.com 的 API 需要 API 密钥吗？'
    a: '不需要。矿池统计、区块、支付、幸运值、状态以及单个矿工的接口都是通过 HTTPS 提供的公开 JSON，curl 命令或家庭仪表盘无需注册即可读取。金额以最小单位表示，除以 100,000,000 即为 BNT，建议每 30 到 60 秒轮询一次。'
  - q: '为什么 My Stats 显示的 Estimated Rewards 是零？'
    a: '矿池在你的份额处于支付窗口内时找到区块，Estimated Rewards（预估收益）才会出现。对新矿工来说，这可能需要挖几轮。你被接受的份额和算力会最先显示，随后是预估收益，它们成熟后转入 Confirmed Rewards（已确认收益），并自动支付。'
sourceHash: 'f785e00cbe03'
translation: reviewed
---

seine 在 bntpool.com 上开始挖矿后，矿池会把它看到的一切展示给你：你的算力、每一个被接受的份额、你已赚取的金额，以及发送到你地址的每一笔支付。本指南介绍 My Stats 页面、展示整个矿池的公开页面，以及可供脚本和家庭仪表盘使用的公开 API。

如果你还没有开始挖矿，[seine 安装指南](https://seineminer.com/zh-hans/blog/how-to-mine-blocknet/)可以让你在几分钟内获得第一批份额。

## 如何在 bntpool.com 上查看矿机统计？

<Answer>
打开 bntpool.com/stats 上的 My Stats，粘贴你的 Blocknet 地址或 @name，然后点击 Lookup。页面会显示你的算力、被接受和被拒绝的份额、预估、已确认和已支付的余额、算力图表、每台矿机一行的列表，以及你最近的支付。只要标签页开着，它就会自动刷新。
</Answer>

前往 [My Stats](/stats)，输入你提供给 seine 的同一个地址或 [Blocknet ID 短名称](https://seineminer.com/zh-hans/blog/blocknet-id-pay-by-name/)。像 `@name` 或 `$name` 这样的名称会通过 blocknet.id 解析，随后搜索框会显示它对应的完整地址。页面会在该浏览器中记住你上一次的查询，下次打开时你的数据已经在那里了。

## My Stats 页面显示什么？

页面分为若干组卡片和表格，每一组回答一个关于你矿机的问题。

### Balance（余额）

- **Estimated Rewards：** 矿池已找到、但仍在成熟中的区块里属于你的份额。
- **Confirmed Rewards：** 已成熟、等待下一次支付的余额。
- **Paid Balance：** 已经发送到你地址的全部金额。

当你有尚在途中的奖励时，名为 **Estimated From Recent Blocks** 的表格会列出每个区块的高度、你的预估收益、剩余确认数和找到的时间。每个高度都链接到 [Blocknet 区块浏览器](https://explorer.blocknetcrypto.com)上的对应区块，你可以自己核对。[PPLNS 收益如何支付](/blog/zh-hans/how-pplns-payouts-work/)一文解释了每个区块的奖励如何分配，以及余额何时支付。

### Mining（挖矿）和份额

**Mining** 卡片显示你当前的 **Hashrate**（算力）、你自己的矿机为矿池找到的区块数 **Blocks Found**，以及你开始挖矿的日期 **Mining Since**。下面是 **Shares Accepted**（已接受份额）、**Shares Rejected**（被拒份额）、**Reject Rate**（拒绝率）和 **Avg Difficulty**（平均难度）。运行良好的矿机会持续产生被接受的份额，拒绝率接近零。

**Hashrate History** 以 1h、24h、7d 或 30d 为范围绘制你的算力。较长的范围最适合查看一台矿机是否整夜都在运行。

### 每台矿机一行

如果你运行多台机器，给每台机器起一个名字，矿池就会分别列出它们，显示算力、被接受和被拒绝的份额，以及最后一个份额的时间。seine 首次运行时会自动选一个名称，即 `seine-` 加四位数字，你也可以在命令行中自己指定：

```sh
./seine --pool-worker rig-01
```

运行旧版 seine 的矿机会显示 **update available** 标记，并链接到 [seine 发布页面](https://github.com/zpalmtree/seine/releases)，方便你发现忘记升级的机器。

### 支付和最近的份额

**Recent Payouts** 列出每一笔支付到你地址的款项，包括金额、区块浏览器上的交易链接，以及 queued（排队中）、unconfirmed（未确认）或 confirmed（已确认）状态。**Recent Shares** 显示你最近 20 个份额的任务、矿机、难度和状态，这是确认刚启动的机器已经连上矿池的最快方法。

## 为什么矿池的算力和 seine 的不一样？

seine 统计的是你的机器算出的每一个哈希。矿池只能看到你提交的份额，因此它根据最近一小时内你被接受份额的总难度来估算算力。每个份额都是随机事件，而 bntpool.com 为每个连接设置的难度目标大约是每 5 分钟 10 个份额，所以短时间窗口会有一些波动。

两个习惯可以让数字对得上：

- 比较平均值：用 My Stats 上的 24h 图表对比 seine 在同一时段的平均算力。
- 给新矿机一些时间。对刚启动的矿机，矿池按它已经挖矿的分钟数取平均，所以第一个读数只基于少量份额。一小时后就会采用完整的一小时窗口。

即使算力数字还没稳定，Recent Shares 表格也能立即告诉你份额正在到达。

<span id="zero-hashrate"></span>

## 为什么 seine 正在挖矿，My Stats 却显示零算力？

先检查收款地址、矿池连接和已接受的份额。本地算力表示机器正在计算哈希；矿池估算需要您所查询地址的已接受份额。

1. **核对地址。** 查询与 seine 使用的相同收款地址或已注册的 `@name`。如果您更改了短名称对应的地址，请重启 seine 以重新解析名称，再到 My Stats 查询解析出的地址。
2. **核对矿池。** 确认 seine 使用 `stratum+tcp://bntpool.com:3333`。在其他矿池或单挖模式下运行的机器不会向这里提交份额。[矿池设置指南](/blog/zh-hans/mine-blocknet-on-bntpool/)介绍了连接步骤。
3. **检查 Recent Shares。** 等待份额被接受，再等待页面刷新。份额到达的间隔并不固定；本地显示算力并不保证矿池立即给出估算。如果已接受的份额持续到达，请比较较长时间内的平均值。
4. **阅读连接消息。** 如果矿工尚未产生已接受的份额，请参考下表。通过 [Status](/status) 查看当前故障。

| seine 中的消息 | 下一步检查 |
|---|---|
| `failed to connect to pool at` | 核对主机名和端口、网络连接，以及是否允许向端口 3333 发起出站 TCP 连接。 |
| `login rejected:` | 阅读后续原因；检查收款地址或短名称，并使用当前版本的 seine。 |
| `stale job` | 该份额对应旧任务。观察是否收到新任务以及份额是否被接受；如果持续出现，请检查连接和矿工版本。 |
| `submit interrupted during reconnect` | Seine 正在重新连接。连接恢复后，观察登录是否被接受，以及新份额是否被接受。 |

**Estimated Rewards** 为零是另一个问题：矿池区块奖励入账前，份额就可能已被接受。[PPLNS 指南](/blog/zh-hans/how-pplns-payouts-work/)介绍了这一流程。

## 哪些公开页面展示整个矿池？

这些页面无需查询，公开展示矿池所做的一切。[仪表盘](/)汇集了主要数字：在线矿工数、矿池和全网算力、已找到的区块数和平均幸运值。

### Blocks（区块）

[Blocks](/blocks) 按从新到旧列出 bntpool.com 找到的每一个区块：高度（链接到区块浏览器）、奖励、Effort（努力值）、本轮用时、状态和找到的时间。可以按 Confirmed、Pending 或 Orphaned 筛选，查看每个区块的状态。

### Payouts（支付）

[Payouts](/payouts) 列出每一批支付的总金额、获得支付的矿工数、交易链接和状态。一批支付广播后立即显示为 unconfirmed，在第一个区块确认后不久变为 confirmed，所以你可以从矿池这一侧和区块浏览器上跟踪自己的付款。

### Luck（幸运值）

[Luck](/luck) 显示每一轮的 Effort。Effort 是本轮矿池接受的份额工作量除以该区块的难度：100% 表示区块正好用了预期的工作量，更低则表示矿池更早找到了它。单轮数值上下波动很大，所以应关注仪表盘上的 **Average Luck**（平均幸运值）。仪表盘的 **Pool Luck History** 表格通过 **View All** 链接到这里。

### Status（状态）

[Status](/status) 是矿池自己的健康页面。它显示矿池、公共 HTTP、API、Stratum、数据库和节点是否在线，当前任务模板的新鲜程度，链高度，最近 10 分钟、6 小时、24 小时和 7 天的在线率，以及事件历史。在线率有两种测量方式：矿池服务器上的监控程序，以及运行在 Cloudflare 上的外部探测。

## 如何使用 bntpool.com 的 API？

<Answer>
bntpool.com 的 API 是通过 HTTPS 提供的公开 JSON，无需密钥。调用 https://bntpool.com/api/miner/ 加上你的地址，可获得你的算力、矿机、份额和支付；加上 /balance 获取余额；用 /api/stats 获取矿池和全网算力。金额以最小单位表示：除以 100,000,000 即为 BNT。
</Answer>

上面每个页面都读取这些接口，所以你在网站上看到的任何内容都可以自己获取：

| 接口 | 返回内容 |
|---|---|
| `/api/stats` | 在线矿工数、矿池算力、已找到区块数、全网算力、当前高度 |
| `/api/stats/history?range=24h` | 矿池算力随时间的变化（`1h`、`24h`、`7d`、`30d`） |
| `/api/stats/insights` | 当前轮次、平均努力值、最近的轮次 |
| `/api/info` | 矿池设置：Stratum 端口、支付方式和窗口、最低支付额 |
| `/api/blocks?status=confirmed` | 已找到的区块，可按状态筛选 |
| `/api/payouts/recent` | 支付批次及交易哈希 |
| `/api/luck` | 每一轮的努力值和用时 |
| `/api/status` | 服务健康状况、在线率窗口、事件 |
| `/api/miner/{address}` | 你的算力、总计、矿机、最新份额、支付 |
| `/api/miner/{address}/balance` | 预估、已确认和已支付余额 |
| `/api/miner/{address}/hashrate?range=24h` | 你的算力历史 |

列表接口接受 `limit`（默认 25，最多 200）和 `offset` 参数，返回 `items` 和 `total`。矿工接口需要完整地址，所以在 My Stats 上用 @name 查询后，请从搜索框复制完整地址。

在终端中读取你的矿机数据：

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

配合 `jq` 可以提取单个数值，例如你的算力和被接受的份额，或以 BNT 表示的已支付总额：

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### 在家庭仪表盘上显示你的矿机

同一个接口可以作为家庭仪表盘中的传感器。在 Home Assistant 中，RESTful 集成只需在 `configuration.yaml` 中写几行即可读取它：

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

每 30 到 60 秒轮询一次就足够了：算力数字是一小时的平均值，而 My Stats 本身每 30 秒刷新一次你的矿工详情。公开 API 按客户端限制请求频率。如果脚本收到 HTTP 429 响应，应当放慢请求，等待 `Retry-After` 头给出的时间后再重试。

## seine 自己的仪表盘呢？

矿池显示的是它记入你名下的数据。若要实时查看机器本身，seine 的终端仪表盘会即时显示算力和份额；seine 还可以开启本地控制 API，提供 REST、服务器推送事件流以及位于 `/metrics` 的 Prometheus 指标，默认绑定到 `127.0.0.1:9977`。[控制 API 指南](https://github.com/zpalmtree/seine/blob/master/docs/API.md)介绍了相关参数和接口。许多矿工两者都用：用 seine 的指标查看每台机器，用矿池 API 查看余额和支付。

第一次使用 seine？[下载它](https://seineminer.com/zh-hans/#download)，使用默认矿池，在你的第一个份额被接受后几分钟内，你的地址就会出现在 My Stats 上。

<CTA target="pool" title="开始挖矿并实时查看" text="几分钟内加入 bntpool.com，然后在“我的统计”里查看你的算力、份额和余额。" />
