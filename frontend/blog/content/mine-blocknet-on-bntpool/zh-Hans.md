---
title: 在 bntpool.com 挖 Blocknet
description: 加入 seine 的默认 Blocknet 矿池 bntpool.com：用你的地址或 @name 连接 stratum+tcp://bntpool.com:3333，为矿机命名，并查看你的挖矿统计。
tags: [挖矿指南, bntpool, Blocknet ID, seine]
faq:
  - q: 'bntpool.com 的 Stratum 地址是什么？'
    a: '地址是 stratum+tcp://bntpool.com:3333，也就是 seine 首次运行时建议的矿池。在矿池提示处按回车即可接受，也可以用 --pool-url 参数传入。你的 Blocknet 地址或 Blocknet ID 名称就是登录名，矿池会把收益支付到该地址。'
  - q: '可以用 Blocknet ID 名称在 bntpool.com 挖矿吗？'
    a: '可以。在 seine 或矿池要求输入地址的地方填写 @name 或 $name 即可。矿池会在 blocknet.id 核验签名映射，然后把份额和收益记入该名称对应的完整地址。需要 seine v0.2.16 或更新版本；在命令行中请给名称加上引号，避免被 shell 改写。'
  - q: '在 bntpool.com 挖矿需要注册账户吗？'
    a: '不需要。没有注册、密码或邮箱。你的 Blocknet 地址，或指向它的 Blocknet ID 名称，既是登录名也是收款地址。用它启动 seine，然后在 My Stats 页面查询，就能看到你的算力、份额、矿机、余额和付款记录。'
  - q: '可以用多台电脑挖到同一个地址吗？'
    a: '可以。在每台电脑上用相同的地址或名称运行 seine，并给每台设置不同的矿机名称。矿池按地址统计份额、余额和付款，所以所有矿机都计入同一个余额、合并为同一笔付款，而 My Stats 页面会把每台矿机单独列成一行。'
sourceHash: '3126e8e7e396'
translation: reviewed
---

本指南介绍如何用 seine 加入 bntpool.com 矿池：矿池地址、用 Blocknet 地址或 Blocknet ID `@name` 登录、为每台矿机命名，以及在统计页面上确认你的份额已经到达。如果还没有安装 seine，请先按照 [seine 安装指南](https://seineminer.com/zh-hans/blog/how-to-mine-blocknet/)操作，再回到这里。

## bntpool.com 的矿池地址是什么？

<Answer>
bntpool.com 的 Stratum 地址是 `stratum+tcp://bntpool.com:3333`。seine 首次运行时会把它作为默认矿池，所以在矿池提示处按回车就能连接。你用自己的 Blocknet 地址或 Blocknet ID `@name` 登录。无需创建账户，收益会发送到该地址。
</Answer>

以下是矿池的设置，来自其公开的 `/api/info` 接口（2026 年 10 月 25 日）：

| 设置 | 值 |
|---|---|
| Stratum 地址 | `stratum+tcp://bntpool.com:3333` |
| 登录 | Blocknet 地址、`@name` 或 `$name` |
| 支付方式 | PPLNS，6 小时窗口 |
| 付款前所需确认数 | 60 个区块 |
| 最低付款额 | 0.1 BNT |

你随时可以在矿池的 [Get Started](/start)（开始挖矿）页面查看这些值。

## 为什么 bntpool.com 是 seine 的默认矿池？

第一次运行 seine 时，它会询问矿池，并把 `stratum+tcp://bntpool.com:3333` 填为默认值。矿池和矿工软件在几个实际方面相互配合：

- **同一套协议。** bntpool.com 运行的是 seine 所使用的 Stratum 协议（第 2 版），因此除了你的地址之外，seine 连接时无需任何配置。
- **两端都支持名称。** seine 和矿池都接受 Blocknet ID 名称，并依据 blocknet.id 上同一份签名记录进行核验。
- **版本检查。** 每台矿机提交份额时会报告自己的 seine 版本。如果某台矿机运行的是较旧的 seine，统计页面会给它标上“update available”（有可用更新）标记，并链接到 seine 的发布页面。
- **seine 设置指南。** 矿池的 Get Started 页面会一步步介绍如何下载和运行 seine，并提供一条可以直接复制的命令。

## 如何把 seine 连接到 bntpool.com？

第一次启动 seine 时，它会问两个问题：

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

粘贴你的地址或输入你的名称，然后按回车接受 bntpool.com。seine 会把这两个答案保存到 `seine-data/seine.config.json`，之后每次启动都会直接开始挖矿。它还会自动选一个矿机名称并保存在同一个文件里（下文详述）。

在无显示器的矿机上、作为服务运行或在脚本中，没有人来回答这些提示。请在首次启动时用参数传入这两个值，这也正是 Get Started 页面给出的命令：

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

以后要更换矿池或地址，可以编辑或删除 `seine-data/seine.config.json`，或在命令行上用 `--address` 和 `--pool-url` 覆盖已保存的值。

## 可以挖到 Blocknet ID @name 吗？

<Answer>
可以。凡是 bntpool.com 接受地址的地方，都可以使用 Blocknet ID 短名称，写作 `@name` 或 `$name`。矿池会核验来自 blocknet.id 的签名映射，验证该名称指向的地址，并在整个连接期间把份额、余额和付款都记入这个完整地址。
</Answer>

名称不区分大小写，所以 `@Alice` 和 `@alice` 是同一个登录名。seine 从 v0.2.16 开始支持名称，任何当前版本都已具备。开始挖矿前，seine 会显示名称解析出的地址，方便你确认它与自己的钱包一致。在命令行中，请用单引号把名称括起来，否则 shell 会把 `$name` 当作变量展开：

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

如果之后你把名称指向了另一个钱包，请重启 seine。seine 每次启动时都会重新解析已保存的名称，并在整个运行期间使用该地址；矿池对每个连接也是如此。已经挣到的余额会留在挣得它的那个地址上，并支付到那里，所以修改名称不会转移你的币。

你可以在 [blocknet.id](https://blocknet.id) 注册名称。[Blocknet ID 指南](https://seineminer.com/zh-hans/blog/blocknet-id-pay-by-name/)介绍了按名称付款的原理。

## 如何给矿机命名？

每个连接在登录时都会附带一个矿机名称和你的地址。首次运行时，seine 会替你选一个名称，即 `seine-` 加四位数字，并保存在配置文件中，所以同一台机器重启后名称不变。想用一个容易辨认的名称，可以在命令行上设置：

```sh
./seine --pool-worker rig-01
```

矿池最多保留名称的 64 个字符。固定的名称在重启后也有好处：矿池会按你的地址和矿机名称，把每台矿机的份额难度记住一小时，所以重新连接的矿机会从它已经稳定下来的难度继续。

## 可以用多台矿机挖到同一个地址吗？

可以。在每台机器上用相同的地址或 `@name`、不同的矿机名称启动 seine。矿池按地址统计份额、余额和付款，所以你所有的矿机都会计入同一个余额、合并为同一笔付款。在统计页面上，每台矿机各占一行，一眼就能看出每台机器都在挖矿。

## 如何确认矿机在正常工作？

seine 的仪表盘会在本机显示你的算力和已接受的份额。要查看矿池这一侧的情况，请打开 [My Stats](/stats)（我的统计），输入你的地址、`@name` 或 `$name`，然后点击 **Lookup**（查询）。如果输入的是名称，页面会在搜索框旁显示它解析出的地址。页面还会在该浏览器中记住你上一次的查询，[查看矿机指南](/blog/zh-hans/track-your-miner-on-bntpool/)介绍了页面上的每一张卡片。

请留意三点：

- **算力和 Shares Accepted（已接受份额）。** 算力卡片显示矿池看到的你的算力，随着矿机提交工作，已接受份额的数量会不断增加。
- **每台矿机一行。** 每行显示矿机名称、算力、已接受和被拒绝的份额，以及最后一次提交份额的时间。如果最后一次份额就在刚才，说明这台矿机已连接并在工作。
- **Recent Shares（最近份额）。** 最新的份额会连同提交它的矿机、难度和状态一起列出。

矿池会为每个连接单独设置份额难度，并在挖矿过程中不断调整，所以即使是小型 CPU 也能稳定地提交份额，你的第一批被接受的份额会在连接后几分钟内出现。至于你的硬件能有多少算力，请参阅 [Blocknet 挖矿硬件指南](https://seineminer.com/zh-hans/blog/blocknet-mining-hardware-hashrate/)。

接下来是收益。当矿池找到区块、而你的份额处在六小时窗口内时，Estimated Rewards（预估收益）就会出现。该区块获得 60 个确认后，你的那一份会转入 Confirmed Rewards（已确认收益），当你累计到至少 0.1 BNT 时，矿池会自动发送。[PPLNS 收益如何支付](/blog/zh-hans/how-pplns-payouts-work/)一文解释了每个区块的奖励如何分配。全矿池的历史记录见 [Blocks](/blocks)（区块）和 [Payouts](/payouts)（付款）页面。

## Get Started 页面包含哪些内容？

矿池的 [Get Started](/start) 页面把所有内容放在一页上：

- **矿池信息。** Stratum 地址（点击即可复制）、支付方式、最低付款额，以及付款前所需的区块确认数，均实时读取自矿池。
- **三步快速开始。** 从 [seineminer.com](https://seineminer.com/zh-hans/#download) 下载 seine，运行一次并输入你的地址或名称以及矿池 URL，然后开始挖矿，在 My Stats 上关注你的算力和余额。
- **可复制的命令。** 已填好矿池 URL 的完整 `./seine` 命令。换上你的地址，或加引号的 `'@yourname'` 即可。
- **设置说明。** seine 把配置保存在哪里、如何重置，以及用来覆盖配置的参数。
- **收益如何计算。** 对 PPLNS 的简短说明：区块被找到之前你提交的份额分得该区块的奖励，之后增加的工作则计入后续的区块。页面还说明，未确认区块带来的收益会在 My Stats 上单独显示，直到这些区块获得确认。
- **截图。** 展示 seine 的首次运行设置界面和挖矿仪表盘，让你在开始前就知道会看到什么。

<CTA target="pool" title="在 bntpool.com 开始挖矿" text="用你的地址或 @name 将 seine 连接到 bntpool.com，然后在“我的统计”里查看你的份额。" />
