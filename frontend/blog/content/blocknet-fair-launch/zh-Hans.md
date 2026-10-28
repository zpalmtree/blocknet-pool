---
title: Blocknet 的公平启动：没有预挖，每一枚币都是挖出来的
description: "核对 Blocknet 的空创世区块、首笔挖矿奖励与发行规则。了解 BNT 如何进入流通，以及挖出的币如何经由后续转账到达其他持有者手中。"
tags: [Blocknet, 公平启动, 创世区块, 挖矿, bntpool]
faq:
  - q: 'Blocknet 有预挖吗？'
    a: '没有。Blocknet 的创世区块创建于 2026 年 2 月 15 日，里面没有任何交易，因此在挖矿开始之前不存在任何币。第一批币来自区块 1 的区块奖励，此后的每一枚 BNT 都支付给了挖出对应区块的人。'
  - q: 'Blocknet 有开发基金或国库吗？'
    a: "已发布规则没有协议强制要求的开发基金或国库分配。创世之后每个区块只有一个 coinbase 输出，等于规定奖励。这描述的是新币发行，而非后续持有者的身份。任何拥有 BNT 的人都能进行普通转账，包括自愿向开发者付款。"
  - q: 'Blocknet 的创世区块是什么时候创建的？'
    a: '创世区块的时间戳是 2026 年 2 月 15 日 20:58:38（美国东部标准时间 EST），即 UTC 时间 2 月 16 日 01:58:38。它带有消息 "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything"，它的前一区块哈希就是这条消息的 SHA3-256 哈希。'
  - q: '如何亲自核实 Blocknet 的公平启动？'
    a: '在 Blocknet 区块浏览器上打开区块 0，你会看到交易数为零、默克尔根全为零。然后阅读开源的节点代码：创世区块以空交易列表硬编码在代码中，coinbase 检查只接受一个金额等于区块奖励的输出。'
  - q: '如何开始挖 Blocknet？'
    a: '下载适用于 Windows、macOS 或 Linux 的 seine 挖矿软件，运行它，并在提示时粘贴你的 Blocknet 地址或 Blocknet ID 短名称。按回车接受默认矿池 bntpool.com，seine 会立即开始挖矿。之后你可以在 bntpool.com 的统计页面查询自己的地址。'
sourceHash: a283d654ab64
translation: draft
---

了解 BNT 如何进入流通：从空的创世区块、首笔挖矿奖励，到持有者之间后续的转账。本指南核对启动记录与发行规则，说明这些证据能证明什么，并介绍如何在 bntpool.com 挖矿来获得新发行的 BNT。

## Blocknet 有预挖吗？

<Answer>
没有。Blocknet 的创世区块创建于 2026 年 2 月 15 日，里面没有任何交易，因此在挖矿开始之前不存在任何币。从区块 1 起，每个区块都以单个 coinbase 输出支付奖励，金额恰好等于计划数额，所以流通中的每一枚 BNT 都是有人运行工作量证明挖出来的。
</Answer>

创世区块没有向创始人、开发者、投资者或基金会分配储备。之后的区块按计划创建挖矿奖励，没有协议强制要求的额外国库输出。这些是发行规则，并不能确定谁挖到了奖励，或谁后来通过转账收到了已有的币。

## Blocknet 的创世区块里有什么？

创世区块硬编码在节点程序中，因此每个节点都从同一个区块 0 开始。它的字段是固定的：

| 字段 | 值 |
|---|---|
| 高度 | 0 |
| 时间戳 | 2026 年 2 月 15 日 20:58:38 EST（UTC 时间 2 月 16 日 01:58:38） |
| 消息 | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| 前一区块哈希 | 这条消息的 SHA3-256：`a94a7f2b…d909df` |
| 难度 | 4，即网络最低难度 |
| Nonce | 0 |
| 交易 | 无 |
| 默克尔根 | 全为零 |

这条消息是 Wired 一篇日期为 2026 年 2 月 15 日的报道的标题，代码中注明了这篇文章是出处。由于前一区块哈希由这条消息计算得出，这个标题就固定在区块 0 以及其后整条链的身份之中。

节点程序中创世函数上方的注释写得很直白："Genesis has no transactions - no coinbase, no burned coins. First real block at height 1 will have the first coinbase."（创世区块没有交易：没有 coinbase，也没有销毁的币。高度 1 的第一个真正区块才会有第一笔 coinbase。）验证规则也印证了这条注释。节点在检查区块 0 时，只要它不是恰好零笔交易、默克尔根不是全零，就会拒绝它，所以即使有人修改了节点，也无法把币塞进创世区块而不被其他所有节点拒绝。

## 共识如何杜绝开发基金或国库？

<Answer>
创世区块之后，每个区块都以一笔没有输入、手续费为零、且恰好有一个输出的 coinbase 交易开始，输出金额等于规定奖励。已发布规则没有在这笔交易中设置独立的创始人或国库输出。持有者仍可通过普通交易转移自己已有的 BNT。
</Answer>

节点程序中的几项检查共同保证了这一点：

- **每个区块一笔 coinbase。** 第一笔交易必须是 coinbase，区块中的其他交易都不能是 coinbase。
- **一个输出，没有输入。** 有零个或两个输出、或者带有输入的 coinbase 都是无效的。
- **精确的奖励。** Blocknet 用 Pedersen 承诺隐藏金额，但 coinbase 是特例。它的盲化因子由公开数据（交易公钥和区块高度）推导而来，因此每个节点都能为计划奖励重建承诺并进行比较。如果输出承诺的是其他任何金额，区块就会被拒绝。
- **空的备注。** coinbase 的备注必须为空，因此区块奖励也不能携带额外数据。

普通交易无法创造币。每笔交易都必须平衡：输入等于输出加手续费，这一点直接在承诺上检查；每个输出还附带 Bulletproofs 范围证明，证明其金额在有效范围内、不可能为负。金额正是这样保持私密，而发行量仍然严格遵循计划。

## 如何亲自核实 Blocknet 的公平启动？

你不必相信任何人的说法。链和代码都是公开的。

**在区块浏览器上。** 打开 [Blocknet 区块浏览器上的区块 0](https://explorer.blocknetcrypto.com/block/0)。页面显示交易数为 0、默克尔根是 64 个零、nonce 为 0，前一区块哈希以 `a94a7f2b` 开头。区块奖励一栏显示的是计划中该高度对应的数额，但区块里没有交易，也就没有接收它的输出。接着打开[区块 1](https://explorer.blocknetcrypto.com/block/1)：它唯一的交易是一笔没有输入、只有一个输出的 coinbase，这就是链上的第一批币。

你可以确认前一区块哈希确实是创世消息的哈希。在 OpenSSL 1.1.1 或更新版本下运行：

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

输出的结尾是 `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`，与区块浏览器为区块 0 显示的值相同。

**在代码中。** 节点程序在 [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core) 开源。在 [`block.go`](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) 中查找以下函数：

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` 以空交易列表构建区块 0，`validateGenesisBlock` 拒绝任何带有交易的区块 0，`validateCoinbaseConsensus` 则强制要求唯一一个金额等于区块奖励的输出。在 `transaction.go` 中，`validateCoinbase` 和 `verifyCommitmentBalance` 分别负责 coinbase 的结构检查和普通交易的平衡检查。

## 新挖出的 BNT 如何到达其他持有者手中？

浏览器记录的创世时间为 2026 年 2 月 15 日 20:58:38 EST。区块 1 的时间为 2026 年 2 月 20 日 14:29:20 EST，包含第一笔 coinbase 交易。两者是不同的记录：创世区块定义共同起点，区块 1 开始带奖励的发行。浏览器为空创世区块显示的奖励只是计划金额，没有任何输出领取它。

挖矿奖励可以花费后，所有者就能把 BNT 转给他人。矿池同样会从收到的奖励中发出后续付款。这些转账重新分配已有的币，而不是再次发行。因此，即使没有亲自挖出最初创建这些币的区块，你也可以持有和使用 BNT。[收发指南](https://seineminer.com/zh-hans/blog/send-receive-blocknet/)介绍普通支付，[供应量指南](https://seineminer.com/zh-hans/blog/blocknet-supply/)解释整体货币发行计划。

## 公平启动对矿工为什么重要？

对矿工来说，公平启动意味着区块奖励就是币的全部来源。

- **所有新币都归矿工。** 没有预挖，也没有国库输出，100% 的发行量都支付给找到区块的人。区块 1 在它唯一的 coinbase 输出中支付了 723.25093035 BNT，此后的每个区块都以同样的方式支付全部奖励。
- **所有人遵循同样的规则。** 从区块 1 起，获得新 BNT 的唯一方式就是运行工作量证明。[奖励计划](/blog/zh-hans/blocknet-block-reward-schedule/)写在代码中，对第一位矿工和今天才开始的人同样适用。
- **工作量证明适合普通电脑。** Blocknet 使用每次哈希需要 2 GiB 内存的 Argon2id，其设计目的是抵抗 ASIC。这让挖矿对人们已有的台式机、笔记本和服务器保持开放。[seineminer.com 上的 Argon2id 详解](https://seineminer.com/zh-hans/blog/argon2id-proof-of-work-explained/)解释了原因。
- **发行量易于审计。** 每一枚币都可以追溯到一个经过每个节点检查的 coinbase 输出，因此任何高度的累计发行总量都可以直接由奖励计划得出。

如果你刚开始了解这枚币，[Blocknet 是什么？](https://seineminer.com/zh-hans/blog/what-is-blocknet/)介绍了隐私、按名称付款和钱包。

## 如何亲自挖 Blocknet？

要通过挖矿获得新发行的 BNT，请将 seine 矿工连接到 bntpool.com：

1. **准备地址。** 安装官方 Blocknet [命令行钱包](https://github.com/blocknetprivacy/blocknet/releases/latest)或[桌面钱包](https://github.com/blocknetprivacy/blocknet-gui/releases/latest)并复制你的地址，或者使用 `@name` 这样的 Blocknet ID 短名称。
2. **下载 seine。** 从 [seineminer.com](https://seineminer.com/zh-hans/#download) 获取适用于 Windows、macOS 或 Linux 的压缩包并解压。无需安装。
3. **运行并接受默认设置。** 启动 seine，粘贴你的地址或 `@name`，按回车接受默认矿池 `stratum+tcp://bntpool.com:3333`。seine 会立即开始计算哈希，并在运行中调整线程数和计算内核；在 Windows 和 Linux 上，它会同时使用 CPU 和 NVIDIA 显卡。
4. **查看你的矿机。** 在[统计页面](/stats)查询你的地址，查看算力、份额和余额，并在[区块页面](/blocks)关注矿池找到的区块。

[在 bntpool.com 挖 Blocknet](/blog/zh-hans/mine-blocknet-on-bntpool/)一文介绍了如何用地址或名称登录并查看矿机，[入门页面](/start)列出了矿池的支付设置，[如何挖 Blocknet 指南](https://seineminer.com/zh-hans/blog/how-to-mine-blocknet/)则一步步介绍了在各个系统上安装 seine 的方法。

<CTA target="seine" title="挖到属于你的那份" text="每一枚 BNT 都是挖出来的。下载 seine，几分钟内即可在 bntpool.com 上开始挖矿。" />
