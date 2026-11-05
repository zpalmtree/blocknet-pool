---
title: '把 Blocknet 直接挖到交易所'
description: '在 bntpool.com 上把 Blocknet 直接挖到 SafeTrade 或其他交易所的充值地址。在 seine 中添加充值备注，矿池就会支付到你的账户。'
tags: [挖矿指南, bntpool, SafeTrade, 支付]
sourceHash: 8f73aae25c7a
translation: draft
faq:
  - q: '可以把 Blocknet 直接挖到交易所吗？'
    a: '可以。用交易所充值地址和你的充值备注启动 seine v0.3.1 或更新版本，例如 ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO。bntpool.com 会把收益连同备注一起支付到这个地址，交易所就会把它记入你的账户。'
  - q: '为什么 seine 会询问支付备注？'
    a: 'SafeTrade 等交易所让所有客户共用一个充值地址，并用备注区分每笔充值。seine 认识这些共用地址，所以你输入其中一个时，它会询问你的备注，并与地址一起保存。请从交易所的充值页面准确复制备注。'
  - q: '在 bntpool.com 上支付到交易所的最低支付额是多少？'
    a: '根据矿池在 2026年11月4日报告的数据，带备注的支付从 10 BNT 起付。你的已确认余额会在矿池中累积，达到该数额后，矿池把它连同备注发送到交易所地址。支付到个人钱包则从 1 BNT 起付。'
  - q: '如何查看挖到交易所的余额？'
    a: '在 bntpool.com 统计页面输入交易所充值地址，选择添加备注并输入你的备注。每个地址和备注的组合都有自己的余额，所以你只会看到自己的算力、矿机和支付，而不会看到使用同一交易所地址的其他客户的数据。'
---

本指南介绍如何在 bntpool.com 上把 Blocknet 直接挖到你的交易所账户。你把交易所的充值地址和你的充值备注告诉 seine，矿池就会把收益连同备注支付到那里，交易所再把它记入你的账户。本文涵盖命令、最低支付额以及如何查看余额。

## 可以把 Blocknet 直接挖到交易所吗？

<Answer>
可以。bntpool.com 会把挖矿收益连同你的充值备注一起支付到交易所充值地址。用交易所地址和你的备注启动 seine v0.3.1 或更新版本：./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO。带备注的支付从 10 BNT 起付，交易所会把每一笔都记入你的账户。
</Answer>

如果你打算交易挖到的币，直接挖到交易所可以省去一步。收益直接进入你的交易所账户，可以立即在 [BNT/USDT 市场](/blog/zh-hans/sell-blocknet-bnt-safetrade/)出售，中间无需从钱包转账。

SafeTrade 等交易所给所有客户同一个充值地址，并用备注（你账户专属的一段短代码）区分每笔充值。这就是备注重要的原因：它把一笔支付与你关联起来。矿池为每个地址和备注的组合分别记账，所以你的收益永远不会与使用同一交易所的其他矿工混在一起。

## 如何在 seine 中设置挖到交易所？

<Answer>
从交易所的充值页面复制 BNT 充值地址和你的充值备注。用这两项启动 seine：--address 填地址，--payout-memo 填备注。在首次运行的提示中输入已知的交易所地址时，seine 会自动询问备注并保存。在日志中确认矿池已经确认，然后照常挖矿。
</Answer>

1. **获取充值信息。** 在交易所打开 BNT 充值页面，复制充值地址和备注。请准确复制备注：大写字母和开头的零都有区别。
2. **用这两项启动 seine。** 在 macOS 或 Linux 上：

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   在 Windows 上，于 PowerShell 中运行：

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **让 seine 保存。** seine 会把备注和地址一起保存，所以之后的运行会继续支付到你的交易所账户。如果你在首次运行的提示中输入已知的交易所地址，seine 会询问备注并把两者都保存下来。

矿池的 [Get Started 页面](/start)有一个“挖矿到交易所？”部分，可以帮你生成这条命令：粘贴地址和备注，然后复制结果。对于 SafeTrade 这类已知的共用交易所地址，seine 要求填写备注。服务、脚本和其他无人值守的矿机必须在命令行中传入 `--payout-memo`，因为没有可以回答的提示。

seine 登录时会向矿池核对备注。bntpool.com 确认后就开始挖矿。要给矿机命名，像使用个人地址时一样传入 `--pool-worker rig-01`。

## 支付到交易所的款项如何发放？

收益和 bntpool.com 的其他支付走同样的流程。你的份额在六小时 PPLNS 窗口内分得矿池找到的每个区块的一部分，每个区块的奖励在 60 个区块后确认。之后你的已确认余额会在矿池中累积。

矿池的 `/api/info` 接口在 2026年11月4日报告的最低支付额如下：

| 支付目标 | 最低支付额 |
|---|---|
| 带备注的交易所地址 | 10 BNT |
| 个人钱包 | 1 BNT |

已确认余额达到 10 BNT 后，矿池会把它连同备注发送到交易所地址。交易所随后按自己的确认数入账，所需确认数列在交易所的充值页面上。[PPLNS 收益如何支付](/blog/zh-hans/how-pplns-payouts-work/)详细介绍了矿池这一侧的流程。

单独挖矿也可以使用备注。在交易所地址前加上 `solo:`，并保留 `--payout-memo`，矿池就会把你找到的每个区块的全部奖励支付到你的交易所账户。[在 bntpool.com 上单独挖 Blocknet（solo）](/blog/zh-hans/solo-mine-blocknet-on-bntpool/)介绍了单独挖矿的收益。

## 如何查看余额？

打开[统计页面](/stats)，粘贴交易所充值地址，选择 **添加备注** 并输入你的备注。页面随后会显示该地址和备注组合下你自己的算力、矿机、余额和支付。同一交易所的其他客户有各自的组合和余额。

**Recent Payouts** 中的每一笔支付都链接到区块浏览器上的交易。把它与交易所的充值记录对照，就能跟踪这些币进入你的账户。[在 bntpool.com 上查看你的矿机](/blog/zh-hans/track-your-miner-on-bntpool/)介绍了统计页面的其余部分。

## 如何切换回个人钱包？

用你的新地址和 `--no-payout-memo` 启动一次 seine：

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine 会保存不带备注的新地址，新的份额会计入你的个人钱包。已经记入交易所组合的余额会留在该组合中，达到最低支付额后支付给它；你可以在统计页面上跟踪。[更改挖矿钱包地址](/blog/zh-hans/change-mining-wallet-address/)介绍了更改地址后矿池如何记账。

## 哪些设置支持备注？

备注需要 seine 直接连接矿池，这也是默认方式。seine 的 daemon 模式对接你自己的节点挖矿并支付到节点的钱包，因此没有备注。矿场代理不传递备注，所以请让挖到交易所的矿机直接连接矿池。seine 只在确认了你的备注的矿池上挖矿，所以你的工作总是支付到你选择的账户。

<CTA target="pool" title="挖矿到你的交易所账户" text="用你的充值地址和备注启动 seine，bntpool.com 会从 10 BNT 起把收益直接支付到你的交易所账户。" />
