---
title: "如何在 SafeTrade 出售 Blocknet（BNT）"
description: "在 SafeTrade 将 Blocknet 换成 USDT：从钱包转入可花费 BNT，确认充值到账，提交卖单，并检查实际成交数量与收到的资金。"
tags: [Blocknet, BNT, SafeTrade]
faq:
  - q: "在哪里可以出售 Blocknet BNT？"
    a: "SafeTrade 的 BNT/USDT 现货市场支持卖出 Blocknet 换取 USDT。按照账户当前充值说明转入 BNT，等待可用余额，再核对卖单。通过成交数量和交易历史确认实际卖出了多少。"
  - q: "可以出售挖矿获得的 BNT 吗？"
    a: "可以。矿池付款到达钱包并成为可花费余额后，可转入自己的 SafeTrade 充值地址，在 BNT/USDT 出售。矿池中的待结算奖励属于更早阶段；转账前应确认钱包到账和可花费状态。"
  - q: "卖出 BNT 换成 USDT 会直接入账银行卡吗？"
    a: "BNT/USDT 成交后，交易所账户得到的是 USDT。银行入账需要另一个支持所在地区和目标法币的服务或提现渠道。转账前查看其当前要求；这笔交易本身只是两种加密资产之间的兑换。"
sourceHash: "4fd7f07f3525"
translation: draft
---

在 SafeTrade 出售 Blocknet，需要先充值可花费的 BNT，再进入 **BNT/USDT** 提交卖单。如果使用 seine 在 bntpool.com 挖矿，先确认付款已进入自己的钱包并可以发送。

## 在哪里出售 Blocknet BNT？
<Answer>
使用 [SafeTrade BNT/USDT 市场](https://safetrade.com/exchange/BNT-USDT)。按照 BNT 充值说明转账，等待入账余额可用于交易，再按所选条件提交卖单。到交易历史核对实际成交数量与收到的 USDT。
</Answer>

[上线公告](https://safetrade.com/announcements/blocknet-listing)日期为 2026年10月28日。公开市场提供订单簿、最近交易和订单表单。如果想买币，请阅读[购买指南](/blog/zh-hans/buy-blocknet-bnt-safetrade/)。

## 挖矿奖励可以发送了吗？
区分矿池账目、收款钱包和交易所账户三个阶段。矿池中的估算奖励还不是交易所充值；矿池付款交易也不表示 SafeTrade 已经入账。使用挖矿时的同一钱包身份打开 [My Stats](/stats)，查看付款记录，再到已同步的钱包中查找收入。[PPLNS 付款指南](/blog/zh-hans/how-pplns-payouts-work/)解释奖励何时可支付；已发出但钱包中找不到时，先按[到账排查指南](/blog/zh-hans/pool-payout-missing-from-wallet/)检查。
根据钱包的可花费余额决定发送金额。可见入账和可花费状态可能因确认进度不同而先后出现；参见[转账时间指南](https://seineminer.com/zh-hans/blog/blocknet-transfer-time/)。预留发送金额及钱包审核页显示的交易费用。

## 将 BNT 充值到 SafeTrade
从官方网站登录，完成账户和所在地区要求的步骤，并阅读当前[条款](https://safetrade.com/terms)及充值说明。选择 Blocknet BNT，从自己账户的充值页复制地址。
确认充值已开放、最低金额、确认数量及其他必填信息，准确使用交易所提供的目标和字段。如果充值暂不可用，应等待恢复后再发送；交易与充值可用性由不同控制决定。
在钱包中准备支付，批准前核对完整地址、金额和费用。[桌面钱包指南](https://seineminer.com/zh-hans/blog/blocknet-desktop-wallet/)介绍界面。保存交易 ID，用于对应钱包转出与交易所充值。广播或进入区块可能早于交易所入账；等待余额可用，并遵循当前充值页的确认要求，不要假定它与钱包要求一致。

## 如何提交 BNT 卖单？
在 BNT/USDT 的卖出侧，交出 BNT 并收到 USDT，价格单位是 USDT/BNT。先看可用余额，其他未完成订单可能占用一部分。
**限价卖单**设定可接受的最低单价和数量。**演算示例**：100 BNT 全部按每枚 0.10 USDT 成交，费用前交易额为 10 USDT；不是实时行情。订单可能保持挂单或分批成交。
**市价卖单**匹配可用买单，平均价格由实际成交决定。最新价格描述已完成交易，订单簿显示当前买入报价。确认订单类型前核对表单和估算结果。

## 确认出售并跟踪 USDT
查看未完成订单、已成交数量和交易历史。只有已成交部分算卖出；取消剩余部分会释放未卖出的币，不会撤销已经完成的交易。记录实际数量和所得，而非最初请求总额。
成交后的 USDT 留在交易所账户，转出需要另一次提币。确保目标支持同一资产和网络，查看最低额、费用及接收说明，以[费用页面](https://safetrade.com/fees)和最终表单为准。
如果目标是银行资金，先寻找支持所在国家和目标币种的 USDT 兑换提现服务，转账前确认账户及转账要求。BNT/USDT 本身结束于 USDT，后续兑换和提现记录应分别保存。

## 将挖矿与交易记录对应起来
分别保存矿池付款、钱包入账、钱包转出、交易所充值及成交记录，用相关交易 ID 和平台编号关联。这样即使多笔奖励合并充值或一个订单分批成交，也能追踪从 BNT 到 USDT 的过程。
你可以继续挖矿到自己的钱包，并选择转出可花费余额的时间。[矿池设置指南](/blog/zh-hans/mine-blocknet-on-bntpool/)介绍连接 [seine](https://seineminer.com/zh-hans/) 和检查收到的工作。奖励结算、钱包转账和交易订单都有各自的完成确认。

<CTA target="pool" title="跟踪 Blocknet 挖矿付款" text="按 bntpool.com 的设置指南连接 seine，跟踪奖励直到自己的钱包。" />
