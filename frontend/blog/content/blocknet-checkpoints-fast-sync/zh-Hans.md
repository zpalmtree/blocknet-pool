---
title: 用 bntpool.com 检查点快速同步 Blocknet 节点
description: Blocknet 节点通过 bntpool.com 的 checkpoints.dat 快速同步。了解检查点如何锁定链，再用几步下载、校验并加载该文件。
tags: [Blocknet 节点, 检查点, 快速同步, bntpool]
faq:
  - q: 'Blocknet 节点从哪里获取检查点？'
    a: '从 bntpool.com 获取。首次启动时，如果数据目录中没有 checkpoints.dat，Blocknet 守护进程会从 https://bntpool.com/checkpoints.dat 下载它并用于快速同步。你也可以在 Checkpoints 页面自行下载该文件，核对 SHA-256 后放入数据目录，再启动节点。'
  - q: 'bntpool.com 多久更新一次 checkpoints.dat？'
    a: '矿池的守护进程会随着链的增长重写该文件，每第 100 个区块一条记录。每条记录都至少比链顶低 100 个区块，超过 Blocknet 的最大重组深度，所以检查点只覆盖已经确定的历史。Checkpoints 页面会显示最新高度和文件最近一次更新的时间。'
  - q: '检查点会改变我的钱包余额或交易历史吗？'
    a: '不会。使用检查点同步的节点仍会保存每个区块，并把每个输出和密钥镜像记录到索引中，所以钱包扫描和余额与完整同步后完全一样。检查点只是让节点不必重新检查那些已由检查点哈希担保的历史区块的工作量证明和签名。'
  - q: '可以不用检查点同步 Blocknet 节点吗？'
    a: '可以。用 --full-sync 参数启动守护进程，或在 blocknet 配置中把 full_sync 设为 true，节点就会跳过检查点下载，从创世区块开始为每个区块重新验证工作量证明和签名。这比检查点同步耗时更长，所以新节点默认使用检查点。'
sourceHash: 7a3491743f7d
translation: reviewed
---

一个新的 Blocknet 节点必须追上自创世区块以来的每个区块，才能为钱包或单独挖矿提供服务。检查点能让这个追赶过程变快。本指南介绍 bntpool.com 的检查点文件包含什么、Blocknet 守护进程如何用它跳过最慢的检查同时始终锁定在真实的链上，以及如何自己下载、校验并加载这个文件。

## 什么是 Blocknet 检查点？

<Answer>
Blocknet 检查点是一份已知区块哈希的列表，每第 100 个区块一条，由 bntpool.com 以 checkpoints.dat 的形式提供。正在同步的节点会核对收到的区块是否与这些哈希一致，并在最后一个检查点之前跳过对工作量证明和签名的重复验证，因此首次同步会快得多。
</Answer>

该文件是纯文本，每行一个 `height:hash` 对。2026-10-25 时文件的前几行如下：

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

每个哈希都是区块的 ID，即区块头的 SHA3-256 哈希。矿池自己的守护进程根据它的链写出这个文件，并且只收录深度至少为 100 个区块的区块。100 个区块是 Blocknet 的硬最终性，也就是协议允许的最深重组，所以每个检查点都位于已经确定的历史中。

2026-10-25 02:04（美国东部夏令时间）时，该文件包含 689 个检查点，从高度 100 到高度 68,900，大小为 48,811 字节。它大约每 100 个区块增加一行，按五分钟出块约每八小时一次，当前高度请查看 [Checkpoints 页面](/checkpoints)。

## Blocknet 节点如何使用检查点？

加载了检查点的节点会把最后一个检查点的高度当作快速同步的目标。在链到达该高度之前，它依靠已发布的检查点完成两项开销最大的检查，其他检查照常进行：

- **工作量证明依靠检查点。** 每个 Blocknet 区块的工作量证明都是一次占用 2 GiB 内存的 Argon2id 哈希。为每个历史区块重新计算一次代价很高，而检查点哈希已经为这段历史作了担保。
- **RingCT 签名和范围证明同样依靠检查点。** 守护进程自己的代码注释指出，输出范围证明的验证是同步中最大的一项开销，因为每个输出都带有一个范围证明，包括每个区块的 coinbase。
- **其他检查全部照常进行。** 区块链接关系、LWMA 难度规则、时间戳规则、区块大小和结构、密钥镜像的双花检查，以及每个输入与其签名之间的绑定检查都照常运行。输出和密钥镜像照常写入节点索引，所以钱包扫描和余额与完整同步后相同。

跳过只适用于延伸节点当前链顶的区块，并且只在最后一个检查点之下。节点越过该高度后，会完整验证每个新区块，包括工作量证明。

### 检查点如何锁定可信的链

当节点向最后一个检查点同步时，任何位于检查点高度的区块都必须与文件中的哈希完全一致。哈希不同的区块会以检查点不匹配的错误被拒绝。由于检查点每 100 个区块一个，对等节点无法把快速同步中的节点引向另一段历史：下一个检查点高度就会暴露它。

检查点还能加快每次重启。守护进程启动时如果磁盘上已有区块，它会找到与已存储的链相符的最高检查点，只重新检查其后的区块，而不必从创世区块开始遍历整条链。

## 新节点会自动下载检查点吗？

<Answer>
会。Blocknet 守护进程启动时，如果数据目录中没有 checkpoints.dat，它会从 https://bntpool.com/checkpoints.dat 下载该文件，加载后快速同步到最后一个检查点。下载是尽力而为的：如果无法完成，节点照样启动，并完整验证整条链。
</Answer>

这个地址是写在 Blocknet 核心程序中的默认值，所以开箱即用时，每个新节点都会用 bntpool.com 发布的文件快速同步。守护进程只在文件不存在时下载，请求时限为 15 秒，下载上限为 32 MiB。

## 如何一步步使用 bntpool.com 检查点？

如果你想自己获取并核对文件，或者想在长时间重新同步之前更新旧的副本，可以按以下步骤手动完成。

### 1. 找到数据目录

检查点文件位于节点的数据目录中，与链数据库放在一起：

- **`blocknet` 管理程序：** macOS 和 Linux 上为 `~/.config/bnt/data/mainnet/`，Windows 上为 `C:\Users\<you>\.config\bnt\data\mainnet\`，除非你在其配置中设置了 `data_dir`。
- **直接运行核心程序：** 启动目录下的 `./blocknet-data-mainnet`，或你通过 `--data` 指定的目录。

### 2. 下载 checkpoints.dat

打开 [Checkpoints](/checkpoints) 并点击 **Download checkpoints.dat**，或在终端中获取。下面的示例写入核心程序的默认数据目录：

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

下载到已有文件的位置会覆盖它，这就是更新旧副本的方法。

### 3. 核对 SHA-256

Checkpoints 页面公布了当前文件的 SHA-256。计算你的副本的哈希并比较：

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

在 macOS 上请改用 `shasum -a 256`。每当矿池增加一个检查点，哈希就会变化，所以请在下载后立即与页面比较。

### 4. 在区块浏览器上抽查检查点

你可以在公开的 [Blocknet 区块浏览器](https://explorer.blocknetcrypto.com)上核对任意一行。打开文件中某个高度的区块页面，例如 `explorer.blocknetcrypto.com/block/68900`，把它的哈希与 68,900 那一行比较。我们在 2026-10-25 对最后一个检查点做了这项核对，哈希一致。

### 5. 启动节点

启动或重启守护进程。它会在启动时读取 `checkpoints.dat`，并快速同步到最后一个检查点。如果你以交互式命令行（`--cli`）运行核心程序，`load-checkpoints` 命令会把文件加载到正在运行的节点中，并显示快速同步的目标高度：

```text
> load-checkpoints
```

检查点在节点落后时发挥作用。一旦你的链越过最后一个检查点的高度，节点就已经在完整验证每个区块，新的文件不会带来任何变化。

## 如何用脚本检查检查点文件？

矿池在 `https://bntpool.com/api/checkpoints` 以 JSON 公布文件信息，无需 API 密钥：

| 字段 | 含义 |
|---|---|
| `available` | 矿池是否有可提供的文件 |
| `url` | 下载地址 |
| `entries` | 文件中的检查点数量 |
| `latest_height`、`latest_hash` | 最高的检查点及其区块哈希 |
| `bytes`、`sha256` | 文件大小和 SHA-256 |
| `updated_at` | 文件最近一次更新的时间（Unix 时间戳） |

下载响应本身带有 `X-Checkpoint-Height` 和 `X-Checkpoint-Sha256` 头，以及根据哈希生成的 `ETag`，所以脚本可以发送 `If-None-Match`，在没有变化时快速得到 `304 Not Modified`：

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

响应最多可缓存 60 秒，对于约每八小时变化一次的文件来说已经足够。

<Note>
blocknetcrypto.com 表示，计划中的 Faraday 升级将带来更轻量的工作量证明，让新节点更快追上链。在此之前，检查点是让节点快速同步到最新的方法。
</Note>

## 同步好的节点可以做什么？

同步好的节点为 Blocknet 钱包和节点 API 提供支持，也是你用 `--mode daemon` 单独挖矿时 seine 连接的对象。[Blocknet 矿池挖矿与单独挖矿](/blog/zh-hans/pool-vs-solo-mining-blocknet/)介绍如何判断单独挖矿是否适合你的硬件。如果你想不运行节点就开始挖矿，seine 默认就在 bntpool.com 挖矿：[seine 安装指南](https://seineminer.com/zh-hans/blog/how-to-mine-blocknet/)带你获得第一批份额，[在 bntpool.com 挖矿](/blog/zh-hans/mine-blocknet-on-bntpool/)介绍矿池这一侧。

<CTA target="pool" title="在提供 Blocknet 检查点的矿池挖矿" text="几分钟内加入 bntpool.com，实时查看你的算力、份额和付款。" />
