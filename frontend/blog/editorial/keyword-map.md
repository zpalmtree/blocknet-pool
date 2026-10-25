# bntpool.com blog keyword map

Which bntpool.com blog URL owns which search job. The build fails if a published post has no row.
Same rules as the seineminer.com keyword map (seine repo, `site/editorial/keyword-map.md`); never
target a search job a seineminer.com post already owns.

## Current ownership

| Primary keyword cluster | URL | Search job | Supporting phrases |
|---|---|---|---|
| bntpool.com, bntpool stratum address, Blocknet mining pool | `/blog/mine-blocknet-on-bntpool/` | Join and use bntpool.com with seine: pool address, address or `@name` login, rig names, confirming shares on My Stats, the Get Started page | stratum+tcp://bntpool.com:3333, mine to @name, Blocknet pool worker name, several rigs one address, bntpool 矿池地址 |
| bntpool PPLNS, Blocknet pool payouts | `/blog/how-pplns-payouts-work/` | How bntpool.com splits block rewards and when it pays | PPLNS 6-hour window, 60 confirmations, 0.1 BNT minimum payout, estimated vs confirmed rewards, pool effort and luck, 矿池 PPLNS 收益支付 |
| bntpool stats, Blocknet miner stats, bntpool API | `/blog/track-your-miner-on-bntpool/` | Read your miner's stats and the pool's public pages, and query the public API | check Blocknet hashrate on pool, bntpool.com API miner endpoint, pool hashrate differs from miner, Blocknet pool luck/effort page, bntpool status page, Home Assistant mining sensor, 矿池统计 / 礦池統計, 查看矿机算力 |
| blocknet block reward, blocknet emission schedule | `/blog/blocknet-block-reward-schedule/` | Look up the reward per block by month, the tail, supply before the tail | blocknet supply, blocknet tail emission, BNT per block, blocks per day, 区块奖励时间表 / 區塊獎勵時程表 |
| blocknet fair launch | `/blog/blocknet-fair-launch/` | Show that Blocknet had no premine, dev fund or treasury, how to verify it, and how to mine it | Blocknet premine, Blocknet genesis block, Blocknet dev fund, Blocknet no premine, Blocknet 公平启动, Blocknet 有预挖吗 |

## Boundaries with seineminer.com

| seineminer.com owner | bntpool.com owner | Boundary |
|---|---|---|
| `/blog/how-to-mine-blocknet/` (install seine, first shares on any OS) | `/blog/mine-blocknet-on-bntpool/` | The seineminer.com guide owns installing the miner; the pool post owns joining and using the pool and links to the guide for installation. |
