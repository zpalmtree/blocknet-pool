// Every published article has one topic; translations share the source slug.
export const TOPICS = [
  {
    "id": "getting-started",
    "key": "topicGettingStarted"
  },
  {
    "id": "hardware",
    "key": "topicHardware"
  },
  {
    "id": "troubleshooting",
    "key": "topicTroubleshooting"
  },
  {
    "id": "rewards",
    "key": "topicRewards"
  },
  {
    "id": "blocknet",
    "key": "topicBlocknet"
  }
];
const groups = {
  "getting-started": [
    "change-mining-wallet-address",
    "mine-multiple-computers-one-wallet",
    "stratum-mining-protocol",
    "mine-blocknet-on-bntpool",
    "what-is-a-mining-pool"
  ],
  "troubleshooting": [
    "internet-speed-mining",
    "accepted-rejected-stale-shares",
    "track-your-miner-on-bntpool",
    "what-is-hashrate"
  ],
  "rewards": [
    "mine-blocknet-part-time",
    "pool-payout-missing-from-wallet",
    "mining-pool-fees-vs-miner-fees",
    "block-confirmations-mining-rewards",
    "mining-difficulty-explained",
    "how-pplns-payouts-work",
    "mining-pool-luck-explained",
    "estimate-blocknet-mining-rewards",
    "blocknet-block-reward-schedule",
    "pool-vs-solo-mining-blocknet"
  ],
  "blocknet": [
    "how-mining-secures-blocknet-payments",
    "blocknet-fair-launch",
    "blocknet-checkpoints-fast-sync"
  ]
};
export function topicFor(slug) {
    const matches = TOPICS.filter(t => groups[t.id]?.includes(slug));
    if (matches.length !== 1) throw new Error(`Assign exactly one blog topic to ${slug}`);
    return matches[0];
}
