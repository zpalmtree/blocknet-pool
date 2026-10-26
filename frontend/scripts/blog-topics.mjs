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
    "mine-blocknet-on-bntpool",
    "what-is-a-mining-pool"
  ],
  "troubleshooting": [
    "accepted-rejected-stale-shares",
    "track-your-miner-on-bntpool",
    "what-is-hashrate"
  ],
  "rewards": [
    "how-pplns-payouts-work",
    "mining-pool-luck-explained",
    "estimate-blocknet-mining-rewards",
    "blocknet-block-reward-schedule",
    "pool-vs-solo-mining-blocknet"
  ],
  "blocknet": [
    "blocknet-fair-launch",
    "blocknet-checkpoints-fast-sync"
  ]
};
export function topicFor(slug) {
    const matches = TOPICS.filter(t => groups[t.id]?.includes(slug));
    if (matches.length !== 1) throw new Error(`Assign exactly one blog topic to ${slug}`);
    return matches[0];
}
