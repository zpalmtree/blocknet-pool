---
title: 'What is Stratum? How miners talk to a pool'
description: 'Follow the Stratum conversation between seine and bntpool.com: login, mining jobs, share targets, submissions and replies, plus how to read connection activity.'
date: 2026-10-26
tags: ['Mining basics', 'Stratum', 'Pool mining']
faq:
  - q: 'What does Stratum do in pool mining?'
    a: 'Stratum carries the conversation between a miner and a pool: identifying the mining session, delivering work and returning share submissions and responses. The miner performs the hashing locally. The pool connects that work to its upstream node and maintains the accounting needed for its payout scheme.'
  - q: 'Is a Stratum address the same as a website address?'
    a: 'They serve different connections. The bntpool.com website uses HTTPS for pages and API data, while its documented mining endpoint is stratum+tcp://bntpool.com:3333. Enter the mining endpoint in the miner''s pool setting. Opening the website successfully alone does not establish a mining connection.'
  - q: 'Does an accepted Stratum share mean a payout was sent?'
    a: 'No. A share response reports a submission outcome within the mining session. Verification, a network-valid block, reward eligibility and a completed payout are separate stages. Follow accepted mining activity on My Stats and use the payout guide to understand confirmations, PPLNS and the payment threshold.'
related: [mine-blocknet-on-bntpool, what-is-a-mining-pool, mining-difficulty-explained, accepted-rejected-stale-shares]
---

Follow a mining job from bntpool.com to seine and a share back to the pool. Understanding that conversation makes connection messages, job updates and submission replies easier to read. This guide describes bntpool.com's implementation; for the initial endpoint and payout-address setup, use the [pool setup guide](/blog/mine-blocknet-on-bntpool/).

## What is Stratum in mining?

<Answer>
Stratum is the communication layer a miner uses to receive pool work and submit results. With seine and bntpool.com, a persistent TCP connection carries JSON messages for login, jobs and shares. Your machine computes the hashes locally, while the pool obtains block templates from its node and processes submitted work.
</Answer>

*A miner sends login and share messages to a pool, receives jobs and replies, and the pool exchanges templates and block candidates with its node.*

## The endpoint identifies the mining connection

bntpool.com's documented endpoint is `stratum+tcp://bntpool.com:3333`. The scheme tells the miner which transport to use, the hostname identifies the service and the port selects its mining listener. The browser's `https://bntpool.com` connection serves a different purpose. Copy the complete mining endpoint from [Get Started](/start) into the pool setting so the client can open the intended connection.

The persistent connection lets both sides send messages as work becomes available. bntpool uses newline-delimited JSON, with request IDs connecting replies to requests. These details describe this pool's wire format; a miner must support that format and its capabilities as well as Blocknet's hashing algorithm. The [protocol source](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) records the current fields.

## What happens when the miner logs in?

The client sends a `login` request containing its payout address, rig name, protocol version and supported capabilities. The pool checks the login and returns its protocol information and required capabilities. This establishes how the session will exchange work. A successful TCP connection is the first step; an accepted login shows that the application-level conversation has also begun.

The rig name helps identify one machine's activity under an address. Use distinct names when following several rigs, while keeping the intended payout address consistent. Login needs the receiving address, not the wallet's recovery phrase or private spending key. Once the session is accepted and work is available, the pool sends a `job` message so the miner can begin hashing.

## What information is in a mining job?

A job includes identifiers for the assignment and underlying template, the header data, block height, share target, network target and an assigned nonce range. A nonce is the changing value the miner tries while hashing. The range gives that session a defined search space. The job identifier lets the pool connect a later submission to the work and conditions originally assigned.

The share target determines which results should be submitted as evidence of work. The network target determines which results can be block candidates. Most qualifying shares serve the pool's accounting without becoming blocks. The [difficulty guide](/blog/mining-difficulty-explained/) explains the relationship between target size and expected work; the protocol supplies those targets to the miner as part of its assignment.

## Jobs can change while the connection stays open

A new job does not by itself mean the miner disconnected. The pool can distribute updated work as its upstream template changes or send a new assignment carrying a changed share difficulty. Its variable-difficulty system adjusts the submission threshold to the observed flow of work. The miner follows the new assignment while the persistent connection continues carrying messages in both directions.

Keep job IDs and template IDs distinct when reading technical logs. Multiple assignments can refer to the same underlying template while carrying different session details. A height alone does not describe every change in work. For a normal operator, the useful checks are that jobs arrive, hashing progresses and submissions receive replies; the [tracking guide](/blog/track-your-miner-on-bntpool/) explains where to see the resulting activity.

## What does a share submission contain?

When seine finds a qualifying result, it sends a `submit` request identifying the job, nonce and claimed hash. The pool uses the assignment context to process it and returns a response associated with the request ID. The claim is part of the message, while the pool's verification and block-validation paths determine how the result proceeds. A candidate block is submitted upstream to the daemon for validation.

The response can report acceptance, verification status and difficulty information. Read these as separate fields rather than treating every successful reply as a block discovery. Pool accounting and payout sending happen beyond this message exchange. The [share outcomes guide](/blog/accepted-rejected-stale-shares/) explains submission results, and the [PPLNS guide](/blog/how-pplns-payouts-work/) explains how eligible work relates to rewards and payments.

## How do you use this sequence when checking a miner?

Read the stages in order: endpoint reached, login accepted, job received, hashing active and submissions answered. If a connection is lost, the client reconnects and establishes a session before receiving more work. Use the exact log message and its timestamp to locate the stage involved. A working pool webpage is useful context, but it does not prove that the mining port is reachable from that machine.

Open [My Stats](/stats) for the intended payout address to follow the rig, and check [pool status](/status) for service context. Allow share-based hashrate estimates time to accumulate observations after startup or reconnection. Keep the protocol conversation separate from payout timing: login gets the session ready, jobs supply work, shares report results and the pool's accounting determines the later reward stages.

<CTA target="pool" title="Connect seine to bntpool.com" text="Use the pool setup page, then follow jobs, shares and your rig&#x27;s activity on My Stats." />
