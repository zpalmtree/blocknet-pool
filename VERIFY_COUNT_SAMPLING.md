# Count-based share verification

Status: implemented behind `verification_policy` (default `"ratio"`, which
leaves today's behaviour unchanged). Not deployed. Intended only if verifier
load nears capacity before the Faraday fork; from the fork every share is
checked inline (one BLAKE2b compression) and sampling stops mattering.

## Problem

Share verification is Argon2id (2 GiB, ~540 ms, memory-bandwidth bound,
~11 verifies/s practical capacity). Today every address is verified in
proportion to its shares:

- the live sampler audits `sample_rate` (0.08 in growth mode) plus every
  `min_sample_every`-th share (25), about 11.7% of shares;
- the payout cap pays provisional difficulty only up to 19x verified
  difficulty, so an address needs 5% verified difficulty for full credit;
- the backlog hold trips when provisional difficulty inside the 15-minute
  provisional delay exceeds 8x verified (verified below ~11.1%), and both
  holds trigger live hold audits (25%) and background backfill.

So a farm sending 7,100 shares/h gets ~830 audits/h, while its sampling
noise around 11.7% keeps tripping an 11.1% hold. Statistically, a few dozen
checks per hour say as much about that farm as 830 do (see the security
section). The ratio rules spend most verifier time on the 41 largest
addresses, which send ~67% of shares.

## Policy (`verification_policy: "count"`)

Definitions: `c0` = the ratio sampler's coverage, `1 - (1 - sample_rate)(1 -
1/min_sample_every)` (11.68% at 0.08/25); `E` = the address's accepted shares
in the last `count_window` (1 h), counted from its in-memory PPLNS window.

1. **Sampling.** An address is sampled with probability
   `p = max(count_target_samples / E, count_coverage_floor)` when that is
   below `c0`, as a single random draw (no every-Nth component). Otherwise
   (smaller addresses, `E <= count_target_samples / c0`, ~428 shares/h at the
   defaults) it keeps the ratio sampler unchanged. Scaling is never applied:
   - to a source (address + IP group) that is not *established* (fewer than 3
     verified samples on that pair), so a fresh IP on a big address is sampled
     at `c0`;
   - for the rest of the PPLNS window after any failed sample on the address,
     so evidence of fakes restores today's detection rate;
   - during warmup. In Shed mode the rate is `min(shed rate, p)`; Emergency
     still samples nothing.
2. **Count coverage of a window.** An address's PPLNS window is *covered by
   count* when it holds at least `count_payout_min_samples` (30) clean
   verified samples and every payout-eligible provisional share has a clean
   sample of the same address within `count_sample_radius` (10 min) of it.
   The radius is the "recent ones" requirement: any stretch the verifier did
   not look at (overload, outage) is uncovered, wherever it is.
3. **Payout.** Provisional credit is the larger of the ratio credit (today's
   `min(P, 19 V)`) and the count credit: the difficulty of covered
   provisional shares, scaled by `min(1, samples / count_payout_min_samples)`.
   Count mode therefore never pays an address less than ratio mode does;
   small miners are paid exactly as today. Addresses under forced review still
   get no provisional credit; `payout_min_verified_shares` still applies.
4. **Holds.** The ratio holds still decide, but are waived when the count
   criterion is met:
   - backlog hold: waived when the shares inside the provisional delay hold
     at least `floor(count_target_samples x delay / count_window / 4)` clean
     samples (3 at the defaults, against 12.5 expected);
   - payout-coverage hold: waived when the window is covered by count (the
     same condition under which payouts give full credit).
   A held count-sampled address has at most `4 p` of its shares hold-audited
   (instead of 25%), since a handful of samples clears its hold.
5. **Backfill.** For a held address, background audits pick whichever plan
   needs fewer audits: today's (oldest unaudited shares until the 19x cap is
   met) or the count plan (a greedy interval cover of the uncovered shares,
   one audit per uncovered stretch of up to `2 x radius`, plus evenly spread
   audits up to `count_payout_min_samples`). The count plan is used only if
   it can fully satisfy the count criterion.
6. **Revocation and strikes are unchanged.** A proven fake strikes its
   source (full strike: revoke every unverified share of the pair in the
   PPLNS window, ban, disconnect; soft strike for established sources within
   the honest-error band: reject, then 10 boost audits). Unattributed
   failures escalate the address to forced review as before. None of that
   code reads the policy.

### Configuration

| Setting | Default | Reload | Validation |
|---|---|---|---|
| `verification_policy` | `"ratio"` | restart | `"ratio"` or `"count"` |
| `count_target_samples` | 50 | hot | > 0, <= 1e6 |
| `count_window` | `"1h"` | hot | > 0, <= `pplns_window_duration` |
| `count_coverage_floor` | 0.01 | hot | (0, 1] |
| `count_payout_min_samples` | 30 | restart | >= 1 |
| `count_sample_radius` | `"10m"` | restart | > 0, <= `pplns_window_duration` |

The policy and the two payout-rule settings are restart-only because the
payout processor reads its config at startup; letting validation change rule
while payouts kept the old one would underpay large farms (validation stops
holding them, payouts still cap them at 19x). With the gateway, a restart is a
handover and disconnects no one.

## Security argument

Model. Sampling is content-blind and unobservable: the decision is made at
submit time, the miner gets the same accept either way, and a sampled fake is
always detected (the pool recomputes the hash; a fake fails "invalid share
proof" with certainty). Each share of an address is sampled independently
with probability `p`. Credit for a share is only applied when its block
matures (60 confirmations), and a strike revokes the source's unverified
shares in the whole 6-hour PPLNS window before that.

Key property. Under the count policy the number of samples per hour of a
scaled address is `p E = max(count_target_samples, floor x E)`, i.e. at least
50/h whatever its size. A cheater faking a fraction `f` of an address's
shares is therefore caught at rate `>= 50 f` per hour regardless of how many
shares it sends, and inflating its own volume with fakes raises `f` rather
than lowering the sample count. Since `p >= count_coverage_floor` always, the
expected number of fakes before the first detection is at most
`1 / floor = 100` under any strategy, including rate manipulation (e.g. a big
farm that stops mining and drips fakes while its 1-hour rate estimate lags).

### Numbers (growth-mode config, 2026-10-29 mix)

`p` and expected fakes before the first detection, `(1 - p) / p`:

| Address | Policy | p | samples/h | fakes before 1st detection | = output time |
|---|---|---|---|---|---|
| largest, 7,100/h | ratio | 0.117 | 829 | 7.6 | 4 s |
| largest, 7,100/h | count | 0.010 | 71 | 99 | 51 s |
| 2,000/h | ratio | 0.117 | 234 | 7.6 | 15 s |
| 2,000/h | count | 0.025 | 50 | 39 | 72 s |
| median, 68/h | both | 0.117 | 7.9 | 7.6 | 7.6 min |

Time to the first detected fake, and the chance a stream with fake fraction
`f` survives a whole 6-hour window unseen:

| Address | Policy | f = 100% | f = 10% | f = 3% |
|---|---|---|---|---|
| largest | ratio | 4 s / 0 | 43 s / 1e-216 | 2.4 min / 1e-65 |
| largest | count | 51 s / 1e-185 | 8.5 min / 3e-19 | 28 min / 3e-6 |
| 2,000/h | count | 72 s / 5e-131 | 12 min / 9e-14 | 40 min / 1e-4 |
| median | both | 7.6 min / 2e-21 | 76 min / 0.009 | 4.2 h / 0.24 |

Every count-mode large-address figure is better than what the median miner
gets today under either policy; count mode makes the per-hour detection power
roughly uniform across large addresses instead of proportional to size.

### Worst-case gain and loss for a cheater faking a fraction f

- **Unestablished source, or no clean history (full strike on first
  detection).** Gain is zero unless a fake stays undetected until its block
  matures. Expected fakes accepted before detection are `<= 1/p` (8.6 today;
  up to 100 on the largest addresses in count mode, at their share size, i.e.
  under a minute of that farm's output), and a source sending them slowly
  enough to outlast maturity collects at most that many per identity (IP
  group). Count mode does not scale fresh sources, so for them this stays at
  today's 8.6. Loss on detection: every unverified share of the pair in the
  window. For a mixed honest/fake pair on the largest farm that is ~6 h of
  output (~42,000 shares at `p = 0.01`, ~37,600 today) against a gain of `f`
  per share; expected value is negative for every `f > 0` as long as the
  expected samples per window (`6 p E`, 426 for the largest in count mode) is
  well above 1.
- **Established source within the honest-error band (soft strike).** The
  strike rule tolerates ~2% invalid proofs (+3 sigma) from sources with clean
  history: a detected fake is rejected, 10 boost audits follow, no revocation.
  The steady gain inside the band is `f (1 - p)`, ~2% of output, under both
  policies; this band is the pool's main standing exposure and count mode
  does not change its width. What count mode changes is how fast a cheater
  just above the band reaches a full strike, since that test needs samples:
  at `f = 3%`, ~1,960 samples (2.4 h today on the largest farm, 27.6 h with
  count sampling alone). The failure pause closes most of that gap: the first
  sampled fake (~28 min at `f = 3%`) returns the address to `c0`, so the full
  strike lands after ~2.8 h, before the shares mature, versus 2.4 h today. At
  `f = 5%`: 0.31 h today, ~0.6 h with the pause; at 10%: 4 min today,
  ~12 min. A cheater who keeps `f` tiny avoids detection under both policies;
  the band, not the sampler, bounds that gain.
- **Median miner.** Identical to today in every respect.

### What revocation recovers

A full strike revokes every unverified share the pair submitted inside the
PPLNS window (6 h), honest and fake, before the window's blocks mature, so
all fakes detected within ~maturity of being submitted are recovered in full
and the honest provisional credit is forfeited on top. Count mode samples
fewer of a large farm's shares, so more of the revoked shares are
unverified: the forfeit grows from ~88% to ~99% of the window. Nothing is
recovered for blocks that have already been credited; under both policies
that requires a fake to survive past maturity, whose probability is in the
"6-hour window" column above.

## Parameter choices and failure modes

- `count_target_samples` = 50/h. Rule of three: 0 failures in 300 samples
  (one PPLNS window) bounds the fake fraction at 1% with 95% confidence for
  every scaled address. Lower values cut load further but lengthen time to
  detection linearly. Hot-reloadable.
- `count_coverage_floor` = 0.01. Caps expected fakes before detection at 100
  and keeps the largest farm (7,100/h) at 71 samples/h. A lower floor saves
  little at today's mix (largest farms are near 50/h already) and raises the
  worst case.
- `count_payout_min_samples` = 30 per window, `count_sample_radius` = 10 min.
  At 50 samples/h the mean gap is 72 s, so the chance a given share has no
  sample within 10 min is `e^-16.7` (~6e-8); a 42,600-share window expects
  0.002 uncovered shares, so an honest farm is essentially never held for
  bad luck. A smaller radius would start tripping holds on honest farms; a
  larger one tolerates longer unverified stretches. Thirty samples are reached
  in ~36 minutes by any scaled address.
- Failure modes:
  - *Rate estimate lag* (1-hour window): bounded by the floor, as above.
  - *Restart / handover*: source establishment is in memory, so every pair
    is sampled at `c0` until it has 3 verified samples (~26 shares); a brief
    load bump, not a weakening. The failure pause is also in memory; a restart
    forgets it (conservative alternative: persist it).
  - *Griefing*: anyone can submit a fake to a public address. That pauses
    count scaling for that address for 6 h (load returns to today's for it);
    it cannot reduce anyone's verification or credit.
  - *Overload*: Shed/Emergency reduce or stop sampling exactly as today. The
    stretch is then uncovered, so the coverage hold and the count backfill
    re-audit it (about one audit per 20 minutes of gap per address, versus 5%
    of the window's difficulty under the ratio rule).
  - *Honest hardware errors*: a farm with occasional invalid proofs loses
    scaling for 6 h after each one. A farm erring more than ~1 per 6 h gets
    no saving, but no worse treatment than today.
  - *CPU*: the count criterion is one pass over the address's in-memory
    window, like the existing ratio checks, run under the same lock.
- **API previews.** The API's pending-balance estimate and reward-status
  labels still apply the ratio cap (they use per-address aggregates, not
  share timelines). With count mode on they would understate large farms'
  pending balances and label them "capped provisional". Payouts themselves
  are correct. This must be updated before enabling (see open items).

## Interaction with growth mode

Growth mode changes `sample_rate` and the share size, both of which feed the
count policy: `c0` follows the growth sample rate, and larger shares mean a
lower `E`, so fewer addresses are scaled. Under the count policy verifier
load is roughly `sum(min(c0 E, max(50, 0.01 E)))` per hour: proportional to
volume for small addresses but ~50/h per large one, so it depends on the
number of large addresses more than on the share rate. Growth mode keys off
the share rate, so with count mode it would switch on earlier than verifier
load requires. It still reduces submit-path and database load, so leave its
thresholds as they are when enabling count mode, and revisit them once
production numbers exist.

## When to enable

Enable when sustained verifier demand approaches capacity (for example
`BlocknetPoolBeyondGrowthCapacity`, or live + background audits above ~8-9/s
for an hour with growth level 2 already active), and after:

1. the API preview follow-up below is done;
2. one production reload in shadow (or a short enabled window) confirms the
   replay: large-address coverage ~2-3%, no rise in holds or in
   `forced_verify_addresses`, payouts for the top addresses unchanged.

Rollback is a config change plus a handover; provisional shares accumulated
under count mode are still creditable under ratio (credit falls back to the
19x cap for large farms until backfill restores 5%, which is the main cost
of switching back).

## Replay results (T2)

`replay_production_share_mix`, 2026-10-29 mix (380 addresses, synthesized
from the 2026-10-27 counts halved for growth mode plus 101 small addresses:
max 7,100/h, median ~75/h, 39 addresses >= 500/h with ~70% of shares),
30 shares/s at 1x, growth-mode sampling (0.08, every 25th, warmup 5, hold
0.25), capacity 11/s, 500 ms single hash, 5x speedup, 40 simulated minutes,
6 hours of seeded history per address and a share store behind background
audits (`VERIFY_REPLAY_BACKFILL=1 VERIFY_REPLAY_SEED_HOURS=6`).

| Load | Policy | Verifies/s (live + background) | Normal/Shed/Emergency % | Audit drops/s | Large addr. coverage (samples/addr/h) | Small addr. coverage |
|---|---|---|---|---|---|---|
| 1x (30/s) | ratio | 4.71 (3.88 + 0.78) | 100/0/0 | 0 | 12.8% (246/h), 39 addr. | 21.4% |
| 1x | count | 2.58 / 2.62 (1.75-1.80 + 0.76) | 100/0/0 | 0 | 2.5-2.8% (49-53/h) | 21.5% |
| 2x (60/s) | ratio | 8.31 (7.52 + 0.74) | 100/0/0 | 0 | 12.3% (333/h), 61 addr. | 18.6% |
| 2x | count | 3.45 / 3.48 (2.66-2.71 + 0.73-0.75) | 100/0/0 | 0 | 2.1% (55-58/h) | 17.5% |
| 3x (90/s) | ratio | 9.11 (9.04 + 0) | 5/57/38, 64 transitions | 0.20 | 10.5% | 8.3% |
| 3x | count | 4.08 (3.45 + 0.57) | 100/0/0 | 0 | 1.9% (63/h), 79 addr. | 14.8% |

Count rows show two runs where repeated. Capacity freed: ~2.1 verifies/s
at 1x (-45%) and ~4.9/s at 2x (-58%); at 2x the ratio policy uses ~76% of
the 11/s capacity and the count policy ~31%. At 3x (no growth level 2 in
the replay) the ratio policy is overloaded most of the time and its coverage
of small addresses halves, while the count policy stays Normal with small
addresses at 15%. Live-audit queue wait p99 at 2x: 1.25 s (ratio) vs <=5 ms
(count). No block candidate was refused in any run (candidate p99
0.5-0.9 s). Raw: `perf-results/2026-10-29/pool-verify/replay-count-sampling/`.

The small-address coverage (~21% at 1x) is above the sampler's 11.7%
because backlog and coverage holds trip often for addresses with few
shares; that is today's behaviour, unchanged by count mode.

Background audits in the replay (~0.8/s) are well below production's ~2.4/s:
production backfills the union of all unconfirmed blocks' windows (~11 h),
and also runs degraded-window reviews and source boost audits, which the
replay does not model. Count mode's backfill savings on large addresses are
therefore likely understated; live-audit numbers match production (~3.9/s
replayed vs ~4.2/s live).

## Open items for the owner

- Update the API's pending estimate and reward-status to the count rule
  (`payout::count_policy_provisional_credit` takes the share timeline).
- Decide whether to persist the failure pause and source establishment
  across handovers (today both reset to the conservative side).
- The honest-error band (2% + 3 sigma, soft strikes) is the dominant standing
  exposure under both policies; count mode slows full strikes just above it
  on large addresses (by ~20% at f = 3% up to ~3x at f = 10%, all still well
  before maturity) even with the pause. Tightening the band
  for large addresses is a separate decision.
- Production share mix: the replay uses a synthesized 2026-10-29 mix; rerun
  with a fresh `address-share-counts-1h` export before enabling.
