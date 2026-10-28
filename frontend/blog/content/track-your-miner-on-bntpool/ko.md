---
title: bntpool.com에서 채굴기 상태 확인하기
description: bntpool.com에서 주소나 @name으로 해시레이트, 셰어, 잔액, 지급 내역을 확인하세요. 블록, 운, 가동률을 살펴보거나 공개 API를 활용할 수 있습니다.
tags: [bntpool, 채굴 통계, 풀 API, Blocknet]
sourceHash: '40197b2fa166'
translation: draft
faq:
  - q: 'bntpool.com의 해시레이트가 seine에 표시되는 값과 다른 이유는 무엇인가요?'
    a: 'seine는 머신이 계산하는 모든 해시를 세지만, 풀은 승인한 셰어의 난이도를 바탕으로 지난 한 시간의 평균 해시레이트를 추정합니다. 셰어는 무작위 시점에 도착하므로 풀의 값은 더 많이 움직이다가 시간이 지나며 안정됩니다. 정확히 비교하려면 My Stats의 24시간 차트와 같은 기간의 seine 평균을 비교하세요.'
  - q: '@name으로 bntpool.com 통계를 조회할 수 있나요?'
    a: '네. My Stats 검색창에 @name이나 $name 같은 Blocknet ID 짧은 이름을 입력하세요. 페이지가 blocknet.id를 통해 주소로 변환하고 통계를 표시한 다음, 변환된 전체 주소를 검색창에 넣습니다. 스크립트에서 풀 API를 호출할 때는 이 전체 주소를 사용하세요.'
  - q: 'bntpool.com API에 API 키가 필요한가요?'
    a: '아니요. 풀 통계, 블록, 지급, 운, 상태, 개별 채굴자 엔드포인트는 HTTPS를 통해 공개 JSON을 제공하므로 curl 명령이나 홈 대시보드에서 가입 없이 읽을 수 있습니다. 금액은 원자 단위이므로 100,000,000으로 나누면 BNT가 됩니다. 조회 간격은 30~60초로 설정하세요.'
  - q: 'My Stats의 Estimated Rewards가 0인 이유는 무엇인가요?'
    a: 'Estimated Rewards는 셰어가 지급 계산 창 안에 있는 동안 풀이 블록을 찾으면 나타납니다. 새 채굴자는 몇 라운드 동안 채굴해야 할 수 있습니다. 승인된 셰어와 해시레이트가 먼저 표시되고, 이후 예상 보상이 나타납니다. 이 보상은 성숙하면 Confirmed Rewards로 이동하며 자동 지급됩니다.'
---

첫 셰어가 승인되면 My Stats를 열어 풀이 작업을 받고 있는지 확인하세요. 내 컴퓨터를 찾고, 시간을 두고 해시레이트를 비교한 다음, 예상 보상이 확정되어 지급되는 과정을 살펴봅니다. 스크립트와 공개 API는 마지막에 설명합니다.

아직 채굴을 시작하지 않았다면 [seine 설치 가이드](https://seineminer.com/ko/blog/how-to-mine-blocknet/)를 따라 몇 분 안에 첫 셰어를 제출할 수 있습니다.

## bntpool.com에서 채굴기 통계를 어떻게 확인하나요?

<Answer>
bntpool.com/stats의 My Stats를 열고 Blocknet 주소나 @name을 붙여 넣은 뒤 Lookup을 누르세요. 해시레이트, 승인 및 거부된 셰어, 예상·컨펌·지급 잔액, 해시레이트 차트, 채굴 리그별 행, 최근 지급 내역이 표시됩니다. 탭이 열려 있는 동안 페이지가 자동으로 갱신됩니다.
</Answer>

[My Stats](/stats)로 이동해 seine에 입력했던 주소나 [Blocknet ID 짧은 이름](https://seineminer.com/ko/blog/blocknet-id-pay-by-name/)을 입력하세요. `@name`이나 `$name` 같은 이름은 blocknet.id를 통해 주소로 변환되며, 검색창에는 해당 전체 주소가 표시됩니다. 페이지가 브라우저의 마지막 조회를 기억하므로 다음 방문 때도 자신의 수치를 바로 볼 수 있습니다.

1. **Shares Accepted**와 **Recent Shares**를 확인하세요. 승인된 셰어가 있으면 보상 잔액이 모두 0이어도 작업이 풀에 도착한 것입니다.
2. **Workers**에서 내 컴퓨터를 찾으세요. Seine의 이름과 일치하는지, 마지막 셰어 시간이 계속 갱신되는지 확인하세요.
3. **Blocks Found**가 0이어도 정상입니다. 이 수치는 내 컴퓨터가 해결한 블록 수이며, 셰어가 풀 보상을 배분받는지를 뜻하지 않습니다.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-first-mobile.webp" width="352" height="379"><img src="/blog/tracking/tracking-first-desktop.webp" width="1180" height="900" alt="먼저 승인된 셰어를 확인하세요. 실제 영어 인터페이스에 예시 데이터를 표시한 것으로, 실시간 수익이 아닙니다." loading="lazy" decoding="async"></picture>
<figcaption>먼저 승인된 셰어를 확인하세요. 실제 영어 인터페이스에 예시 데이터를 표시한 것으로, 실시간 수익이 아닙니다.</figcaption>
</figure>

## My Stats 페이지에는 무엇이 표시되나요?

페이지는 카드와 표 그룹으로 나뉘며, 각 그룹은 채굴기에 관한 한 가지 질문에 답합니다.

### 채굴과 셰어

**Mining** 카드에는 현재 **Hashrate**, 자신의 리그가 풀을 위해 해결한 **Blocks Found** 수, 시작 날짜인 **Mining Since**가 표시됩니다. 아래에는 **Shares Accepted**, **Shares Rejected**, **Reject Rate**, **Avg Difficulty**가 있습니다. 정상적인 리그는 승인된 셰어가 꾸준히 들어오고 거부율이 0에 가깝습니다.

**Hashrate History**는 1h, 24h, 7d, 30d 범위의 해시레이트를 그립니다. 리그가 밤새 작동했는지 확인할 때는 긴 기간의 차트가 가장 유용합니다.

### 리그별 행

머신을 여러 대 운영한다면 각각 이름을 지정하세요. 풀이 해시레이트, 승인 및 거부된 셰어, 마지막 셰어 시각과 함께 별도로 표시합니다. seine는 처음 실행할 때 `seine-` 뒤에 네 자리 숫자를 붙인 이름을 고릅니다. 명령줄에서 직접 지정할 수도 있습니다.

```sh
./seine --pool-worker rig-01
```

**Workers** 표에는 각 컴퓨터의 마이너 버전도 표시됩니다. 오래되거나 알 수 없는 버전이 있으면 통계 위에 알림이 나타나며 [seine 릴리스 페이지](https://github.com/zpalmtree/seine/releases)로 연결됩니다.

## 풀의 해시레이트가 seine와 다른 이유는 무엇인가요?

seine는 머신이 계산하는 모든 해시를 셉니다. 풀은 제출된 셰어만 볼 수 있으므로 지난 한 시간 동안 승인된 셰어의 총 난이도로 해시레이트를 추정합니다. 각 셰어는 무작위 사건입니다. bntpool.com은 연결마다 약 5분에 10개 셰어를 목표로 난이도를 설정하므로 짧은 구간에는 어느 정도 변동이 있습니다.

다음 두 가지 습관이 수치를 비교하는 데 도움이 됩니다.

- 평균을 비교하세요. My Stats의 24h 차트와 같은 기간의 seine 평균을 비교합니다.
- 새 리그에는 시간을 주세요. 방금 시작한 리그는 실제로 채굴한 시간만큼 평균을 내므로 첫 수치는 몇 개의 셰어에 의존합니다. 한 시간이 지나면 전체 한 시간 창이 적용됩니다.

Recent Shares 표를 보면 해시레이트 수치가 안정되기 전에도 셰어가 도착하는지 바로 알 수 있습니다.

## 첫 지급은 언제 받을 수 있나요?

첫 셰어부터 첫 지급까지 정해진 시간은 없습니다. 내 셰어가 지급 창 안에 있는 동안 풀이 블록을 찾아야 합니다. 이후 해당 블록에 60번의 확인이 필요합니다. **블록을 찾은 뒤** 평균 약 5시간이지만 실제 시간은 달라집니다. 확정 잔액이 현재 최소 지급액인 0.1 BNT에 도달하면 자동 지급 처리 대상이 됩니다. 현재 설정은 [Get Started](/start)에서 확인하세요.

**Recent Payouts**에서 지급 상태와 트랜잭션 링크를 확인하세요. 대기 중인 지급은 아직 네트워크에 전송되지 않았습니다. 전송된 지급이 지갑에 없다면 [지급 문제 해결 가이드](/blog/ko/pool-payout-missing-from-wallet/)를 참고하세요.

### 잔액

- **Estimated Rewards:** 풀이 찾았지만 아직 성숙 중인 블록에서 받을 몫입니다.
- **Confirmed Rewards:** 성숙을 마치고 다음 지급을 기다리는 잔액입니다.
- **Paid Balance:** 이미 주소로 보낸 모든 금액입니다.

처리 중인 보상이 있으면 **Estimated From Recent Blocks** 표에 각 블록의 높이, 예상 적립액, 남은 컨펌 수, 발견 시각이 표시됩니다. 각 높이는 [Blocknet 탐색기](https://explorer.blocknetcrypto.com)의 해당 블록으로 연결되어 직접 확인할 수 있습니다. [PPLNS 지급 방식](/blog/ko/how-pplns-payouts-work/)에서는 블록 보상 분배와 잔액 지급 시점을 설명합니다.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-rewards-mobile.webp" width="352" height="524"><img src="/blog/tracking/tracking-rewards-desktop.webp" width="1081" height="217" alt="예상·확정·지급 완료 잔액은 서로 다른 단계입니다. 실제 영어 인터페이스의 예시 금액이며, 예상 수익이 아닙니다." loading="lazy" decoding="async"></picture>
<figcaption>예상·확정·지급 완료 잔액은 서로 다른 단계입니다. 실제 영어 인터페이스의 예시 금액이며, 예상 수익이 아닙니다.</figcaption>
</figure>

### 지급과 최근 셰어

**Recent Payouts**에는 주소로 보내는 각 지급의 금액, 탐색기 트랜잭션 링크, queued·unconfirmed·confirmed 상태가 표시됩니다. **Recent Shares**에는 최근 20개 셰어의 작업, 리그, 난이도, 상태가 표시됩니다. 방금 시작한 머신이 풀에 도달하고 있는지 가장 빠르게 확인하는 방법입니다.

<span id="zero-hashrate"></span>

## seine이 채굴 중인데 My Stats의 해시레이트가 0인 이유는 무엇인가요?

먼저 지급 주소, 풀 연결, 승인된 셰어를 확인하세요. 로컬 해시레이트는 컴퓨터가 해시를 계산하고 있다는 뜻입니다. 풀 추정치에는 조회한 주소에 대해 승인된 셰어가 필요합니다.

1. **주소를 맞춰 보세요.** seine이 사용하는 것과 동일한 수신 주소 또는 등록된 `@name`을 조회하세요. 짧은 이름에 연결된 주소를 바꿨다면 seine을 다시 시작해 이름을 다시 확인한 뒤, 확인된 주소를 My Stats에서 조회하세요.
2. **풀을 맞춰 보세요.** seine이 `stratum+tcp://bntpool.com:3333`을 사용하는지 확인하세요. 다른 곳이나 솔로 모드에서 채굴하는 컴퓨터는 여기에 셰어를 추가하지 않습니다. [풀 설정 가이드](/blog/ko/mine-blocknet-on-bntpool/)에서 연결 방법을 설명합니다.
3. **Recent Shares를 확인하세요.** 승인된 셰어가 도착하고 페이지가 새로 고쳐질 때까지 기다리세요. 셰어는 일정하지 않은 간격으로 도착하므로, 로컬 해시레이트가 표시돼도 풀 추정치가 즉시 나오지는 않을 수 있습니다. 승인된 셰어가 도착하고 있다면 시간을 두고 평균을 비교하세요.
4. **연결 메시지를 읽으세요.** 아직 승인된 셰어가 없다면 아래 표를 참고하세요. 현재 장애는 [Status](/status)에서 확인하세요.

| seine의 메시지 | 다음 확인 사항 |
|---|---|
| `failed to connect to pool at` | 호스트 이름과 포트, 연결 상태, 포트 3333으로 나가는 TCP 연결이 허용되는지 확인하세요. |
| `login rejected:` | 뒤에 나오는 이유를 읽고 수신 주소 또는 짧은 이름을 확인하세요. 최신 seine 버전을 사용하세요. |
| `stale job` | 셰어가 오래된 작업을 가리킵니다. 새 작업과 승인된 셰어가 오는지 지켜보고, 계속되면 연결과 채굴기 버전을 확인하세요. |
| `submit interrupted during reconnect` | Seine이 다시 연결하는 중입니다. 연결이 복구되면 로그인 승인과 새 셰어 승인을 확인하세요. |

**Estimated Rewards**가 0인 것은 별개의 문제입니다. 풀 블록의 보상이 적립되기 전에도 셰어는 승인될 수 있습니다. [PPLNS 가이드](/blog/ko/how-pplns-payouts-work/)에서 순서를 설명합니다.

## 어떤 공개 페이지에서 풀 전체를 볼 수 있나요?

이 페이지들은 주소 조회 없이 풀의 모든 활동을 공개적으로 보여 줍니다. [대시보드](/)에는 연결된 채굴자, 풀과 네트워크 해시레이트, 발견한 블록, 평균 운 같은 주요 수치가 모여 있습니다.

### 블록

[Blocks](/blocks)는 bntpool.com이 찾은 모든 블록을 최신순으로 표시합니다. 높이와 탐색기 링크, 보상, 투입 작업 비율, 라운드 시간, 상태, 발견 시각을 볼 수 있습니다. Confirmed, Pending, Orphaned로 필터링해 각 블록의 상태를 확인하세요.

### 지급

[Payouts](/payouts)는 각 지급 묶음의 총액, 지급받는 채굴자 수, 트랜잭션 링크, 상태를 표시합니다. 묶음은 전파되는 즉시 unconfirmed로 나타나며 첫 블록 컨펌 직후 confirmed로 바뀝니다. 따라서 자신의 지급을 풀 쪽과 탐색기 양쪽에서 추적할 수 있습니다.

### 운

[Luck](/luck)은 각 라운드의 투입 작업 비율인 effort를 보여 줍니다. 이는 라운드 중 풀이 승인한 셰어 작업량을 블록 난이도로 나눈 값입니다. 100%는 예상한 작업량이 정확히 들었다는 뜻이며, 더 낮으면 풀이 더 빨리 블록을 찾았다는 의미입니다. 개별 라운드는 양방향으로 크게 흔들리므로 대시보드의 **Average Luck**을 살펴보세요. 대시보드의 **Pool Luck History** 표에서 **View All**을 누르면 이 페이지로 이동합니다.

### 상태

[Status](/status)는 풀 자체의 상태 페이지입니다. 풀, 공개 HTTP, API, Stratum, 데이터베이스, 데몬이 온라인인지, 현재 작업 템플릿이 얼마나 최신인지, 체인 높이, 지난 10분·6시간·24시간·7일의 가동률, 장애 이력을 보여 줍니다. 가동률은 풀 서버의 모니터와 Cloudflare에서 실행되는 외부 점검기로 두 번 측정합니다.

## seine 자체 대시보드는 어떤가요?

풀은 자신이 적립한 내용을 보여 줍니다. 머신 자체의 실시간 상태를 보려면 seine 터미널 대시보드에서 해시레이트와 셰어를 즉시 확인할 수 있습니다. seine는 REST, 서버 전송 이벤트 스트림, `/metrics`의 Prometheus 지표를 제공하는 로컬 제어 API도 노출할 수 있으며, 기본 바인딩 주소는 `127.0.0.1:9977`입니다. [제어 API 가이드](https://github.com/zpalmtree/seine/blob/master/docs/API.md)에서 플래그와 엔드포인트를 설명합니다. 많은 채굴자는 머신별 상태에는 seine 지표를, 잔액과 지급에는 풀 API를 함께 사용합니다.

## bntpool.com API는 어떻게 사용하나요?

<Answer>
bntpool.com API는 HTTPS를 통해 공개 JSON을 제공하며 키가 필요 없습니다. https://bntpool.com/api/miner/ 뒤에 주소를 붙여 호출하면 해시레이트, 리그, 셰어, 지급 내역을 받습니다. 잔액은 /balance를 추가하고, 풀과 네트워크 해시레이트는 /api/stats를 사용하세요. 금액은 원자 단위이므로 100,000,000으로 나누면 BNT가 됩니다.
</Answer>

위의 모든 페이지가 다음 엔드포인트에서 데이터를 읽으므로 사이트에서 보는 내용을 직접 가져올 수 있습니다.

| 엔드포인트 | 반환 내용 |
|---|---|
| `/api/stats` | 연결된 채굴자, 풀 해시레이트, 발견한 블록, 네트워크 해시레이트, 현재 높이 |
| `/api/stats/history?range=24h` | 시간에 따른 풀 해시레이트 (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | 현재 라운드, 평균 투입 작업 비율, 최근 라운드 |
| `/api/info` | 풀 설정: Stratum 포트, 지급 방식과 계산 창, 최소 지급액 |
| `/api/blocks?status=confirmed` | 발견한 블록, 상태별 필터 가능 |
| `/api/payouts/recent` | 트랜잭션 해시가 포함된 지급 묶음 |
| `/api/luck` | 각 라운드의 투입 작업 비율과 소요 시간 |
| `/api/status` | 서비스 상태, 가동률 구간, 장애 |
| `/api/miner/{address}` | 내 해시레이트, 누적 수치, 리그, 최근 셰어, 지급 내역 |
| `/api/miner/{address}/balance` | 예상·컨펌·지급 잔액 |
| `/api/miner/{address}/hashrate?range=24h` | 내 해시레이트 기록 |

목록 엔드포인트는 `limit`(기본 25, 최대 200)과 `offset`을 받으며 `items`와 `total`을 반환합니다. 채굴자 엔드포인트에는 전체 주소가 필요합니다. My Stats에서 @name을 조회한 뒤 검색창의 전체 주소를 복사하세요.

터미널에서 채굴자 데이터를 읽으려면 다음을 실행합니다.

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

`jq`를 사용하면 해시레이트와 승인된 셰어 또는 BNT 단위의 지급 총액처럼 개별 값을 추출할 수 있습니다.

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

<CTA target="pool" title="채굴을 시작하고 실시간으로 확인하세요" text="몇 분 안에 bntpool.com에 참여하고 My Stats에서 해시레이트, 셰어, 잔액을 확인하세요." />
