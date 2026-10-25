---
title: 'bntpool.com에서 Blocknet 채굴하기'
seoTitle: 'bntpool.com에서 Blocknet 채굴하기'
description: 'Seine의 기본 Blocknet 풀인 bntpool.com에 참여하세요. 주소나 @name으로 Stratum에 연결하고, 장비 이름을 지정하고, 통계를 확인하는 방법을 안내합니다.'
tags: [채굴 가이드, bntpool, Blocknet ID, seine]
faq:
  - q: 'bntpool.com의 Stratum 주소는 무엇인가요?'
    a: '주소는 stratum+tcp://bntpool.com:3333이며 seine이 처음 실행될 때 제안하는 풀입니다. 풀 입력에서 Enter를 눌러 선택하거나 --pool-url 플래그로 지정하세요. Blocknet 주소 또는 Blocknet ID 이름이 로그인 정보이며, 풀은 해당 주소로 보상을 지급합니다.'
  - q: 'Blocknet ID 이름으로 bntpool.com에서 채굴할 수 있나요?'
    a: '네. Seine이나 풀이 주소를 요청하는 곳에 @name 또는 $name을 입력하세요. 풀은 blocknet.id의 서명된 매핑을 확인한 뒤 이름에 연결된 전체 주소에 셰어와 지급을 반영합니다. Seine v0.2.16 이상이 필요하며, 명령줄에서는 셸이 이름을 변경하지 않도록 따옴표로 감싸세요.'
  - q: 'bntpool.com에서 채굴하려면 계정이 필요한가요?'
    a: '아니요. 가입, 비밀번호, 이메일이 없습니다. Blocknet 주소 또는 그 주소를 가리키는 Blocknet ID 이름이 로그인 정보이자 지급 대상입니다. 이를 지정해 seine을 실행한 뒤 My Stats에서 조회하면 해시레이트, 셰어, 장비, 잔액, 지급을 확인할 수 있습니다.'
  - q: '여러 컴퓨터에서 하나의 주소로 채굴할 수 있나요?'
    a: '네. 각 컴퓨터에서 같은 주소나 이름으로 seine을 실행하고 장비마다 다른 이름을 지정하세요. 풀은 주소별로 셰어, 잔액, 지급을 집계하므로 모든 장비가 하나의 잔액과 지급에 합산되고, My Stats에는 장비별 행이 표시됩니다.'
sourceHash: f6080e03ffd0
translation: draft
---

이 가이드는 seine으로 bntpool.com 풀에 참여하는 방법을 설명합니다. 풀 주소, Blocknet 주소 또는 Blocknet ID `@name`으로 로그인하기, 장비별 이름 지정, 통계 페이지에서 셰어 도착 확인을 다룹니다. Seine을 아직 설치하지 않았다면 먼저 [seine 설치 가이드](https://seineminer.com/ko/blog/how-to-mine-blocknet/)를 따른 뒤 돌아오세요.

## bntpool.com의 풀 주소는 무엇인가요?

<Answer>
bntpool.com의 Stratum 주소는 `stratum+tcp://bntpool.com:3333`입니다. Seine은 처음 실행할 때 기본 풀로 제안하므로 풀 입력에서 Enter를 누르면 연결됩니다. Blocknet 주소 또는 Blocknet ID `@name`으로 로그인합니다. 계정을 만들 필요가 없으며 보상은 그 주소로 지급됩니다.
</Answer>

2026년 10월 25일 공개 `/api/info` 엔드포인트에 표시된 풀 설정은 다음과 같습니다.

| 설정 | 값 |
|---|---|
| Stratum 주소 | `stratum+tcp://bntpool.com:3333` |
| 로그인 | Blocknet 주소, `@name` 또는 `$name` |
| 보상 방식 | 6시간 구간의 PPLNS |
| 지급 전 승인 수 | 60블록 |
| 최소 지급액 | 0.1 BNT |

풀의 [Get Started](/start) 페이지에서 언제든 같은 값을 확인할 수 있습니다.

## bntpool.com이 seine의 기본 풀인 이유는 무엇인가요?

Seine을 처음 실행하면 풀을 묻고 `stratum+tcp://bntpool.com:3333`을 기본값으로 제시합니다. 풀과 채굴 프로그램은 다음과 같이 함께 작동합니다.

- **같은 프로토콜.** bntpool.com은 seine이 사용하는 Stratum 프로토콜 버전 2를 실행하므로 주소 외에는 설정할 필요가 없습니다.
- **양쪽 모두 이름 지원.** Seine과 풀은 모두 Blocknet ID 이름을 받아 blocknet.id의 같은 서명된 기록으로 검증합니다.
- **버전 확인.** 각 장비는 셰어와 함께 seine 버전을 보고합니다. 통계 페이지는 오래된 버전의 장비에 seine 릴리스 페이지로 연결되는 “update available” 배지를 표시합니다.
- **Seine 설정 가이드.** 풀의 Get Started 페이지에서 다운로드와 실행 과정을 안내하고 복사할 명령을 제공합니다.

## Seine을 bntpool.com에 어떻게 연결하나요?

Seine을 처음 실행하면 두 가지를 묻습니다.

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

주소를 붙여 넣거나 이름을 입력한 뒤 Enter를 눌러 bntpool.com을 선택하세요. Seine은 두 답을 `seine-data/seine.config.json`에 저장하여 다음 실행부터 바로 채굴합니다. 장비 이름도 정해 같은 파일에 저장합니다. 아래에서 자세히 설명합니다.

화면 없는 장비, 서비스, 스크립트에서는 프롬프트에 답할 사람이 없습니다. 처음 실행할 때 두 값을 플래그로 지정하세요. Get Started 페이지에서 제공하는 명령과 같습니다.

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

나중에 풀이나 주소를 변경하려면 `seine-data/seine.config.json`을 수정하거나 삭제하거나, 명령줄에서 `--address`와 `--pool-url`로 저장된 값을 덮어쓰세요.

## Blocknet ID @name으로 채굴할 수 있나요?

<Answer>
네. bntpool.com은 주소를 받는 모든 곳에서 `@name` 또는 `$name` 형식의 Blocknet ID 짧은 이름을 받습니다. 풀은 blocknet.id의 서명된 매핑과 연결된 주소를 검증한 뒤, 연결이 유지되는 동안 셰어, 잔액, 지급을 해당 전체 주소에 반영합니다.
</Answer>

이름은 대소문자를 구분하지 않으므로 `@Alice`와 `@alice`는 같은 로그인입니다. 이름 지원은 seine v0.2.16에서 추가되었으며 현재 릴리스는 모두 지원합니다. 채굴 전에 seine이 이름으로 확인된 주소를 출력하므로 자신의 지갑과 일치하는지 확인할 수 있습니다. 명령줄에서는 이름을 작은따옴표로 감싸세요. 그렇지 않으면 셸이 `$name`을 변수로 확장하려고 합니다.

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

나중에 이름을 다른 지갑에 연결했다면 seine을 재시작하세요. Seine은 시작할 때마다 저장된 이름을 확인하고 실행 내내 그 주소를 사용하며, 풀도 연결마다 같은 방식으로 처리합니다. 이미 얻은 잔액은 획득 당시 주소에 남아 그곳으로 지급되므로 이름을 옮겨도 코인은 이동하지 않습니다.

[blocknet.id](https://blocknet.id)에서 이름을 등록할 수 있습니다. [Blocknet ID 가이드](https://seineminer.com/ko/blog/blocknet-id-pay-by-name/)는 이름으로 송금하는 방식을 설명합니다.

## 장비 이름은 어떻게 지정하나요?

모든 연결은 주소와 함께 장비 이름으로 로그인합니다. Seine은 첫 실행에서 `seine-` 뒤에 네 자리 숫자가 붙은 이름을 정하고 설정 파일에 저장하므로 재시작해도 같은 이름을 유지합니다. 알아보기 쉬운 이름을 사용하려면 명령줄에서 지정하세요.

```sh
./seine --pool-worker rig-01
```

풀은 이름을 최대 64자까지 저장합니다. 고정된 이름은 재시작 후에도 도움이 됩니다. 풀은 주소와 장비 이름을 키로 장비의 셰어 난이도를 한 시간 동안 기억하므로, 재연결하면 이미 안정화된 난이도에서 이어갑니다.

## 여러 장비에서 하나의 주소로 채굴할 수 있나요?

네. 각 머신에서 같은 주소나 `@name`, 서로 다른 장비 이름으로 seine을 실행하세요. 풀은 주소별로 셰어, 잔액, 지급을 계산하므로 모든 장비가 하나의 잔액과 지급에 합산됩니다. 통계 페이지에는 각 장비의 행이 따로 표시되어 모든 머신이 채굴 중인지 쉽게 확인할 수 있습니다.

## 채굴기가 작동하는지 어떻게 확인하나요?

Seine의 대시보드는 머신에서 직접 해시레이트와 승인된 셰어를 보여 줍니다. 풀에서 본 상태를 확인하려면 [My Stats](/stats)를 열고 주소, `@name` 또는 `$name`을 입력한 뒤 **Lookup**을 누르세요. 이름을 입력하면 확인된 주소가 검색 상자 옆에 표시됩니다. 페이지는 해당 브라우저에서 마지막 조회도 기억합니다. [채굴기 모니터링 가이드](/blog/ko/track-your-miner-on-bntpool/)에서 모든 카드를 설명합니다.

세 가지를 확인하세요.

- **Hashrate와 Shares Accepted.** 해시레이트 카드는 풀이 관측한 값을 보여 주며, 장비가 작업을 제출하면 승인 수가 증가합니다.
- **장비별 행.** 각 행에는 이름, 해시레이트, 승인·거부 셰어, 마지막 셰어 시각이 표시됩니다. 마지막 셰어가 방금 전이라면 연결되어 작동 중입니다.
- **Recent Shares.** 최신 셰어에 제출 장비, 난이도, 상태가 표시됩니다.

풀은 연결별로 셰어 난이도를 정하고 채굴 중 조정하므로 작은 CPU도 꾸준히 셰어를 보내며, 연결 후 몇 분 안에 첫 승인 셰어가 표시됩니다. 하드웨어별 기대 해시레이트는 [Blocknet 채굴 하드웨어 가이드](https://seineminer.com/ko/blog/blocknet-mining-hardware-hashrate/)를 참고하세요.

그다음 보상이 들어옵니다. 자신의 셰어가 6시간 구간 안에 있을 때 풀이 블록을 찾으면 Estimated Rewards가 표시됩니다. 블록이 60회 승인되면 자신의 몫이 Confirmed Rewards로 옮겨지고, 최소 0.1 BNT가 되면 자동 지급됩니다. [PPLNS 보상 방식](/blog/ko/how-pplns-payouts-work/)에서 블록 보상 분배를 설명합니다. 풀 전체 기록은 [Blocks](/blocks)와 [Payouts](/payouts) 페이지에 있습니다.

## Get Started 페이지에는 무엇이 있나요?

풀의 [Get Started](/start) 페이지는 한 화면에 필요한 정보를 모았습니다.

- **풀 정보.** 클릭해 복사할 수 있는 Stratum 주소, 보상 방식, 최소 지급액, 지급 전 블록 승인 수를 풀에서 실시간으로 읽습니다.
- **세 단계 빠른 시작.** [seineminer.com](https://seineminer.com/ko/#download)에서 seine을 내려받고, 처음 실행하여 주소나 이름과 풀 URL을 입력한 뒤, 채굴을 시작하고 My Stats에서 해시레이트와 잔액을 확인합니다.
- **복사할 명령.** 풀 URL이 채워진 전체 `./seine` 명령입니다. 자신의 주소나 따옴표로 감싼 `'@yourname'`으로 바꾸세요.
- **설정 안내.** Seine의 설정 저장 위치, 초기화 방법, 저장값을 덮어쓰는 플래그를 설명합니다.
- **보상 집계 방식.** 블록을 찾기 전에 제출한 셰어가 해당 보상을 얻고, 그 뒤 추가한 작업은 다음 블록에 반영된다는 PPLNS 설명입니다. 미확정 블록의 보상은 확정될 때까지 My Stats에 별도로 표시된다는 점도 안내합니다.
- **스크린샷.** Seine의 첫 실행 설정과 채굴 대시보드를 보여 주어 시작 전에 모습을 확인할 수 있습니다.

<CTA target="pool" title="bntpool.com에서 채굴을 시작하세요" text="자신의 주소나 @name으로 seine을 bntpool.com에 연결하고 My Stats에서 셰어가 도착하는 것을 확인하세요." />
