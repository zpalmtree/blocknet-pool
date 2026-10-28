---
title: 'Blocknet의 공정한 출시: 프리마인 없는 코인'
description: "Blocknet의 빈 제네시스 블록, 첫 보상과 발행 규칙을 확인하세요. BNT가 어떻게 유통되기 시작했으며 채굴된 코인이 다른 보유자에게 어떻게 전달되는지 알아봅니다."
tags: [Blocknet, 공정한 출시, 제네시스 블록, 채굴, bntpool]
sourceHash: a283d654ab64
translation: draft
faq:
  - q: 'Blocknet에는 프리마인이 있었나요?'
    a: '없었습니다. 2026년 2월 15일 생성된 제네시스 블록에는 트랜잭션이 하나도 없어 채굴 전에는 코인이 존재하지 않았습니다. 첫 코인은 블록 1의 보상에서 나왔고 이후 모든 BNT는 해당 블록을 채굴한 사람에게 지급됐습니다.'
  - q: 'Blocknet에 개발 기금이나 트레저리가 있나요?'
    a: "공개된 규칙에는 프로토콜이 의무화한 개발 기금이나 재무 기금 배정이 없습니다. 제네시스 이후 각 블록에는 예정된 보상과 같은 coinbase 출력이 하나 있습니다. 이는 신규 발행에 관한 설명이며 이후 보유자의 신원을 말하지 않습니다. BNT 소유자는 개발자에게 자발적으로 지급하는 경우를 포함해 일반 전송을 할 수 있습니다."
  - q: 'Blocknet 제네시스 블록은 언제 생성됐나요?'
    a: '타임스탬프는 2026년 2월 15일 20:58:38 EST, 즉 2월 16일 01:58:38 UTC입니다. 메시지는 "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything"이며 이전 블록 해시는 이 메시지의 SHA3-256입니다.'
  - q: '공정한 출시를 직접 확인하려면 어떻게 하나요?'
    a: 'Blocknet 탐색기에서 블록 0을 열면 트랜잭션 0개와 모두 0인 머클 루트가 보입니다. 공개 데몬 코드에서는 제네시스가 빈 트랜잭션 목록으로 고정되어 있고 coinbase 검사가 블록 보상과 같은 출력 하나만 허용합니다.'
  - q: 'Blocknet 채굴은 어떻게 시작하나요?'
    a: 'Windows, macOS, Linux용 seine을 다운로드해 실행하고 Blocknet 주소나 Blocknet ID 짧은 이름을 입력하세요. Enter로 기본 풀 bntpool.com을 선택하면 즉시 채굴합니다. 이후 풀 통계에서 주소를 검색할 수 있습니다.'
---

BNT가 유통되기 시작한 과정을 살펴봅니다. 빈 제네시스 블록, 첫 채굴 보상, 이후 보유자 간 전송을 차례로 확인합니다. 이 가이드는 출범 기록과 발행 규칙이 무엇을 입증하는지 설명하고, bntpool.com에서 채굴해 새로 발행된 BNT를 받는 방법을 안내합니다.

## Blocknet에 프리마인이 있었나요?

<Answer>
없었습니다. 2026년 2월 15일 생성된 제네시스 블록에는 트랜잭션이 없어 채굴 전 코인도 없었습니다. 블록 1부터 모든 블록은 예정된 정확한 금액을 단일 coinbase 출력으로 지급했습니다. 유통되는 모든 BNT는 작업 증명을 수행한 누군가가 채굴했습니다.
</Answer>

제네시스 블록은 창립자, 개발자, 투자자 또는 재단에 준비금을 배정하지 않습니다. 이후 블록은 예정된 채굴 보상을 생성하며, 프로토콜이 의무화한 별도의 재무 기금 출력은 없습니다. 이는 발행 규칙입니다. 누가 보상을 채굴했는지, 누가 나중에 기존 코인을 전송받았는지는 밝히지 않습니다.

## Blocknet 제네시스 블록에는 무엇이 있나요?

제네시스는 데몬에 하드코딩되어 모든 노드가 같은 블록 0에서 출발합니다. 필드는 고정되어 있습니다.

| 필드 | 값 |
|---|---|
| 높이 | 0 |
| 타임스탬프 | 2026년 2월 15일 20:58:38 EST (2월 16일 01:58:38 UTC) |
| 메시지 | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| 이전 해시 | 메시지의 SHA3-256: `a94a7f2b…d909df` |
| 난이도 | 네트워크 최솟값 4 |
| Nonce | 0 |
| 트랜잭션 | 없음 |
| 머클 루트 | 모두 0 |

메시지는 2026년 2월 15일 Wired 기사 제목이며 코드에 출처가 있습니다. 이전 해시가 이 메시지에서 파생되므로 제목은 블록 0과 그 위에 쌓인 모든 것의 정체성에 고정됩니다.

제네시스 함수 위 주석은 분명합니다. “제네시스에는 트랜잭션이 없다. coinbase도, 소각된 코인도 없다. 높이 1의 첫 실제 블록에 첫 coinbase가 들어간다.” 검증도 이를 강제합니다. 트랜잭션 수가 정확히 0이 아니거나 머클 루트가 모두 0이 아니면 블록 0을 거부합니다. 변조한 노드라도 제네시스에 코인을 끼워 넣으면 다른 노드가 모두 거부합니다.

## 합의 규칙은 개발 기금과 트레저리를 어떻게 막나요?

<Answer>
제네시스 이후 각 블록은 입력이 없고 수수료가 0이며 예정된 보상과 같은 출력이 정확히 하나인 coinbase 거래로 시작합니다. 공개된 규칙은 이 거래에 창립자나 재무 기금을 위한 별도 출력을 두지 않습니다. 보유자는 이미 가진 BNT를 일반 거래로 계속 전송할 수 있습니다.
</Answer>

여러 검사가 함께 작동합니다.

- **블록당 coinbase 하나.** 첫 트랜잭션은 coinbase여야 하고 다른 트랜잭션은 coinbase일 수 없습니다.
- **출력 하나, 입력 없음.** 출력이 0개나 2개이거나 입력이 있으면 무효입니다.
- **정확한 보상.** Blocknet은 페더슨 커밋먼트로 금액을 숨기지만 coinbase는 특별합니다. 블라인딩 인자는 공개 데이터인 트랜잭션 공개 키와 블록 높이에서 파생됩니다. 모든 노드가 예정된 보상의 커밋먼트를 재구성해 비교할 수 있으며 다른 금액이면 블록을 거부합니다.
- **빈 메모.** coinbase 메모는 비어 있어야 하므로 블록 보상에 추가 데이터를 실을 수도 없습니다.

일반 트랜잭션은 코인을 만들 수 없습니다. 입력은 출력과 수수료의 합과 같아야 하며 커밋먼트 자체로 검사합니다. 모든 출력에는 금액이 유효한 범위 안에 있고 음수가 아님을 증명하는 Bulletproofs 범위 증명이 있습니다. 금액의 프라이버시를 유지하면서 발행량은 정확히 일정을 따릅니다.

## 공정한 출시를 직접 확인하는 방법은?

누구의 말만 믿을 필요가 없습니다. 체인과 코드가 모두 공개되어 있습니다.

**탐색기에서.** [블록 0](https://explorer.blocknetcrypto.com/block/0)을 열면 트랜잭션 0개, 64개의 0으로 된 머클 루트, nonce 0, `a94a7f2b`로 시작하는 이전 해시가 보입니다. 보상 행은 해당 높이의 예정 금액을 표시하지만 트랜잭션이 없어 받을 출력도 없습니다. [블록 1](https://explorer.blocknetcrypto.com/block/1)을 열면 유일한 트랜잭션이 입력 없이 출력 하나인 coinbase이며 이것이 체인의 첫 코인입니다.

이전 해시가 메시지의 해시인지 확인할 수 있습니다. OpenSSL 1.1.1 이상에서 실행하세요.

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

결과는 `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`로 끝나며 블록 0의 탐색기 값과 같습니다.

**코드에서.** 데몬 소스는 [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core)에 있습니다. [`block.go`](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go)에서 다음 함수를 찾으세요.

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock`은 빈 목록으로 블록 0을 만들고, `validateGenesisBlock`은 트랜잭션이 있는 블록 0을 거부하며, `validateCoinbaseConsensus`는 보상 금액의 단일 출력을 강제합니다. `transaction.go`의 `validateCoinbase`와 `verifyCommitmentBalance`는 coinbase 구조와 일반 트랜잭션 균형을 검사합니다.

## 새로 채굴된 BNT는 어떻게 다른 보유자에게 전달됐나요?

탐색기는 제네시스 시각을 2026년 2월 15일 20:58:38 EST로 기록합니다. 블록 1의 시각은 2026년 2월 20일 14:29:20 EST이며 첫 coinbase 거래를 포함합니다. 이 둘은 별개의 기록입니다. 제네시스는 공통 시작 블록을 정하고, 블록 1은 보상이 지급되는 발행을 시작합니다. 탐색기에 표시되는 빈 제네시스의 보상은 일정상 금액일 뿐, 이를 받는 출력은 없습니다.

채굴 보상을 사용할 수 있게 되면 소유자는 다른 사람에게 BNT를 전송할 수 있습니다. 풀도 받은 보상에서 나중에 지급금을 보냅니다. 이러한 전송은 기존 코인을 재분배하며 발행을 반복하지 않습니다. 따라서 코인이 처음 생성된 블록을 직접 채굴하지 않아도 BNT를 보유하고 사용할 수 있습니다. [보내기와 받기 가이드](https://seineminer.com/ko/blog/send-receive-blocknet/)는 일반 결제를, [공급량 가이드](https://seineminer.com/ko/blog/blocknet-supply/)는 전체 통화 발행 일정을 설명합니다.

## 공정한 출시가 채굴자에게 중요한 이유는?

코인의 출처 전체가 블록 보상으로 설명된다는 뜻입니다.

- **모든 새 코인은 채굴자에게 갑니다.** 프리마인과 트레저리 출력 없이 발행량 100%가 블록 발견자에게 지급됩니다. 블록 1은 단일 출력으로 723.25093035 BNT를 지급했고 이후에도 전체 보상이 같은 방식으로 지급됐습니다.
- **모두에게 같은 규칙입니다.** 블록 1부터 새 BNT를 얻는 유일한 방법은 작업 증명입니다. [보상 일정](/blog/ko/blocknet-block-reward-schedule/)은 코드에 정해져 첫 채굴자와 오늘 시작하는 사람에게 똑같이 적용됩니다.
- **일반 컴퓨터에 맞는 작업 증명입니다.** Blocknet은 해시당 2 GiB를 쓰는 Argon2id로 ASIC에 저항하도록 설계됐습니다. 이미 보유한 데스크톱, 노트북, 서버로 참여할 수 있습니다. [Argon2id 설명](https://seineminer.com/ko/blog/argon2id-proof-of-work-explained/)에서 이유를 알아보세요.
- **발행량을 감사하기 쉽습니다.** 모든 코인은 모든 노드가 검사한 coinbase 출력에서 나오므로 어느 높이의 누적 발행량도 일정에서 바로 계산됩니다.

코인 자체가 처음이라면 [Blocknet이란?](https://seineminer.com/ko/blog/what-is-blocknet/)에서 프라이버시, 이름 결제, 지갑을 알아보세요.

## 직접 Blocknet을 채굴하려면?

채굴을 통해 새로 발행된 BNT를 받으려면 seine 채굴기를 bntpool.com과 함께 사용하세요.

1. **주소를 준비하세요.** 공식 [명령줄 지갑](https://github.com/blocknetprivacy/blocknet/releases/latest)이나 [데스크톱 지갑](https://github.com/blocknetprivacy/blocknet-gui/releases/latest)을 설치해 주소를 복사하거나 `@name` 같은 Blocknet ID를 사용하세요.
2. **seine을 다운로드하세요.** [seineminer.com](https://seineminer.com/ko/#download)에서 운영체제용 압축 파일을 받아 푸세요. 설치할 것은 없습니다.
3. **실행하고 기본값을 선택하세요.** 주소나 `@name`을 넣고 Enter로 `stratum+tcp://bntpool.com:3333`을 선택합니다. seine은 즉시 해싱하며 CPU와 Windows·Linux의 NVIDIA GPU에서 스레드와 커널을 튜닝합니다.
4. **채굴기를 확인하세요.** [통계](/stats)에서 주소를 검색해 해시레이트, 셰어, 잔액을 보고 [블록](/blocks)에서 풀의 발견을 확인하세요.

[bntpool.com 채굴 가이드](/blog/ko/mine-blocknet-on-bntpool/)는 주소·이름 로그인과 모니터링을, [Get Started](/start)는 지급 설정을, [Blocknet 채굴 가이드](https://seineminer.com/ko/blog/how-to-mine-blocknet/)는 각 운영체제의 seine 설정을 단계별로 설명합니다.

<CTA target="seine" title="내 몫 채굴하기" text="모든 BNT는 채굴됩니다. seine을 다운로드하고 몇 분 안에 bntpool.com에서 시작하세요." />
