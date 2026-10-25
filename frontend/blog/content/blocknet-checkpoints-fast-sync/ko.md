---
title: bntpool.com 체크포인트로 Blocknet 빠르게 동기화하기
description: Blocknet 노드는 bntpool.com의 checkpoints.dat로 빠르게 동기화합니다. 체인을 고정하는 원리를 알아보고 파일을 다운로드, 검증, 로드하세요.
tags: [Blocknet 노드, 체크포인트, 빠른 동기화, bntpool]
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Blocknet 노드는 체크포인트를 어디서 받나요?'
    a: 'bntpool.com에서 받습니다. 처음 시작할 때 데이터 디렉터리에 checkpoints.dat가 없으면 데몬이 https://bntpool.com/checkpoints.dat를 받아 빠른 동기화에 사용합니다. Checkpoints 페이지에서 직접 받아 SHA-256을 확인하고 시작 전에 데이터 디렉터리에 넣어도 됩니다.'
  - q: 'bntpool.com은 checkpoints.dat를 얼마나 자주 갱신하나요?'
    a: '풀 데몬이 체인이 늘어날 때 파일을 다시 쓰며 100번째 블록마다 항목이 있습니다. 각 항목은 체인 끝에서 최소 100블록 아래로, 최대 재구성 깊이를 지난 확정된 이력만 포함합니다. 페이지에 최신 높이와 변경 시각이 표시됩니다.'
  - q: '체크포인트가 지갑 잔액이나 내역을 바꾸나요?'
    a: '아닙니다. 모든 블록을 저장하고 모든 출력과 키 이미지를 인덱싱하므로 지갑 스캔과 잔액은 전체 동기화와 같습니다. 체크포인트 해시가 보증하는 과거 구간의 작업 증명과 서명 재검증만 생략합니다.'
  - q: '체크포인트 없이 동기화할 수 있나요?'
    a: '네. --full-sync로 데몬을 시작하거나 blocknet 설정의 full_sync를 true로 지정하면 다운로드를 건너뛰고 제네시스부터 모든 블록의 작업 증명과 서명을 재검증합니다. 더 오래 걸리므로 새 노드는 체크포인트가 기본입니다.'
---

새 Blocknet 노드가 지갑이나 솔로 채굴기를 지원하려면 제네시스 이후 모든 블록을 따라잡아야 합니다. 체크포인트는 이 과정을 빠르게 합니다. 파일의 내용, 실제 체인에 고정된 채 가장 느린 검사를 생략하는 원리, 직접 다운로드하고 검증해 로드하는 방법을 설명합니다.

## Blocknet 체크포인트란?

<Answer>
100번째 블록마다 알려진 해시를 모아 bntpool.com이 checkpoints.dat로 제공하는 목록입니다. 동기화 중인 노드는 받은 블록이 해시와 일치하는지 검사하고 마지막 체크포인트까지 작업 증명과 서명 재검증을 생략해 첫 동기화를 훨씬 빨리 마칩니다.
</Answer>

파일은 한 줄마다 `height:hash` 쌍이 있는 일반 텍스트입니다. 2026년 10월 25일 첫 줄들은 다음과 같았습니다.

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

각 해시는 블록 ID, 즉 헤더의 SHA3-256입니다. 풀 데몬이 자기 체인에서 파일을 만들고 최소 100블록 깊이만 포함합니다. 100블록은 Blocknet의 강제 최종성 경계이자 프로토콜이 허용하는 최대 재구성 깊이이므로 모든 체크포인트는 확정된 과거에 있습니다.

2026년 10월 25일 02:04 EDT 기준 파일에는 높이 100부터 68,900까지 689개 항목이 있었고 크기는 48,811바이트였습니다. 약 100블록마다 한 줄, 5분 블록 기준 약 8시간마다 늘어납니다. 최신 높이는 [Checkpoints](/checkpoints)에서 보세요.

## 노드는 체크포인트를 어떻게 사용하나요?

로드한 마지막 높이가 빠른 동기화 목표가 됩니다. 그곳에 도달할 때까지 가장 비싼 두 검사는 공개 체크포인트에 의존하고 나머지는 정상 수행합니다.

- **작업 증명은 체크포인트에 의존합니다.** 블록마다 2 GiB 메모리의 Argon2id를 재계산하면 비용이 큽니다. 체크포인트 해시는 이미 그 이력을 보증합니다.
- **RingCT 서명과 범위 증명도 마찬가지입니다.** 데몬 주석은 출력 범위 증명 검증을 동기화에서 가장 큰 단일 비용이라고 설명합니다. 블록의 coinbase를 포함한 모든 출력에 증명이 있기 때문입니다.
- **나머지 검사는 모두 실행합니다.** 블록 연결, LWMA 난이도 규칙, 타임스탬프, 크기와 구조, 키 이미지 이중 지출, 입력과 서명 연결 검사를 그대로 수행합니다. 출력과 키 이미지를 정상 인덱싱하므로 지갑 스캔과 잔액도 전체 동기화와 같습니다.

생략은 현재 끝을 확장하는 블록에만, 마지막 체크포인트 아래에서만 적용됩니다. 그 높이를 지나면 작업 증명을 포함해 모든 새 블록을 완전히 검증합니다.

### 체크포인트가 신뢰하는 체인을 고정하는 원리

동기화 중 체크포인트 높이의 블록은 파일과 정확히 같은 해시여야 합니다. 다르면 체크포인트 불일치 오류로 거부합니다. 100블록마다 확인하므로 피어가 다른 이력으로 노드를 유도하면 다음 체크포인트에서 드러납니다.

재시작도 빨라집니다. 디스크에 블록이 있으면 데몬은 저장된 체인과 맞는 가장 높은 체크포인트를 찾고 이후만 재검증합니다. 제네시스부터 전체를 훑지 않습니다.

## 새 노드는 체크포인트를 자동 다운로드하나요?

<Answer>
네. 데이터 디렉터리에 checkpoints.dat가 없으면 시작 시 https://bntpool.com/checkpoints.dat를 받아 로드하고 마지막 지점까지 빠르게 동기화합니다. 다운로드는 최선 노력 방식이라 실패해도 노드는 시작하며 전체 체인을 검증합니다.
</Answer>

이 주소는 Blocknet core 자체의 기본값입니다. 새 노드는 별도 설정 없이 bntpool.com 파일을 사용합니다. 파일이 없을 때만 받고 요청 제한은 15초, 최대 크기는 32 MiB입니다.

## 체크포인트를 직접 사용하는 단계는?

직접 파일을 받아 확인하거나 긴 재동기화 전에 오래된 사본을 갱신하려면 다음 단계를 따르세요.

### 1. 데이터 디렉터리 찾기

체크포인트 파일은 체인 데이터베이스 옆에 둡니다.

- **`blocknet` 관리자:** macOS·Linux는 `~/.config/bnt/data/mainnet/`, Windows는 `C:\Users\<you>\.config\bnt\data\mainnet\`입니다. 설정에서 `data_dir`를 바꿨다면 그 경로를 씁니다.
- **core 직접 실행:** 시작한 폴더의 `./blocknet-data-mainnet` 또는 `--data`로 지정한 경로입니다.

### 2. checkpoints.dat 다운로드

[Checkpoints](/checkpoints)에서 **Download checkpoints.dat**를 누르거나 터미널에서 받으세요. 예시는 core 기본 디렉터리에 저장합니다.

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

기존 파일에 내려받으면 교체되어 오래된 사본이 갱신됩니다.

### 3. SHA-256 확인

페이지에 현재 파일의 SHA-256이 있습니다. 사본의 해시를 계산해 비교하세요.

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

macOS에서는 `shasum -a 256`을 사용합니다. 체크포인트가 추가될 때마다 해시가 바뀌므로 다운로드 직후 페이지와 비교하세요.

### 4. 탐색기에서 한 항목 대조

어느 줄이든 [Blocknet 탐색기](https://explorer.blocknetcrypto.com)로 확인할 수 있습니다. 파일의 높이, 예를 들어 `explorer.blocknetcrypto.com/block/68900`을 열어 68,900행 해시와 비교하세요. 2026년 10월 25일 마지막 체크포인트를 확인했을 때 일치했습니다.

### 5. 노드 시작

데몬을 시작하거나 재시작하면 `checkpoints.dat`를 읽고 마지막 지점까지 빠르게 동기화합니다. core의 대화형 셸 `--cli`에서는 `load-checkpoints`로 실행 중인 노드에 파일을 로드하고 목표 높이를 확인할 수 있습니다.

```text
> load-checkpoints
```

체크포인트는 노드가 뒤처졌을 때 유용합니다. 이미 마지막 높이를 지났다면 모든 블록을 완전히 검증 중이므로 더 새 파일은 동작을 바꾸지 않습니다.

## 스크립트로 파일 정보를 확인하려면?

풀은 API 키 없이 `https://bntpool.com/api/checkpoints`에서 JSON 정보를 제공합니다.

| 필드 | 의미 |
|---|---|
| `available` | 제공할 파일이 있는지 |
| `url` | 다운로드 위치 |
| `entries` | 체크포인트 수 |
| `latest_height`, `latest_hash` | 가장 높은 지점과 블록 해시 |
| `bytes`, `sha256` | 파일 크기와 SHA-256 |
| `updated_at` | 마지막 변경 Unix 타임스탬프 |

다운로드에는 `X-Checkpoint-Height`, `X-Checkpoint-Sha256` 헤더와 해시 기반 `ETag`가 있습니다. 스크립트가 `If-None-Match`를 보내면 변경이 없을 때 빠르게 `304 Not Modified`를 받을 수 있습니다.

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

응답은 60초 캐시될 수 있습니다. 약 8시간마다 바뀌는 파일에는 충분합니다.

<Note>
blocknetcrypto.com에 따르면 Faraday는 더 가벼운 작업 증명으로 새 노드가 체인을 빨리 따라잡게 할 예정입니다. 그전까지 체크포인트가 빠른 동기화 방법입니다.
</Note>

## 동기화한 노드로 무엇을 할 수 있나요?

Blocknet 지갑과 노드 API를 지원하고, seine이 `--mode daemon` 솔로 채굴에서 연결하는 대상이 됩니다. [풀과 솔로 채굴 비교](/blog/ko/pool-vs-solo-mining-blocknet/)에서 하드웨어에 맞는지 판단하세요. 노드 없이 시작하려면 seine은 기본적으로 bntpool.com을 사용합니다. [설치 가이드](https://seineminer.com/ko/blog/how-to-mine-blocknet/)는 첫 셰어까지, [bntpool.com 가이드](/blog/ko/mine-blocknet-on-bntpool/)는 풀 사용을 설명합니다.

<CTA target="pool" title="Blocknet 체크포인트를 제공하는 풀에서 채굴하기" text="몇 분 안에 bntpool.com에 연결하고 해시레이트, 셰어, 지급을 실시간으로 확인하세요." />
