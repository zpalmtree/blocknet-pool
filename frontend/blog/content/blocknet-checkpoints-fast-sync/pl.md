---
title: Szybka synchronizacja Blocknetu z punktami bntpool.com
description: Węzły Blocknet szybko synchronizują się z checkpoints.dat bntpool.com. Poznaj punkty kontrolne oraz pobierz, sprawdź i wczytaj plik w kilku krokach.
tags: [Węzeł Blocknet, Punkty kontrolne, Szybka synchronizacja, bntpool]
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Skąd węzeł Blocknet pobiera punkty kontrolne?'
    a: 'Z bntpool.com. Przy pierwszym uruchomieniu, jeśli katalog danych nie zawiera checkpoints.dat, demon Blocknet pobiera go z https://bntpool.com/checkpoints.dat i używa do szybkiej synchronizacji. Możesz też sam pobrać plik ze strony Checkpoints, sprawdzić SHA-256 i umieścić go w katalogu danych przed uruchomieniem węzła.'
  - q: 'Jak często bntpool.com aktualizuje checkpoints.dat?'
    a: 'Demon puli zapisuje plik ponownie w miarę wzrostu łańcucha, z jednym wpisem dla co setnego bloku. Każdy wpis jest co najmniej 100 bloków poniżej końca łańcucha, poza maksymalną głębokością reorganizacji Blocknetu, więc punkt kontrolny obejmuje wyłącznie ustaloną historię. Strona Checkpoints pokazuje najnowszą wysokość i czas ostatniej zmiany pliku.'
  - q: 'Czy punkty kontrolne zmieniają saldo lub historię portfela?'
    a: 'Nie. Węzeł synchronizowany z punktami kontrolnymi nadal przechowuje wszystkie bloki i zapisuje każde wyjście oraz obraz klucza w indeksach, więc skanowanie portfela i salda działają dokładnie jak po pełnej synchronizacji. Punkty kontrolne pozwalają jedynie pominąć ponowne sprawdzanie proof of work i podpisów historii, którą już poświadczają ich hashe.'
  - q: 'Czy mogę zsynchronizować węzeł Blocknet bez punktów kontrolnych?'
    a: 'Tak. Uruchom demona z flagą --full-sync lub ustaw full_sync na true w konfiguracji blocknet, a węzeł pominie pobieranie punktów kontrolnych i ponownie zweryfikuje proof of work oraz podpisy każdego bloku od genezy. Trwa to dłużej niż synchronizacja z punktami kontrolnymi, dlatego są one domyślnym rozwiązaniem dla nowych węzłów.'
---

Nowy węzeł Blocknet musi nadrobić wszystkie bloki od genezy, zanim zacznie obsługiwać portfel lub górnika solo. Punkty kontrolne przyspieszają ten proces. Ten poradnik wyjaśnia, co zawiera plik punktów kontrolnych bntpool.com, jak demon Blocknet używa go do pomijania najwolniejszych kontroli przy zachowaniu zgodności z prawdziwym łańcuchem oraz jak samodzielnie pobrać, sprawdzić i wczytać plik.

## Czym są punkty kontrolne Blocknetu?

<Answer>
Punkty kontrolne Blocknetu to lista znanych hashy bloków, po jednym dla co setnego bloku, udostępniana przez bntpool.com jako checkpoints.dat. Synchronizujący się węzeł sprawdza zgodność otrzymywanych bloków z tymi hashami i pomija ponowną weryfikację proof of work oraz podpisów do ostatniego punktu kontrolnego, dzięki czemu pierwsza synchronizacja kończy się znacznie szybciej.
</Answer>

Plik jest zwykłym tekstem z jedną parą `height:hash` w wierszu. Pierwsze wiersze pliku 25 października 2026 r. wyglądały tak:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Każdy hash jest identyfikatorem bloku, hashem SHA3-256 jego nagłówka. Własny demon puli zapisuje plik na podstawie swojego łańcucha i uwzględnia tylko bloki znajdujące się co najmniej 100 bloków w jego głębi. Sto bloków to twarda finalność Blocknetu, najgłębsza reorganizacja dopuszczana przez protokół, więc każdy punkt kontrolny leży w ustalonej historii.

25 października 2026 r. o 02:04 EDT plik zawierał 689 punktów kontrolnych, od wysokości 100 do 68 900, i miał 48 811 bajtów. Rośnie o jeden wiersz mniej więcej co 100 bloków, czyli około co osiem godzin przy pięciominutowych blokach. Sprawdź aktualną wysokość na [stronie Checkpoints](/checkpoints).

## Jak węzeł Blocknet korzysta z punktów kontrolnych?

Węzeł z wczytanymi punktami kontrolnymi traktuje wysokość ostatniego z nich jako cel szybkiej synchronizacji. Dopóki jego łańcuch nie osiągnie tej wysokości, opiera dwie najdroższe kontrole na opublikowanych punktach kontrolnych, a wszystkie pozostałe wykonuje normalnie:

- **Proof of work opiera się na punktach kontrolnych.** Proof of work każdego bloku Blocknetu jest hashem Argon2id przetwarzającym 2 GiB pamięci. Ponowne obliczanie go dla każdego historycznego bloku jest kosztowne, a hashe punktów kontrolnych już poświadczają tę historię.
- **Podpisy RingCT i dowody zakresu również się na nich opierają.** Komentarze samego demona określają weryfikację dowodów zakresu wyjść jako największy pojedynczy koszt synchronizacji, ponieważ ma je każde wyjście, także coinbase każdego bloku.
- **Wszystkie pozostałe kontrole nadal działają.** Powiązania bloków, reguła trudności LWMA, reguły znaczników czasu, rozmiar i struktura bloków, kontrola podwójnego wydawania na obrazach kluczy oraz sprawdzanie powiązania każdego wejścia z podpisem działają normalnie. Wyjścia i obrazy kluczy trafiają do indeksów węzła jak zwykle, więc skanowanie portfela i salda są takie same jak po pełnej synchronizacji.

Pomijanie dotyczy wyłącznie bloków przedłużających bieżący koniec łańcucha węzła i tylko poniżej ostatniego punktu kontrolnego. Po przekroczeniu tej wysokości węzeł w pełni weryfikuje każdy nowy blok, łącznie z proof of work.

### Jak punkty kontrolne wyznaczają zaufany łańcuch

Podczas synchronizacji do ostatniego punktu kontrolnego każdy blok na wysokości punktu kontrolnego musi mieć dokładnie hash zapisany w pliku. Blok z innym hashem jest odrzucany z błędem niezgodności punktu kontrolnego. Ponieważ punkty występują co 100 bloków, peer nie może poprowadzić szybko synchronizującego się węzła przez inną historię: następna wysokość punktu kontrolnego ujawniłaby rozbieżność.

Punkty kontrolne przyspieszają też każdy restart. Gdy demon uruchamia się z blokami już zapisanymi na dysku, znajduje najwyższy punkt kontrolny zgodny ze swoim zapisanym łańcuchem i sprawdza ponownie tylko późniejsze bloki, zamiast przechodzić cały łańcuch od genezy.

## Czy nowy węzeł automatycznie pobiera punkty kontrolne?

<Answer>
Tak. Gdy demon Blocknet startuje, a jego katalog danych nie zawiera checkpoints.dat, pobiera plik z https://bntpool.com/checkpoints.dat, wczytuje go i szybko synchronizuje się do ostatniego punktu kontrolnego. Pobranie jest wykonywane w miarę możliwości: jeśli się nie powiedzie, węzeł i tak startuje i w pełni weryfikuje łańcuch.
</Answer>

Ten adres jest domyślnie zapisany w samym rdzeniu Blocknetu, więc każdy nowy węzeł od razu szybko synchronizuje się z pliku publikowanego przez bntpool.com. Demon pobiera go tylko wtedy, gdy plik nie istnieje, daje żądaniu 15 sekund i ogranicza pobieranie do 32 MiB.

## Jak użyć punktów kontrolnych bntpool.com krok po kroku?

Jeśli wolisz sam pobrać i sprawdzić plik lub chcesz odświeżyć starszą kopię przed długą ponowną synchronizacją, wykonaj te kroki ręcznie.

### 1. Znajdź katalog danych

Plik punktów kontrolnych znajduje się w katalogu danych węzła, obok bazy łańcucha:

- **Menedżer `blocknet`:** `~/.config/bnt/data/mainnet/` w macOS i Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` w Windows, chyba że ustawisz `data_dir` w konfiguracji.
- **Rdzeń uruchamiany bezpośrednio:** `./blocknet-data-mainnet` w folderze, z którego go uruchamiasz, lub ścieżka podana przez `--data`.

### 2. Pobierz checkpoints.dat

Otwórz [Checkpoints](/checkpoints) i naciśnij **Download checkpoints.dat** albo pobierz plik z terminala. Ten przykład zapisuje go w domyślnym katalogu danych rdzenia:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Pobranie do istniejącego pliku zastępuje go — tak odświeżasz starą kopię.

### 3. Sprawdź SHA-256

Strona Checkpoints publikuje SHA-256 bieżącego pliku. Oblicz hash swojej kopii i porównaj:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

W macOS użyj zamiast tego `shasum -a 256`. Hash zmienia się za każdym razem, gdy pula dodaje punkt kontrolny, więc porównaj go ze stroną bezpośrednio po pobraniu.

### 4. Sprawdź przykładowy punkt w eksploratorze

Każdy wiersz możesz potwierdzić w publicznym [eksploratorze Blocknetu](https://explorer.blocknetcrypto.com). Otwórz stronę bloku dla wysokości z pliku, na przykład `explorer.blocknetcrypto.com/block/68900`, i porównaj hash z wierszem dla 68 900. Zrobiliśmy to dla ostatniego punktu kontrolnego 25 października 2026 r. i hashe były zgodne.

### 5. Uruchom węzeł

Uruchom lub zrestartuj demona. Przy starcie odczytuje `checkpoints.dat` i szybko synchronizuje się do ostatniego punktu kontrolnego. Jeśli uruchamiasz rdzeń z interaktywną powłoką (`--cli`), polecenie `load-checkpoints` wczytuje plik do działającego węzła i podaje wysokość szybkiej synchronizacji:

```text
> load-checkpoints
```

Punkty kontrolne pomagają, gdy węzeł jest w tyle. Gdy Twój łańcuch przekroczy wysokość ostatniego punktu kontrolnego, węzeł już weryfikuje w pełni każdy blok i nowszy plik niczego nie zmienia.

## Jak sprawdzić plik punktów kontrolnych ze skryptu?

Pula publikuje szczegóły pliku jako JSON pod adresem `https://bntpool.com/api/checkpoints`, bez klucza API:

| Pole | Znaczenie |
|---|---|
| `available` | Czy pula ma plik do udostępnienia |
| `url` | Skąd go pobrać |
| `entries` | Liczba punktów kontrolnych w pliku |
| `latest_height`, `latest_hash` | Najwyższy punkt kontrolny i hash jego bloku |
| `bytes`, `sha256` | Rozmiar pliku i SHA-256 |
| `updated_at` | Czas ostatniej zmiany pliku jako znacznik czasu Unix |

Samo pobranie zawiera nagłówki `X-Checkpoint-Height` i `X-Checkpoint-Sha256` oraz `ETag` utworzony z hasha, więc skrypt może wysłać `If-None-Match` i szybko otrzymać `304 Not Modified`, gdy nic się nie zmieniło:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Odpowiedzi mogą być przechowywane w pamięci podręcznej przez 60 sekund, co w zupełności wystarcza dla pliku zmieniającego się mniej więcej co osiem godzin.

<Note>
blocknetcrypto.com podaje, że planowana aktualizacja Faraday przyniesie lżejszy proof of work, aby nowe węzły szybciej doganiały łańcuch. Do tego czasu punkty kontrolne są szybkim sposobem aktualizacji węzła.
</Note>

## Co możesz zrobić ze zsynchronizowanym węzłem?

Zsynchronizowany węzeł obsługuje portfel Blocknet i API węzła oraz jest tym, z czym seine komunikuje się podczas kopania solo z `--mode daemon`. [Kopanie Blocknetu w puli a solo](/blog/pl/pool-vs-solo-mining-blocknet/) pokazuje, jak ocenić, czy kopanie solo pasuje do Twojego sprzętu. Jeśli wolisz zacząć kopać bez prowadzenia węzła, seine domyślnie kopie na bntpool.com: [poradnik instalacji seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) prowadzi do pierwszych udziałów, a [kopanie na bntpool.com](/blog/pl/mine-blocknet-on-bntpool/) opisuje stronę puli.

<CTA target="pool" title="Kop w puli udostępniającej punkty kontrolne Blocknetu" text="Dołącz do bntpool.com w kilka minut i śledź na bieżąco hashrate, udziały i wypłaty." />
