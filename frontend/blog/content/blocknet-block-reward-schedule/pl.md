---
title: Harmonogram nagród za bloki Blocknetu
description: Nagroda Blocknetu miesiąc po miesiącu, od 723,25 BNT do stałej emisji 2,0 BNT od miesiąca 48, z 288 blokami dziennie i 98,6 mln BNT przed tym progiem.
tags: [Blocknet, Nagroda za blok, Emisja, Kopanie]
sourceHash: abea18c2c21b
translation: draft
faq:
  - q: 'Jaka jest aktualna nagroda za blok Blocknetu?'
    a: 'Zależy od miesiąca łańcucha, ponieważ nagroda maleje co 8640 bloków. 25 października 2026 r. łańcuch był w miesiącu 7, wypłacając 467,67 BNT na blok, a miesiąc 8 zaczyna się na wysokości 69 120 z nagrodą 439,46 BNT. Znajdź bieżącą wysokość bloku w panelu bntpool.com i odszukaj ją w tabeli.'
  - q: 'Ile bloków Blocknetu jest kopanych dziennie?'
    a: 'Docelowo Blocknet tworzy blok co pięć minut, czyli 288 bloków dziennie i 8640 bloków w każdym 30-dniowym miesiącu. Trudność dostosowuje się co blok na podstawie średniej ważonej czasów ostatnich 60 bloków, więc średnia pozostaje bliska pięciu minutom mimo dołączania i odłączania górników.'
  - q: 'Jaka jest całkowita podaż Blocknetu?'
    a: 'Przed rozpoczęciem emisji ogonowej w miesiącu 48 zostaje wykopane około 98,56 miliona BNT. Kod i oficjalna strona zaokrąglają to do około 100 milionów. Potem każdy blok zawsze wypłaca 2,0 BNT, dodając 576 BNT dziennie lub około 210 240 BNT rocznie, więc podaż nadal powoli rośnie.'
  - q: 'Kiedy zaczyna się emisja ogonowa Blocknetu?'
    a: 'Zaczyna się w miesiącu 48, na wysokości bloku 414 720. To 48 miesięcy po 30 dni, czyli 1440 dni pięciominutowych bloków od startu — mniej więcej cztery lata. Dokładna data zależy od rzeczywistych czasów bloków. Od tej wysokości każdy blok wypłaca 2,0 BNT.'
  - q: 'Po jakim czasie można wydać wykopane monety Blocknetu?'
    a: 'Nagrodę za blok można wydać po 60 blokach, czyli około pięciu godzinach przy pięciominutowym czasie bloku Blocknetu. Nazywa się to dojrzewaniem coinbase. Pula bntpool.com także czeka 60 bloków przed wypłatą nagrody, więc wypłaca tylko nagrody z dojrzałych bloków.'
---

Ta strona wymienia nagrodę za blok Blocknetu dla każdego miesiąca harmonogramu emisji, wyjaśnia jej obliczanie i pokazuje, co harmonogram oznacza dla Ciebie jako górnika.

## Jaka jest nagroda za blok Blocknetu?

<Answer>
Nagroda za blok Blocknetu zaczęła się od 723,25 BNT i maleje raz na każdy 30-dniowy miesiąc liczący 8640 bloków, zgodnie z gładką krzywą zaniku. W obrębie miesiąca jest taka sama dla każdego bloku. Od miesiąca 48, na wysokości 414 720, każdy blok wypłaca stałą nagrodę ogonową 2,0 BNT.
</Answer>

Docelowo Blocknet tworzy blok co pięć minut (300 sekund). Daje to 288 bloków dziennie i 8640 bloków w każdym 30-dniowym miesiącu. Trudność dostosowuje się po każdym bloku na podstawie średniej ważonej czasów ostatnich 60 bloków, więc tempo pozostaje bliskie pięciu minutom mimo zmian hashrate sieci.

Nagroda za dowolny blok zależy wyłącznie od miesiąca, do którego należy jego wysokość. Miesiąc to wysokość bloku podzielona przez 8640, zaokrąglona w dół, więc wysokości od 0 do 8639 należą do miesiąca 0, od 8640 do 17 279 do miesiąca 1 i tak dalej.

## Jak oblicza się nagrodę?

Demon oblicza nagrodę każdego miesiąca jednym wzorem:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

Pierwszy składnik to część nagrody powyżej ogona 2,0 BNT. Co rok zmniejsza się o czynnik e^-0,75, więc po dwunastu miesiącach wynosi około 47% wartości początkowej. W podziale na miesiące część nagrody powyżej ogona wynosi w każdym miesiącu około 93,9% wartości z poprzedniego, więc nagroda maleje małymi, regularnymi krokami.

Wynik jest zaokrąglany w dół do pełnych jednostek atomowych (1 BNT to 100 000 000 jednostek atomowych). Poniższa tabela pokazuje każdą nagrodę zaokrągloną do dwóch miejsc po przecinku.

## Tabela nagród Blocknetu według miesiąca

| Miesiąc | Pierwszy blok | Nagroda za blok (BNT) | Łącznie wykopane na koniec miesiąca (mln BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8640 | 679,55 | 12,12 |
| 2 | 17 280 | 638,50 | 17,64 |
| 3 | 25 920 | 599,94 | 22,82 |
| 4 | 34 560 | 563,71 | 27,69 |
| 5 | 43 200 | 529,68 | 32,27 |
| 6 | 51 840 | 497,71 | 36,57 |
| 7 | 60 480 | 467,67 | 40,61 |
| 8 | 69 120 | 439,46 | 44,40 |
| 9 | 77 760 | 412,96 | 47,97 |
| 10 | 86 400 | 388,06 | 51,33 |
| 11 | 95 040 | 364,67 | 54,48 |
| 12 | 103 680 | 342,69 | 57,44 |
| 13 | 112 320 | 322,05 | 60,22 |
| 14 | 120 960 | 302,66 | 62,83 |
| 15 | 129 600 | 284,45 | 65,29 |
| 16 | 138 240 | 267,33 | 67,60 |
| 17 | 146 880 | 251,26 | 69,77 |
| 18 | 155 520 | 236,16 | 71,81 |
| 19 | 164 160 | 221,97 | 73,73 |
| 20 | 172 800 | 208,64 | 75,53 |
| 21 | 181 440 | 196,12 | 77,23 |
| 22 | 190 080 | 184,36 | 78,82 |
| 23 | 198 720 | 173,31 | 80,32 |
| 24 | 207 360 | 162,93 | 81,73 |
| 25 | 216 000 | 153,18 | 83,05 |
| 26 | 224 640 | 144,02 | 84,29 |
| 27 | 233 280 | 135,42 | 85,46 |
| 28 | 241 920 | 127,33 | 86,56 |
| 29 | 250 560 | 119,74 | 87,60 |
| 30 | 259 200 | 112,61 | 88,57 |
| 31 | 267 840 | 105,91 | 89,49 |
| 32 | 276 480 | 99,61 | 90,35 |
| 33 | 285 120 | 93,70 | 91,16 |
| 34 | 293 760 | 88,14 | 91,92 |
| 35 | 302 400 | 82,92 | 92,63 |
| 36 | 311 040 | 78,02 | 93,31 |
| 37 | 319 680 | 73,41 | 93,94 |
| 38 | 328 320 | 69,09 | 94,54 |
| 39 | 336 960 | 65,02 | 95,10 |
| 40 | 345 600 | 61,20 | 95,63 |
| 41 | 354 240 | 57,62 | 96,13 |
| 42 | 362 880 | 54,25 | 96,60 |
| 43 | 371 520 | 51,08 | 97,04 |
| 44 | 380 160 | 48,11 | 97,45 |
| 45 | 388 800 | 45,31 | 97,85 |
| 46 | 397 440 | 42,69 | 98,21 |
| 47 | 406 080 | 40,22 | 98,56 |
| 48 i kolejne | 414 720 | 2,00 | +0,017 miesięcznie |

Blok genezy na wysokości 0 nie ma nagrody, więc miesiąc 0 zawiera 8639 nagradzanych bloków. Każde BNT pochodzi z tych nagród, bez premine, jak wyjaśnia strona [uczciwego startu Blocknetu](/blog/pl/blocknet-fair-launch/). 25 października 2026 r. pula pracowała nad blokiem 69 016, blisko końca miesiąca 7. [Panel bntpool.com](/) pokazuje bieżącą wysokość bloku, więc możesz znaleźć jej wiersz powyżej, a [strona bloków](/blocks) wymienia bloki znalezione przez pulę.

## Co dzieje się w miesiącu 48?

<Answer>
Miesiąc 48 zaczyna się na wysokości bloku 414 720 i od tego miejsca każdy blok Blocknetu zawsze wypłaca stałe 2,0 BNT. To emisja ogonowa. Utrzymuje nagrodę za każdy blok przez cały czas działania łańcucha, więc górnicy zawsze otrzymują zapłatę za zabezpieczanie sieci.
</Answer>

Wzór zaniku dotyczy miesięcy od 0 do 47 i kończy się na 40,22 BNT za blok w miesiącu 47. Od miesiąca 48 demon używa bezpośrednio wartości ogonowej, więc nagroda staje się równa 2,0 BNT i taka pozostaje. Przy 288 blokach dziennie emisja ogonowa dodaje 576 BNT dziennie lub około 210 240 BNT rocznie.

Miesiąc 48 zaczyna się po 1440 dniach pięciominutowych bloków od startu, czyli mniej więcej po czterech latach. Data kalendarzowa zależy od rzeczywistych czasów bloków, więc wiarygodnym wskaźnikiem jest wysokość bloku.

## Ile BNT zostaje wykopane przed emisją ogonową?

Suma wszystkich bloków od wysokości 1 do 414 719 wynosi 98 561 905,08 BNT, około 98,6 miliona. Kod i oficjalna strona opisują harmonogram jako około 100 milionów monet przez cztery lata plus emisja ogonowa, a dokładna suma jest nieco niższa od tej okrągłej liczby.

Większość monet zostaje wykopana wcześnie. W podziale na lata harmonogramu:

| Miesiące | Wykopane BNT | Udział w całości przed emisją ogonową |
|---|---|---|
| 0-11 (rok 1) | 54,48 miliona | 55,3% |
| 12-23 (rok 2) | 25,84 miliona | 26,2% |
| 24-35 (rok 3) | 12,32 miliona | 12,5% |
| 36-47 (rok 4) | 5,93 miliona | 6,0% |

## Kiedy można wydać wykopane BNT?

Nagrodę za blok można wydać po dodaniu nad nim 60 kolejnych bloków, około pięciu godzin później. Ta reguła nazywa się dojrzewaniem coinbase. Pula bntpool.com czeka te same 60 bloków przed wypłatą nagrody za blok, a [strona wypłat](/payouts) wymienia wysłane płatności.

## Co harmonogram oznacza dla górników?

We wcześniejszych miesiącach nagrody są większe. Blok znaleziony w miesiącu 12 wypłaca 342,69 BNT, około 47% z 723,25 BNT wypłacanych w miesiącu 0, a blok w miesiącu 24 wypłaca 162,93 BNT. Każdy nowy miesiąc daje mniej za blok niż poprzedni, aż do emisji ogonowej.

Twój udział w tych nagrodach odpowiada udziałowi w hashrate sieci. Przy kopaniu solo oczekiwana liczba bloków znajdowanych dziennie wynosi 288 × (Twój hashrate ÷ hashrate sieci), a każdy wypłaca nagrodę swojego miesiąca. W puli te same obliczenia dotyczą hashrate puli, a nagroda za każdy blok jest dzielona między górników, którzy wnieśli pracę. Zarabiasz więc z każdego bloku znalezionego przez pulę, zamiast czekać na samodzielne znalezienie całego bloku. [Panel bntpool.com](/) pokazuje bieżący hashrate sieci i puli, a [strona statystyk](/stats) pokazuje Twój hashrate po rozpoczęciu kopania.

Aby zacząć kopać teraz:

1. Pobierz seine z [seineminer.com](https://seineminer.com/pl/#download). Działa w Windows, macOS i Linux oraz sam dostraja się do sprzętu.
2. Uruchom go i wklej adres Blocknet lub `@name`.
3. Zaakceptuj domyślną pulę `stratum+tcp://bntpool.com:3333`.

[Poradnik kopania na bntpool.com](/blog/pl/mine-blocknet-on-bntpool/) opisuje każdy krok po stronie puli, [strona Get Started](/start) wymienia jej ustawienia, a [poradnik kopania z seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) prowadzi krok po kroku przez konfigurację. Aby zobaczyć wyniki różnych procesorów, przeczytaj [sprzęt do kopania Blocknetu i hashrate](https://seineminer.com/pl/blog/blocknet-mining-hardware-hashrate/).

<Note>
Liczby na tej stronie pochodzą z kodu nagród demona Blocknet, sprawdzonego 25 października 2026 r. Według [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) Blocknet planuje aktualizację sieci o nazwie Faraday około 16 lutego 2027 r. Aktualizuj seine: program sprawdza nowe wydania przy uruchomieniu i co 12 godzin.
</Note>

<CTA target="seine" title="Kop Blocknet z seine" text="seine jest bezpłatny dla Windows, macOS i Linux i domyślnie kopie na bntpool.com." />
