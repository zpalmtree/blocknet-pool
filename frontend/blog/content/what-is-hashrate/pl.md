---
title: Czym jest hashrate w kopaniu kryptowalut?
description: Hashrate to liczba hashy obliczanych przez górnika na sekundę. Poznaj jednostki i różnice między hashrate Twoim, puli i sieci oraz przyczyny ich zmian.
tags: [Hashrate, Podstawy kopania, Blocknet, bntpool]
sourceHash: 0bfdcdca348e
translation: draft
faq:
  - q: 'Jaki hashrate może osiągnąć komputer podczas kopania Blocknetu?'
    a: 'W pomiarach seine najlepszy wynik dla każdej generacji sprzętu wynosi 49,4 H/s dla serwera AMD EPYC Zen 5, 35,7 H/s dla AMD EPYC Zen 4, 30,9 H/s dla laptopa Apple M4 Max i 21,5 H/s dla serwera Arm AWS Graviton4. Twój wynik zależy głównie od przepustowości pamięci i liczby wątków po 2 GB, które pomieści RAM.'
  - q: 'Dlaczego hashrate Blocknetu mierzy się w H/s zamiast MH/s lub TH/s?'
    a: 'Każdy hash Blocknetu to jedno obliczenie Argon2id, które zapisuje około 2 GiB pamięci i ponownie ją odczytuje, więc nawet duży serwer wykonuje tylko dziesiątki hashy na sekundę. Algorytmy proof of work oparte na szybkich funkcjach skrótu, takich jak SHA-256, osiągają znacznie większe liczby, dlatego używają większych jednostek. Porównuj hashrate wyłącznie w obrębie tego samego algorytmu.'
  - q: 'Dlaczego pula pokazuje inny hashrate niż mój program?'
    a: 'Program zlicza każdy ukończony hash. Pula widzi tylko przesłane udziały i szacuje Twój hashrate na podstawie ich trudności z ostatniej godziny. Udziały przychodzą w losowych momentach, więc wartość puli bardziej się waha i stabilizuje z czasem. Aby porównanie było miarodajne, zestaw wykres 24-godzinny na My Stats ze średnią programu.'
  - q: 'Czy wyższy hashrate oznacza większe nagrody za kopanie?'
    a: 'Tak, proporcjonalnie. Każdy hash ma taką samą szansę znalezienia bloku, więc Twój oczekiwany udział w blokach sieci jest równy Twojemu udziałowi w jej hashrate. Podwojenie hashrate podwaja oczekiwane nagrody, a wzrost hashrate sieci zmniejsza Twój udział. Na bntpool.com otrzymujesz go jako część każdego bloku znalezionego przez pulę.'
---

Hashrate to liczba, którą każdy program do kopania, pula i eksplorator bloków pokazuje na pierwszym miejscu. Ten poradnik wyjaśnia, co mierzy, jak odczytywać jednostki od H/s wzwyż, jak Twój hashrate ma się do hashrate puli i sieci oraz dlaczego każda wartość zmienia się z minuty na minutę. Przykładem są Blocknet i panel bntpool.com.

## Czym jest hashrate?

<Answer>
Hashrate określa, ile hashy górnik oblicza na sekundę. W kopaniu opartym na proof of work każdy hash jest jedną próbą znalezienia rozwiązania bloku, a każda próba ma taką samą niewielką szansę powodzenia. Wyższy hashrate oznacza więcej prób na sekundę, więc Twój oczekiwany udział w blokach rośnie proporcjonalnie do udziału w łącznym hashrate.
</Answer>

Łańcuch oparty na proof of work wymaga od górników znalezienia nagłówka bloku, którego hash jest mniejszy od celu. Nie ma drogi na skróty: program zmienia w nagłówku liczbę zwaną nonce, oblicza hash wyniku, porównuje go z celem i powtarza czynność. Hashrate zlicza, ile takich prób maszyna wykonuje w każdej sekundzie.

W Blocknecie jedna próba to jeden hash Argon2id. Nonce służy jako hasło, pozostała część nagłówka bloku jako sól, a funkcja przetwarza 2 GiB pamięci, zanim zwróci 32-bajtowy wynik. Zarówno seine, jak i pula podają hashrate w ukończonych hashach na sekundę, więc 1 H/s oznacza jedno pełne obliczenie Argon2id na sekundę.

## Co oznaczają H/s, kH/s i MH/s?

Jednostki hashrate stosują zwykłe przedrostki metryczne, a każdy kolejny stopień jest tysiąc razy większy od poprzedniego:

| Jednostka | Hashe na sekundę | Typowe zastosowanie |
|---|---|---|
| H/s | 1 | Pojedynczy komputer w Blocknecie |
| kH/s (KH/s) | 1000 | Łączne wartości puli i sieci Blocknet |
| MH/s | 1 000 000 | Szybsze algorytmy proof of work |
| GH/s | 1 000 000 000 | Szybkie funkcje skrótu, takie jak SHA-256 |
| TH/s i więcej | 1 000 000 000 000 i więcej | Łączne wartości sieci korzystających z szybkich funkcji skrótu |

Panel bntpool.com zapisuje jednostkę tysięcy jako KH/s i przechodzi do wyższej jednostki po przekroczeniu 1000, więc 44,44 KH/s oznacza 44 440 H/s. Dzieląc jeden hashrate przez drugi, najpierw przelicz oba na H/s.

## Dlaczego hashrate Blocknetu jest tak niski?

Proof of work Blocknetu jest pamięciochłonny. Każdy hash zapisuje około 2 GiB pamięci i odczytuje około 2 GiB, znacznie więcej, niż mieści jakakolwiek pamięć podręczna procesora, więc tempo wyznacza szybkość podsystemu pamięci. Każdy wątek kopania potrzebuje własnych 2 GiB, dlatego RAM określa, ile wątków może uruchomić maszyna.

Oto najlepsze wyniki zmierzone przez seine dla każdej generacji sprzętu:

| Sprzęt | Wątki | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (MacBook Pro 48 GB) | 16 | 30,9 H/s |
| Serwer Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Nawet serwer Zen 5 z 32 wątkami wykonuje około 1,5 hasha na wątek w każdej sekundzie. Algorytmy oparte na szybkich funkcjach skrótu dają znacznie większe liczby: wiki Bitcoina podaje, że wydajność koparek Bitcoin wykonujących podwójne SHA-256 już w 2014 r. wynosiła miliardy hashy na sekundę. Te dwie liczby opisują różną ilość pracy na hash, więc hashrate ma znaczenie tylko w porównaniu z wynikami tego samego algorytmu. [Wyjaśnienie proof of work Argon2id](https://seineminer.com/pl/blog/argon2id-proof-of-work-explained/) szczegółowo opisuje algorytm, a [poradnik sprzętu do kopania Blocknetu](https://seineminer.com/pl/blog/blocknet-mining-hardware-hashrate/) pokazuje, jak przepustowość pamięci i RAM kształtują te wyniki.

## Twój hashrate, hashrate puli i sieci

Dla każdego kopiącego w [puli](/blog/pl/what-is-a-mining-pool/) ważne są trzy wartości hashrate, a każdą mierzy się inaczej.

### Twój hashrate

seine zlicza każdy hash ukończony przez Twoją maszynę i pokazuje w panelu dwie wartości: **Current**, czyli aktualne tempo, oraz **Average**, czyli średnią z całej sesji. Aby uzyskać stabilny pomiar możliwości maszyny, użyj średniej po kilku minutach, gdy seine dostroi liczbę wątków i jądro obliczeniowe CPU na rzeczywistej pracy wydobywczej.

Pula nie widzi Twoich hashy. Widzi przesyłane udziały, czyli hashe spełniające łatwiejszy cel ustawiony dla Twojego połączenia. bntpool.com szacuje Twój hashrate na podstawie łącznej trudności zaakceptowanych udziałów z ostatniej godziny i pokazuje go na [My Stats](/stats). [Monitorowanie górnika na bntpool.com](/blog/pl/track-your-miner-on-bntpool/) opisuje tę stronę.

### Hashrate puli

Karta **Pool Hashrate** w panelu sumuje pracę udziałów wszystkich połączonych górników z tego samego jednogodzinnego okna. Pokazuje, jaką część pracy sieci wykonuje bntpool.com, a tym samym jak często pula może oczekiwać znalezienia bloku. Panel wyświetla ten czas jako **Expected Block Time**.

### Hashrate sieci

Nikt nie może policzyć wszystkich hashy w sieci, więc jej hashrate oblicza się na podstawie bloków. Trudność to oczekiwana liczba hashy potrzebna do znalezienia bloku, a docelowy czas bloku w Blocknecie wynosi 300 sekund.

## Jak oblicza się hashrate sieci?

<Answer>
Hashrate sieci szacuje się na podstawie trudności i czasu bloków. W Blocknecie trudność jest w przybliżeniu oczekiwaną liczbą hashy na blok, a docelowy czas bloku wynosi 300 sekund, więc hashrate sieci to w przybliżeniu trudność podzielona przez 300. Panel bntpool.com oblicza go z rzeczywistej pracy i znaczników czasu dziesięciu najnowszych bloków.
</Answer>

Oto te obliczenia z bieżącymi wartościami odczytanymi 25 października 2026 r. o 02:26 EDT. [Eksplorator Blocknetu](https://explorer.blocknetcrypto.com) pokazywał następną trudność 16 943 147, więc sieć miała wymagać około 16,9 miliona hashy na blok. Po podzieleniu przez 300 sekund daje to około 56 477 H/s.

W tym samym momencie panel bntpool.com pokazywał hashrate sieci 44,44 KH/s, czyli 44 440 H/s. Obie wartości szacują to samo z różnych perspektyw. Wartość wynikająca z trudności to tempo, które dawałoby bloki dokładnie co docelowe pięć minut. Wartość w panelu dzieli rzeczywistą pracę ostatnich dziesięciu bloków przez faktyczny czas ich powstania, więc gdy ostatnie bloki trwały dłużej niż pięć minut, jest niższa. Blocknet dostosowuje trudność po każdym bloku, co z czasem utrzymuje te dwie wartości blisko siebie.

## Dlaczego hashrate się waha?

Każda z trzech wartości zmienia się z innych powodów.

- **Twoja maszyna.** W pierwszych minutach seine porównuje liczby wątków i jądra obliczeniowe podczas kopania, po czym wybiera najszybsze, więc tempo rośnie w miarę strojenia. Później inne zadania korzystające z pamięci lub rdzeni komputera mogą zabierać część czasu obliczeń.
- **Twój wynik widziany przez pulę.** Udziały są zdarzeniami losowymi. bntpool.com ustawia trudność każdego połączenia tak, aby uzyskać około 10 udziałów na pięć minut, czyli mniej więcej 120 na godzinę. Szczęśliwszy lub spokojniejszy okres zmienia więc oszacowanie. Dla dopiero podłączonej maszyny średnia obejmuje minuty jej dotychczasowego kopania, aż zapełni się pełna godzina.
- **Sieć.** Czasy bloków także są losowe, a próba dziesięciu bloków jest mała. 25 października 2026 r. panel pokazywał hashrate sieci około 50 057 H/s o 00:26 EDT, 62 329 H/s o 02:03 i 44 440 H/s o 02:26. Dołączający i odłączający się górnicy dodają do tych wahań rzeczywiste zmiany.

Podejmując decyzje, korzystaj ze średnich: średniej sesji seine, wykresu 24-godzinnego na My Stats oraz hashrate sieci sprawdzonego kilka razy w ciągu dnia.

## Co Twój hashrate oznacza dla nagród?

Ponieważ każdy hash ma taką samą szansę powodzenia, Twój udział w hashrate sieci jest Twoim oczekiwanym udziałem w blokach. Blocknet tworzy 288 bloków dziennie, więc górnik z 0,1% mocy sieci może w dłuższym okresie oczekiwać nagród odpowiadających około 0,29 bloku dziennie. [Jak szacować nagrody za kopanie Blocknetu](/blog/pl/estimate-blocknet-mining-rewards/) przelicza to na BNT dziennie przy użyciu danych z [panelu](/).

Na bntpool.com ten oczekiwany udział trafia do Ciebie jako część każdego bloku znalezionego przez pulę, rozliczana według zaakceptowanych udziałów, więc nagrody przyrastają małymi krokami przez całą dobę. Jeśli jeszcze nie zacząłeś, [poradnik instalacji seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) przeprowadzi Cię od pobrania do pierwszych udziałów w kilka minut.

<Note>
Wartości hashrate w tym poradniku dotyczą obecnego proof of work Argon2id w Blocknecie. Według [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) Blocknet planuje aktualizację sieci o nazwie Faraday około 16 lutego 2027 r., która ma zmienić proof of work. Aktualizuj seine: program sprawdza nowe wydania przy uruchomieniu i co 12 godzin.
</Note>

<CTA target="seine" title="Sprawdź swój hashrate" text="Pobierz seine, pozwól mu dostroić się przez kilka minut i obserwuj hashrate w jego panelu oraz na My Stats." />
