---
title: Czym jest pula wydobywcza?
description: Pula wydobywcza łączy hashrate wielu górników i dzieli między nich nagrodę za każdy blok. Poznaj działanie pul, PPLNS, PPS i kopania solo.
tags: [Podstawy kopania, Pule wydobywcze, PPLNS, bntpool]
sourceHash: 7b8c04cd86ae
translation: draft
faq:
  - q: 'Czy dołączenie do puli wydobywczej zwiększa ilość wykopanych monet?'
    a: 'Pula nie zmienia Twojego hashrate ani udziału w pracy całej sieci. Zmienia sposób otrzymywania nagród. Kopiąc samodzielnie, czekasz na całe bloki w losowych odstępach. W puli otrzymujesz niewielką część każdego znalezionego przez nią bloku, więc płatności przychodzą często, a ich wysokość odpowiada Twojemu hashrate.'
  - q: 'Czym różnią się PPLNS i PPS?'
    a: 'PPLNS wypłaca część każdego bloku faktycznie znalezionego przez pulę, dzieląc nagrodę według pracy przesłanej przez górników w niedawnym oknie czasowym. PPS płaci stałą kwotę za każdy zaakceptowany udział, obliczoną na podstawie jego wartości oczekiwanej, niezależnie od tego, czy pula znajdzie blok. bntpool.com stosuje PPLNS z sześciogodzinnym oknem.'
  - q: 'Czy do kopania w puli potrzebuję własnego węzła?'
    a: 'Nie. Pula uruchamia węzeł, tworzy szablony bloków, sprawdza udziały i przesyła znalezione bloki. Na bntpool.com potrzebujesz tylko programu seine oraz adresu Blocknet lub nazwy Blocknet ID. Uruchom seine, wklej adres, zaakceptuj domyślną pulę, a Twoja część każdego bloku zostanie wypłacona na ten adres.'
  - q: 'Jak zacząć kopać na bntpool.com?'
    a: 'Pobierz seine dla Windows, macOS lub Linux ze strony seineminer.com, uruchom go i wklej adres Blocknet lub @name, gdy program o to poprosi. bntpool.com jest domyślną pulą seine, więc wystarczy ją zaakceptować i rozpocząć kopanie. Wyszukaj swój adres na stronie statystyk puli, aby zobaczyć hashrate, udziały i saldo.'
---

Ten poradnik wyjaśnia, czym jest pula wydobywcza, dlaczego większość górników do niej dołącza i jak pula ustala zarobki każdego z nich. Poznasz popularne systemy wypłat — PPLNS, PPS i kopanie solo — oraz dowiesz się, co sprawdzić przed połączeniem programu z pulą. Przykładem w całym tekście jest bntpool.com, pula wydobywcza Blocknetu.

## Czym jest pula wydobywcza?

<Answer>
Pula wydobywcza to serwer, który łączy [hashrate](/blog/pl/what-is-hashrate/) wielu górników, aby wspólnie znajdowali bloki. Pula przydziela wszystkim pracę nad tym samym blokiem, zlicza pracę przesyłaną przez każdego i proporcjonalnie dzieli między nich nagrodę za każdy blok. Górnicy otrzymują małe, częste płatności zamiast rzadkich całych bloków.
</Answer>

W kryptowalucie opartej na proof of work górnicy rywalizują o znalezienie następnego bloku, obliczając hash jego nagłówka z różnymi wartościami nonce, aż jeden wynik będzie mniejszy od celu sieci. Każdy hash jest niezależną próbą z taką samą niewielką szansą powodzenia, a osoba, która znajdzie zwycięski hash, otrzymuje nagrodę za blok.

Pula zamienia tę rywalizację w pracę zespołową. Wszyscy połączeni z bntpool.com pracują nad blokami tworzonymi przez pulę, a gdy ktoś znajdzie zwycięski hash, nagroda zostaje podzielona między wszystkich górników, których praca przyczyniła się do jego znalezienia.

## Dlaczego górnicy dołączają do puli wydobywczej?

<Answer>
Górnicy dołączają do puli, aby otrzymywać regularne wypłaty. Typowy górnik pracujący samodzielnie dysponuje niewielkim ułamkiem hashrate sieci i może czekać na blok wiele dni, w losowych odstępach. Łączny hashrate puli pozwala często znajdować bloki, a każdy górnik otrzymuje część każdego z nich, dzięki czemu dochód przychodzi regularnie i odpowiada jego hashrate.
</Answer>

W Blocknecie łatwo sprawdzić te obliczenia. Docelowo sieć tworzy jeden blok co pięć minut, czyli 288 bloków dziennie. Górnik pracujący samodzielnie może więc oczekiwać 288 × (jego hashrate ÷ hashrate sieci) bloków dziennie.

Przyjmijmy okrągłe, hipotetyczne liczby: maszyna o wydajności 50 H/s w sieci o wydajności 50 000 H/s ma 0,1% mocy sieci i znajduje średnio 0,288 bloku dziennie, czyli jeden blok mniej więcej co trzy i pół dnia. To tylko średnia. Znajdowanie bloków jest losowe, więc rzeczywisty odstęp między blokami tej maszyny może być znacznie krótszy lub dłuższy.

Podłącz tę samą maszynę do puli o łącznej wydajności 5000 H/s, a pula znajdzie około 28,8 bloku dziennie, średnio jeden co 50 minut. Maszyna wykonała 1% pracy puli, więc otrzyma 1% nagrody za każdy z tych bloków. Jej wkład pracy pozostaje taki sam; zmienia się to, że nagroda przychodzi w małych częściach wiele razy dziennie. Poradnik [kopanie Blocknetu w puli a solo](/blog/pl/pool-vs-solo-mining-blocknet/) przedstawia to porównanie także dla większych instalacji.

## Jak działa pula wydobywcza?

Każda pula działa według tego samego podstawowego cyklu. Na bntpool.com wygląda on tak:

1. **Pula tworzy szablon bloku.** bntpool.com prowadzi własny węzeł Blocknet i pobiera z niego szablon bloku, więc górnicy nie muszą samodzielnie uruchamiać ani synchronizować węzła.
2. **Wysyła pracę górnikom.** Górnicy łączą się przez Stratum, protokół służący pulom do wysyłania pracy, pod adresem `stratum+tcp://bntpool.com:3333`. Pula wysyła każdemu połączeniu zadanie oparte na bieżącym szablonie.
3. **Górnicy obliczają hashe i przesyłają udziały.** Udział (share) to hash spełniający łatwiejszy cel ustalony przez pulę. Udziały potwierdzają ilość pracy wykonywanej przez każdego górnika.
4. **Udział spełniający również cel sieci jest blokiem.** Pula sprawdza każdy udział względem obu celów. Gdy spełnia cel sieci, pula przesyła blok do swojego węzła.
5. **Nagroda zostaje podzielona i wypłacona.** Pula dzieli nagrodę za blok według swojego systemu wypłat i wysyła każdemu górnikowi jego część po jej potwierdzeniu i osiągnięciu minimalnej kwoty wypłaty.

## Czym jest udział w puli wydobywczej?

Udział to hash, który spełnia łagodniejsze wymaganie niż prawdziwy blok. Blok wymaga hasha mniejszego od celu sieci, natomiast udział wymaga jedynie hasha mniejszego od znacznie łatwiejszego celu udziałów puli. Większość udziałów nigdy nie staje się blokami, ale regularnie pokazują puli, ile obliczeń wykonuje każdy górnik.

Udziały są ważone trudnością. W Blocknecie trudność D oznacza, że przed osiągnięciem celu oczekuje się około D hashy, więc udział o trudności 60 reprezentuje około 60 hashy pracy. bntpool.com ustawia trudność udziałów dla każdego połączenia i na bieżąco ją dostosowuje, dążąc do około 10 udziałów na pięć minut i rozpoczynając nowe połączenia od trudności 60. Szybki serwer dostaje trudniejsze udziały, laptop łatwiejsze, a oba otrzymują rozliczenie za faktycznie wykonaną pracę. Pula pamięta też trudność każdej maszyny przez godzinę, więc po szybkim restarcie można kontynuować od poprzedniej wartości.

## Popularne systemy wypłat w pulach

System wypłat określa, jak pula przelicza udziały na zarobki. Te spotkasz najczęściej.

| System | Za co otrzymujesz zapłatę | Jak działa |
|---|---|---|
| PPLNS (pay per last N shares) | Bloki znalezione przez pulę | Nagroda za każdy blok jest dzielona między udziały przesłane w niedawnym oknie czasowym, według ich trudności |
| PPS (pay per share) | Każdy zaakceptowany udział | Pula płaci stałą kwotę za udział na podstawie jego wartości oczekiwanej, niezależnie od znalezienia bloku |
| Solo | Bloki znalezione samodzielnie | Kopiesz przez własny węzeł i zachowujesz całą nagrodę za każdy znaleziony blok |

**PPLNS** patrzy wstecz od każdego bloku znalezionego przez pulę. Na bntpool.com „ostatnie N udziałów” oznacza okno sześciu godzin: pula sumuje trudność wszystkich zaakceptowanych udziałów z sześciu godzin przed blokiem i przyznaje każdemu górnikowi taką część nagrody, jaką stanowi jego udział w tej pracy. Każdy udział liczy się do wszystkich bloków znalezionych, gdy znajduje się w oknie, więc górnik pracujący stale zawsze ma za sobą pełne okno, a restart niczego nie zeruje. [Jak działają wypłaty PPLNS na bntpool.com](/blog/pl/how-pplns-payouts-work/) wyjaśnia podział krok po kroku.

**PPS** płaci za każdy udział po jego otrzymaniu, korzystając z jego wartości oczekiwanej: trudność udziału podzielona przez trudność sieci, pomnożona przez nagrodę za blok. Operator puli bierze na siebie wahania częstotliwości znajdowania bloków, a pomiędzy nimi płaci górnikom z własnego salda.

**Kopanie solo** pomija pulę. Własny węzeł tworzy szablony, a każdy znaleziony blok wypłaca całą nagrodę do Twojego portfela. Jest odpowiednie dla instalacji wystarczająco dużych, by często znajdować bloki samodzielnie. Program seine obsługuje oba tryby: domyślnie kopie na bntpool.com, a `./seine --mode daemon` kopie przez Twój własny, zsynchronizowany węzeł Blocknet.

## Na co zwracać uwagę przy wyborze puli?

Porównując pule, zwróć uwagę na szczegóły decydujące o wypłatach i łatwości ich sprawdzenia:

- **Jasny system wypłat.** Pula powinna podać system i jego ustawienia, takie jak okno PPLNS. bntpool.com wymienia je na [stronie Get Started](/start) i publikuje pod adresem `https://bntpool.com/api/info`.
- **Potwierdzenia i minimalna wypłata.** Nagrody zwykle czekają, aż blok znajdzie się wystarczająco głęboko w łańcuchu, a płatności wychodzą po osiągnięciu minimum przez saldo. Na bntpool.com nagroda za blok zostaje potwierdzona po 60 blokach, zgodnie z okresem dojrzewania coinbase w Blocknecie, a salda wynoszące co najmniej 0,1 BNT są wypłacane automatycznie.
- **Publiczne, sprawdzalne liczby.** Powinieneś móc zobaczyć każdy blok znaleziony przez pulę i każdą wysłaną płatność. bntpool.com pokazuje je na stronach [bloków](/blocks) i [wypłat](/payouts), nakład pracy w każdej rundzie na [stronie szczęścia](/luck), a stan usług na [stronie statusu](/status).
- **Własne statystyki.** Wyszukaj swój adres, aby zobaczyć hashrate, udziały i salda. Na bntpool.com służy do tego [strona statystyk](/stats); poradnik [monitorowania górnika](/blog/pl/track-your-miner-on-bntpool/) wyjaśnia każdą kartę.
- **Łatwa konfiguracja.** Im mniej ustawień, tym szybciej zaczniesz kopać. bntpool.com jest domyślną pulą seine, który sam dostraja wątki, jądra obliczeniowe i pamięć. Możesz też zalogować się za pomocą [nazwy Blocknet ID](https://seineminer.com/pl/blog/blocknet-id-pay-by-name/), takiej jak `@name`, zamiast pełnego adresu.

## bntpool.com w skrócie

| Ustawienie | bntpool.com |
|---|---|
| System wypłat | PPLNS |
| Okno udziałów | 6 godzin |
| Potwierdzenia przed wypłatą | 60 bloków |
| Minimalna wypłata | 0,1 BNT |
| Adres Stratum | `stratum+tcp://bntpool.com:3333` |

[Panel puli](/) pokazuje bieżący stan: połączonych górników, hashrate puli i sieci, znalezione bloki oraz nakład pracy bieżącej rundy. Według odczytu z 25 października 2026 r. o 02:25 EDT pula osiągała około 5200 H/s w sieci o wydajności około 44 400 H/s, przy 246 połączonych górnikach. To około 12% sieci, wystarczająco, by oczekiwać około 34 bloków dziennie, jednego mniej więcej co 42 minuty. Te liczby stale się zmieniają, więc sprawdź aktualne wartości w panelu i skorzystaj z [poradnika szacowania nagród](/blog/pl/estimate-blocknet-mining-rewards/), aby przeliczyć je na BNT dla swojego hashrate.

Aby dołączyć, [pobierz seine](https://seineminer.com/pl/#download), uruchom go, wklej adres Blocknet lub `@name` i zaakceptuj domyślną pulę. [Poradnik kopania z seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) opisuje każdy krok, a [kopanie Blocknetu na bntpool.com](/blog/pl/mine-blocknet-on-bntpool/) wyjaśnia nazwy maszyn i sprawdzanie pierwszych udziałów.

<CTA target="pool" title="Dołącz do bntpool.com" text="Połącz seine z pulą i otrzymuj część każdego znalezionego przez nią bloku, automatycznie wysyłaną na Twój adres po osiągnięciu 0,1 BNT." />
