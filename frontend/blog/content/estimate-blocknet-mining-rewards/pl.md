---
title: "Kalkulator wydobycia Blocknet"
description: "Oszacuj BNT dziennie i tygodniowo na podstawie swojego hashrate, bieżących danych sieci i harmonogramu nagród. Opłaty są pokazane osobno."
tags: [Nagrody za kopanie, Poradnik kopania, Hashrate, bntpool]
sourceHash: 0e1d600e0b00
translation: draft
faq:
  - q: 'Jak obliczyć nagrody za kopanie Blocknetu?'
    a: 'Podziel swój hashrate przez hashrate sieci, pomnóż przez 288 — liczbę bloków tworzonych dziennie przez Blocknet — i przez aktualną nagrodę za blok. Wynikiem jest oczekiwana liczba BNT brutto dziennie. Hashrate sieci odczytaj w panelu bntpool.com, a własny w seine lub na My Stats.'
  - q: 'Gdzie zobaczę hashrate sieci Blocknet?'
    a: 'Panel bntpool.com pokazuje go na karcie Network Hashrate, zwykle w KH/s, więc pomnóż przez 1000, aby uzyskać H/s. Ta sama wartość jest dostępna w H/s w polu network_hashrate API puli pod adresem bntpool.com/api/stats. Jest mierzona na podstawie dziesięciu najnowszych bloków i aktualizowana z każdym blokiem.'
  - q: 'Czy nagroda za blok Blocknetu zmienia się z czasem?'
    a: 'Tak. Nagroda jest stała w każdym 30-dniowym miesiącu liczącym 8640 bloków i maleje na początku następnego, aż w miesiącu 48 osiągnie stałe 2,0 BNT na blok. Aby ustalić bieżącą nagrodę, podziel wysokość bloku przez 8640, zaokrąglij w dół i znajdź ten miesiąc w harmonogramie nagród.'
  - q: 'Dlaczego moje dzienne nagrody w puli różnią się od oszacowania?'
    a: 'Oszacowanie jest średnią. W danym dniu pula może znaleźć więcej lub mniej bloków niż oczekiwano, a hashrate sieci zmienia się wraz z dołączaniem górników. Po tygodniu lub dłuższym okresie regularnego kopania dzienne wahania się uśredniają, a przyznane nagrody możesz śledzić na stronie My Stats.'
  - q: 'Czy kopanie w puli zmienia oczekiwaną ilość BNT?'
    a: 'Twój oczekiwany udział w nagrodach sieci pozostaje taki sam, ponieważ zależy wyłącznie od udziału w hashrate sieci. Pula zmienia moment otrzymywania nagród. Zamiast czekać dni na cały blok, otrzymujesz część każdego bloku znalezionego przez bntpool.com, więc nagrody przychodzą w małych, regularnych kwotach.'
---

<RewardsCalculator />

Ten poradnik pokazuje, jak oszacować, ile BNT dziennie kopie Twój sprzęt. Poznasz wzór, miejsca odczytu poszczególnych danych w [panelu bntpool.com](/) oraz obliczenia na datowanym przykładzie opartym na bieżących danych puli i najlepszych zmierzonych wynikach seine.

## Ile BNT dziennie wykopie Twój sprzęt?

<Answer>
Oczekiwana liczba BNT dziennie to Twój hashrate podzielony przez hashrate sieci, pomnożony przez 288 bloków dziennie i aktualną nagrodę za blok. Własny hashrate bierzesz z seine lub My Stats, hashrate sieci z panelu bntpool.com, a nagrodę z bieżącej wysokości bloku i harmonogramu nagród Blocknetu.
</Answer>

Zapisane jako wzór:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Każdy element ma proste uzasadnienie:

- **288 bloków dziennie.** Docelowo Blocknet tworzy blok co pięć minut, a 86 400 sekund doby podzielone przez 300 sekund na blok daje 288. Trudność dostosowuje się po każdym bloku, więc łańcuch pozostaje blisko tego tempa mimo dołączania górników.
- **Twój udział w hashrate odpowiada udziałowi w blokach.** Trudność Blocknetu jest w przybliżeniu oczekiwaną liczbą hashy potrzebnych do znalezienia bloku. Każdy hash ma taką samą szansę jego rozwiązania, więc maszyna wykonująca 1% hashy sieci może w dłuższym okresie oczekiwać 1% bloków.
- **Nagrodę za blok określa wysokość.** Każdy blok w tym samym 30-dniowym miesiącu liczącym 8640 bloków wypłaca tę samą nagrodę, więc dla dowolnego dnia w obrębie miesiąca nagroda jest znaną stałą.

Wynik to Twoja oczekiwana nagroda brutto, uśredniona w czasie.

## Gdzie znaleźć poszczególne dane?

### Twój hashrate

seine pokazuje hashrate w swoim panelu terminalowym od razu po uruchomieniu. Daj mu kilka minut: seine dostraja liczbę wątków i jądro obliczeniowe CPU podczas rzeczywistego kopania, a wynik stabilizuje się po znalezieniu najlepszych ustawień dla maszyny.

Gdy kopiesz już w puli, karta **Hashrate** na [My Stats](/stats) pokazuje Twój hashrate widziany przez pulę, uśredniony z ostatniej godziny. Do oszacowania użyj ustabilizowanego wyniku seine lub wykresu 24-godzinnego puli.

### Hashrate sieci

Karta **Network Hashrate** w panelu pokazuje hashrate całej sieci. Jest wyrażony w KH/s, więc odczyt 62,33 KH/s oznacza 62 330 H/s. Przelicz go na H/s przed dzieleniem, inaczej wynik będzie błędny tysiąckrotnie.

Pula mierzy tę wartość na podstawie pracy i znaczników czasu dziesięciu najnowszych bloków, więc zmienia się ona z bloku na blok; [czym jest hashrate?](/blog/pl/what-is-hashrate/) wyjaśnia obliczenia i przyczyny zmian. Aby uzyskać stabilniejszą daną, sprawdź ją kilka razy w ciągu dnia i użyj średniej. Możesz też odczytać ją w H/s z API:

```sh
curl -s https://bntpool.com/api/stats
```

Sekcja `chain` odpowiedzi zawiera `network_hashrate` w H/s oraz `current_job_height`, czyli blok, nad którym pracuje pula.

### Nagroda za blok

Karta **Current Block** w panelu pokazuje wysokość, nad którą pracuje sieć. Podziel ją przez 8640 i zaokrąglij w dół, aby otrzymać miesiąc, a następnie znajdź go w [harmonogramie nagród za bloki Blocknetu](/blog/pl/blocknet-block-reward-schedule/). [Strona bloków](/blocks) pozwala szybko sprawdzić wynik: kolumna Reward pokazuje pełną nagrodę za każdy blok znaleziony przez pulę.

## Przykład z 25 października 2026 r.

<Note>
Ten przykład przedstawia odczyt bieżących wartości z https://bntpool.com/api/stats z 25 października 2026 r., 02:03 EDT. Hashrate sieci i nagroda za blok się zmieniają, więc zanim oprzesz się na wyniku, wstaw dzisiejsze wartości z panelu.
</Note>

| Dane | Wartość w chwili odczytu |
|---|---|
| Bieżący blok | 69 020 (miesiąc 7, ponieważ 69 020 ÷ 8640 = 7,99) |
| Nagroda za blok | 467,67 BNT |
| Hashrate sieci | 62 329 H/s (pokazywany jako 62,33 KH/s) |
| Hashrate bntpool.com | 5240 H/s |

Weźmy serwer AMD EPYC Zen 5 z testów seine, osiągający 49,4 H/s:

1. Udział w sieci: 49,4 ÷ 62 329 = 0,0793%.
2. Oczekiwane bloki dziennie: 0,000793 × 288 = 0,2283.
3. Oczekiwane BNT dziennie: 0,2283 × 467,67 = około 106,8 BNT.

Miesiąc 8 zaczyna się na wysokości 69 120, około 100 bloków po odczycie, i wypłaca 439,46 BNT na blok. Przy tym samym hashrate sieci oszacowanie dla tego samego serwera wynosi 0,2283 × 439,46 = około 100,3 BNT dziennie. Dlatego warto sprawdzać nagrodę na początku każdego miesiąca.

Oto te same przykładowe obliczenia przy hashrate sieci z odczytu, dla najlepszego wyniku zmierzonego przez seine na każdej generacji sprzętu:

| Sprzęt | Wątki | Hashrate | Udział w sieci | BNT/dzień przy 467,67 | BNT/dzień przy 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (MacBook Pro 48 GB) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Serwer Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

Liczy się wynik Twojej własnej maszyny. Uruchom seine na jakiś czas, odczytaj ustabilizowany hashrate i użyj go zamiast wartości z tabeli. Poradnik [sprzętu do kopania Blocknetu i hashrate](https://seineminer.com/pl/blog/blocknet-mining-hardware-hashrate/) wyjaśnia, dlaczego przepustowość pamięci i RAM określają te wyniki, a pełne pomiary są na [stronie wydajności](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md) seine.

## Jak kopanie na bntpool.com zmienia oszacowanie?

Twój oczekiwany udział w puli pozostaje taki sam. Zmienia się sposób, w jaki dociera nagroda. bntpool.com wypłaca przez PPLNS: nagroda za każdy blok jest dzielona między udziały przesłane przez górników w sześciu godzinach przed nim, proporcjonalnie do ich trudności. [Jak działają wypłaty PPLNS](/blog/pl/how-pplns-payouts-work/) opisuje ten podział.

Możesz wykonać to samo oszacowanie od strony puli i otrzymasz ten sam wynik. W chwili odczytu 5240 H/s puli stanowiło 8,4% sieci, więc mogła oczekiwać 288 × 5240 ÷ 62 329 = około 24,2 bloku dziennie, mniej więcej jednego na godzinę. Panel pokazuje to jako **Expected Block Time** w sekcji Round Progress. Serwer 49,4 H/s ma 0,94% hashrate puli, a 0,94% z 24,2 bloku po 467,67 BNT to znów około 106,8 BNT dziennie. Hashrate puli skraca się w obliczeniu.

Różnicą jest czas otrzymywania nagród. Kopiąc samodzielnie, ten serwer Zen 5 oczekiwałby jednego całego bloku mniej więcej co 4,4 dnia; [kopanie Blocknetu w puli a solo](/blog/pl/pool-vs-solo-mining-blocknet/) porównuje oba tryby dokładniej. Na bntpool.com otrzymuje część każdego z około 24 bloków znajdowanych dziennie przez pulę, więc saldo rośnie małymi krokami przez całą dobę. Opłata puli i opłata deweloperska seine są odliczane od wartości brutto podanej przez wzór.

Dwa szczegóły puli wpływają na pierwszy dzień:

- **Okno zapełnia się przez sześć godzin.** Część każdego bloku przypadająca nowemu górnikowi rośnie w miarę zapełniania udziałami sześciogodzinnego okna PPLNS, a potem pozostaje na pełnym poziomie, dopóki górnik kontynuuje kopanie.
- **Nagrody dojrzewają przed wypłatą.** Nagroda za każdy blok przechodzi do Confirmed Rewards po 60 potwierdzeniach, około pięciu godzinach, a pula automatycznie wysyła saldo po osiągnięciu 0,1 BNT.

## Jak porównać oszacowanie z rzeczywistymi wynikami?

Oszacowanie jest średnią, a wyniki poszczególnych dni wahają się wokół niej wraz ze zmianami szczęścia puli między rundami. W wielu rundach średni nakład pracy zbliża się do 100%, więc tydzień regularnego kopania to dobry okres do porównania. Zapisz sumę Confirmed Rewards i Paid Balance na [My Stats](/stats) na początku tygodnia i ponownie na końcu. Przyrost to kwota przyznana przez pulę z bloków, które dojrzały w tym tygodniu. Porównaj ją z oszacowaniem dla tych samych dni, korzystając z obowiązujących wówczas nagród i hashrate sieci.

Oblicz wzór ponownie, gdy zmieni się któraś dana:

- **Na początku każdego nowego miesiąca**, gdy maleje nagroda. Granice miesięcy przypadają co 8640 bloków, mniej więcej co 30 dni.
- **Gdy zmienia się hashrate sieci**, wraz z dołączaniem górników.
- **Gdy zmienia się Twój hashrate**, po dodaniu maszyny, zarezerwowaniu dużych stron pamięci w Linux lub dodaniu GPU NVIDIA, na którym seine kopie równolegle z CPU.

<Note>
Wartości hashrate w tym poradniku dotyczą obecnego proof of work Argon2id w Blocknecie. Według [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) Blocknet planuje aktualizację sieci o nazwie Faraday około 16 lutego 2027 r., która ma zmienić proof of work. Aktualizuj seine: program sprawdza nowe wydania przy uruchomieniu i co 12 godzin.
</Note>

<CTA target="seine" title="Zmierz swój hashrate" text="Pobierz seine, pozwól mu dostroić się przez kilka minut i użyj jego hashrate we wzorze wraz z dzisiejszymi wartościami z panelu." />
