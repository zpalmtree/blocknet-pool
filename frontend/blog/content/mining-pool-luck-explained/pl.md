---
title: 'Szczęście puli wydobywczej: wysiłek i czas znajdowania bloków'
description: 'Poznaj szczęście puli, wysiłek 100% i długie rundy. Odczytuj historię bntpool.com oraz oczekiwany czas bloku i odróżniaj znajdowanie bloków od wypłat.'
tags: ['Pule wydobywcze', 'Szczęście puli', 'Blocknet']
faq:
  - q: 'Czy wysiłek 100% oznacza, że pula zaraz znajdzie blok?'
    a: 'Nie. Oznacza, że zarejestrowana praca udziałów osiągnęła ilość oczekiwaną dla jednego bloku przy trudności użytej w tym obliczeniu. To porównanie ze średnią, a nie procent ukończenia. Każdy nowy hash nadal musi spełnić cel sieci.'
  - q: 'Czy niższy procent szczęścia jest lepszy na bntpool.com?'
    a: 'Najpierw przeczytaj etykietę i definicję. Kolumna Wysiłek i karta Średnie szczęście na bntpool.com przedstawiają pracę względem oczekiwanej, więc niższe wartości oznaczają mniej zarejestrowanej pracy na blok. Inne witryny mogą stosować odwrotną konwencję. Porównuj zgodne wskaźniki i rozmiary próbek, nie tylko procenty.'
  - q: 'Czy długa runda zwiększy moją następną wypłatę?'
    a: 'Długa runda nie gwarantuje większej indywidualnej wypłaty. Znalezienie bloku określa, kiedy nagroda staje się dostępna, a rozliczenie PPLNS wyznacza twój udział na podstawie kwalifikującej się zaakceptowanej pracy. Potem obowiązują reguły potwierdzeń i wypłat. Sprawdź salda w Moich statystykach, a kolejność etapów w poradniku wypłat.'
sourceHash: dfd8c710a719
translation: draft
---

Panel puli pokazuje zarówno pracę wydobywczą, jak i znalezione dzięki niej bloki. Ten poradnik wyjaśnia procenty wysiłku, oczekiwany czas bloku i historię rund na bntpool.com. Nauczysz się odróżniać średnią statystyczną od odliczania i sprawdzać własne połączenie z pulą bez czekania na jej następny blok.

## Czym jest szczęście puli wydobywczej?

<Answer>
Szczęście puli opisuje, jak rzeczywiste znaleziska bloków mają się do średniej oczekiwanej z wykonanej pracy. Bloki są znajdowane losowo: jedne wymagają mało pracy, inne więcej. Na bntpool.com wysiłek rundy porównuje pracę zaakceptowanych udziałów z trudnością bloku; niższy wysiłek oznacza, że runda znalazła blok przy mniejszej zarejestrowanej pracy.
</Answer>

Pula łączy próby wielu górników, ale nadal musi znaleźć hash spełniający cel sieci. Zaakceptowane udziały mierzą wkład przy łatwiejszym celu i pomagają oszacować pracę haszowania między znaleziskami. [Wprowadzenie do pul wydobywczych](/blog/pl/what-is-a-mining-pool/) wyjaśnia tę zależność; szczęście opisuje zmienność uzyskiwanych bloków.

## Jak pula oblicza wysiłek?

Dla zakończonej rundy bntpool.com dzieli pracę zaakceptowanych udziałów przez trudność bloku i mnoży przez 100. Praca udziałów jest ważona trudnością, więc nie jest jedynie liczbą przesłanych udziałów. Udział o wyższej przydzielonej trudności reprezentuje więcej oczekiwanej pracy haszowania niż udział o niższej trudności.

**Wysiłek (%) = praca zaakceptowanych udziałów ÷ trudność bloku × 100**

Załóżmy, że blok ma trudność 10 000, a pula rejestruje w jego rundzie 5000 jednostek zaakceptowanej pracy. Wysiłek wynosi 50%. Przy 20 000 jednostek i tej samej trudności byłoby to 200%. Są to przykłady arytmetyczne, a nie pomiary konkretnego bloku w działającej sieci.

| Wysiłek | Jak odczytywać |
|---|---|
| 50% | Połowa oczekiwanej pracy przy tej trudności |
| 100% | Ilość pracy oczekiwana dla jednego bloku |
| 200% | Dwukrotność oczekiwanej pracy przy tej trudności |

Wysiłek bieżącej rundy korzysta z aktualnej trudności, która może się zmieniać wraz z rozwojem łańcucha. Historyczne wiersze korzystają z trudności swojego bloku. Liczba porównuje zatem pracę, a nie pokazuje postęp, który musi zatrzymać się na 100%. Różni się też od czasu rundy: szybsza pula może wykonać tę samą pracę w krótszym czasie.

## Co oznacza oczekiwany czas bloku?

To średni odstęp wynikający z szacowanego udziału puli w hashrate sieci. Panel bntpool.com oblicza go jako 300 sekund razy hashrate sieci podzielony przez hashrate puli. Używaj tych samych jednostek dla obu wartości. [Poradnik hashrate](/blog/pl/what-is-hashrate/) wyjaśnia, dlaczego szacunki zmieniają się wraz z zaakceptowanymi udziałami i nowymi blokami.

**Oczekiwane sekundy = 300 × H/s sieci ÷ H/s puli**

Jeśli sieć jest szacowana na 100 000 H/s, a pula na 10 000 H/s, pula ma jedną dziesiątą szacowanej całości. Oczekiwany odstęp wynosi 3000 sekund, czyli 50 minut. Taka średnia może opisywać mieszankę krótkich i długich rund; nie planuje następnego bloku dokładnie za 50 minut.

Panel porównuje też czas, który upłynął, z czasem oczekiwanym. Ten procent oparty na czasie może różnić się od wysiłku opartego na udziałach, ponieważ korzysta z innych danych. Sprawdź etykietę przed porównaniem dwóch liczb. Zmiany hashrate puli lub trudności sieci mogą zmienić szacunek, nawet gdy nadal trwa ta sama runda.

## Czy po 100% wysiłku należy się blok?

<Answer>
Puli nie należy się blok po osiągnięciu 100% wysiłku. Wcześniejsze nieudane hashe nie zwiększają szansy, że następny spełni niezmieniony cel sieci. Przy stałym hashrate i trudności oczekiwany czas od teraz pozostaje taki sam, nawet po długiej rundzie. Więcej pracy daje więcej prób, nie termin zakończenia.
</Answer>

Prosty model prawdopodobieństwa ułatwia zrozumienie. Przy niezależnych próbach i stałym tempie znajdowania bloków czas oczekiwania ma w przybliżeniu rozkład wykładniczy. Szansa na brak bloku po jednym oczekiwanym odstępie wynosi około 36,8%; po dwóch 13,5%; po trzech 5,0%. Te procenty opisują wiele hipotetycznych rund w stałych warunkach, a nie prognozę obecnej rundy puli.

| Liczba minionych oczekiwanych odstępów | Przybliżona szansa, że nadal nie ma bloku |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Obliczenie to `exp(-t / T)`, gdzie `T` jest średnim odstępem, a `t` czasem, który upłynął. [Dokumentacja NIST dotycząca rozkładu wykładniczego](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) podaje tę funkcję przeżycia. Rzeczywisty hashrate i trudność zmieniają się, więc tabela wyjaśnia losowość. Seria szybkich bloków również nie zmienia prawdopodobieństwa następnego hasha, jeśli cel pozostaje taki sam.

## Jak czytać historię szczęścia bntpool.com?

Otwórz [Szczęście](/luck), aby porównać zakończone rundy. Każdy wiersz pokazuje blok, wysiłek, czas rundy, status i czas znalezienia. Patrz na kilka rund łącznie zamiast wybierać jedną niezwykle krótką lub długą. Link bloku pozwala sprawdzić odpowiedni zapis łańcucha, a podział na strony umożliwia przejrzenie szerszej historii.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Prawdziwa strona historii szczęścia bntpool.com z wysiłkiem, czasem rundy i statusem bloków" width="1200" height="833" loading="lazy" />

Ten zrzut wykonano 26 października 2026 r. Wiersze i statusy są historycznym zapisem chwili; aktualne wartości znajdziesz na działającej stronie. Status oczekujący lub potwierdzony opisuje stan potwierdzenia bloku. Nie zmienia znaczenia zarejestrowanego procentu wysiłku i jest odrębny od płatności na twój adres.

Na panelu Średnie szczęście jest średnią arytmetyczną zarejestrowanych procentów wysiłku dla nieosieroconych bloków do bieżącej wysokości łańcucha. Mimo nazwy karta stosuje konwencję wysiłku: niższa wartość oznacza mniej zarejestrowanej pracy na blok. Średnie podsumowują tę próbkę. Nie obiecują, że przyszłe rundy zrekompensują konkretny wcześniejszy wynik.

## Jak szczęście puli wpływa na nagrody?

Szczęście puli zmienia momenty znajdowania bloków. PPLNS określa, jak nagroda znalezionego bloku jest dzielona między kwalifikującą się zaakceptowaną pracę. To odrębne etapy. Granica rundy nie zmienia okna PPLNS w regułę wypłaty wyłącznie za tę rundę; przeczytaj [jak działają wypłaty PPLNS](/blog/pl/how-pplns-payouts-work/), aby poznać rzeczywiste okno, potwierdzenia i próg wypłaty.

Dlatego saldo może zmieniać się skokowo, choć miner nadal przesyła zaakceptowane udziały. [Kalkulator nagród](/blog/pl/estimate-blocknet-mining-rewards/#mining-calculator) szacuje wydobycie z hashrate i danych sieci. Pomaga określać oczekiwania w czasie; nie przewiduje bloku ani chwili, w której płatność dotrze do portfela.

## Co sprawdzać podczas długiej rundy?

Sprawdzaj to, co można bezpośrednio obserwować: miner działa, udziały nadal są akceptowane, a właściwy adres widnieje w [Moich statystykach](/stats). [Status](/status) pokazuje publiczny stan usług. Jeśli twoje zaakceptowane udziały przestaną napływać, skorzystaj z [diagnostyki statystyk minera](/blog/pl/track-your-miner-on-bntpool/#zero-hashrate); długa runda puli i odłączona koparka wymagają innych kontroli.

Gdy połączenie działa prawidłowo, oceniaj szczęście na podstawie miarodajnej historii i tych samych definicji. Każda liczba na panelu ma wtedy jasną rolę: hashrate szacuje pracę na sekundę, wysiłek porównuje ją z pracą oczekiwaną dla bloku, a salda pokazują rozliczenie nagród. Żaden z tych wskaźników nie wymaga zgadywania, kiedy pojawi się kolejny zwycięski hash.

<CTA target="pool" title="Zacznij wydobywać na bntpool.com" text="Połącz seine, sprawdź zaakceptowane udziały i śledź historię rund oraz nagrody na panelu puli." />
