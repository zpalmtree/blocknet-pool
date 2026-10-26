---
title: 'Trudność kopania: sieć i udziały'
description: 'Poznaj cel trudności kopania, sposób jego korekty w Blocknet i różnicę między trudnością sieci a udziałów oraz ich wpływ na statystyki kopania.'
tags: ['Podstawy kopania', 'Trudność', 'Blocknet']
faq:
  - q: 'Czy wyższa trudność zmniejsza hashrate?'
    a: 'Zmienia cel, który musi spełnić wynik, a nie bezpośrednio liczbę obliczeń na sekundę. Przy stałym hashrate wyższa trudność sieci wydłuża oczekiwany średni odstęp między blokami. Sprawdzaj wydajność minera oddzielnie od częstotliwości bloków i udziałów.'
  - q: 'Czy trudniejszy udział daje więcej?'
    a: 'Reprezentuje więcej oczekiwanej pracy, ale przy tym samym hashrate jest wysyłany rzadziej. Porównuj pracę ważoną trudnością w tym samym okresie. W bntpool.com nagrody zależą też od zaakceptowanych bloków i PPLNS, nie od samej liczby udziałów.'
  - q: 'Jak często Blocknet zmienia trudność?'
    a: 'Po każdym bloku oblicza trudność następnego z niedawnej historii. Obecna reguła używa liniowo ważonej średniej ruchomej z 60 odstępów i celu 300 sekund na blok. Nowsze odstępy mają większą wagę; pojedynczy blok nadal może nadejść wcześniej lub później.'
sourceHash: '5c203e8c7523'
translation: draft
---

Trudność wiąże pracę sprzętu z blokami przyjmowanymi przez sieć. Poradnik wyjaśnia cel, korektę Blocknet i osobną trudność udziałów. Zrozum jej zmiany przed porównaniem hashrate, czasu bloku lub [kalkulatora nagród](/blog/pl/estimate-blocknet-mining-rewards/#mining-calculator).

## Czym jest trudność kopania?

<Answer>
Opisuje oczekiwaną pracę haszującą potrzebną do znalezienia wyniku spełniającego cel. Wyższa trudność daje rzadsze wyniki przy tym samym hashrate. W Blocknet trudność sieci odpowiada w przybliżeniu oczekiwanej liczbie hashy na blok; trudność udziałów dotyczy osobnego celu zgłoszeń.
</Answer>

Każdy hash daje liczbę ocenianą względem progu. Mniejszy cel akceptuje mniejszą część możliwych wyników i oznacza wyższą trudność. Blocknet dzieli największą wartość 256-bitową przez trudność, otrzymując cel 256-bitowy. Podwojenie trudności w przybliżeniu podwaja oczekiwaną pracę bez zmiany operacji haszowania.

## Jak Blocknet koryguje trudność sieci?

Oblicza ją po każdym bloku. Obecna reguła mainnet używa liniowo ważonej średniej ruchomej, LWMA, z 60 ostatnich odstępów, mocniej ważąc nowsze. Cel to 300 sekund na blok. Połączenie wcześniejszych trudności i ważonych czasów sprawia, że trwale szybsze bloki zwykle podnoszą trudność, a wolniejsze obniżają.

Kod ogranicza odstępy użyte w obliczeniu i stosuje minimum trudności. Do 60 odstępów potrzeba 61 znaczników czasu; korekta nie odbywa się dopiero co 60 bloków. Reguły zbliżają tempo do celu przy zmianach mocy. Pięć minut jest średnią, a pojedynczy blok może nadejść dużo wcześniej lub później.

## Trudność sieci a trudność udziałów

Pula wyznacza cel udziału, aby obserwować wkład między blokami. Zwykle jest łatwiejszy niż cel sieci. Obie wartości opisują oczekiwaną pracę, ale odpowiadają na różne pytania i ustalają je inne części systemu:

| Trudność | Co określa |
|---|---|
| Sieci | Próg pracy kandydata na blok według zasad konsensusu. |
| Udziału | Próg zgłoszenia przypisany przez pulę połączeniu lub zadaniu. |

bntpool.com używa zmiennej trudności, vardiff, regulując częstość zgłoszeń. Przy tym samym hashrate wyższa trudność zwykle daje mniej udziałów, każdy z większą oczekiwaną pracą. Nie zmienia trudności sieci. Wynik spełniający też cel sieci może być kandydatem na blok, nadal weryfikowanym przez daemon. [Poradnik udziałów](/blog/pl/accepted-rejected-stale-shares/) opisuje przyjęcia i odrzucenia.

## Jak trudność wpływa na oczekiwany czas bloku?

W Blocknet podziel trudność sieci przez hashrate wykonujący pracę w H/s, aby oszacować średni odstęp w sekundach. Używaj wartości z tego samego okresu. Przykład utrzymuje obie stałe, ilustrując arytmetykę; to nie aktualne pomiary ani prognoza dla konkretnego minera.

**Oczekiwane sekundy na blok ≈ trudność sieci ÷ hashrate w H/s**

| Trudność sieci | Hashrate | Oczekiwany odstęp |
|---|---|---|
| 30 000 | 100 H/s | 300 s |
| 60 000 | 100 H/s | 600 s |
| 60 000 | 200 H/s | 300 s |

Przy 30 000 i 100 H/s średnia wynosi 300 sekund. Przy 60 000 i tej samej mocy rośnie do 600 sekund. Gdy hashrate też wzrośnie dwukrotnie do 200 H/s, wraca do 300 sekund. To średnie z wielu prób. [Poradnik szczęścia puli](/blog/pl/mining-pool-luck-explained/) wyjaśnia różnice rzeczywistych rund.

## Dlaczego nagrody zmieniają się przy stałym hashrate?

Wyższa trudność przy stałej własnej mocy zmniejsza oczekiwane bloki na jednostkę czasu, dopóki obowiązuje. Gdy rośnie hashrate sieci, a trudność podąża za nim, maleje twój udział pracy. Nagroda za blok zmienia się niezależnie według [harmonogramu](/blog/pl/blocknet-block-reward-schedule/). Trudność nie określa liczby BNT w planowej nagrodzie.

Używaj aktualnych danych w kalkulatorze, badając oczekiwane BNT. Rozróżnij hashrate minera i puli, aby właściwie uwzględnić opłatę minera dla swojej wersji. Estymacje to średnie przy określonych założeniach. Wypłata zależy też od znalezionych bloków, potwierdzeń i [PPLNS](/blog/pl/how-pplns-payouts-work/); sama trudność nie przewiduje jej terminu.

## Którą trudność sprawdzać?

Zacznij od pytania. Dla bloków użyj trudności sieci i mocy badanego minera lub puli. Dla częstości udziałów użyj trudności ich zadań. Saldo i płatności sprawdzaj w [My Stats](/stats) i [historii wypłat](/payouts). Zachowuj czas, zakres oraz jednostki przy porównywanych wartościach.

[Panel puli](/) i [historia bloków](/blocks) pokazują aktualny kontekst. Hashrate sieci szacuje się z ostatniej pracy i czasu bloków, więc może różnić się od następnej trudności podzielonej przez 300 sekund. [Poradnik hashrate](/blog/pl/what-is-hashrate/) wyjaśnia różnicę. Oddzielaj trudność, moc i szczęście w diagnozie: opisują inne aspekty.

<CTA target="pool" title="Połącz miner z bntpool.com" text="Użyj strony konfiguracji, potem śledź udziały i aktywność w My Stats." />
