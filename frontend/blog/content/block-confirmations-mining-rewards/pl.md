---
title: 'Potwierdzenia bloków i niedojrzałe nagrody za kopanie'
description: 'Prześledź blok Blocknet do dojrzałej nagrody i wypłaty. Poznaj sposób liczenia głębokości przez bntpool i dowiedz się, dlaczego potwierdzenia liczy się blokami.'
tags: ['Nagrody za kopanie', 'Potwierdzenia', 'Blocknet']
faq:
  - q: 'Ilu potwierdzeń wymaga bntpool?'
    a: 'Podczas kontroli 26 października 2026 bntpool.com publikuje wymóg 60 bloków. Odejmuje wysokość znalezionego bloku od bieżącej i porównuje hash z węzłem. Aktualny parametr sprawdzisz w Get Started lub publicznym API.'
  - q: 'Czy 60 bloków oznacza dokładnie pięć godzin?'
    a: 'Nie. Sześćdziesiąt docelowych interwałów po pięć minut daje szacunek 300 minut, pięciu godzin. Rzeczywisty postęp zależy od bloków o zmiennych czasach nadejścia. Dojrzałość, próg salda i późniejsza transakcja są osobnymi etapami.'
  - q: 'Czy muszę dalej kopać, aby nagroda dojrzała?'
    a: 'Znaleziony blok zyskuje głębokość z nowymi blokami sieci, nawet przy odłączonym minerze. Uprawnienie wynika z zasad rozliczeń puli. Zatrzymanie nie przyspiesza ani nie zeruje dojrzewania; obserwuj zapis w My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Prześledź blok Blocknet przez potwierdzenie, dojrzewanie i wypłatę do portfela. Ten poradnik wyjaśnia głębokość za kartami nagród bntpool.com. Podział opisuje [poradnik PPLNS](/blog/pl/how-pplns-payouts-work/), a start [poradnik konfiguracji](/blog/pl/mine-blocknet-on-bntpool/).

## Czym są potwierdzenia i niedojrzałe nagrody?

<Answer>
Potwierdzenia opisują pozycję bloku poniżej bieżącego końca łańcucha. Niedojrzała nagroda pochodzi z nowego bloku bez wymaganej głębokości. bntpool sprawdza głębokość i tożsamość przed potwierdzeniem; kwalifikacja salda i wypłata następują osobno.
</Answer>

*Blok zyskuje głębokość, osiąga dojrzałość, a uprawnione saldo prowadzi do osobnej wypłaty.*

## Jak bntpool liczy głębokość?

Pula odejmuje wysokość znalezionego bloku od bieżącej. Dnia 26 października 2026 opublikowany wymóg wynosił 60 bloków. Blok zaczyna w tej konwencji od zera. Inna strona może liczyć sam blok jako pierwsze potwierdzenie; porównuj też regułę.

Przykład: blok 70 000 ma głębokość zero przy końcu 70 000, jeden przy 70 001 i 60 przy 70 060. To liczby ilustracyjne. Pula porównuje również hash z węzłem. [Implementacja](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) zapisuje oba kontrole.

## Dlaczego pięć godzin to szacunek?

Blocknet celuje w pięć minut między blokami. Sześćdziesiąt razy pięć to 300 minut, pięć godzin. Pomaga planować, lecz warunkiem jest liczba bloków. Obserwuj wzrost łańcucha od wysokości znalezienia do wymaganej głębokości, nie stały termin zegarowy.

Każdy blok zaakceptowanego łańcucha zwiększa głębokość, nie tylko blok bntpool. Szybkość minera nie tworzy prywatnego zegara. Interwały się zmieniają, więc czas może różnić się od szacunku. Poradniki [trudności](/blog/pl/mining-difficulty-explained/) i [szczęścia](/blog/pl/mining-pool-luck-explained/) opisują regulację sieci i zmienność rund.

## Rozróżniaj oczekujące, potwierdzone i wypłacone

Nowy blok oczekuje, zdobywając głębokość. Szacowany udział może pojawić się w [My Stats](/stats), jeśli praca kwalifikuje się do nagrody. Nagroda tworzy monety; reguła coinbase portfela Blocknet wymaga 60 bloków przed wydaniem. Odpowiedni okres puli śledzi ten proces.

Po kontroli głębokości i tożsamości pula może potwierdzić blok i przydzielić nagrody do sald. Nie oznacza to natychmiastowej wypłaty każdemu adresowi. Proces osobno ocenia potwierdzone salda i warunki wysyłki. Status bloku, saldo i historia opisują różne etapy.

## Dlaczego sprawdzany jest hash?

Wysokość oznacza pozycję, hash konkretny blok. Węzły mogą znać konkurencyjne bloki; zaakceptowany łańcuch wyznacza zajmujący pozycję. Pula pyta więc węzeł i porównuje hash. Sama głębokość nie dowodzi, że pierwotny blok nadal należy do łańcucha.

Blok poza nim może zostać osierocony, a jego wstępna nagroda nie kwalifikuje się już jako nagroda zaakceptowanego łańcucha. Stan jest obsługiwany osobno. Użyj odnośnika hash na [Blocks](/blocks): dwa wpisy o tej samej wysokości nie muszą być tym samym zdarzeniem. Szacunek pozostaje związany z blokiem.

## Czym wypłata różni się od nagrody?

Nagroda zaczyna w wyjściu coinbase bloku. Wypłata puli to późniejsza transakcja na twój adres. W dniu kontroli publikowane minimum wynosi 0,1 BNT. Dojrzałość i próg salda są różnymi warunkami; [poradnik PPLNS](/blog/pl/how-pplns-payouts-work/) szczegółowo opisuje podział i wypłatę.

Po wysłaniu śledź transakcję oraz odbiór i możliwość wydania w portfelu. Jej potwierdzenia dotyczą wypłaty, nie pierwotnego bloku. Nie trzeba zmieniać adresu ani łączyć minera ponownie podczas dojrzewania. Porównuj [historię wypłat](/payouts) i portfel dla tego samego adresu.

## Jak śledzić całą drogę nagrody?

Na [Blocks](/blocks) ustal wysokość, hash i status. Otwórz [My Stats](/stats) z adresem minera i zobacz szacowane oraz potwierdzone nagrody. Gdy powstanie wypłata, przejdź do jej transakcji z historii. Każda obserwacja dotyczy bloku, salda konta lub płatności do portfela.

Sprawdź wartości w [Get Started](/start) lub `https://bntpool.com/api/info` oraz [status puli](/status). Przy analizie zanotuj adres, ID bloku lub transakcji i czas; nigdy nie udostępniaj słów odzyskiwania. [Poradnik śledzenia](/blog/pl/track-your-miner-on-bntpool/) opisuje resztę. Bloki zyskują głębokość offline; kwalifikacja wynika z zapisanej pracy i łańcucha.

<CTA target="pool" title="Śledź nagrody za kopanie" text="Połącz się z bntpool.com i obserwuj szacunki, potwierdzone salda oraz wypłaty w My Stats." />
