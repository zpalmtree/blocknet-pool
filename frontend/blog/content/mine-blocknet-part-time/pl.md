---
title: 'Czy można kopać Blocknet tylko w wybranych godzinach?'
description: 'Kop Blocknet w krótkich sesjach: śledź udziały w sześciogodzinnym oknie PPLNS, wracaj na ten sam adres i sprawdzaj niewypłacone saldo.'
tags: ['Poradnik kopania', 'PPLNS', 'Harmonogram']
faq:
  - q: 'Czy trzeba kopać sześć godzin, zanim udziały zaczną się liczyć?'
    a: 'Nie. Uprawnione udziały mogą wejść do okna nagrody bloku bez sześciogodzinnego oczekiwania. Twoja część zależy od pracy ważonej trudnością względem całej pracy puli w oknie. Aby przydzielić nagrodę, pula musi znaleźć blok.'
  - q: 'Czy miner musi być połączony, aby otrzymać istniejącą wypłatę?'
    a: 'Aktywne połączenie nie jest warunkiem wypłaty. Potwierdzone salda są rozpatrywane według zasad puli, w tym aktualnego minimum. Rozłączenie samo nie zeruje salda i nie żąda natychmiastowego przelewu.'
  - q: 'Czy ponowne połączenie odnawia wiek starych udziałów?'
    a: 'Nie. Zachowują swoje znaczniki czasu. Nowa praca trafia do właściwych okien po powrocie; połączenie nie odnawia starych udziałów ani nie dodaje pracy za godziny offline. Po uruchomieniu sprawdź nowe zaakceptowane udziały.'
sourceHash: '0a11c393062e'
translation: draft
---

Możesz uruchamiać miner Blocknet w wybranych godzinach i wracać później. Rozróżnij aktualne połączenie, już wysłane udziały oraz przydzielone nagrody. Poradnik śledzi krótką sesję, aby pomóc zatrzymać, ponownie uruchomić i sprawdzić konto bntpool.com z jasnymi oczekiwaniami.

## Czy krótka sesja może otrzymać nagrody?

<Answer>
Tak. Uprawniona praca krótkiej sesji może uczestniczyć w blokach, których okna PPLNS obejmują te udziały. Nie trzeba najpierw ukończyć sześciu godzin. Nagrody zależą od znalezionych bloków i twojego udziału ważonego trudnością. Po zatrzymaniu nie dodajesz pracy; stare udziały normalnie się starzeją, a saldo podlega zasadom wypłat.
</Answer>

*Oś czasu pokazuje sesję, przerwę offline i powrót, z oddzielnymi oznaczeniami uprawnień udziałów oraz istniejącego salda.*

## Rozróżnij poszczególne etapy czasu

bntpool obecnie publikuje sześciogodzinne okno PPLNS, 60 bloków przed możliwością wypłaty oraz minimum 0,1 BNT na [stronie startowej](/start). To trzy etapy: uwzględniana praca, dojrzewanie bloku i próg salda. Sześć godzin nie jest zegarem obiecującym wypłatę.

[Poradnik PPLNS](/blog/pl/how-pplns-payouts-work/) wyjaśnia wzór. Uprawnione udziały są ważone trudnością wobec całej uprawnionej pracy w oknie. Minuty online nie ustalają procentu następnej nagrody. Sprzęt, uczestnictwo w puli i wyniki udziałów zmieniają wagi nawet przy równej długości sesji.

## Prześledź udział podczas przerwy

Przykład czasu lokalnego: kopiesz od 18:00 do 20:00, a uprawniony udział zapisano o 19:00. Blok z oknem kończącym się o 22:00 patrzy sześć godzin wstecz do 16:00. Udział mieści się w oknie, choć jesteś offline. To ilustracja kwalifikacji, nie prognoza znalezienia bloku o tej godzinie.

Okno kończące się o 02:00 następnego dnia zaczyna się o 20:00, więc udział z 19:00 jest poza nim. Powrót o 08:00 nie zmienia znacznika czasu. Nowa praca zasila późniejsze okna. [Poradnik szczęścia puli](/blog/pl/mining-pool-luck-explained/) opisuje zmienność bloków; nie wybieraj godzin, zakładając, że blok już się należy.

## Zatrzymaj się z czytelnym zapisem

Przed przerwą zapisz adres odbiorczy, nazwę maszyny, przybliżoną godzinę i ostatnie komunikaty akceptacji. Sprawdź konto przez [poradnik monitorowania](/blog/pl/track-your-miner-on-bntpool/). Taki punkt odniesienia pozwala odróżnić wyłączoną maszynę od innego adresu lub nieaktualnego jeszcze widoku.

Zakończ seine normalnie przez `q` w panelu lub `Ctrl+C` w terminalu. Zachowaj działające polecenie i konfigurację. [Poradnik używania PC](https://seineminer.com/pl/blog/mine-while-using-pc/) opisuje łączenie kopania z innymi zadaniami. Zatrzymanie nie wymaga kasowania ustawień, zmiany portfela ani zamykania konta puli.

## Co dzieje się z niewypłaconym saldem offline?

Wyjście udziału z okna późniejszego bloku nie usuwa nagrody już przypisanej do wcześniejszego. Szacunki nadal zależą od stanu i dojrzewania bloku; potwierdzone niewypłacone kwoty pozostają na koncie do przetworzenia. [Poradnik potwierdzeń](/blog/pl/block-confirmations-mining-rewards/) odróżnia tymczasowy kredyt od dojrzałego salda.

Potwierdzone saldo poniżej 0,1 BNT pozostaje poniżej obecnego progu automatycznej wypłaty, aż kolejne kredyty osiągną minimum. Samo połączenie nie zwiększa salda ani nie wymusza wypłaty. Uprawnione saldo nie wymaga też minera online podczas przetwarzania. [Historia wypłat](/payouts) rozróżnia niewypłacone saldo i wysłaną transakcję.

## Wróć na to samo konto i sprawdź nową pracę

Uruchom zapisane polecenie i sprawdź pulę, adres oraz nazwę maszyny przed porównaniem statystyk. Adres identyfikuje obserwowane konto. Przy świadomej zmianie celu użyj [poradnika zmiany adresu](/blog/pl/change-mining-wallet-address/): stare saldo nie jest po cichu przenoszone na nowe konto.

Szukaj aktualnego zadania i nowych zaakceptowanych udziałów, potem pozwól zaktualizować się oknu pomiarowemu puli. Średnia świeżo rozpoczętej sesji może różnić się od kroczącej estymacji puli. [Poradnik hashrate](/blog/pl/what-is-hashrate/) wyjaśnia te okresy. Zweryfikuj pracę i właściwe konto, zanim uznasz krótką różnicę za brak nagród.

## Planuj według własnej rutyny

Wybierz godziny zgodnie z wykorzystaniem PC i budżetem prądu. [Poradnik energii](https://seineminer.com/pl/blog/mining-electricity-use/) wylicza zużycie dla rzeczywistego harmonogramu, także z różnymi taryfami. Krótsza sesja skraca pracę, ale dokładna energia nadal zależy od zmierzonego sprzętu i obciążenia.

Zapisuj starty, zatrzymania, przyjętą pracę i salda przez wiele sesji. [Kalkulator nagród](/blog/pl/estimate-blocknet-mining-rewards/) daje oczekiwanie pod założeniami, nie obietnicę konkretnego wieczoru. Oceniaj reprezentatywny okres i oddzielaj połączenie, okno udziałów, dojrzewanie bloków i wysłane wypłaty.

<CTA target="pool" title="Śledź sesje na bntpool.com" text="Wracaj z tym samym adresem, sprawdzaj nowe udziały i obserwuj normalne etapy wypłaty salda." />
