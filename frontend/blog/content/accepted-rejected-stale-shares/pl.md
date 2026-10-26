---
title: 'Udziały zaakceptowane, odrzucone i nieaktualne'
description: 'Poznaj zaakceptowane, odrzucone i nieaktualne udziały w bntpool.com, znaczenie komunikatów oraz wpływ trudności udziałów na statystyki kopania twojej maszyny.'
tags: ['Poradnik kopania', 'Udziały', 'Rozwiązywanie problemów']
faq:
  - q: 'Czy zaakceptowany udział oznacza znalezienie bloku?'
    a: 'Pula przyjęła przesłany wynik według reguł przetwarzania udziałów. Blok musi spełnić osobny cel sieci i zostać przyjęty przez daemona. Nagrody następują po znalezieniu bloku, walidacji i procesie wypłaty. Potwierdzenie udziału nie jest powiadomieniem o płatności.'
  - q: 'Dlaczego nieaktualny udział jest odrzucany?'
    a: 'Stale job oznacza przypisanie, które nie jest już dopuszczalne, na przykład wygasłe lub oparte na starym szablonie po okresie tolerancji. Sprawdź nowe zadania i powrót akceptowanych udziałów. Powtarzające się komunikaty wymagają sprawdzenia połączenia i wersji minera.'
  - q: 'Czy wyższa trudność udziałów zmniejsza nagrodę?'
    a: 'Trudność zmienia pracę reprezentowaną przez kwalifikujący się udział i oczekiwaną częstotliwość. Porównuj pracę ważoną trudnością w równych okresach, nie surowe liczby. Nagroda zależy też od okna PPLNS, kwalifikującej się pracy, znalezionych bloków i reguł walidacji.'
sourceHash: 5be830208e6c
translation: draft
---

Wynik udziału w logu pomaga zdecydować: kontynuować, sprawdzić połączenie czy zbadać powtarzające się odrzucenia. Ten poradnik wyjaśnia odpowiedzi bntpool.com i trudność stojącą za licznikami. Zacznij od [konfiguracji puli](/blog/pl/mine-blocknet-on-bntpool/) lub [poradnika śledzenia](/blog/pl/track-your-miner-on-bntpool/#zero-hashrate), jeśli adres nie pokazuje aktywności.

## Czym jest zaakceptowany udział?

<Answer>
Udział to wynik kopania przesłany względem celu przydzielonego przez pulę. Akceptacja potwierdza przyjęcie do przetwarzania i pokazuje, że praca dociera do puli. Znalezienie bloku, weryfikacja udziału i kwalifikacja do wypłaty są osobnymi etapami; sama akceptacja nie oznacza płatności.
</Answer>

Pomiędzy udziałami miner oblicza wiele hashy. Większość nie spełnia celu i nie jest wysyłana. Licznik akceptacji liczy wyniki przesłań, nie wszystkie hashe CPU czy GPU. [Poradnik hashrate](/blog/pl/what-is-hashrate/) rozróżnia lokalny licznik i szacunek puli na podstawie przesłanej pracy.

## Czym udziały różnią się od bloków?

Pula przydziela połączeniu zadania i trudność. Wynik spełniający cel udziału dowodzi pracy o tej trudności. Kandydat na blok musi też spełnić cel sieci i kontrole daemona. Znalezienie udziału nie oznacza dodania bloku do łańcucha.

bntpool.com zapisuje udziały zweryfikowane i tymczasowe. Zaakceptowane przesłanie może być tymczasowe; waga do wypłaty uwzględnia walidację i kwalifikację. Traktuj akceptację jako sygnał połączenia, a pracę i nagrody sprawdzaj w [Moich statystykach](/stats). [Poradnik PPLNS](/blog/pl/how-pplns-payouts-work/) wyjaśnia etapy po bloku puli.

## Co oznaczają udziały odrzucone i nieaktualne?

Odrzucone przesłanie nie przeszło odpowiedniej kontroli. Powód jest bardziej przydatny niż licznik. Stale job to jedna kategoria: przypisanie nie jest już dopuszczalne. Porównaj dokładny komunikat z równoczesnym ponownym połączeniem lub nowym zadaniem.

| Komunikat | Znaczenie i następny krok |
|---|---|
| `stale job` | Przypisanie nie istnieje, wygasło, zostało zastąpione poza tolerancją lub dotyczy niedostępnego szablonu. Sprawdź powrót nowych zadań i akceptowanych udziałów. |
| `duplicate share` | Ten sam szablon i nonce już przesłano. Sprawdź powtórzenia; ta sama praca nie daje dodatkowego wkładu. |
| `low difficulty share` | Hash nie spełnia przydzielonego celu. Przy powtórzeniach sprawdź wersję minera oraz obsługę zadań i trudności. |
| `nonce out of assigned range` | Nonce jest poza przydzielonym zakresem. Sprawdź wersję minera i własne proxy lub integracje. |

Nieaktualny udział dotyczy pracy przypisania, a osierocony blok miejsca bloku w łańcuchu. Oddziel log udziałów od [listy bloków](/blocks). Sam stale job nie oznacza znalezienia osieroconego bloku przez pulę.

## Dlaczego trudność udziałów się zmienia?

<Answer>
Zmienna trudność, vardiff, dostosowuje cel połączenia do obserwowanej aktywności udziałów. Wyższa zwykle daje mniej udziałów z większą pracą każdy; niższa odwrotnie. Porównuj kwalifikującą się pracę ważoną trudnością w tym samym okresie, zamiast tylko liczby udziałów.
</Answer>

Przykład: dziesięć kwalifikujących się udziałów trudności 100 to łącznie tyle co jeden trudności 1000: oba wyniki wynoszą 1000. To ilustracja wagi, nie obietnica nagrody ani terminów. Odstępy są zmienne, a walidacja określa pracę zaliczaną do rozliczeń.

Liczy się trudność przypisania przy przesłaniu. Nie porównuj starego wyniku tylko z najnowszą trudnością na ekranie. Pula zachowuje przypisania do kontroli. Normalna zmiana nie jest błędem; wymuszanie łatwiejszych udziałów nie tworzy dodatkowej pracy obliczeniowej.

## Jak czytać wskaźnik odrzuceń?

Porównuj przyjęcia i odrzucenia w tym samym okresie i zakresie. Miner może liczyć jedną sesję, a pula inny okres lub kilka maszyn. Restart zmienia lokalne okno, ale nie usuwa historii adresu w puli.

99 akceptacji i jedno odrzucenie dają 1% według liczby. Nie jest to automatycznie procent utraconej nagrody: trudności i zasady kwalifikacji mogą się różnić. Sprawdź przyczyny, momenty i dalszą akceptację pracy przed zmianą ustawień.

## Co robić przy powtarzających się odrzuceniach?

Zacznij od jednej maszyny. Zapisz wersję, dokładny komunikat, początek i to, czy przychodzą nowe zadania. Sprawdź [Status](/status), potem przerwy połączenia. Aktualizuj z [oficjalnych wydań seine](https://github.com/zpalmtree/seine/releases) i porównuj po jednej zmianie naraz.

Własne proxy lub integracja musi zachować zadanie, zakres nonce i trudność podczas przekazywania. Jeśli problem trwa, wyślij krótki fragment logu i dane maszyny przez kanał pomocy projektu. Usuń sekrety i prywatne dane; fraza odzyskiwania portfela nigdy nie jest potrzebna do diagnozy udziałów.

## Jak potwierdzić poprawne kopanie?

Sprawdź nowe zadania i zaakceptowane udziały, potem otwórz [Moje statystyki](/stats) z adresem odbiorczym minera. Potwierdź świeżą aktywność maszyny. Poczekaj, aż szacunek hashrate zbierze więcej pracy, zanim porównasz średnie. Następnie śledź szacowane nagrody, saldo i wypłaty jako osobne etapy poradnika PPLNS.

<CTA target="pool" title="Połącz z bntpool.com" text="Skorzystaj z konfiguracji, potwierdź akceptowane udziały i śledź aktywność według adresu odbiorczego." />
