---
title: 'Wypłata wysłana, ale nie ma jej w portfelu?'
description: 'Prześledź wypłatę Blocknet przez transakcję, adres odbiorczy i synchronizację portfela oraz zbierz informacje do wyjaśnienia brakującego wpisu.'
tags: ['Wypłaty', 'Portfel', 'Rozwiązywanie problemów']
faq:
  - q: 'Czy wypłata w kolejce została już odebrana?'
    a: 'Kolejka jest stanem przetwarzania po stronie puli. Najpierw sprawdź, czy wpis ma identyfikator transakcji i jaki jest jego bieżący status. Zapis wypłaty, trafienie transakcji do łańcucha i rozpoznanie wyjścia przez portfel to osobne etapy. Śledź konkretną transakcję, gdy będzie dostępna.'
  - q: 'Czy eksplorator pokazuje saldo prywatnego portfela?'
    a: 'Eksplorator pozwala sprawdzić publiczny stan transakcji w łańcuchu. Blocknet używa prywatnych wyjść, więc zwykłe wyszukiwanie adresu nie przedstawia salda portfela ani nie dowodzi własności danego wyjścia. Portfel skanuje łańcuch, aby rozpoznać własne wyjścia.'
  - q: 'Jakie dane przekazać w sprawie brakującej wypłaty?'
    a: 'Podaj pełny adres odbiorczy użyty w wyszukiwaniu puli, identyfikator transakcji, przybliżony czas, status puli i postęp synchronizacji. Wyjaśnij, czy transakcja jest w historii. Dane konta przekazuj tylko we właściwym kanale wsparcia, nigdy frazę odzyskiwania, klucze prywatne ani hasło portfela.'
sourceHash: '915075cf4e2b'
translation: draft
---

Prześledź wypłatę Blocknet od wpisu puli do docelowego portfela. Poradnik zaczyna się od rozpoznawalnego wpisu wypłaty na bntpool.com. Jeśli widzisz tylko szacowane lub potwierdzone nagrody czekające na wypłatę, zacznij od [poradnika PPLNS](/blog/pl/how-pplns-payouts-work/): to wcześniejszy etap z własnymi zasadami rozliczeń i wypłat.

## Jak prześledzić wypłatę niewidoczną w portfelu?

<Answer>
Znajdź wpis dla pełnego adresu odbiorczego, zapisz identyfikator transakcji i status, a potem sprawdź stan w łańcuchu. Otwórz portfel tego samego odbiorcy i pozwól synchronizacji oraz skanowaniu nadrobić zaległości. Porównaj historię, zanim zdecydujesz, czy badać płatność, czy lokalny widok portfela.
</Answer>

*Wpis wypłaty prowadzi do transakcji w łańcuchu, a potem do portfela skanującego własne otrzymane wyjścia.*

## Zacznij od konkretnego wpisu wypłaty

Otwórz My Stats i wyszukaj pełny adres używany podczas zdobywania nagród. Zapisz kwotę, czas, status i identyfikator, jeśli istnieje. Pracuj na tym wpisie, nie na sumie wszystkich wypłat, która może obejmować wiele płatności. [Poradnik śledzenia](/blog/pl/track-your-miner-on-bntpool/) wskazuje odpowiednie pola.

Publiczna strona Payouts opisuje partie wypłat, które mogą zawierać wielu odbiorców i transakcje. Zachowaj wpis własnego adresu obok badanej transakcji. Suma partii nie jest kwotą przydzieloną twojemu portfelowi. Gdy identyfikatorów jest kilka, rozwiń listę i zapisz właściwe pełne wartości zamiast skróconej etykiety.

## Czy wypłata jest w kolejce, niepotwierdzona czy potwierdzona?

Przeczytaj bieżący status przed uznaniem wszystkich etapów za zakończone. Wpis w kolejce nadal jest przetwarzany po stronie puli; niepotwierdzona transakcja wymaga potwierdzenia w łańcuchu. Odśwież wpis i otwórz dostępny link. [Strona statusu](/status) daje kontekst operacyjny, gdy kilka obserwacji wskazuje przerwę w usłudze.

Oddziel dojrzewanie nagrody od potwierdzenia transakcji wypłaty. Blok, który wygenerował nagrodę, i późniejsza płatność mają inne identyfikatory oraz role. [Poradnik potwierdzeń](/blog/pl/block-confirmations-mining-rewards/) wyjaśnia różnicę. Identyfikator to konkretny obiekt do zbadania, ale nie dowodzi, że lokalny portfel już przeskanował i rozpoznał płatność.

## Sprawdź transakcję w łańcuchu

Otwórz link eksploratora przy wypłacie i porównaj pełny identyfikator z notatkami. Odczytaj stan w łańcuchu oraz dostępne dane bloku. Jeśli eksplorator jeszcze jej nie znajduje, porównaj odświeżony wpis puli i status usługi przed wyciąganiem wniosków. Sam chwilowy wynik wyszukiwania nie określa przyczyny.

Prywatność Blocknet oznacza, że zwykłe wyszukiwanie adresu nie pokazuje publicznego salda odbiorcy. Stan transakcji w eksploratorze i sprawdzenie własności przez portfel odpowiadają na inne pytania. Portfel rozpoznaje otrzymane wyjścia podczas skanowania łańcucha. [Poradnik adresów stealth](https://seineminer.com/pl/blog/stealth-addresses-explained/) wyjaśnia proces; publiczna lista nie odtwarza prywatnej historii portfela.

## Czy sprawdzasz właściwy portfel odbiorczy?

Porównaj adres wyszukiwania puli z adresem otwartego portfela. Użyj pełnych wartości; podobne skrócone początki nie wystarczą. Przy Blocknet ID zachowaj pełny adres pierwotnej pracy. Aktualna nazwa po zmianie przypisania może prowadzić gdzie indziej, podczas gdy wcześniejsza wypłata pozostaje związana ze starym odbiorcą.

Sprawdź starsze portfele używane przed zmianą ustawień kopania. Aktualizacja adresu minera lub przypisania nazwy nie przenosi już przypisanej płatności. [Poradnik zmiany adresu](/blog/pl/change-mining-wallet-address/) opisuje śledzenie obu historii. Zachowaj pierwotny portfel i kopię, odróżniając jego otwarcie od utworzenia nowego portfela z innym adresem.

## Pozwól portfelowi się zsynchronizować i przeskanować łańcuch

Sprawdź, czy węzeł dogania aktualny łańcuch, a skan portfela dotarł do bloku transakcji wypłaty. Synchronizacja węzła i skanowanie portfela mają osobne postępy. Świeżo otwarty lub odzyskany portfel może potrzebować czasu na wykrycie wyjść, choć eksplorator już pokazuje transakcję w bloku.

Utrzymuj połączenie i stosuj udokumentowaną procedurę synchronizacji. Porównaj historię wpływów oraz saldo, bo późniejsze wydatki zmieniają sumę. Jeśli wpis jest obecny, sprawdź jego potwierdzenie lub dostępność. Zacznij od obserwacji przed odzyskiwaniem czy pełnym skanem i zachowaj kopie przed konserwacją. [Poradnik portfela](https://seineminer.com/pl/blog/blocknet-wallet-for-mining/) opisuje dostęp i kopie.

## Jakie szczegóły pomagają wyjaśnić pozostałą różnicę?

Zapisz pełny adres puli, identyfikator, przybliżony czas i bieżący status puli. Dodaj wersję portfela, postęp węzła i skanowania oraz obecność transakcji w historii. Opisz wykonane kontrole. To odróżnia stan transakcji od błędnego adresu lub portfela, który nadal nadrabia zaległości.

Korzystaj z oficjalnej drogi wsparcia i udostępniaj dane konta tylko w odpowiednim miejscu. Badanie transakcji nie wymaga frazy odzyskiwania, kluczy prywatnych ani hasła. Sprawdź zawartość plików diagnostycznych przed publikacją. Dopasuj ten sam wpis, transakcję i portfel; świeży hashrate ani kolejny restart minera nie potwierdza otrzymania wcześniejszej płatności.

<CTA target="pool" title="Śledź wypłaty na bntpool.com" text="Użyj pełnego adresu odbiorczego i porównaj transakcję wypłaty z historią zsynchronizowanego portfela." />
