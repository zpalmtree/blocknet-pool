---
title: "Przyjmuj płatności Blocknet za towary i usługi"
description: "Przyjmuj BNT za zamówienia, dopasowuj wpłaty w portfelu Blocknet, sprawdzaj potwierdzenia i obsługuj płatności częściowe oraz zwroty krok po kroku."
tags: [Blocknet, BNT]
faq:
  - q: "Skąd wiem, że klient zapłacił w Blocknet?"
    a: "Znajdź wpłatę we własnym zsynchronizowanym portfelu, porównaj kwotę i numer zamówienia oraz sprawdź potwierdzenia. Zapisz pełny identyfikator transakcji. Zrzut klienta lub link płatniczy nie dowodzi odbioru; eksplorator również nie pokazuje prywatnej kwoty przekazanej na Twój adres."
  - q: "Co zrobić, gdy klient wyśle za mało BNT?"
    a: "Pozostaw zamówienie częściowo opłacone i oblicz resztę na podstawie zweryfikowanych wpłat. Poproś o różnicę z tą samą referencją i zapisz oba identyfikatory. Każdą wpłatę licz raz. Przy uzgodnionym anulowaniu potwierdź adres zwrotu w dotychczasowej rozmowie dotyczącej zamówienia."
  - q: "Czy memo automatycznie oznacza zamówienie jako opłacone?"
    a: "Nie. Memo pomaga dopasować wpłatę, ale płacący może je zmienić lub pominąć. Sprawdź kwotę i stan transakcji. Portfel tworzy prośby i pokazuje wpłaty; ten ręczny proces nie dodaje automatycznej kasy ani systemu zarządzania zamówieniami."
sourceHash: '3309a069e5de'
translation: draft
---

Przyjmuj Blocknet, podając jasną kwotę w BNT, sprawdzając odbiór w portfelu i zapisując go przy zamówieniu. Przykład dotyczy fikcyjnego zamówienia na **1.25 BNT** z referencją **order-1042**. Używamy wydanej wersji portfela desktopowego v0.22.0; zrzuty zawierają oznaczone dane demonstracyjne.

## Jak przyjąć płatność Blocknet?

<Answer>
Uzgodnij kwotę w BNT i referencję, wyślij prośbę o płatność na kontrolowany adres i poczekaj na wpłatę w zsynchronizowanym portfelu. Dopasuj kwotę i referencję, sprawdź potwierdzenia, zapisz identyfikator transakcji i oznacz zamówienie jako opłacone zgodnie z zasadą potwierdzeń uzgodnioną z klientem.
</Answer>

[Poradnik portfela desktopowego](https://seineminer.com/pl/blog/blocknet-desktop-wallet/) pomaga go utworzyć lub otworzyć. Kopię odzyskiwania przechowuj oddzielnie od zamówień. Odbieranie zapłaty nie wymaga uruchamiania minera ani dołączania do puli.

## Ustal kwotę i oznacz zamówienie

Zapisz referencję, kwotę BNT, adres odbiorczy i przedmiot zakupu. Przy wycenie w innej walucie uzgodnij kwotę BNT i ważność wyceny przed płatnością. Link zawiera żądaną kwotę, lecz nie aktualizuje kursu ani nie egzekwuje terminu.

W przykładzie poproś o **1.25 BNT** z memo **order-1042**. Numer zamówienia wystarczy; adres dostawy i dane klienta trzymaj w prywatnym systemie. [Zaszyfrowane memo](https://seineminer.com/pl/blog/blocknet-encrypted-memos/) towarzyszy płatności, ale memo zapisane w udostępnionym linku jest czytelne dla każdego odbiorcy linku.

W **Receive → Payment Request** wpisz kwotę i memo, następnie wybierz **Generate Payment Link**. Sprawdź odbiorcę i kwotę przed udostępnieniem. [Poradnik linków płatniczych](https://seineminer.com/pl/blog/blocknet-payment-links/) opisuje formaty i odbiorców wskazanych nazwą. Przy konkretnej fakturze zachowaj pełny adres wraz z prośbą.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Wydany portfel Blocknet z prośbą o 1.25 BNT, memo order-1042 i adresem demonstracyjnym." loading="lazy" decoding="async"></a>
<figcaption>Prawdziwy angielski interfejs z danymi demonstracyjnymi. Utworzenie prośby nie przesuwa środków. Otwórz obraz, aby go powiększyć.</figcaption>
</figure>

## Sprawdź wpłatę we własnym portfelu

Poproś klienta o referencję i zachowanie identyfikatora transakcji. Po synchronizacji węzła i portfela otwórz **History** i znajdź wpłatę. Porównaj kwotę i memo z zamówieniem. Płacący może zmienić oba pola, więc prośba nie dowodzi, co wysłał.

Zapisz pełny identyfikator i odebraną kwotę. Nie licz transakcji dwukrotnie, gdy widzisz ją w Dashboard i History albo klient ponownie wysyła tę samą identyfikację. Dopasowuj konkretne wpłaty, nie zmianę całego salda, obejmującego też mining, innych klientów i własne przelewy.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History portfela Blocknet pokazuje demonstracyjną wpłatę 1.25 BNT z memo order-1042." loading="lazy" decoding="async"></a>
<figcaption>Przykładowa wpłata odpowiada kwocie i referencji. Sprawdź stan transakcji przed realizacją zamówienia.</figcaption>
</figure>

Jeśli wpłaty brakuje, porównaj z klientem pełny adres docelowy i identyfikator, a potem sprawdź synchronizację. Zrzut ekranu nadawcy daje kontekst; Twój portfel ustala otrzymaną kwotę.

## Kiedy oznaczyć zamówienie jako opłacone?

Rozróżniaj niepotwierdzony wpływ, transakcję w bloku i środki dostępne do wydania. Wydany portfel czeka na dziesięć kolejnych bloków po bloku zwykłego przelewu, zanim jego wyjścia staną się wydawalne. [Poradnik czasu transferu](https://seineminer.com/pl/blog/blocknet-transfer-time/) wyjaśnia liczniki i zmienną długość oczekiwania.

Podaj zasadę potwierdzeń przed zapłatą. Prosty ręczny proces czeka z realizacją, aż odpowiednia wpłata stanie się wydawalna. Sam widoczny wpływ nie kończy kontroli. Nie obiecuj stałej godziny dostawy na podstawie odliczania: odstępy między blokami są zmienne. [Jak mining zabezpiecza płatności](/blog/pl/how-mining-secures-blocknet-payments/) wyjaśnia głębokość potwierdzeń.

## Obsłuż wpłaty częściowe, nadpłaty i brak referencji

| Zweryfikowana wpłata | Następna czynność |
|---|---|
| 1.00 BNT za zamówienie na 1.25 BNT | Zapisz częściową zapłatę i poproś o pozostałe 0.25 BNT. |
| Późniejsze 0.25 BNT | Zachowaj oba identyfikatory, policz łącznie 1.25 BNT tylko raz i sprawdź potwierdzenia obu wpłat. |
| Więcej niż 1.25 BNT | Uzgodnij zwrot albo saldo do wykorzystania i zapisz decyzję. |
| Poprawna kwota bez memo | Potwierdź z klientem pełny identyfikator i cel przed przypisaniem. |
| Oczekująca płatność | Pozostaw zamówienie oczekujące na potwierdzenie i sprawdź stan ponownie. |

Przy zwrocie poproś o potwierdzenie adresu odbiorczego w dotychczasowej rozmowie. Nie wywodź go z publicznej transakcji: wejścia Blocknet nie podają zwykłego adresu nadawcy. Przed zatwierdzeniem nowego przelewu sprawdź kwotę, adres i opłatę; zachowaj jego identyfikator przy pierwotnej wpłacie.

## Udostępniaj tylko potrzebne dowody

Eksplorator pomaga sprawdzić umieszczenie transakcji w bloku, ale nie ujawnia prywatnej kwoty dla Twojego adresu. W razie sporu nadawca może dostarczyć dowód konkretnej transakcji według [poradnika dowodów płatności](https://seineminer.com/pl/blog/blocknet-payment-proof/). Sprawdź dopasowanie odbiorcy, kwotę i stan łańcucha; sama poprawność klucza nie wystarczy.

Przechowuj prywatnie razem zamówienie, prośbę, identyfikatory wpłat, sprawdzone kwoty, decyzję o potwierdzeniach i zwrot. Udostępniaj tylko odpowiedni dokument lub dowód. Nigdy nie wysyłaj słów odzyskiwania, hasła ani prywatnego klucza podglądu całego portfela jako dowodu płatności.

Jeśli także kopiesz BNT, oddziel [wypłaty z puli](/blog/pl/track-your-miner-on-bntpool/) od zamówień, nawet w jednym portfelu. Dzięki temu każda faktura ma jednoznacznie przypisane przelewy.


<CTA target="pool" title="Odbieraj także nagrody za mining" text="Poradnik puli pokazuje, jak podłączyć minera i śledzić wypłaty obok innych wpływów Blocknet." />
