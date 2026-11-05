---
title: Kop Blocknet prosto na giełdę
description: 'Kop Blocknet prosto na adres wpłaty SafeTrade lub innej giełdy na bntpool.com. Dodaj memo wpłaty w seine, a pula wypłaci środki na Twoje konto.'
tags: [Poradnik kopania, bntpool, SafeTrade, Wypłaty]
sourceHash: '8f73aae25c7a'
translation: draft
faq:
  - q: 'Czy mogę kopać Blocknet bezpośrednio na giełdę?'
    a: 'Tak. Uruchom seine v0.3.1 lub nowszy z adresem wpłaty na giełdę i swoim memo wpłaty, jak w ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com wypłaca nagrody na ten adres z dołączonym memo, więc giełda zapisuje je na Twoim koncie.'
  - q: 'Dlaczego seine pyta o memo wypłaty?'
    a: 'Giełdy takie jak SafeTrade używają jednego wspólnego adresu wpłaty dla wszystkich klientów i rozróżniają wpłaty po memo. Seine zna te wspólne adresy, więc gdy wpiszesz jeden z nich, pyta o Twoje memo i zapisuje je razem z adresem. Skopiuj memo dokładnie ze strony wpłat giełdy.'
  - q: 'Jaka jest minimalna wypłata na giełdę na bntpool.com?'
    a: 'Wypłaty z memo zaczynają się od 10 BNT według danych puli z 4 listopada 2026. Potwierdzone saldo rośnie w puli, aż osiągnie tę kwotę, a wtedy pula wysyła je na adres giełdy z Twoim memo. Wypłaty do własnego portfela zaczynają się od 1 BNT.'
  - q: 'Jak sprawdzić saldo kopania na giełdę?'
    a: 'Na stronie statystyk bntpool.com wpisz adres wpłaty na giełdę, wybierz Dodaj memo i wpisz swoje memo. Każda para adresu i memo ma własne saldo, więc widzisz tylko swój hashrate, koparki i wypłaty, a nie dane innych klientów korzystających z tego samego adresu giełdy.'
---

Ten poradnik pokazuje, jak kopać Blocknet prosto na swoje konto giełdowe na bntpool.com. Podajesz seine adres wpłaty na giełdę i swoje memo wpłaty, a pula wypłaca tam nagrody z dołączonym memo, więc giełda zapisuje je na Twoim koncie. Omawia polecenie, minimalną wypłatę i sprawdzanie salda.

## Czy można kopać Blocknet bezpośrednio na giełdę?

<Answer>
Tak. bntpool.com wypłaca nagrody za kopanie na adres wpłaty na giełdę z dołączonym memo wpłaty. Uruchom seine v0.3.1 lub nowszy z adresem giełdy i swoim memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Wypłaty z memo zaczynają się od 10 BNT, a giełda zapisuje każdą z nich na Twoim koncie.
</Answer>

Kopanie na giełdę oszczędza jeden krok, gdy planujesz handlować tym, co wykopiesz. Nagrody trafiają bezpośrednio na konto giełdowe, gotowe do sprzedaży na [rynku BNT/USDT](/blog/pl/sell-blocknet-bnt-safetrade/), bez przelewu z portfela po drodze.

Giełdy takie jak SafeTrade dają wszystkim klientom ten sam adres wpłaty i rozróżniają wpłaty po memo, krótkim kodzie unikalnym dla Twojego konta. Dlatego memo jest ważne: to ono łączy wypłatę z Tobą. Pula prowadzi osobne saldo dla każdej pary adresu i memo, więc Twoje nagrody nigdy nie mieszają się z nagrodami innych górników korzystających z tej samej giełdy.

## Jak skonfigurować kopanie na giełdę w seine?

<Answer>
Skopiuj adres wpłaty BNT i swoje memo wpłaty ze strony wpłat giełdy. Uruchom seine z oboma: --address dla adresu i --payout-memo dla memo. Gdy przy pierwszym uruchomieniu wpiszesz znany adres giełdy, seine sam zapyta o memo i je zapisze. Sprawdź w logu potwierdzenie puli i kop jak zwykle.
</Answer>

1. **Pobierz dane wpłaty.** Na giełdzie otwórz stronę wpłat BNT i skopiuj adres wpłaty oraz memo. Skopiuj memo dokładnie: wielkie litery i zera na początku mają znaczenie.
2. **Uruchom seine z oboma.** Na macOS lub Linuksie:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   W Windows, w PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Pozwól seine je zapisać.** Seine zapisuje memo razem z adresem, więc kolejne uruchomienia nadal płacą na Twoje konto giełdowe. Jeśli wpiszesz znany adres giełdy przy pierwszym uruchomieniu, seine zapyta o memo i zapisze oba.

Strona puli [Get Started](/start) ma sekcję „Kopiesz na giełdę?”, która zbuduje to polecenie za Ciebie: wklej adres i memo, a potem skopiuj wynik. Seine wymaga memo dla znanych wspólnych adresów giełd, takich jak adres SafeTrade. Usługi, skrypty i inne koparki działające bez nadzoru muszą podawać `--payout-memo` w wierszu poleceń, bo nie ma kto odpowiedzieć na pytanie.

Seine sprawdza memo w puli przy logowaniu. bntpool.com je potwierdza, a potem zaczyna się kopanie. Aby nazwać koparki, podaj `--pool-worker rig-01`, tak jak przy własnym adresie.

## Jak wypłacane są nagrody na giełdę?

Nagrody przechodzą tę samą drogę co każda wypłata bntpool.com. Twoje udziały dają część każdego bloku znalezionego przez pulę w sześciogodzinnym oknie PPLNS, a nagroda za każdy blok jest potwierdzana po 60 blokach. Potwierdzone saldo rośnie następnie w puli.

Punkt końcowy puli `/api/info` podawał 4 listopada 2026 takie minimalne wypłaty:

| Cel | Minimalna wypłata |
|---|---|
| Adres giełdy z memo | 10 BNT |
| Własny portfel | 1 BNT |

Gdy potwierdzone saldo osiągnie 10 BNT, pula wysyła je na adres giełdy z Twoim memo. Giełda zapisuje wpłatę po własnej liczbie potwierdzeń; jej strona wpłat podaje, ile ich potrzebuje. [Jak działają wypłaty PPLNS](/blog/pl/how-pplns-payouts-work/) szczegółowo wyjaśnia stronę puli.

Kopanie solo też działa z memo. Dodaj `solo:` przed adresem giełdy i zostaw `--payout-memo`, a pula wypłaci całą nagrodę za każdy znaleziony przez Ciebie blok na Twoje konto giełdowe. [Kopanie solo na bntpool.com](/blog/pl/solo-mine-blocknet-on-bntpool/) wyjaśnia nagrody solo.

## Jak sprawdzić saldo?

Otwórz [stronę statystyk](/stats), wklej adres wpłaty na giełdę, wybierz **Dodaj memo** i wpisz swoje memo. Strona pokaże wtedy Twój własny hashrate, koparki, saldo i wypłaty dla tej pary adresu i memo. Inni klienci tej samej giełdy mają własne pary i własne salda.

Każda wypłata w **Ostatnie wypłaty** prowadzi do swojej transakcji w eksploratorze. Porównaj ją z historią wpłat na giełdzie, aby prześledzić monety aż do konta. [Śledzenie koparki na bntpool.com](/blog/pl/track-your-miner-on-bntpool/) omawia resztę strony statystyk.

## Jak wrócić do własnego portfela?

Uruchom seine raz z nowym adresem i `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

Seine zapisze nowy adres bez memo, a nowe udziały będą się liczyć do Twojego własnego portfela. Saldo już zapisane na parze giełdowej pozostaje przy tej parze i jest jej wypłacane po osiągnięciu minimum; możesz je śledzić na stronie statystyk. [Zmiana adresu portfela do kopania](/blog/pl/change-mining-wallet-address/) wyjaśnia, jak pula zalicza pracę po zmianie adresu.

## Które konfiguracje obsługują memo?

Memo wymaga bezpośredniego połączenia seine z pulą, które jest domyślne. Tryb daemon w seine kopie przez Twój własny węzeł i płaci do portfela węzła, więc nie ma memo. Proxy farmy nie przekazuje memo, więc łącz koparki giełdowe z pulą bezpośrednio. Seine kopie tylko w puli, która potwierdza Twoje memo, więc Twoja praca zawsze jest wypłacana na wybrane konto.

<CTA target="pool" title="Kop na swoje konto giełdowe" text="Uruchom seine z adresem wpłaty i memo, a bntpool.com wypłaci nagrody prosto na Twoje konto giełdowe od 10 BNT." />
