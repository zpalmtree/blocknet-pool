---
title: Kop Blocknet na bntpool.com
description: Dołącz do bntpool.com, domyślnej puli Blocknetu w seine. Połącz się z stratum+tcp://bntpool.com:3333 adresem lub @name, nazwij maszyny i sprawdź statystyki.
tags: [Poradnik kopania, bntpool, Blocknet ID, seine]
sourceHash: '3126e8e7e396'
translation: draft
faq:
  - q: 'Jaki jest adres Stratum bntpool.com?'
    a: 'Adres to stratum+tcp://bntpool.com:3333, pula proponowana przez seine przy pierwszym uruchomieniu. Naciśnij Enter przy pytaniu o pulę, aby ją zaakceptować, lub podaj ją flagą --pool-url. Twój adres Blocknet lub nazwa Blocknet ID jest loginem, a pula wypłaca nagrody na ten adres.'
  - q: 'Czy mogę kopać na bntpool.com z nazwą Blocknet ID?'
    a: 'Tak. Wpisz @name lub $name wszędzie, gdzie seine lub pula prosi o adres. Pula sprawdza podpisane przypisanie na blocknet.id, po czym przypisuje udziały i wypłaty do pełnego adresu kryjącego się pod nazwą. Potrzebujesz seine v0.2.16 lub nowszego. W wierszu poleceń ujmij nazwę w cudzysłowy pojedyncze, aby powłoka jej nie zmieniła.'
  - q: 'Czy potrzebuję konta, aby kopać na bntpool.com?'
    a: 'Nie. Nie ma rejestracji, hasła ani adresu e-mail. Twój adres Blocknet lub wskazująca na niego nazwa Blocknet ID jest loginem i miejscem docelowym wypłat. Uruchom z nim seine, a potem wyszukaj go na stronie My Stats, aby zobaczyć hashrate, udziały, maszyny, saldo i wypłaty.'
  - q: 'Czy mogę kopać na jeden adres z kilku komputerów?'
    a: 'Tak. Uruchom seine na każdym komputerze z tym samym adresem lub nazwą i nadaj każdemu własną nazwę maszyny. Pula zlicza udziały, saldo i wypłaty według adresu, więc każda maszyna zasila jedno saldo i jedną wypłatę, a strona My Stats wymienia każdą w osobnym wierszu.'
---

Ten poradnik opisuje dołączanie do puli bntpool.com z seine: adres puli, logowanie adresem Blocknet lub nazwą Blocknet ID `@name`, nadawanie nazw maszynom i sprawdzanie na stronie statystyk, czy udziały docierają. Jeśli seine nie jest jeszcze zainstalowany, najpierw wykonaj [poradnik instalacji seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/), a następnie wróć tutaj.

## Jaki jest adres puli bntpool.com?

<Answer>
Adres Stratum bntpool.com to `stratum+tcp://bntpool.com:3333`. seine proponuje go jako domyślną pulę przy pierwszym uruchomieniu, więc naciśnięcie Enter przy pytaniu o pulę nawiązuje połączenie. Logujesz się adresem Blocknet lub nazwą Blocknet ID `@name`. Nie tworzysz konta, a nagrody trafiają na ten adres.
</Answer>

Oto ustawienia puli według jej publicznego endpointu `/api/info` z 25 października 2026 r.:

| Ustawienie | Wartość |
|---|---|
| Adres Stratum | `stratum+tcp://bntpool.com:3333` |
| Login | Adres Blocknet, `@name` lub `$name` |
| System wypłat | PPLNS z oknem 6 godzin |
| Potwierdzenia przed wypłatą | 60 bloków |
| Minimalna wypłata | 0,1 BNT |

Te same wartości możesz w każdej chwili zobaczyć na stronie [Get Started](/start) puli.

## Dlaczego bntpool.com jest domyślną pulą seine?

Przy pierwszym uruchomieniu seine pyta o pulę i wstawia `stratum+tcp://bntpool.com:3333` jako domyślny adres. Pula i program współpracują na kilka praktycznych sposobów:

- **Jeden protokół.** bntpool.com używa protokołu Stratum obsługiwanego przez seine (wersja 2), więc poza adresem nie musisz niczego konfigurować.
- **Nazwy działają po obu stronach.** seine i pula akceptują nazwy Blocknet ID i weryfikują je na podstawie tego samego podpisanego rekordu na blocknet.id.
- **Kontrola wersji.** Każda maszyna zgłasza wersję seine wraz z udziałami, a strona statystyk oznacza maszynę ze starszym seine etykietą „update available”, która prowadzi do strony wydań seine.
- **Poradnik konfiguracji seine.** Strona Get Started puli opisuje pobranie i uruchomienie seine, podając polecenie do skopiowania.

## Jak połączyć seine z bntpool.com?

Przy pierwszym uruchomieniu seine zadaje dwa pytania:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Wklej adres lub wpisz nazwę, a potem naciśnij Enter, aby zaakceptować bntpool.com. seine zapisuje obie odpowiedzi w `seine-data/seine.config.json`, więc każde kolejne uruchomienie od razu rozpoczyna kopanie. Wybiera też nazwę maszyny i zapisuje ją w tym samym pliku (więcej poniżej).

Na maszynie bez interfejsu, w usłudze lub skrypcie nie ma kto odpowiedzieć na pytania. Przy pierwszym uruchomieniu podaj obie wartości jako flagi — to samo polecenie znajdziesz na stronie Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Aby później zmienić pulę lub adres, edytuj albo usuń `seine-data/seine.config.json` lub nadpisz zapisane wartości flagami `--address` i `--pool-url` w wierszu poleceń.

## Czy można kopać na nazwę Blocknet ID @name?

<Answer>
Tak. bntpool.com akceptuje krótką nazwę Blocknet ID zapisaną jako `@name` lub `$name` wszędzie, gdzie przyjmuje adres. Pula sprawdza podpisane przypisanie z blocknet.id, weryfikuje adres wskazywany przez nazwę i przez całe połączenie przypisuje udziały, saldo oraz wypłaty do tego pełnego adresu.
</Answer>

Wielkość liter nie ma znaczenia, więc `@Alice` i `@alice` to ten sam login. Obsługa nazw pojawiła się w seine v0.2.16; ma ją każde aktualne wydanie. Przed rozpoczęciem kopania seine wyświetla adres przypisany do nazwy, abyś mógł potwierdzić zgodność z portfelem. W wierszu poleceń ujmij nazwę w cudzysłowy pojedyncze. Bez nich powłoka próbowałaby rozwinąć `$name` jako zmienną:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Jeśli później przypiszesz nazwę do innego portfela, zrestartuj seine. Program rozwiązuje zapisaną nazwę przy każdym uruchomieniu i zachowuje ten adres przez całą sesję, a pula robi to samo dla każdego połączenia. Już zarobione saldo pozostaje przy adresie, dla którego je zdobyto, i tam jest wypłacane, więc przeniesienie nazwy nigdy nie przenosi monet.

Nazwę możesz zarejestrować na [blocknet.id](https://blocknet.id). [Poradnik Blocknet ID](https://seineminer.com/pl/blog/blocknet-id-pay-by-name/) wyjaśnia płatności przy użyciu nazw.

## Jak nazwać swoje maszyny?

Każde połączenie loguje się nazwą maszyny wraz z Twoim adresem. Przy pierwszym uruchomieniu seine wybiera ją za Ciebie: `seine-` i cztery cyfry. Zapisuje ją w pliku konfiguracji, dzięki czemu ta sama maszyna zachowuje nazwę po restartach. Aby użyć rozpoznawalnej nazwy, ustaw ją w wierszu poleceń:

```sh
./seine --pool-worker rig-01
```

Pula zachowuje do 64 znaków nazwy. Stała nazwa pomaga też po restarcie: pula przez godzinę pamięta trudność udziałów każdej maszyny według adresu i nazwy, więc ponownie podłączona maszyna wraca do trudności, na której wcześniej się ustabilizowała.

## Czy można kopać na jeden adres z kilku maszyn?

Tak. Uruchom seine na każdej maszynie z tym samym adresem lub `@name` i inną nazwą maszyny. Pula zlicza udziały, salda i wypłaty według adresu, więc wszystkie Twoje maszyny składają się na jedno saldo i jedną wypłatę. Na stronie statystyk każda ma własny wiersz, co ułatwia sprawdzenie, czy wszystkie kopią.

## Jak sprawdzić, czy program kopie?

Panel seine pokazuje hashrate i zaakceptowane udziały na samej maszynie. Aby zobaczyć perspektywę puli, otwórz [My Stats](/stats), wpisz adres, `@name` lub `$name` i naciśnij **Lookup**. Jeśli wpiszesz nazwę, strona pokazuje przypisany do niej adres obok pola wyszukiwania. Zapamiętuje też ostatnie wyszukiwanie w tej przeglądarce, a [poradnik monitorowania górnika](/blog/pl/track-your-miner-on-bntpool/) opisuje wszystkie jej karty.

Zwróć uwagę na trzy rzeczy:

- **Hashrate i Shares Accepted.** Karta hashrate pokazuje wynik widziany przez pulę, a licznik zaakceptowanych udziałów rośnie wraz z pracą przesyłaną przez maszyny.
- **Wiersz dla każdej maszyny.** Każdy wiersz pokazuje nazwę, hashrate, zaakceptowane i odrzucone udziały oraz czas ostatniego udziału. Udział sprzed chwili oznacza, że maszyna jest połączona i pracuje.
- **Recent Shares.** Najnowsze udziały są wymienione z maszyną, która je wysłała, trudnością i statusem.

Pula ustawia trudność udziałów osobno dla każdego połączenia i dostosowuje ją podczas kopania, więc nawet mały CPU regularnie wysyła udziały, a pierwsze zaakceptowane udziały pojawiają się w ciągu kilku minut od połączenia. Oczekiwany hashrate sprzętu znajdziesz w [poradniku sprzętu do kopania Blocknetu](https://seineminer.com/pl/blog/blocknet-mining-hardware-hashrate/).

Potem przychodzą nagrody. Estimated Rewards pojawiają się, gdy pula znajdzie blok, a Twoje udziały są w sześciogodzinnym oknie. Po 60 potwierdzeniach bloku Twoja część przechodzi do Confirmed Rewards, a pula wysyła ją automatycznie, gdy masz co najmniej 0,1 BNT. [Jak działają wypłaty PPLNS](/blog/pl/how-pplns-payouts-work/) wyjaśnia podział nagrody za każdy blok. Historię całej puli znajdziesz na stronach [Blocks](/blocks) i [Payouts](/payouts).

## Co zawiera strona Get Started?

Strona [Get Started](/start) puli mieści wszystko na jednym ekranie:

- **Informacje o puli.** Adres Stratum (kliknij, aby skopiować), system wypłat, minimum wypłaty i liczbę potwierdzeń bloku przed wypłatą, odczytywane na bieżąco z puli.
- **Szybki start w trzech krokach.** Pobierz seine z [seineminer.com](https://seineminer.com/pl/#download), uruchom go raz i wpisz adres lub nazwę oraz URL puli, a następnie zacznij kopać i obserwuj hashrate oraz saldo na My Stats.
- **Polecenie do skopiowania.** Pełne polecenie `./seine` z uzupełnionym adresem puli. Wstaw swój adres lub `'@yourname'` w cudzysłowach pojedynczych.
- **Uwagi dotyczące konfiguracji.** Miejsce zapisu konfiguracji seine, sposób jej resetowania i flagi nadpisujące ustawienia.
- **Sposób liczenia nagród.** Krótkie wyjaśnienie PPLNS: udziały przesłane przed znalezieniem bloku zarabiają jego nagrodę, a praca dodana później liczy się do kolejnych bloków. Wskazuje też, że nagrody z niepotwierdzonych bloków są pokazywane osobno na My Stats do czasu ich potwierdzenia.
- **Zrzuty ekranu** konfiguracji seine przy pierwszym uruchomieniu i panelu kopania, abyś wiedział, czego się spodziewać.

<CTA target="pool" title="Zacznij kopać na bntpool.com" text="Połącz seine z bntpool.com, używając adresu lub @name, i obserwuj napływ udziałów na My Stats." />
