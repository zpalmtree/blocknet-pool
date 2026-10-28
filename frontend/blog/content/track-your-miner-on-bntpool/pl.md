---
title: Monitoruj kopanie na bntpool.com
description: Wyszukaj adres lub @name na bntpool.com, aby zobaczyć hashrate, udziały, saldo i wypłaty. Śledź bloki, szczęście i dostępność lub użyj publicznego API.
tags: [bntpool, Statystyki kopania, API puli, Blocknet]
sourceHash: '40197b2fa166'
translation: draft
faq:
  - q: 'Dlaczego mój hashrate na bntpool.com różni się od wyniku seine?'
    a: 'seine zlicza każdy hash obliczany przez maszynę, a pula szacuje hashrate z trudności zaakceptowanych udziałów, uśrednionej z ostatniej godziny. Udziały przychodzą w losowych momentach, więc wartość puli bardziej się waha i stabilizuje z czasem. Dla miarodajnego porównania zestaw wykres 24-godzinny na My Stats ze średnią seine.'
  - q: 'Czy mogę wyszukać statystyki bntpool.com za pomocą @name?'
    a: 'Tak. Wpisz krótką nazwę Blocknet ID, taką jak @name lub $name, w polu wyszukiwania My Stats. Strona rozwiązuje ją przez blocknet.id, pokazuje statystyki i umieszcza w polu pełny przypisany adres. Użyj tego pełnego adresu przy wywoływaniu API puli ze skryptu.'
  - q: 'Czy API bntpool.com wymaga klucza API?'
    a: 'Nie. Endpointy statystyk puli, bloków, wypłat, szczęścia, statusu i poszczególnych górników udostępniają publiczny JSON przez HTTPS, więc polecenie curl lub domowy panel może odczytać je bez rejestracji. Kwoty są w jednostkach atomowych — podziel przez 100 000 000, aby otrzymać BNT — i odpytuj co 30–60 sekund.'
  - q: 'Dlaczego My Stats pokazuje zero Estimated Rewards?'
    a: 'Estimated Rewards pojawiają się, gdy pula znajdzie blok, a Twoje udziały są w oknie wypłat. U nowego górnika może to wymagać kilku rund kopania. Najpierw pojawiają się zaakceptowane udziały i hashrate, potem szacowane nagrody, które dojrzewają do Confirmed Rewards i są wypłacane automatycznie.'
---

Po pierwszym zaakceptowanym udziale otwórz My Stats, aby sprawdzić, czy pula otrzymuje Twoją pracę. Znajdź swój komputer, porównaj hashrate w dłuższym okresie i śledź szacowane nagrody aż do potwierdzenia i wypłaty. Skrypty oraz publiczne API są na końcu poradnika.

Jeśli jeszcze nie zacząłeś kopać, [poradnik instalacji seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) doprowadzi Cię do pierwszych udziałów w kilka minut.

## Jak sprawdzić statystyki górnika na bntpool.com?

<Answer>
Otwórz My Stats pod adresem bntpool.com/stats, wklej adres Blocknet lub @name i naciśnij Lookup. Strona pokazuje hashrate, zaakceptowane i odrzucone udziały, szacowane, potwierdzone i wypłacone salda, wykres hashrate, wiersz dla każdej maszyny oraz ostatnie wypłaty. Odświeża się automatycznie, dopóki karta jest otwarta.
</Answer>

Przejdź do [My Stats](/stats) i wpisz ten sam adres lub [krótką nazwę Blocknet ID](https://seineminer.com/pl/blog/blocknet-id-pay-by-name/), który podałeś seine. Nazwa taka jak `@name` lub `$name` jest rozwiązywana przez blocknet.id, a pole wyszukiwania pokazuje następnie pełny adres, na który wskazuje. Strona zapamiętuje ostatnie wyszukiwanie w tej przeglądarce, więc następnym razem Twoje liczby już tam są.

1. Sprawdź **Shares Accepted** i **Recent Shares**. Zaakceptowane udziały potwierdzają, że Twoja praca dociera do puli, nawet gdy wszystkie salda nagród wynoszą jeszcze zero.
2. Znajdź komputer w **Workers**. Porównaj nazwę z nazwą w Seine i sprawdź, czy czas ostatniego udziału jest aktualizowany.
3. Zero w **Blocks Found** jest normalne: licznik dotyczy bloków rozwiązanych przez Twoje komputery, a nie udziału w nagrodach puli.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-first-mobile.webp" width="352" height="379"><img src="/blog/tracking/tracking-first-desktop.webp" width="1180" height="900" alt="Najpierw sprawdź zaakceptowane udziały. Prawdziwy interfejs angielski z przykładowymi danymi, nie bieżące zarobki." loading="lazy" decoding="async"></picture>
<figcaption>Najpierw sprawdź zaakceptowane udziały. Prawdziwy interfejs angielski z przykładowymi danymi, nie bieżące zarobki.</figcaption>
</figure>

## Co pokazuje strona My Stats?

Strona jest podzielona na grupy kart i tabel, z których każda odpowiada na jedno pytanie o Twoje kopanie.

### Kopanie i udziały

Karty **Mining** pokazują bieżący **Hashrate**, liczbę **Blocks Found**, czyli bloków rozwiązanych dla puli przez Twoje maszyny, oraz datę rozpoczęcia (**Mining Since**). Poniżej są **Shares Accepted**, **Shares Rejected**, **Reject Rate** i **Avg Difficulty**. Sprawna maszyna pokazuje regularny strumień zaakceptowanych udziałów i odsetek odrzuceń bliski zeru.

**Hashrate History** przedstawia hashrate za 1h, 24h, 7d lub 30d. Dłuższe zakresy najlepiej pokazują, czy maszyna działała przez całą noc.

### Wiersz dla każdej maszyny

Jeśli uruchamiasz więcej niż jeden komputer, nadaj każdemu własną nazwę, a pula wymieni je oddzielnie z hashrate, zaakceptowanymi i odrzuconymi udziałami oraz czasem ostatniego udziału. seine wybiera nazwę przy pierwszym uruchomieniu — `seine-` i cztery cyfry — a własną możesz ustawić w wierszu poleceń:

```sh
./seine --pool-worker rig-01
```

Tabela **Workers** pokazuje też wersję minera na każdym komputerze. Przy starej lub nieznanej wersji komunikat nad statystykami prowadzi do [strony wydań seine](https://github.com/zpalmtree/seine/releases).

## Dlaczego hashrate puli różni się od wyniku seine?

seine zlicza każdy hash obliczany przez maszynę. Pula widzi wyłącznie przesłane udziały, więc szacuje hashrate na podstawie łącznej trudności zaakceptowanych udziałów z ostatniej godziny. Każdy udział jest zdarzeniem losowym, a bntpool.com ustawia trudność dla każdego połączenia, celując w około 10 udziałów na 5 minut, więc krótkie okno zawiera pewien szum.

Dwa nawyki pomagają porównywać liczby:

- Porównuj średnie: wykres 24h na My Stats ze średnią seine z tego samego okresu.
- Daj nowej maszynie czas. Dla dopiero uruchomionej maszyny pula uśrednia wynik z minut dotychczasowego kopania, więc pierwszy odczyt opiera się na garstce udziałów. Po godzinie obejmuje pełne jednogodzinne okno.

Tabela Recent Shares od razu pokazuje, że udziały docierają, jeszcze zanim ustabilizuje się hashrate.

## Kiedy spodziewać się pierwszej wypłaty?

Nie ma stałego czasu od pierwszego udziału do pierwszej wypłaty. Pula musi znaleźć blok, gdy Twoje udziały są w oknie wypłat. Następnie blok potrzebuje 60 potwierdzeń: średnio około pięciu godzin **od znalezienia bloku**, choć rzeczywisty czas jest zmienny. Gdy potwierdzone saldo osiągnie obecne minimum 0,1 BNT, kwalifikuje się do automatycznego cyklu wypłat. Aktualne ustawienia znajdziesz na stronie [Get Started](/start).

Stan płatności i odnośnik do transakcji znajdziesz w **Recent Payouts**. Płatność w kolejce nie została jeszcze rozgłoszona. Jeśli wysłana wypłata nie pojawia się w portfelu, skorzystaj z [poradnika dotyczącego brakujących wypłat](/blog/pl/pool-payout-missing-from-wallet/).

### Saldo

- **Estimated Rewards:** Twoja część bloków znalezionych przez pulę, które wciąż dojrzewają.
- **Confirmed Rewards:** dojrzałe saldo oczekujące na następną wypłatę.
- **Paid Balance:** wszystko, co już wysłano na Twój adres.

Gdy masz oczekujące nagrody, tabela **Estimated From Recent Blocks** wymienia każdy blok z wysokością, szacowaną kwotą, liczbą pozostałych potwierdzeń i czasem znalezienia. Każda wysokość prowadzi do bloku w [eksploratorze Blocknetu](https://explorer.blocknetcrypto.com), więc możesz sprawdzić go samodzielnie. [Jak działają wypłaty PPLNS](/blog/pl/how-pplns-payouts-work/) wyjaśnia podział nagrody za każdy blok i moment wypłaty salda.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-rewards-mobile.webp" width="352" height="524"><img src="/blog/tracking/tracking-rewards-desktop.webp" width="1081" height="217" alt="Saldo szacowane, potwierdzone i wypłacone to odrębne etapy. Prawdziwy interfejs angielski z przykładowymi kwotami, nie prognoza zarobków." loading="lazy" decoding="async"></picture>
<figcaption>Saldo szacowane, potwierdzone i wypłacone to odrębne etapy. Prawdziwy interfejs angielski z przykładowymi kwotami, nie prognoza zarobków.</figcaption>
</figure>

### Wypłaty i ostatnie udziały

**Recent Payouts** wymienia każdą płatność na Twój adres z kwotą, linkiem do transakcji w eksploratorze i statusem queued, unconfirmed lub confirmed. **Recent Shares** pokazuje 20 najnowszych udziałów z zadaniem, maszyną, trudnością i statusem. To najszybszy sposób potwierdzenia, że właśnie uruchomiona maszyna dociera do puli.

<span id="zero-hashrate"></span>

## Dlaczego seine kopie, a My Stats pokazuje zerowy hashrate?

Zacznij od adresu wypłat, połączenia z pulą i zaakceptowanych udziałów. Lokalny hashrate wskazuje, że komputer oblicza hashe; oszacowanie puli wymaga zaakceptowanych udziałów dla wyszukiwanego adresu.

1. **Sprawdź adres.** Wyszukaj ten sam adres odbiorczy lub zarejestrowaną nazwę `@name`, której używa seine. Po zmianie adresu przypisanego do krótkiej nazwy uruchom seine ponownie, aby znów ustalił adres, a następnie sprawdź go w My Stats.
2. **Sprawdź pulę.** Upewnij się, że seine używa `stratum+tcp://bntpool.com:3333`. Komputer kopiący gdzie indziej lub solo nie dodaje tu udziałów. [Poradnik konfiguracji puli](/blog/pl/mine-blocknet-on-bntpool/) opisuje połączenie.
3. **Sprawdź Recent Shares.** Poczekaj na zaakceptowane udziały i odświeżenie strony. Udziały przychodzą w różnych odstępach; działający lokalny hashrate nie gwarantuje natychmiastowego oszacowania puli. Jeśli udziały są akceptowane, porównuj średnie w dłuższym okresie.
4. **Przeczytaj komunikat połączenia.** Skorzystaj z tabeli, jeśli miner nie dostarcza jeszcze zaakceptowanych udziałów. Sprawdź bieżące incydenty na stronie [Status](/status).

| Komunikat w seine | Co sprawdzić dalej |
|---|---|
| `failed to connect to pool at` | Sprawdź nazwę hosta i port, połączenie oraz czy wychodzący ruch TCP do portu 3333 jest dozwolony. |
| `login rejected:` | Przeczytaj podany dalej powód; sprawdź adres odbiorczy lub krótką nazwę i używaj aktualnej wersji seine. |
| `stale job` | Udział dotyczy starego zadania. Obserwuj nowe zadania i zaakceptowane udziały; jeśli problem trwa, sprawdź połączenie i wersję minera. |
| `submit interrupted during reconnect` | Seine łączy się ponownie. Po przywróceniu połączenia poczekaj na zaakceptowanie logowania i nowe zaakceptowane udziały. |

Zero w **Estimated Rewards** to osobna kwestia: udziały mogą być akceptowane, zanim blok puli przyniesie nagrody. Tę kolejność wyjaśnia [poradnik PPLNS](/blog/pl/how-pplns-payouts-work/).

## Które publiczne strony pokazują całą pulę?

Te strony nie wymagają wyszukiwania i jawnie pokazują wszystkie działania puli. [Panel](/) zbiera najważniejsze liczby: połączonych górników, hashrate puli i sieci, znalezione bloki oraz średnie szczęście.

### Bloki

[Blocks](/blocks) wymienia każdy blok znaleziony przez bntpool.com, od najnowszego: wysokość (z linkiem do eksploratora), nagrodę, nakład pracy, czas rundy, status i czas znalezienia. Filtruj według Confirmed, Pending lub Orphaned, aby sprawdzić stan każdego bloku.

### Wypłaty

[Payouts](/payouts) wymienia każdą partię wypłat z łączną kwotą, liczbą opłaconych górników, linkami do transakcji i statusem. Partia pojawia się jako unconfirmed od razu po rozgłoszeniu, a zmienia się na confirmed krótko po pierwszym potwierdzeniu w bloku, więc możesz śledzić własną płatność od strony puli i w eksploratorze.

### Szczęście

[Luck](/luck) pokazuje nakład pracy każdej rundy. To praca udziałów zaakceptowana przez pulę w rundzie podzielona przez trudność bloku: 100% oznacza dokładnie oczekiwaną ilość pracy, a mniej oznacza wcześniejsze znalezienie bloku. Pojedyncze rundy mocno wahają się w obie strony, więc obserwuj **Average Luck** w panelu. Tabela **Pool Luck History** prowadzi tutaj przez **View All**.

### Status

[Status](/status) jest własną stroną stanu usług puli. Pokazuje dostępność puli, publicznego HTTP, API, Stratum, bazy danych i demona, świeżość bieżącego szablonu zadania, wysokość łańcucha, dostępność z ostatnich 10 minut, 6 godzin, 24 godzin i 7 dni oraz historię incydentów. Dostępność jest mierzona na dwa sposoby: przez monitor na serwerze puli i zewnętrzną sondę działającą na Cloudflare.

## A co z własnym panelem seine?

Pula pokazuje, co Ci naliczyła. Bieżący obraz samej maszyny zapewnia terminalowy panel seine, pokazujący hashrate i udziały na żywo. seine może też udostępnić lokalne API sterowania z REST, strumieniem zdarzeń wysyłanych przez serwer i metrykami Prometheus pod `/metrics`, domyślnie powiązane z `127.0.0.1:9977`. [Poradnik API sterowania](https://github.com/zpalmtree/seine/blob/master/docs/API.md) opisuje flagi i endpointy. Wielu górników korzysta z obu: metryk seine dla każdej maszyny i API puli dla sald oraz wypłat.

## Jak korzystać z API bntpool.com?

<Answer>
API bntpool.com udostępnia publiczny JSON przez HTTPS i nie wymaga klucza. Wywołaj https://bntpool.com/api/miner/ z dopisanym adresem, aby otrzymać hashrate, maszyny, udziały i wypłaty; dodaj /balance dla sald i użyj /api/stats dla hashrate puli i sieci. Kwoty są w jednostkach atomowych: podziel przez 100 000 000, aby uzyskać BNT.
</Answer>

Każda powyższa strona czyta z tych endpointów, więc wszystko, co widzisz w witrynie, możesz pobrać samodzielnie:

| Endpoint | Zwracane dane |
|---|---|
| `/api/stats` | Połączeni górnicy, hashrate puli, znalezione bloki, hashrate sieci, bieżąca wysokość |
| `/api/stats/history?range=24h` | Hashrate puli w czasie (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Bieżąca runda, średni nakład pracy, ostatnie rundy |
| `/api/info` | Ustawienia puli: port Stratum, system i okno wypłat, minimalna wypłata |
| `/api/blocks?status=confirmed` | Znalezione bloki z filtrowaniem według statusu |
| `/api/payouts/recent` | Partie wypłat z hashami transakcji |
| `/api/luck` | Nakład pracy i czas każdej rundy |
| `/api/status` | Stan usług, okna dostępności, incydenty |
| `/api/miner/{address}` | Twój hashrate, sumy, maszyny, najnowsze udziały, wypłaty |
| `/api/miner/{address}/balance` | Salda szacowane, potwierdzone i wypłacone |
| `/api/miner/{address}/hashrate?range=24h` | Historia Twojego hashrate |

Endpointy list przyjmują `limit` (domyślnie 25, maksymalnie 200) i `offset`, a odpowiadają polami `items` i `total`. Endpointy górnika wymagają pełnego adresu, więc po wyszukaniu @name na My Stats skopiuj pełny adres z pola wyszukiwania.

Aby odczytać dane górnika z terminala:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Za pomocą `jq` możesz wyodrębnić pojedyncze wartości, na przykład hashrate i zaakceptowane udziały albo łączną wypłaconą kwotę w BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

<CTA target="pool" title="Zacznij kopać i śledź wyniki na żywo" text="Dołącz do bntpool.com w kilka minut, a potem obserwuj hashrate, udziały i saldo na My Stats." />
