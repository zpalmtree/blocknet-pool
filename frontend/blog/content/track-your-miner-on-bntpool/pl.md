---
title: Monitoruj kopanie na bntpool.com
description: Wyszukaj adres lub @name na bntpool.com, aby zobaczyć hashrate, udziały, saldo i wypłaty. Śledź bloki, szczęście i dostępność lub użyj publicznego API.
tags: [bntpool, Statystyki kopania, API puli, Blocknet]
sourceHash: bac503690f3a
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

Gdy seine kopie na bntpool.com, pula pokazuje wszystko, co widzi: Twój hashrate, każdy zaakceptowany udział, zarobki i każdą wypłatę wysłaną na Twój adres. Ten poradnik opisuje stronę My Stats, publiczne strony całej puli oraz publiczne API do użycia w skryptach i domowych panelach.

Jeśli jeszcze nie zacząłeś kopać, [poradnik instalacji seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) doprowadzi Cię do pierwszych udziałów w kilka minut.

## Jak sprawdzić statystyki górnika na bntpool.com?

<Answer>
Otwórz My Stats pod adresem bntpool.com/stats, wklej adres Blocknet lub @name i naciśnij Lookup. Strona pokazuje hashrate, zaakceptowane i odrzucone udziały, szacowane, potwierdzone i wypłacone salda, wykres hashrate, wiersz dla każdej maszyny oraz ostatnie wypłaty. Odświeża się automatycznie, dopóki karta jest otwarta.
</Answer>

Przejdź do [My Stats](/stats) i wpisz ten sam adres lub [krótką nazwę Blocknet ID](https://seineminer.com/pl/blog/blocknet-id-pay-by-name/), który podałeś seine. Nazwa taka jak `@name` lub `$name` jest rozwiązywana przez blocknet.id, a pole wyszukiwania pokazuje następnie pełny adres, na który wskazuje. Strona zapamiętuje ostatnie wyszukiwanie w tej przeglądarce, więc następnym razem Twoje liczby już tam są.

## Co pokazuje strona My Stats?

Strona jest podzielona na grupy kart i tabel, z których każda odpowiada na jedno pytanie o Twoje kopanie.

### Saldo

- **Estimated Rewards:** Twoja część bloków znalezionych przez pulę, które wciąż dojrzewają.
- **Confirmed Rewards:** dojrzałe saldo oczekujące na następną wypłatę.
- **Paid Balance:** wszystko, co już wysłano na Twój adres.

Gdy masz oczekujące nagrody, tabela **Estimated From Recent Blocks** wymienia każdy blok z wysokością, szacowaną kwotą, liczbą pozostałych potwierdzeń i czasem znalezienia. Każda wysokość prowadzi do bloku w [eksploratorze Blocknetu](https://explorer.blocknetcrypto.com), więc możesz sprawdzić go samodzielnie. [Jak działają wypłaty PPLNS](/blog/pl/how-pplns-payouts-work/) wyjaśnia podział nagrody za każdy blok i moment wypłaty salda.

### Kopanie i udziały

Karty **Mining** pokazują bieżący **Hashrate**, liczbę **Blocks Found**, czyli bloków rozwiązanych dla puli przez Twoje maszyny, oraz datę rozpoczęcia (**Mining Since**). Poniżej są **Shares Accepted**, **Shares Rejected**, **Reject Rate** i **Avg Difficulty**. Sprawna maszyna pokazuje regularny strumień zaakceptowanych udziałów i odsetek odrzuceń bliski zeru.

**Hashrate History** przedstawia hashrate za 1h, 24h, 7d lub 30d. Dłuższe zakresy najlepiej pokazują, czy maszyna działała przez całą noc.

### Wiersz dla każdej maszyny

Jeśli uruchamiasz więcej niż jeden komputer, nadaj każdemu własną nazwę, a pula wymieni je oddzielnie z hashrate, zaakceptowanymi i odrzuconymi udziałami oraz czasem ostatniego udziału. seine wybiera nazwę przy pierwszym uruchomieniu — `seine-` i cztery cyfry — a własną możesz ustawić w wierszu poleceń:

```sh
./seine --pool-worker rig-01
```

Maszyna ze starszym seine otrzymuje etykietę **update available**, prowadzącą do [strony wydań seine](https://github.com/zpalmtree/seine/releases), co ułatwia zauważenie komputera, który zapomniałeś zaktualizować.

### Wypłaty i ostatnie udziały

**Recent Payouts** wymienia każdą płatność na Twój adres z kwotą, linkiem do transakcji w eksploratorze i statusem queued, unconfirmed lub confirmed. **Recent Shares** pokazuje 20 najnowszych udziałów z zadaniem, maszyną, trudnością i statusem. To najszybszy sposób potwierdzenia, że właśnie uruchomiona maszyna dociera do puli.

## Dlaczego hashrate puli różni się od wyniku seine?

seine zlicza każdy hash obliczany przez maszynę. Pula widzi wyłącznie przesłane udziały, więc szacuje hashrate na podstawie łącznej trudności zaakceptowanych udziałów z ostatniej godziny. Każdy udział jest zdarzeniem losowym, a bntpool.com ustawia trudność dla każdego połączenia, celując w około 10 udziałów na 5 minut, więc krótkie okno zawiera pewien szum.

Dwa nawyki pomagają porównywać liczby:

- Porównuj średnie: wykres 24h na My Stats ze średnią seine z tego samego okresu.
- Daj nowej maszynie czas. Dla dopiero uruchomionej maszyny pula uśrednia wynik z minut dotychczasowego kopania, więc pierwszy odczyt opiera się na garstce udziałów. Po godzinie obejmuje pełne jednogodzinne okno.

Tabela Recent Shares od razu pokazuje, że udziały docierają, jeszcze zanim ustabilizuje się hashrate.

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

### Pokaż kopanie w domowym panelu

Ten sam endpoint działa jako czujnik w domowym panelu. W Home Assistant integracja RESTful odczytuje go dzięki kilku wierszom `configuration.yaml`:

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Odpytywanie co 30–60 sekund w zupełności wystarcza: hashrate jest średnią godzinową, a samo My Stats odświeża szczegóły górnika co 30 sekund. Publiczne API ogranicza częstotliwość żądań dla każdego klienta. Jeśli skrypt otrzyma odpowiedź HTTP 429, powinien zwolnić i przed ponowną próbą odczekać czas podany w nagłówku `Retry-After`.

## A co z własnym panelem seine?

Pula pokazuje, co Ci naliczyła. Bieżący obraz samej maszyny zapewnia terminalowy panel seine, pokazujący hashrate i udziały na żywo. seine może też udostępnić lokalne API sterowania z REST, strumieniem zdarzeń wysyłanych przez serwer i metrykami Prometheus pod `/metrics`, domyślnie powiązane z `127.0.0.1:9977`. [Poradnik API sterowania](https://github.com/zpalmtree/seine/blob/master/docs/API.md) opisuje flagi i endpointy. Wielu górników korzysta z obu: metryk seine dla każdej maszyny i API puli dla sald oraz wypłat.

Dopiero poznajesz seine? [Pobierz go](https://seineminer.com/pl/#download), skieruj do domyślnej puli, a Twój adres pojawi się na My Stats w ciągu kilku minut od pierwszego zaakceptowanego udziału.

<CTA target="pool" title="Zacznij kopać i śledź wyniki na żywo" text="Dołącz do bntpool.com w kilka minut, a potem obserwuj hashrate, udziały i saldo na My Stats." />
