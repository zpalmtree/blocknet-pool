---
title: 'Zmiana adresu portfela do kopania Blocknet'
description: 'Zmień adres odbiorczy koparek Blocknet, sprawdź nowe udziały na bntpool.com i śledź wcześniejsze nagrody pozostające przy poprzednim adresie portfela.'
tags: ['Poradnik kopania', 'Portfel', 'Wypłaty']
faq:
  - q: 'Czy zmiana adresu przenosi stare saldo?'
    a: 'Nowy cel przypisuje przyszłą pracę nowemu adresowi. Istniejące udziały, naliczenia i wypłaty pozostają przy pierwotnym adresie. Sprawdzaj oba osobno w Moich statystykach i zachowaj dostęp do starego portfela podczas śledzenia pozostałych nagród.'
  - q: 'Czy trzeba zmienić każdy komputer?'
    a: 'Tak. Każdy proces używa własnego skonfigurowanego celu. Najpierw zmień i sprawdź jeden komputer, potem pozostałe, razem z zapisanymi ustawieniami i argumentami. Różne nazwy pomagają rozpoznać maszyny wysyłające pracę na nowy adres.'
  - q: 'Czy zmiana Blocknet ID aktualizuje działającą koparkę?'
    a: 'seine rozwiązuje zapisane ID przy starcie i zachowuje adres przez całą sesję, także przy ponownych połączeniach. Po zmianie przypisania uruchom ponownie każdą koparkę i sprawdź pełny adres. Wcześniejsza praca i salda nie są przypisywane ponownie.'
sourceHash: 'fca912d46d55'
translation: draft
---

Zmień odbiorcę przyszłej pracy, zachowując czytelny zapis poprzednich nagród. Poradnik dotyczy seine już połączonego z bntpool.com. Pierwsze połączenie opisuje [poradnik puli](/blog/pl/mine-blocknet-on-bntpool/), a nowy portfel — [poradnik portfela](https://seineminer.com/pl/blog/blocknet-wallet-for-mining/).

## Jak zmienić adres kopania?

<Answer>
Zatrzymaj jedną koparkę, zmień adres w pliku lub zwykłym poleceniu i uruchom ponownie. Sprawdź rozwiązany pełny adres oraz zaakceptowane udziały, potem wyszukaj adres na bntpool.com. Powtórz na innych maszynach. Poprzednie udziały i nagrody pozostają przy adresie użytym podczas ich zapisania.
</Answer>

*Nowa praca trafia do drugiego portfela; stare nagrody pozostają powiązane z pierwszym.*

## Zapisz oba adresy przed zmianą

Skopiuj nowy adres bezpośrednio z wybranego portfela. Koparka potrzebuje publicznego adresu lub zarejestrowanego Blocknet ID, bez frazy odzyskiwania i klucza prywatnego. Zachowaj też stary pełny adres: nadal służy do wyszukiwania wcześniejszej aktywności.

Otwórz Moje statystyki starego adresu i zanotuj oczekujące nagrody, potwierdzone saldo, ostatnie wypłaty i aktywne nazwy maszyn. To punkt odniesienia; nowa strona nie zawiera starej historii. Zachowaj dostęp do pierwotnego portfela podczas śledzenia pozostałych nagród i płatności.

## Zmień jedną koparkę i jej polecenie

Zatrzymaj program i sprawdź zwykły sposób uruchamiania. Przy istniejącej konfiguracji `--address` ma pierwszeństwo bez zmieniania zapisanego adresu. Przykład PowerShell wybiera cel tej sesji. Zastąp `@yourname` nowym ID lub pełnym adresem, zachowując sprawdzone argumenty puli, danych i sprzętu.

```powershell
.\seine.exe --address "@yourname"
```

Na Linux lub macOS użyj poniższej składni ze zwykłego katalogu. Oba przykłady rozpoczynają prawdziwe kopanie; wybierz właściwy system. Istniejąca instalacja dostarcza pozostałe ustawienia. Zachowaj nazwę maszyny, by rozpoznać jej udziały pod nowym adresem.

```sh
./seine --address "@yourname"
```

## Zachowaj adres przy zwykłym restarcie

Przy zatrzymanym programie zmień `address` w `seine.config.json`. Zachowaj pozostałe pola i poprawny JSON. `./seine-data` jest względne wobec katalogu roboczego; `--data-dir` może wybrać inną lokalizację. Edytuj plik rzeczywiście używany przy normalnym starcie.

Zaktualizuj lub usuń stare `--address` ze skrótu, skryptu, zadania lub usługi. Poprawne ręczne polecenie nie zmienia innych sposobów startu. Uruchom normalnie i sprawdź ponownie. [Poradnik konfiguracji](https://seineminer.com/pl/blog/configure-seine-miner/) wyjaśnia priorytety i wybór innego pliku po zmianie katalogu roboczego.

## Co zmienia używanie Blocknet ID?

seine v0.2.36 rozwiązuje i sprawdza `@name` przy starcie. Pełny adres pozostaje stały przez sesję, również przy ponownych połączeniach. Po zmianie przypisania nazwy uruchom ponownie każdą koparkę i sprawdź cel. Samo ponowne połączenie nie powtarza rozwiązywania nazwy.

Zapisz oba pełne adresy, nawet jeśli nazwa się nie zmienia. Rozliczenia dotyczą rozwiązanego adresu: nowa mapa nie przekierowuje starych udziałów ani sald. Gdy nazwa nie działa, sprawdź przypisanie lub świadomie użyj pełnego adresu zweryfikowanego w portfelu.

## Sprawdź wszystkie komputery pod nowym adresem

Sprawdź cel i przyjęte zgłoszenia, a potem wyszukaj pełny nowy adres w Moich statystykach. Dopasuj nazwę maszyny i ostatnie udziały. Nowa estymacja hashrate potrzebuje pracy i czasu; najpierw sprawdź napływ udziałów. [Poradnik statystyk](/blog/pl/track-your-miner-on-bntpool/) wyjaśnia liczniki i brakujące maszyny.

Po sprawdzeniu pierwszego komputera powtórz zmianę na reszcie. Różne nazwy ułatwiają kontrolę. Przetestuj też normalny start każdej maszyny, by stary skrypt nie przywrócił wcześniejszego celu. [Poradnik wielu komputerów](/blog/pl/mine-multiple-computers-one-wallet/) opisuje organizację pod wspólnym adresem.

## Co dzieje się z poprzednimi nagrodami?

Istniejące naliczenia i wypłaty pozostają przy starym adresie. Wcześniejsze uprawnione udziały mogą nadal liczyć się w odpowiednich oknach PPLNS; nagrody oczekujące podlegają potwierdzeniom. Zerowy hashrate nie oznacza końca rozliczeń. [PPLNS](/blog/pl/how-pplns-payouts-work/) i [potwierdzenia](/blog/pl/block-confirmations-mining-rewards/) rozróżniają pracę, dojrzewanie nagród i wypłaty.

Zmiana celu nie łączy sald ani nie omija minimum wypłaty. Sprawdzaj aktualne zasady i obie strony do wyjaśnienia pozostałych kwot. Zmiana nazwy maszyny także nie przenosi nagród. Potwierdź, że wszystkie wybrane koparki wysyłają nową pracę na nowy adres, i zachowaj stary portfel oraz historię.

<CTA target="pool" title="Sprawdź konfigurację na bntpool.com" text="Potwierdź cel i przyjętą pracę każdej maszyny, a stare i nowe nagrody śledź osobno." />
