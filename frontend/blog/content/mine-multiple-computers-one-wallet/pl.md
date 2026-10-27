---
title: 'Kop Blocknet na wielu komputerach do jednego portfela'
description: 'Dodaj drugi miner Blocknet do tego samego portfela na bntpool.com. Nazwij komputery, sprawdź udziały oraz poznaj wspólne nagrody i zasady powtarzających się nazw.'
tags: ['Poradnik kopania', 'Komputery', 'Konfiguracja']
faq:
  - q: 'Czy każdy komputer potrzebuje własnego portfela?'
    a: 'Nie. Każdy może używać tego samego adresu odbiorczego na bntpool.com, z inną nazwą identyfikującą instalację. Pula rozlicza nagrody według adresu. Komputery kopiące potrzebują jedynie adresu lub Blocknet ID, a nie frazy odzyskiwania portfela.'
  - q: 'Co się dzieje, gdy dwa minery mają tę samą nazwę?'
    a: 'Pod tym samym adresem ich historia udziałów jest grupowana pod wspólną nazwą. Utrudnia to rozpoznanie maszyny stojącej za odczytem. Nadaj każdej instalacji inną nazwę i uruchom ją ponownie; wcześniejsze udziały zachowują oryginalną etykietę.'
  - q: 'Czy dodatkowy komputer dzieli moją wypłatę?'
    a: 'Nazwy porządkują wyświetlaną aktywność i nie tworzą osobnych sald dla tego samego adresu. Prawidłowa praca komputerów zwiększa udział tego adresu w PPLNS. Jego saldo nadal podlega zwykłym zasadom potwierdzeń i wypłat puli.'
sourceHash: '1cfcb79502c0'
translation: draft
---

Dodaj drugi komputer do bntpool.com, zachowując jeden portfel odbiorczy. Nazwij każdą instalację, sprawdź ją osobno, potem obserwuj wspólny adres. Ten poradnik obejmuje dodawanie maszyny i powtarzające się nazwy. Pierwszy miner skonfiguruj najpierw według [poradnika puli](/blog/pl/mine-blocknet-on-bntpool/).

## Czy wiele komputerów może kopać do jednego portfela?

<Answer>
Tak. Uruchom seine na każdym komputerze z tym samym adresem odbiorczym i odrębną nazwą maszyny. bntpool.com rejestruje pracę i nagrody według adresu, a nazwy rozdzielają aktywność w statystykach. Sprawdź zaakceptowane udziały każdej maszyny, zanim polegasz na wspólnym widoku.
</Answer>

*Osobne komputery wysyłają pracę przez pulę do jednego adresu odbiorczego; każdy zachowuje odrębną nazwę.*

## Wybierz adres i schemat nazw

Użyj adresu odbiorczego portfela lub wskazującego go Blocknet ID. Sprawdź rozwiązany adres na każdym minerze. Nazwa upraszcza wpisywanie, ale rozliczenia dotyczą wynikowego adresu. Po zmianie celu ID działający miner może zachować poprzednie rozwiązanie aż do restartu. Już uzyskane nagrody pozostają przy pierwotnym adresie.

[Poradnik portfela do kopania](https://seineminer.com/pl/blog/blocknet-wallet-for-mining/) wyjaśnia uzyskanie adresu. Maszyny potrzebują tylko publicznego celu; frazę odzyskiwania i klucze prywatne zachowaj przy portfelu. Wybierz krótkie nazwy, np. `desktop-01` i `server-02`. Pula usuwa skrajne białe znaki i zachowuje do 64 znaków. Część odróżniającą umieść na początku długiej nazwy.

## Dodaj drugiego minera z własną nazwą

Zainstaluj odpowiednią wersję seine na drugim komputerze i uruchom ją z rozpakowanego katalogu. Pierwszy przykład PowerShell jawnie podaje odbiorcę, pulę i maszynę. Zastąp `@yourname` własnym ID lub pełnym adresem przed wykonaniem. Zachowaj cudzysłowy i dodaj sprawdzone opcje backendu lub strojenia tej maszyny.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Na Linux lub macOS użyj poniższego programu z inną nazwą maszyny. Systemy mogą się różnić; adres odbiorczy powinien być wspólny. Każdy przykład rozpoczyna prawdziwe kopanie. Uruchom odpowiedni na właściwym komputerze i poczekaj na zwykły start oraz strojenie. Nie trzeba wykonywać obu przykładów na jednej maszynie.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Które ustawienia są wspólne?

Wspólne są adres odbiorczy i endpoint puli. Każda instalacja ma własny lokalny katalog danych i nazwę. seine zapisuje połączenie w `seine.config.json` w katalogu danych; domyślne `./seine-data` zależy od folderu startowego. Inny folder może znaleźć inną konfigurację. Stały katalog ułatwia utrzymanie ustawień.

Jeśli kopiujesz konfigurację pierwszego komputera, ustaw odrębną nazwę w poleceniu drugiego i potwierdź ją przy starcie. Zachowaj ją w kolejnych poleceniach: nadpisanie w wierszu poleceń nie zmienia nazwy już zapisanej. Dopasuj sprzętowe opcje i pozwól każdemu komputerowi stroić się lokalnie. Argument adresu wyznacza wypłatę, a nazwy — etykietę. Podawaj je osobno jak wyżej.

## Jak sprawdzić pracę obu komputerów?

Otwórz My Stats na bntpool.com dla wspólnego adresu. Znajdź obie nazwy i ostatni udział każdej. Sprawdź przyjęte zgłoszenia także w lokalnych minerach. Rób to komputer po komputerze, łącząc proces z wierszem puli; łatwiej wtedy wykryć pomyłki niż patrząc tylko na łączną moc obliczeniową.

Pula szacuje hashrate na podstawie przesłanych udziałów. Nowa maszyna potrzebuje czasu i przyjętej pracy do miarodajnego odczytu. Lokalna szybkość i szacunek puli obejmują inne pomiary i okresy. Najpierw potwierdź aktywność udziałami i licznikami, potem porównuj dłuższe okresy. [Poradnik statystyk](/blog/pl/track-your-miner-on-bntpool/) tłumaczy karty i wiersze bez wyciągania liczby urządzeń z liczby połączeń.

## Co zrobić, gdy maszyny brakuje lub nazwy się powtarzają?

Te same nazwy pod jednym adresem grupują udziały. Zmień nazwę jednego minera jego opcją nazwy i zrestartuj go. Nowe zgłoszenia dostaną nową etykietę, stare zachowają poprzednią. Stary wiersz może pozostać widoczny w okresie raportowania. Zmiana nazwy nie przenosi zarobionego salda adresu.

Dla brakującej maszyny porównaj faktycznie rozwiązany adres, endpoint i nazwę z otwartą stroną. Sprawdź połączenie i przyjęte udziały. CPU i GPU w jednym procesie seine nie potrzebują tu osobnych nazw. Przy udziałach odrzuconych lub nieaktualnych skorzystaj z [poradnika statusów](/blog/pl/accepted-rejected-stale-shares/), zamiast wielokrotnie zmieniać adres portfela.

## Jak działają wspólne nagrody i wypłaty?

Prawidłowa praca komputerów wpływa na udział jednego adresu w oknie PPLNS. Nazwy pomagają śledzić wkład, nie tworząc osobnych sald. Dodatkowa maszyna wnosi faktycznie przesłaną użyteczną pracę, nie stałą nagrodę za urządzenie lub nazwę. Sprzęt i czas działania mogą dawać różny wkład; same dodatkowe nazwy nie zwiększają pracy.

[Poradnik PPLNS](/blog/pl/how-pplns-payouts-work/) wyjaśnia podział po znalezieniu bloku. Potwierdzenia i zasady wypłat nadal dotyczą wspólnego salda. Nie trzeba łączyć portfeli ani przesuwać sald między nazwami. Przed pozostawieniem komputerów sprawdź adres, odrębną nazwę i zaakceptowane udziały na każdym. Powtórz listę przy następnej maszynie.

<CTA target="pool" title="Podłącz kolejny komputer do bntpool.com" text="Użyj jednego adresu, nadaj instalacjom różne nazwy i sprawdź zaakceptowane udziały." />
