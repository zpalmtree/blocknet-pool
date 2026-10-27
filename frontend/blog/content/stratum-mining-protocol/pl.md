---
title: 'Czym jest Stratum? Jak miner rozmawia z poolem'
description: 'Prześledź Stratum między seine a bntpool.com: logowanie, zadania, cele udziałów, zgłoszenia i odpowiedzi, aby zrozumieć kolejne etapy połączenia minera.'
tags: ['Podstawy kopania', 'Stratum', 'Kopanie w poolu']
faq:
  - q: 'Co robi Stratum podczas kopania w poolu?'
    a: 'Stratum przenosi rozmowę minera z poolem: identyfikuje sesję, dostarcza pracę oraz przekazuje udziały i odpowiedzi. Miner oblicza hashe lokalnie. Pool łączy tę pracę ze swoim węzłem i prowadzi rozliczenia potrzebne do swojego systemu wypłat.'
  - q: 'Czy adres Stratum jest adresem strony?'
    a: 'Obsługują różne połączenia. Strona bntpool.com używa HTTPS dla stron i API, a udokumentowany adres kopania to stratum+tcp://bntpool.com:3333. Wpisz go w ustawieniu poola w minerze. Samo otwarcie strony nie ustanawia połączenia do kopania.'
  - q: 'Czy zaakceptowany udział Stratum oznacza wysłaną wypłatę?'
    a: 'Nie. Odpowiedź udziału opisuje wynik zgłoszenia w sesji. Weryfikacja, poprawny blok, prawo do nagrody i zakończona wypłata to osobne etapy. Śledź przyjętą pracę w My Stats, a potwierdzenia, PPLNS i próg poznaj w poradniku wypłat.'
sourceHash: '5cc5086d7b28'
translation: draft
---

Prześledź zadanie z bntpool.com do seine i udział wracający do poola. Zrozumienie rozmowy ułatwia czytanie komunikatów połączenia, aktualizacji zadań i odpowiedzi. Opis dotyczy implementacji bntpool.com; adres serwera i wypłat skonfigurujesz z [poradnikiem poola](/blog/pl/mine-blocknet-on-bntpool/).

## Czym jest Stratum w kopaniu?

<Answer>
Stratum to warstwa komunikacji, przez którą miner odbiera pracę i zgłasza wyniki. W seine i bntpool.com trwałe połączenie TCP przenosi wiadomości JSON logowania, zadań i udziałów. Komputer liczy hashe lokalnie; pool pobiera szablony bloków z węzła i przetwarza zgłoszoną pracę.
</Answer>

*Miner wysyła logowanie i udziały, odbiera zadania i odpowiedzi, a pool wymienia z węzłem szablony i kandydatów na bloki.*

## Adres określa połączenie do kopania

Udokumentowany punkt to `stratum+tcp://bntpool.com:3333`. Schemat wskazuje transport, nazwa hosta usługę, a port nasłuch kopania. Połączenie przeglądarki z `https://bntpool.com` ma inne zadanie. Skopiuj pełny adres z [Get Started](/start) do ustawienia poola, aby klient otworzył właściwe połączenie.

Trwałe połączenie pozwala obu stronom wysyłać wiadomości, gdy pojawia się praca. bntpool używa JSON rozdzielanego nowymi liniami, z ID łączącymi żądania i odpowiedzi. To format tego poola: miner musi obsługiwać go, jego możliwości i algorytm Blocknet. [Kod protokołu](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) opisuje bieżące pola.

## Co dzieje się podczas logowania?

Klient wysyła `login` z adresem wypłat, nazwą riga, wersją protokołu i możliwościami. Pool sprawdza login i zwraca informacje protokołu oraz wymagane możliwości. Ustala to zasady wymiany pracy. TCP jest pierwszym krokiem; przyjęty login potwierdza też rozpoczęcie rozmowy aplikacji.

Nazwa riga identyfikuje maszynę pod danym adresem. Dla kilku maszyn używaj różnych nazw i właściwego wspólnego adresu wypłat. Login wymaga adresu odbiorczego, nie frazy odzyskiwania ani prywatnego klucza wydawania. Po przyjęciu sesji i udostępnieniu pracy pool wysyła `job`, aby rozpocząć haszowanie.

## Co zawiera zadanie kopania?

Zadanie zawiera ID przydziału i szablonu, dane nagłówka, wysokość, cele udziału i sieci oraz zakres nonce. Nonce jest zmienianą wartością przy haszowaniu. Zakres określa przestrzeń poszukiwań sesji. ID zadania wiąże późniejsze zgłoszenie z pierwotną pracą i warunkami.

Cel udziału określa wyniki zgłaszane jako dowód pracy. Cel sieci wyznacza kandydatów na blok. Większość udziałów służy rozliczeniom bez stawania się blokami. [Poradnik trudności](/blog/pl/mining-difficulty-explained/) wyjaśnia rozmiar celu i oczekiwaną pracę; protokół dostarcza cele w przydziale.

## Zadania zmieniają się przy otwartym połączeniu

Nowe zadanie samo w sobie nie oznacza rozłączenia. Pool może rozsyłać aktualizacje po zmianie szablonu węzła lub przydział z inną trudnością udziałów. Zmienna trudność dostosowuje próg zgłaszania do obserwowanej pracy. Miner realizuje nowy przydział, a połączenie nadal przenosi wiadomości w obu kierunkach.

Odróżniaj ID zadań i szablonów w logach technicznych. Kilka przydziałów może używać tego samego szablonu z różnymi danymi sesji. Sama wysokość nie opisuje wszystkich zmian. Sprawdzaj odbiór zadań, postęp haszowania i odpowiedzi; [poradnik śledzenia](/blog/pl/track-your-miner-on-bntpool/) wskazuje wynikową aktywność.

## Co zawiera zgłoszenie udziału?

Po znalezieniu odpowiedniego wyniku seine wysyła `submit` z zadaniem, nonce i deklarowanym hashem. Pool przetwarza kontekst przydziału i odpowiada z ID żądania. Deklaracja jest częścią wiadomości; weryfikacja i walidacja bloków określają dalszy przebieg. Kandydat na blok trafia do demona do walidacji.

Odpowiedź może zawierać przyjęcie, status weryfikacji i trudność. Czytaj pola osobno, zamiast utożsamiać każdą udaną odpowiedź ze znalezieniem bloku. Rozliczenia i wypłaty odbywają się poza tą wymianą. [Poradnik udziałów](/blog/pl/accepted-rejected-stale-shares/) opisuje wyniki, a [poradnik PPLNS](/blog/pl/how-pplns-payouts-work/) łączy uprawnioną pracę z nagrodami i płatnościami.

## Jak ta kolejność pomaga sprawdzić minera?

Czytaj kolejno: serwer osiągnięty, login przyjęty, zadanie odebrane, haszowanie aktywne, zgłoszenia obsłużone. Po zerwaniu klient łączy się ponownie i ustanawia sesję przed odbiorem pracy. Dokładna wiadomość i czas wskazują etap. Działająca strona nie dowodzi dostępności portu kopania z tej maszyny.

Otwórz [My Stats](/stats) dla właściwego adresu i [status poola](/status). Po starcie lub ponownym połączeniu daj estymatom hashrate czas na zebranie udziałów. Oddziel protokół od terminu wypłaty: login przygotowuje sesję, zadania dają pracę, udziały raportują wyniki, a rozliczenia ustalają kolejne etapy nagród.

<CTA target="pool" title="Połącz seine z bntpool.com" text="Skorzystaj z konfiguracji poola, potem śledź zadania, udziały i riga w My Stats." />
