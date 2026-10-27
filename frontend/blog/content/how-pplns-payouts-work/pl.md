---
title: Jak działają wypłaty PPLNS na bntpool.com
description: bntpool.com wypłaca nagrody górnikom Blocknetu przez PPLNS z sześciogodzinnym oknem. Poznaj podział nagród, 60 potwierdzeń i minimum 0,1 BNT.
tags: [Wypłaty, PPLNS, bntpool, Poradnik kopania]
sourceHash: 'f216680e1944'
translation: draft
faq:
  - q: 'Jak długie jest okno PPLNS na bntpool.com?'
    a: 'Okno wynosi sześć godzin. Gdy pula znajdzie blok, bierze pod uwagę sześć godzin poprzedzających ten blok i dzieli nagrodę między wszystkich, którzy przesłali w tym czasie udziały, proporcjonalnie do ich trudności. Aktualną wartość podaje strona Get Started puli.'
  - q: 'Jaka jest minimalna wypłata na bntpool.com?'
    a: 'Minimalna wypłata wynosi 0,1 BNT. Gdy potwierdzone saldo osiągnie tę kwotę, pula automatycznie wysyła je na Twój adres Blocknet, bez składania wniosku lub odbierania nagrody. Mniejsze saldo pozostaje na koncie i rośnie z każdym potwierdzonym blokiem, w którym masz udziały, aż przekroczy próg.'
  - q: 'Dlaczego Estimated Rewards różnią się od Confirmed Rewards?'
    a: 'Estimated Rewards to Twoja część bloków znalezionych przez pulę, które wciąż zbierają 60 potwierdzeń, więc wartość może się jeszcze zmienić. Confirmed Rewards pochodzą z bloków znajdujących się 60 bloków w głębi łańcucha i czekają na wypłatę. Gdy każdy blok dojrzewa, jego kwota przechodzi z pierwszej karty do drugiej.'
  - q: 'Czy nagrody kończą się od razu po zatrzymaniu kopania?'
    a: 'Nie. Każdy udział pozostaje w oknie PPLNS przez sześć godzin od przesłania, więc bloki znalezione przez pulę w tym czasie nadal przynoszą Ci nagrody. Już potwierdzone nagrody pozostają na saldzie, a pula wypłaca je po osiągnięciu minimum 0,1 BNT.'
  - q: 'Co oznacza nakład pracy powyżej 100% na stronie szczęścia?'
    a: 'Nakład pracy porównuje pracę wykonaną przez pulę w rundzie z trudnością bloku, czyli oczekiwaną ilością pracy. Wynik powyżej 100% oznacza, że runda trwała dłużej niż przeciętnie, a poniżej 100% — krócej. Oba przypadki są normalne, a w wielu rundach średnia zbliża się do 100%.'
---

Ten poradnik wyjaśnia, jak bntpool.com przelicza udziały przesyłane przez Twój program na wypłaty Blocknetu. Dowiesz się, jak dzielona jest nagroda za blok, dlaczego czeka około pięciu godzin na możliwość wypłaty, kiedy wysyłane jest saldo oraz jak odczytywać karty salda na [stronie statystyk](/stats) i [stronę szczęścia](/luck).

Jeśli dopiero poznajesz kopanie w puli, [czym jest pula wydobywcza?](/blog/pl/what-is-a-mining-pool/) omawia podstawy. Jeśli jeszcze nie zacząłeś kopać, [poradnik kopania z seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) przeprowadzi Cię od pobrania do pierwszych udziałów w kilka minut.

## Czym jest PPLNS?

<Answer>
PPLNS oznacza „pay per last N shares”, czyli wypłatę za ostatnie N udziałów. Gdy bntpool.com znajdzie blok, dzieli nagrodę między udziały przesłane przez górników w sześciu godzinach poprzedzających ten blok, proporcjonalnie do trudności każdego udziału. Im więcej regularnej pracy wnosisz w tym oknie, tym większa jest Twoja część każdego bloku.
</Answer>

Na bntpool.com „ostatnie N” oznacza okres: sześć godzin. [Strona Get Started](/start) puli pokazuje system wypłat, okno, minimum wypłaty i liczbę potwierdzeń. Te same wartości są publikowane pod adresem `https://bntpool.com/api/info`.

| Ustawienie | Wartość na bntpool.com |
|---|---|
| System wypłat | PPLNS |
| Okno udziałów | 6 godzin |
| Potwierdzenia przed wypłatą | 60 bloków |
| Minimalna wypłata | 0,1 BNT |

Udziały zlicza się według trudności. Pula nadaje każdemu połączeniu własną trudność udziałów i dostosowuje ją tak, aby każdy górnik przesyłał równomierny strumień udziałów. Szybsza maszyna dostaje trudniejsze udziały, więc każdy z nich ma większą wagę. O Twojej części decyduje łączna wykonana praca, co jest sprawiedliwą miarą zarówno dla laptopa, jak i serwera.

## Jak bntpool.com dzieli nagrodę za blok?

Po znalezieniu bloku nagroda zawsze przechodzi przez te same etapy:

1. **Blok zostaje zapisany.** Pojawia się na [stronie bloków](/blocks) ze statusem oczekującym.
2. **Pula mierzy okno.** Bierze sześć godzin kończących się w momencie znalezienia bloku i sumuje trudność wszystkich zaakceptowanych udziałów z każdego adresu w tym okresie.
3. **Nagroda zostaje podzielona.** Każdy adres otrzymuje taką część nagrody, jaką stanowi jego udział w łącznej pracy okna.
4. **Blok dojrzewa.** Twoja część jest pokazywana jako oszacowanie, dopóki blok nie znajdzie się 60 bloków w głębi łańcucha, a potem przechodzi na potwierdzone saldo.
5. **Saldo zostaje wypłacone.** Gdy potwierdzone saldo osiągnie 0,1 BNT, pula wysyła je na Twój adres.

## Przykład obliczeń

<Note>
Ten przykład używa wymyślonych, okrągłych liczb, aby pokazać obliczenia. Rzeczywiste wartości zależą od hashrate puli, Twojego hashrate i ówczesnej [nagrody za blok](/blog/pl/blocknet-block-reward-schedule/).
</Note>

Załóżmy, że w sześciu godzinach przed blokiem wszyscy górnicy puli przesłali udziały o łącznej trudności 1 000 000, a Twoje udziały sumują się do 20 000. Twój udział w oknie wynosi 20 000 ÷ 1 000 000 = 2%. Jeśli pula dzieli za ten blok 400 BNT, otrzymujesz 400 × 0,02 = 8 BNT.

Teraz wyobraź sobie drugiego górnika z dokładnie takim samym hashrate, który połączył się 30 minut przed blokiem. Trzydzieści minut to jedna dwunasta sześciu godzin, więc ma w oknie około 1667 trudności i otrzymuje z tego bloku około 0,67 BNT. Jego praca nadal się jednak liczy: każdy z tych udziałów pozostaje w oknie przez następne sześć godzin i przynosi część każdego bloku znalezionego przez pulę w tym czasie.

| Górnik (przykład) | Praca w oknie | Udział | Kwota z wypłaty 400 BNT |
|---|---|---|---|
| Ty, kopiący przez pełne sześć godzin | 20 000 | 2% | 8 BNT |
| Taki sam hashrate, dołączenie 30 minut przed blokiem | 1667 | około 0,17% | około 0,67 BNT |
| Mały górnik, kopiący przez pełne sześć godzin | 100 | 0,01% | 0,04 BNT |

Mały górnik z ostatniego wiersza zarabia 0,04 BNT na blok, więc jego potwierdzone saldo przekracza minimum 0,1 BNT po trzech takich blokach i wtedy zostaje wypłacone.

## Dlaczego PPLNS nagradza regularne kopanie?

Każdy przesłany udział liczy się do wszystkich bloków znalezionych przez pulę w kolejnych sześciu godzinach. Górnik pracujący przez całą dobę zawsze ma za sobą pełne okno pracy, więc każdy nowy blok przynosi mu nagrodę odpowiadającą pełnemu tempu. Jego dochód z kolejnych bloków ściśle odpowiada hashrate.

Dzięki temu PPLNS dobrze znosi przerwy. Restart, aktualizacja sterownika lub krótki zanik sieci nie zerują Twojej pozycji, ponieważ już przesłane udziały pozostają w oknie przez sześć godzin. Praca dodana po znalezieniu bloku liczy się do kolejnych bloków, więc dodatkowy hashrate zawsze jest wykorzystywany.

Aby uzyskać najbardziej regularne wyniki, pozostaw program uruchomiony i pozwól mu się dostroić. seine nadal dostosowuje liczbę wątków i jądro obliczeniowe CPU podczas kopania bez ponownego łączenia z pulą, dzięki czemu strumień udziałów pozostaje ciągły. Możesz [pobrać seine](https://seineminer.com/pl/#download) dla Windows, macOS lub Linux.

## Dlaczego nagroda za blok czeka na 60 potwierdzeń?

Blocknet wymaga, aby nowo wykopane monety odczekały 60 bloków, zanim będzie można je wydać. Reguły demona nazywają to dojrzewaniem coinbase. Przy pięciominutowym czasie bloku Blocknetu trwa to około pięciu godzin (60 × 5 minut = 300 minut). bntpool.com stosuje te same 60 bloków, zanim uzna nagrodę za potwierdzoną, więc każda wypłata pochodzi z monet, które pula może faktycznie wysłać.

Podczas dojrzewania bloku pula sprawdza, czy nadal należy on do głównego łańcucha. Gdy znajdzie się 60 bloków w jego głębi, zostaje oznaczony jako potwierdzony, a nagroda jest dodawana do potwierdzonego salda każdego górnika. W rzadkim przypadku, gdy sieć wybierze inny blok na tej wysokości, blok zostaje oznaczony jako osierocony, a szacowane nagrody są usuwane.

## Kiedy przychodzą wypłaty z bntpool.com?

<Answer>
Po uzyskaniu przez blok 60 potwierdzeń, czyli około pięciu godzinach przy pięciominutowym czasie bloku Blocknetu, Twoja część przechodzi do Confirmed Rewards na stronie statystyk. Gdy potwierdzone saldo osiągnie 0,1 BNT, bntpool.com automatycznie wysyła je na Twój adres Blocknet. Nie trzeba niczego zamawiać ani odbierać.
</Answer>

U nowego górnika pierwsze oszacowanie pojawia się, gdy pula znajdzie blok, a jego udziały znajdują się w oknie. Pierwsza płatność następuje po dojrzeniu tego bloku i osiągnięciu minimum przez saldo. Wysłane płatności są widoczne na [stronie wypłat](/payouts) i we własnej historii na stronie statystyk.

## Jak pokazywane są wstępne nagrody?

Wyszukaj swój adres lub `@name` na [stronie statystyk](/stats), a zobaczysz trzy karty salda:

- **Estimated Rewards:** Twoja część bloków, które jeszcze nie dojrzały. To wstępne kwoty, które mogą się zmieniać do czasu potwierdzenia lub osierocenia każdego bloku.
- **Confirmed Rewards:** dojrzałe saldo oczekujące na wypłatę.
- **Paid Balance:** wszystko, co już wysłano na Twój adres.

Pula oddziela wstępne nagrody, aby potwierdzone saldo zawierało wyłącznie ostateczne kwoty. Pod kartami strona statystyk wymienia każdy niepotwierdzony blok z Twoją szacowaną częścią, dzięki czemu możesz obserwować postęp do potwierdzenia. [Poradnik monitorowania górnika](/blog/pl/track-your-miner-on-bntpool/) omawia resztę strony statystyk.

## Co pokazują strony szczęścia i nakładu pracy?

Nakład pracy określa, ile pracy pula wykonała, aby znaleźć blok. To zaakceptowana praca z rundy podzielona przez trudność bloku, wyrażona procentowo. Trudność Blocknetu odpowiada oczekiwanej liczbie hashy na blok, więc 100% nakładu oznacza dokładnie przeciętną ilość pracy. Poniżej 100% pula znalazła blok szybciej niż przeciętnie, a powyżej 100% — wolniej.

[Strona szczęścia](/luck) wymienia każdą rundę wraz z wysokością bloku, nakładem pracy, czasem rundy, statusem i czasem znalezienia. [Panel puli](/) pokazuje narastający nakład bieżącej rundy oraz średnie szczęście puli. Poszczególne rundy wahają się powyżej i poniżej 100%, co jest normalne dla każdej puli proof of work, a średni nakład w wielu rundach zbliża się do 100%.

Szczęście wpływa na częstotliwość pojawiania się bloków. Twoja część każdego bloku zależy wyłącznie od udziału w pracy okna, a regularne kopanie utrzymuje ten udział na pełnym poziomie niezależnie od długości rundy.

<CTA target="pool" title="Dołącz do puli" text="Kop regularnie na bntpool.com, a Twoja część każdego bloku zostanie automatycznie wypłacona po osiągnięciu 0,1 BNT." />
