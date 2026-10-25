---
title: Kopanie Blocknetu w puli a solo
description: Wybierz kopanie Blocknetu w puli lub solo. Oblicz oczekiwaną liczbę bloków dziennie, poznaj regularne wypłaty bntpool.com i skonfiguruj kopanie solo.
tags: [Poradnik kopania, Kopanie solo, bntpool, PPLNS]
sourceHash: 171d240d6c4b
translation: draft
faq:
  - q: 'Jak często znajdowałbym blok Blocknetu, kopiąc solo?'
    a: 'Średnio znajdujesz 288 × (Twój hashrate ÷ hashrate sieci) bloków dziennie, ponieważ docelowo Blocknet tworzy blok co pięć minut. Przy 50 H/s w sieci o mocy 50 000 H/s daje to 0,288 bloku dziennie, czyli około jednego bloku co trzy i pół dnia. Aktualny hashrate sieci odczytasz w panelu bntpool.com.'
  - q: 'Czy do kopania na bntpool.com muszę uruchomić węzeł Blocknet?'
    a: 'Nie. Pula prowadzi własny węzeł Blocknet, tworzy szablony bloków i przesyła znalezione bloki. Potrzebujesz tylko seine i adresu Blocknet lub nazwy Blocknet ID. Uruchom seine, wklej adres, zaakceptuj domyślną pulę, a pula będzie wypłacać na ten adres Twoją część każdego bloku.'
  - q: 'Czy seine może kopać Blocknet solo?'
    a: 'Tak. Uruchom seine poleceniem ./seine --mode daemon, aby kopać bezpośrednio przez własny, zsynchronizowany węzeł Blocknet, łącząc się za pomocą pliku cookie jego API. Każdy znaleziony blok wypłaca całą nagrodę do portfela węzła. Poradnik kopania solo w repozytorium seine opisuje niestandardowe konfiguracje węzła i hasła portfela.'
  - q: 'Czy mogę później przełączyć się między kopaniem w puli a solo?'
    a: 'Tak. seine używa tego samego pliku do pobrania w obu trybach. Uruchom go normalnie, aby kopać na bntpool.com, lub dodaj --mode daemon, aby kopać przez własny węzeł. Zrestartuj program, gdy chcesz zmienić tryb. Już przesłane udziały pozostają w sześciogodzinnym oknie PPLNS puli i nadal zarabiają do jego końca.'
---

Ten poradnik pomoże Ci zdecydować, czy kopać Blocknet na bntpool.com, czy solo przez własny węzeł. Obliczysz, ile bloków Twój hashrate znajduje samodzielnie, zobaczysz, jak pula zamienia to w regularny strumień wypłat, i dowiesz się, jak skonfigurować oba tryby w seine.

## Kopać Blocknet w puli czy solo?

<Answer>
Oba tryby opierają się na tych samych obliczeniach: Twój udział w hashrate sieci wyznacza udział w jej blokach. Na bntpool.com otrzymujesz część każdego bloku znalezionego przez pulę, więc wypłaty przychodzą regularnie wraz z blokami puli. Kopiąc solo, zachowujesz całą nagrodę za każdy samodzielnie znaleziony blok, co odpowiada dużym instalacjom z własnym węzłem.
</Answer>

Dla większości górników bntpool.com jest łatwym punktem startu. To domyślna pula seine, więc poza adresem niczego nie musisz konfigurować ani uruchamiać i synchronizować węzła. Kopanie solo też jest w pełni obsługiwane: seine ma tryb demona, który kopie bezpośrednio przez Twój węzeł Blocknet. Dalsza część poradnika pokazuje, jak określić tryb odpowiedni dla Twojego sprzętu.

## Ile bloków znalazłbyś solo?

<Answer>
Docelowo Blocknet tworzy blok co pięć minut, czyli 288 bloków dziennie. Kopiąc samodzielnie, możesz oczekiwać znalezienia 288 × (Twój hashrate ÷ hashrate sieci) bloków dziennie. Podziel jeden dzień przez tę liczbę, aby uzyskać średni czas między Twoimi blokami. Aktualny hashrate sieci odczytasz w panelu bntpool.com.
</Answer>

Wzór działa, ponieważ Blocknet dostosowuje trudność tak, aby cała sieć znajdowała blok średnio co 300 sekund, a trudność bloku jest w przybliżeniu liczbą hashy, których sieć powinna spróbować przed powodzeniem. Każdy hash ma taką samą szansę, więc część hashy sieci pochodząca z Twojej maszyny odpowiada części bloków, którą możesz oczekiwać znaleźć.

Potrzebujesz dwóch liczb:

- **Twój hashrate.** seine pokazuje go w swoim panelu podczas kopania, a [strona statystyk](/stats) puli pokazuje go po rozpoczęciu kopania w niej.
- **Hashrate sieci.** [Panel bntpool.com](/) pokazuje go na karcie Network Hashrate, obok karty Pool Hashrate. Obie wartości zmieniają się z czasem, więc sprawdź je przy wykonywaniu obliczeń.

Każdy znaleziony blok wypłaca nagrodę za blok obowiązującą w danym miesiącu. [Harmonogram nagród za bloki](/blog/pl/blocknet-block-reward-schedule/) podaje ją miesiąc po miesiącu.

## Przykład obliczeń

<Note>
Poniższe liczby są hipotetycznymi, okrągłymi wartościami dobranymi do pokazania obliczeń. Nie są danymi bieżącymi. Do własnego oszacowania użyj aktualnego hashrate sieci z panelu.
</Note>

Załóżmy, że sieć osiąga 50 000 H/s. Pojedyncza maszyna o wydajności 50 H/s, zbliżonej do pomiaru seine na jednym serwerze AMD EPYC Zen 5, dysponuje 0,1% sieci. Oto, ile taka maszyna i dwie większe instalacje znajdowałyby samodzielnie:

| Twój hashrate (przykład) | Udział w sieci | Oczekiwane bloki dziennie | Średni czas między blokami |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | około 83 godzin (3,5 dnia) |
| 500 H/s | 1% | 2,88 | około 8,3 godziny |
| 5000 H/s | 10% | 28,8 | około 50 minut |

Teraz podłącz maszynę 50 H/s do puli o łącznej wydajności również 5000 H/s. Pula znajduje około 28,8 bloku dziennie, średnio jeden co 50 minut, a 1% udziału maszyny w pracy puli daje jej 1% każdego z tych bloków. Rozliczana praca odpowiada tym samym 0,288 bloku dziennie. Zmienia się sposób otrzymywania nagrody: mała część mniej więcej co 50 minut, podczas gdy solo przychodzi jako cały blok co kilka dni.

Tabela pokazuje także, dlaczego kopanie solo pasuje do dużych instalacji. Przy 5000 H/s samodzielny górnik znajduje bloki równie często jak pula tej wielkości, więc jego dochód jest regularny bez jej pomocy.

Aby przeliczyć bloki dziennie na BNT dla swojego sprzętu, korzystając z datowanego przykładu z rzeczywistych danych puli, zobacz [jak szacować nagrody za kopanie Blocknetu](/blog/pl/estimate-blocknet-mining-rewards/).

## Dlaczego pula daje bardziej regularne wypłaty?

bntpool.com łączy hashrate wszystkich swoich górników, więc znajduje bloki znacznie częściej niż pojedyncza maszyna. Nagroda za każdy blok jest dzielona między górników, których praca pomogła go znaleźć, przy użyciu PPLNS: pula bierze pod uwagę sześć godzin przed blokiem i dzieli nagrodę proporcjonalnie do trudności udziałów każdego górnika.

Ma to dla Ciebie kilka praktycznych skutków:

- **Zarobki z każdego bloku.** Dopóki Twoje udziały są w sześciogodzinnym oknie, otrzymujesz część każdego bloku znalezionego przez pulę. Możesz obserwować je na [stronie bloków](/blocks).
- **Automatyczne wypłaty.** Gdy blok znajdzie się 60 bloków w głębi łańcucha, Twoja część przechodzi na potwierdzone saldo, a pula wysyła je na Twój adres po osiągnięciu 0,1 BNT. Wysłane płatności są wymienione na [stronie wypłat](/payouts).
- **Ciągłość mimo restartów.** Już przesłane udziały pozostają w oknie przez sześć godzin, więc restart lub aktualizacja sterownika nie zerują Twojej pozycji.
- **Uruchamiasz tylko program do kopania.** Pula prowadzi węzeł, tworzy szablony bloków i przesyła bloki. Możesz kopać z kilku maszyn na jeden adres lub nazwę Blocknet ID `@name`.

[Poradnik wypłat PPLNS](/blog/pl/how-pplns-payouts-work/) opisuje podział krok po kroku na własnym przykładzie. [Strona Get Started](/start) puli podaje aktualne ustawienia wypłat.

## Dlaczego kopać Blocknet solo?

Kopanie solo daje Ci kontrolę nad każdym etapem procesu. Twój węzeł tworzy każdy szablon bloku i sam sprawdza wszystkie bloki, a każdy znaleziony przez Ciebie blok wypłaca całą nagrodę bezpośrednio do portfela węzła.

Najlepiej sprawdza się wtedy, gdy Twój hashrate często znajduje bloki. W powyższym przykładzie instalacja z 10% mocy sieci znajduje blok mniej więcej co 50 minut, więc wypłaty solo przychodzą równie regularnie jak w puli. Jeśli już prowadzisz węzeł Blocknet dla portfela lub innych usług, kopanie solo wymaga dodania do niego programu do kopania.

seine jest przystosowany również do tego trybu. Kopie na szablonach bloków węzła, a jeśli węzeł zapomni szablon, na przykład po restarcie, seine ponownie przesyła cały rozwiązany blok, aby znaleziony blok nadal dotarł do łańcucha.

## Jak skonfigurować kopanie solo z seine?

1. **Zainstaluj i zsynchronizuj węzeł Blocknet.** Przy użyciu programu `blocknet` uruchom:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Zapisuje to plik cookie API węzła w `~/.config/bnt/data/mainnet/api.cookie`. Aby skrócić pierwszą synchronizację, bntpool.com publikuje plik `checkpoints.dat` do szybkiej synchronizacji początkowej na [stronie punktów kontrolnych](/checkpoints); [synchronizacja węzła Blocknet z punktami kontrolnymi](/blog/pl/blocknet-checkpoints-fast-sync/) wyjaśnia, jak węzeł z niego korzysta.
2. **Poczekaj na zakończenie synchronizacji.** Komunikaty `[sync] progress` węzła osiągną 100%. Węzeł udostępnia szablony bloków dopiero po dogonieniu łańcucha.
3. **Uruchom seine w trybie demona.**

   ```sh
   ./seine --mode daemon
   ```

   Przy niestandardowej konfiguracji węzła sam wskaż seine adres API i plik cookie:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

[Poradnik kopania solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) omawia hasła portfela i niestandardowe konfiguracje, a [poradnik kopania z seine](https://seineminer.com/pl/blog/how-to-mine-blocknet/) opisuje pobranie seine i uzyskanie pierwszych udziałów. Możesz [pobrać seine](https://seineminer.com/pl/#download) dla Windows, macOS lub Linux.

## Co wybrać?

Zastosuj wzór z własnym hashrate i bieżącym hashrate sieci z [panelu](/), a następnie sprawdź średni czas między blokami.

- **Zacznij na bntpool.com**, jeśli kopiesz na komputerze stacjonarnym, laptopie lub kilku serwerach albo chcesz regularnych wypłat bez prowadzenia węzła. Uruchom seine, wklej adres lub `@name` i zaakceptuj domyślną pulę.
- **Kop solo**, jeśli Twoja instalacja ma duży udział w sieci, dzięki czemu samodzielnie często znajduje bloki, a Ty już prowadzisz lub chcesz prowadzić własny zsynchronizowany węzeł.

seine ułatwia późniejszą zmianę decyzji. Kopanie w puli i tryb demona korzystają z tego samego programu, więc wraz z rozbudową sprzętu możesz przełączyć się jedną flagą i restartem. Jeśli prowadzisz lokalny węzeł podczas kopania w puli, seine pokazuje także saldo portfela węzła obok salda puli.

<Note>
Według [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) Blocknet planuje aktualizację sieci o nazwie Faraday około 16 lutego 2027 r. Ma ona zmienić proof of work. Aktualizuj seine: program sprawdza nowe wydania przy uruchomieniu i co 12 godzin.
</Note>

<CTA target="pool" title="Zacznij kopać na bntpool.com" text="Uruchom seine, wklej adres i otrzymuj część każdego bloku znalezionego przez pulę, wypłacaną automatycznie od 0,1 BNT." />
