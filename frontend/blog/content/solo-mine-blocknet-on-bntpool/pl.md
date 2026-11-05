---
title: Kopanie solo Blocknet na bntpool.com
description: 'Kop Blocknet solo bez własnego węzła. Dodaj solo: przed adresem w seine, a bntpool.com wypłaci ci całą nagrodę za każdy znaleziony przez ciebie blok.'
tags: [Poradnik kopania, Kopanie solo, bntpool, seine]
sourceHash: '623b1e0b356a'
translation: draft
faq:
  - q: 'Jak kopać Blocknet solo na bntpool.com?'
    a: 'Zaktualizuj seine do v0.3.1 lub nowszej wersji i dodaj solo: przed adresem wypłat, na przykład ./seine --address solo:YOUR_ADDRESS. Seine łączy się z bntpool.com jak zwykle, a pula wypłaca ci całą nagrodę za każdy blok znaleziony przez twoją koparkę w ramach zwykłych wypłat. Nie musisz uruchamiać węzła.'
  - q: 'Czy do kopania solo potrzebuję własnego węzła Blocknet?'
    a: 'Nie. Z prefiksem solo: bntpool.com sam uruchamia węzeł, tworzy szablony bloków i wysyła znalezione przez ciebie bloki. Na koparce działa tylko seine. Drugim sposobem kopania solo jest tryb daemon w seine, na twoim zsynchronizowanym węźle; pasuje on tym, którzy już go mają.'
  - q: 'Czy mogę kopać solo z nazwą Blocknet ID?'
    a: 'Tak. Użyj solo:@name lub solo:$name zamiast solo:ADDRESS i ujmij wartość w cudzysłów w wierszu poleceń, aby powłoka przekazała ją bez zmian. Pula zamienia nazwę na pełny adres, a następnie nalicza twoje bloki solo i wypłaca je na ten adres jak każdą inną wypłatę.'
  - q: 'Jak wrócić do wspólnych nagród puli?'
    a: 'Usuń prefiks solo: z adresu i uruchom seine ponownie. Koparka znów otrzymuje część nagród PPLNS z każdego bloku znalezionego przez pulę. Każda koparka wybiera tryb przy połączeniu, więc z jednym adresem część koparek może kopać solo, a reszta na wspólnych nagrodach.'
---

Ten poradnik pokazuje, jak kopać Blocknet solo na bntpool.com: dodaj jeden prefiks do adresu w seine, a pula wypłaci ci całą nagrodę za każdy blok znaleziony przez twoje koparki. Nie instalujesz ani nie synchronizujesz węzła. Skonfigurujesz tryb, zobaczysz, jak wypłacane są bloki solo, i dowiesz się, gdzie śledzić je w puli.

## Czym jest kopanie solo na bntpool.com?

<Answer>
Kopanie solo na bntpool.com płaci tylko za bloki znalezione przez twoje własne koparki, a każdy z nich przynosi na twój adres całą nagrodę za blok. Pula nadal uruchamia węzeł, tworzy szablony bloków i wysyła twoje bloki, więc nagrody solo dostajesz przy tej samej konfiguracji co w puli. Włączasz to, dodając solo: przed adresem.
</Answer>

Przy zwykłym kopaniu w puli bntpool.com dzieli każdy blok między górników, których udziały mieszczą się w sześciogodzinnym oknie PPLNS, więc otrzymujesz część każdego bloku znalezionego przez pulę. Koparka solo zatrzymuje za to całą nagrodę za własne bloki i nic nie dostaje z bloków znalezionych przez innych. Oba tryby używają tego samego serwera, tego samego adresu i tych samych wypłat. Zmienia się tylko login.

Kopanie solo przez pulę daje nagrody solo bez pracy przy węźle:

- **Bez własnego węzła.** Węzeł puli śledzi łańcuch, dostarcza świeże szablony i wysyła twoje rozwiązane bloki.
- **Automatyczne wypłaty.** Twoje bloki są wypłacane na twój adres lub nazwę Blocknet ID jak każda inna wypłata bntpool.com.
- **Statystyki na żywo.** Twoje koparki, hashrate i bloki widać na [stronie statystyk](/stats) puli.
- **Przełączanie jednym prefiksem.** Usuń `solo:` i uruchom seine ponownie, aby wrócić do wspólnych nagród.

Drugim sposobem kopania solo jest tryb daemon w seine, na twoim zsynchronizowanym węźle Blocknet. [Pula czy kopanie solo Blocknet](/blog/pl/pool-vs-solo-mining-blocknet/) porównuje wszystkie trzy opcje i pokazuje, jak często twój hashrate znajduje blok.

## Jak zacząć kopanie solo z seine?

<Answer>
Zainstaluj seine v0.3.1 lub nowszy i uruchom go z solo: przed adresem Blocknet, na przykład ./seine --address solo:YOUR_ADDRESS na macOS lub Linuksie. Seine łączy się z bntpool.com, swoją domyślną pulą, a panel oznacza twoje konto jako Solo. Możesz też wpisać solo: i adres, gdy seine przy pierwszym uruchomieniu zapyta o adres wypłat.
</Answer>

1. **Zaktualizuj seine.** Logowanie solo wymaga seine v0.3.1 lub nowszego. [Pobierz seine](https://seineminer.com/pl/#download) albo pozwól, by zainstalowały go automatyczne aktualizacje.
2. **Uruchom seine z prefiksem.** Na macOS lub Linuksie:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   W Windows, w PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Seine domyślnie kopie na bntpool.com. Strona puli [Get Started](/start) pokazuje to samo polecenie z wypisanym adresem puli.
3. **Sprawdź panel.** Panel salda oznacza twoje konto jako **Solo**, a w logu pojawia się potwierdzenie puli, że otrzymujesz zapłatę za bloki znalezione przez ten adres.

Prefiks działa wszędzie, gdzie seine przyjmuje adres: w pytaniu przy pierwszym uruchomieniu, w zapisanej konfiguracji, w lokalnym API sterowania i na koparkach za proxy farmy. Seine zapisuje go razem z adresem, więc kolejne uruchomienia nadal kopią solo.

## Czy można kopać solo z nazwą Blocknet ID?

Tak. Nazwa Blocknet ID działa tak samo jak pełny adres. Wpisz `solo:@name` lub `solo:$name` i ujmij w cudzysłów, aby powłoka przekazała wartość bez zmian:

```sh
./seine --address 'solo:@yourname'
```

Pula zamienia nazwę przez blocknet.id i wypłaca twoje bloki solo na adres, który za nią stoi. Aby nazwać koparkę, dodaj nazwę do pełnego adresu po kropce, jak w `solo:YOUR_ADDRESS.rig1`, albo przekaż `--pool-worker rig1` z dowolnym adresem lub nazwą. Wielkość liter w prefiksie nie ma znaczenia, więc `SOLO:` też działa.

## Jak wypłacane są bloki solo?

Gdy twoja koparka znajdzie blok, pula zapisuje go jako blok solo. Gdy znajdzie się 60 bloków w głąb łańcucha, po około pięciu godzinach, pula dopisuje do twojego salda całą nagrodę za blok pomniejszoną o opłatę puli. Na dzień 4 listopada 2026 opłata wynosi 0%. Saldo jest następnie wypłacane na twój adres w ramach zwykłych wypłat puli.

Wypłaty podlegają tym samym zasadom co kopanie w puli. Każdy blok czeka na 60 potwierdzeń, aby krótka reorganizacja łańcucha nie mogła cofnąć już wypłaconej nagrody; [potwierdzenia bloków i nagrody za kopanie](/blog/pl/block-confirmations-mining-rewards/) wyjaśniają dlaczego. Punkt końcowy puli `/api/info` podawał 4 listopada 2026 minimalną wypłatę 1 BNT, a jedna nagroda za blok Blocknet jest znacznie wyższa, więc potwierdzony blok solo jest wypłacany według zwykłego harmonogramu wypłat puli. [Strona wypłat](/payouts) wymienia każdą płatność wysłaną przez pulę.

Jeśli kopiesz na adres depozytowy giełdy z memo wypłaty, solo działa tak samo. Memo i jego własna minimalna wypłata dotyczą twoich bloków solo tak samo jak wspólnych nagród.

Praca twojej koparki nadal liczy się do hashrate puli, effort rundy i szczęścia, a blok solo kończy rundę puli jak każdy inny blok. Udziały solo nigdy nie trafiają do okna PPLNS, więc inni górnicy nic nie dostają z twoich bloków, a ty nic z ich bloków.

## Jak często koparka solo znajduje blok?

Blocknet zakłada jeden blok co pięć minut, czyli 288 bloków dziennie. Koparka solo znajduje średnio 288 × (twój hashrate ÷ hashrate sieci) z nich dziennie. Odczytaj hashrate sieci na [stronie głównej bntpool.com](/), a swój hashrate na panelu seine, a następnie podziel dobę przez wynik, aby uzyskać średni czas między twoimi blokami.

Nagrody solo przychodzą w postaci całych bloków, więc pasują górnikom, których hashrate często znajduje bloki: dużym farmom, flotom serwerów i każdemu z dużym udziałem w sieci. Mniejsze koparki zarabiają średnio tyle samo na wspólnych nagrodach PPLNS, wypłacanych w regularnych częściach z każdego bloku puli. [Pula czy kopanie solo Blocknet](/blog/pl/pool-vs-solo-mining-blocknet/) omawia przykłady dla trzech wartości hashrate.

## Gdzie zobaczyć koparki i bloki solo?

Wyszukaj swój adres na [stronie statystyk](/stats). Możesz wkleić go z prefiksem `solo:` lub bez niego. Strona oznacza wtedy twoją pracę solo:

- **Workers** wymienia każdą koparkę z kolumną Type, w której widnieje **Solo** dla koparek, które wysłały udziały solo w oknie hashrate.
- **Estimated rewards** oznacza twoje bloki solo, gdy czekają na potwierdzenia.
- **Blocks** i **Luck** pokazują kolumnę Type, Pool lub Solo, przed statusem każdego bloku. [Strona bloków](/blocks) wymienia wszystkie bloki znalezione przez pulę, od najnowszych.

[Śledzenie koparki na bntpool.com](/blog/pl/track-your-miner-on-bntpool/) omawia resztę strony statystyk.

## Czy można łączyć koparki solo i w puli?

Tak. Każda koparka wybiera tryb przy połączeniu, więc jeden adres może jednocześnie obsługiwać koparki solo i koparki w puli. Farma może kopać solo na największych serwerach, a komputery stacjonarne zostawić na wspólnych nagrodach, wszystko wypłacane na jedno saldo. Pula trzyma oba rodzaje pracy osobno: udział znaleziony w połączeniu solo liczy się tylko do bloków solo tego adresu.

Aby przełączyć koparkę z powrotem, usuń `solo:` z jej adresu w poleceniu lub zapisanej konfiguracji i uruchom seine ponownie. Udziały wysłane już przez koparkę w trybie solo pozostają solo, a nowe od razu trafiają do okna PPLNS.

<CTA target="pool" title="Kop solo na bntpool.com" text="Dodaj solo: do adresu w seine i zatrzymuj całą nagrodę za każdy blok znaleziony przez twoje koparki, bez własnego węzła." />
