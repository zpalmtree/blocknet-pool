---
title: 'Uczciwy start Blocknetu: bez premine, każda moneta wykopana'
description: Blocknet wystartował bez premine, funduszu deweloperów i skarbca. Sprawdź pusty blok genezy i regułę jednego wyjścia coinbase, a potem kop BNT.
tags: [Blocknet, Uczciwy start, Blok genezy, Kopanie, bntpool]
sourceHash: 814839500c3d
translation: draft
faq:
  - q: 'Czy Blocknet miał premine?'
    a: 'Nie. Blok genezy Blocknetu, utworzony 15 lutego 2026 r., nie zawiera żadnych transakcji, więc przed rozpoczęciem kopania nie istniały żadne monety. Pierwsze monety pochodziły z nagrody za blok 1, a każde BNT od tego czasu wypłacono osobie, która wykopała blok będący jego źródłem.'
  - q: 'Czy Blocknet ma fundusz deweloperów lub skarbiec?'
    a: 'Nie. Konsensus dopuszcza dokładnie jedną transakcję coinbase w bloku, która musi mieć dokładnie jedno wyjście o wartości równej nagrodzie za blok. Nie ma drugiego wyjścia dla dewelopera, założyciela ani skarbca, a blok próbujący je dodać zostaje odrzucony przez każdy węzeł sieci.'
  - q: 'Kiedy utworzono blok genezy Blocknetu?'
    a: 'Znacznik czasu bloku genezy to 15 lutego 2026 r., 20:58:38 EST, czyli 16 lutego o 01:58:38 UTC. Zawiera wiadomość "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything", a hash poprzedniego bloku jest hashem SHA3-256 tej wiadomości.'
  - q: 'Jak samodzielnie zweryfikować uczciwy start Blocknetu?'
    a: 'Otwórz blok 0 w eksploratorze Blocknetu: zobaczysz zero transakcji i korzeń Merkle składający się z samych zer. Następnie przeczytaj otwarty kod demona, w którym blok genezy jest zapisany na stałe z pustą listą transakcji, a kontrola coinbase akceptuje jedno wyjście równe nagrodzie za blok.'
  - q: 'Jak zacząć kopać Blocknet?'
    a: 'Pobierz program seine dla Windows, macOS lub Linux, uruchom go i wklej adres Blocknet lub krótką nazwę Blocknet ID, gdy o to poprosi. Naciśnij Enter, aby zaakceptować domyślną pulę bntpool.com, a seine od razu zacznie kopać. Potem możesz wyszukać swój adres na stronie statystyk bntpool.com.'
---

Blocknet wystartował bez przydziału monet komukolwiek. Ta strona pokazuje zawartość bloku genezy, reguły konsensusu uniemożliwiające premine, fundusz deweloperów lub skarbiec, sposoby ich sprawdzenia w eksploratorze i kodzie źródłowym oraz rozpoczęcie samodzielnego kopania BNT na bntpool.com.

## Czy Blocknet miał premine?

<Answer>
Nie. Blok genezy Blocknetu, utworzony 15 lutego 2026 r., nie zawiera transakcji, więc przed rozpoczęciem kopania nie istniały monety. Od bloku 1 każdy blok wypłaca nagrodę w jednym wyjściu coinbase o wartości dokładnie zgodnej z harmonogramem, więc każde BNT w obiegu wykopał ktoś wykonujący proof of work.
</Answer>

Nie ma przydziału dla założycieli, deweloperów, inwestorów ani fundacji — ani w bloku genezy, ani w żadnym późniejszym bloku. Podaż monet pochodzi wyłącznie z nagród za bloki, a egzekwujące to reguły są częścią konsensusu, więc każdy węzeł sieci sprawdza je dla każdego bloku.

## Co zawiera blok genezy Blocknetu?

Blok genezy jest zapisany na stałe w demonie, więc każdy węzeł zaczyna od tego samego bloku 0. Jego pola są ustalone:

| Pole | Wartość |
|---|---|
| Wysokość | 0 |
| Znacznik czasu | 15 lutego 2026 r., 20:58:38 EST (16 lutego o 01:58:38 UTC) |
| Wiadomość | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Poprzedni hash | SHA3-256 wiadomości: `a94a7f2b…d909df` |
| Trudność | 4, minimum sieci |
| Nonce | 0 |
| Transakcje | brak |
| Korzeń Merkle | same zera |

Wiadomość to nagłówek artykułu Wired z 15 lutego 2026 r., a kod wskazuje ten artykuł jako źródło. Ponieważ poprzedni hash jest wyprowadzony z wiadomości, nagłówek zostaje trwale związany z tożsamością bloku 0 i wszystkiego, co na nim zbudowano.

Komentarz nad funkcją genezy w demonie mówi wprost: „Genesi nie ma transakcji — ani coinbase, ani spalonych monet. Pierwszy rzeczywisty blok na wysokości 1 będzie miał pierwszy coinbase”. Walidacja potwierdza ten komentarz. Węzeł sprawdzający blok 0 odrzuca go, jeśli nie ma dokładnie zera transakcji i korzenia Merkle z samych zer, więc nawet zmodyfikowany węzeł nie mógłby przemycić monet do bloku genezy bez odrzucenia przez pozostałych.

## Jak konsensus wyklucza fundusz deweloperów lub skarbiec?

<Answer>
Każdy blok Blocknetu musi zaczynać się dokładnie jedną transakcją coinbase, a ta musi nie mieć wejść, mieć zerową opłatę i dokładnie jedno wyjście o kwocie równej nagrodzie za blok na danej wysokości. Nie ma miejsca na drugą płatność, więc fundusz deweloperów, nagroda dla założyciela ani wyjście skarbca nie mogą istnieć w łańcuchu.
</Answer>

Współdziała tu kilka kontroli demona:

- **Jeden coinbase na blok.** Pierwsza transakcja musi być coinbase, a żadna inna transakcja w bloku nie może nim być.
- **Jedno wyjście, brak wejść.** Coinbase z zerem lub dwoma wyjściami albo z wejściem jest nieprawidłowy.
- **Dokładna nagroda.** Blocknet ukrywa kwoty za pomocą zobowiązań Pedersena, ale coinbase jest szczególnym przypadkiem. Jego czynnik zaślepiający jest wyprowadzany z danych publicznych (klucza publicznego transakcji i wysokości bloku), więc każdy węzeł może odtworzyć zobowiązanie dla nagrody z harmonogramu i je porównać. Jeśli wyjście zobowiązuje do innej kwoty, blok zostaje odrzucony.
- **Puste memo.** Memo coinbase musi być puste, więc nagrody za bloki nie mogą też przenosić dodatkowych danych.

Zwykłe transakcje nie mogą tworzyć monet. Każda musi się bilansować: wejścia równają się wyjściom plus opłata, co jest sprawdzane na samych zobowiązaniach. Każde wyjście zawiera też dowód zakresu Bulletproofs wykazujący, że kwota mieści się w prawidłowym zakresie i nie może być ujemna. W ten sposób kwoty pozostają prywatne, a emisja nadal dokładnie przestrzega harmonogramu.

## Jak samodzielnie sprawdzić uczciwy start Blocknetu?

Nie musisz wierzyć nikomu na słowo. Łańcuch i kod są publiczne.

**W eksploratorze.** Otwórz [blok 0 w eksploratorze Blocknetu](https://explorer.blocknetcrypto.com/block/0). Zobaczysz 0 transakcji, korzeń Merkle z 64 zer, nonce 0 i poprzedni hash zaczynający się od `a94a7f2b`. Wiersz nagrody za blok pokazuje kwotę z harmonogramu dla tej wysokości, ale bez transakcji w bloku nie istnieje wyjście, które mogłoby ją otrzymać. Następnie otwórz [blok 1](https://explorer.blocknetcrypto.com/block/1): jego jedyną transakcją jest coinbase bez wejść i z jednym wyjściem — pierwsze monety w łańcuchu.

Możesz potwierdzić, że poprzedni hash rzeczywiście jest hashem wiadomości genezy. Mając OpenSSL 1.1.1 lub nowszy, uruchom:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Wynik kończy się na `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, czyli tej samej wartości, którą eksplorator pokazuje dla bloku 0.

**W kodzie.** Otwarty kod demona znajduje się na [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). W [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go) znajdź te funkcje:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` tworzy blok 0 z pustą listą transakcji, `validateGenesisBlock` odrzuca każdy blok 0 zawierający transakcje, a `validateCoinbaseConsensus` wymusza pojedyncze wyjście warte nagrodę za blok. W `transaction.go` funkcje `validateCoinbase` i `verifyCommitmentBalance` odpowiadają za strukturę coinbase i kontrolę bilansu zwykłych transakcji.

## Dlaczego uczciwy start ma znaczenie dla górników?

Dla górników uczciwy start oznacza, że nagroda za blok w pełni wyjaśnia pochodzenie monet.

- **Wszystkie nowe monety trafiają do górników.** Bez premine i wyjścia skarbca 100% emisji jest wypłacane osobom znajdującym bloki. Blok 1 wypłacił 723,25093035 BNT w swoim jedynym wyjściu coinbase, a każdy kolejny blok wypłacił całą nagrodę w ten sam sposób.
- **Wszystkich obowiązują te same reguły.** Od bloku 1 jedynym sposobem zdobycia nowych BNT jest wykonywanie proof of work. [Harmonogram nagród](/blog/pl/blocknet-block-reward-schedule/) jest zapisany w kodzie i dotyczy w równym stopniu pierwszego górnika oraz osoby zaczynającej dziś.
- **Proof of work pasuje do zwykłych komputerów.** Blocknet używa Argon2id z 2 GiB pamięci na hash, zaprojektowanego z myślą o odporności na ASIC. Dzięki temu kopanie pozostaje dostępne dla komputerów stacjonarnych, laptopów i serwerów, które ludzie już mają. [Wyjaśnienie Argon2id na seineminer.com](https://seineminer.com/pl/blog/argon2id-proof-of-work-explained/) opisuje dlaczego.
- **Emisję łatwo zweryfikować.** Pochodzenie każdej monety można prześledzić do wyjścia coinbase sprawdzonego przez każdy węzeł, więc łączna emisja na dowolnej wysokości wynika bezpośrednio z harmonogramu nagród.

Jeśli dopiero poznajesz tę kryptowalutę, [Czym jest Blocknet?](https://seineminer.com/pl/blog/what-is-blocknet/) opisuje prywatność, płatności nazwą i portfele.

## Jak samodzielnie kopać Blocknet?

Udział w uczciwym starcie oznacza kopanie. Najszybszym sposobem jest seine na bntpool.com:

1. **Zdobądź adres.** Zainstaluj oficjalny [portfel wiersza poleceń](https://github.com/blocknetprivacy/blocknet/releases/latest) lub [portfel desktopowy](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) Blocknetu i skopiuj adres albo użyj krótkiej nazwy Blocknet ID, takiej jak `@name`.
2. **Pobierz seine.** Pobierz archiwum dla Windows, macOS lub Linux z [seineminer.com](https://seineminer.com/pl/#download) i rozpakuj je. Niczego nie trzeba instalować.
3. **Uruchom program i zaakceptuj ustawienia domyślne.** Uruchom seine, wklej adres lub `@name` i naciśnij Enter, aby zaakceptować domyślną pulę `stratum+tcp://bntpool.com:3333`. seine od razu zaczyna obliczać hashe i podczas pracy dostraja wątki oraz jądra obliczeniowe na CPU, a także na kartach NVIDIA w Windows i Linux.
4. **Obserwuj kopanie.** Wyszukaj swój adres na [stronie statystyk](/stats), aby zobaczyć hashrate, udziały i saldo, oraz śledź odkrycia puli na [stronie bloków](/blocks).

[Poradnik kopania na bntpool.com](/blog/pl/mine-blocknet-on-bntpool/) opisuje logowanie adresem lub nazwą i kontrolowanie programu, [strona Get Started](/start) podaje ustawienia wypłat puli, a [poradnik kopania Blocknetu](https://seineminer.com/pl/blog/how-to-mine-blocknet/) prowadzi krok po kroku przez instalację seine na każdym systemie.

<CTA target="seine" title="Wykop swoją część" text="Każde BNT jest wykopane. Pobierz seine i zacznij kopać na bntpool.com w kilka minut." />
