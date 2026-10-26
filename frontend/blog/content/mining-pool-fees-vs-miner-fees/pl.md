---
title: 'Opłaty puli a opłaty dewelopera minera'
description: 'Zrozum opłaty puli i minera Blocknet: gdzie obowiązują, jak je łączyć i jak uniknąć dwukrotnego odjęcia tej samej opłaty.'
tags: ['Poradnik kopania', 'Opłaty puli', 'Nagrody']
faq:
  - q: 'Czy to ta sama opłata?'
    a: 'Nie. Opłata puli dotyczy podziału nagród, opłata dewelopera należy do oprogramowania. seine v0.2.36 przeznacza 2% naliczanego czasu kopania na rozwój po okresie początkowym. Osobna opłata bntpool wynosiła 0% przy kontroli 26 października 2026.'
  - q: 'Jak je połączyć?'
    a: 'Pomnóż szacunek brutto przez jeden minus ułamek minera, następnie jeden minus ułamek puli. Użyj rzeczywistej wersji i aktualnych ustawień. To szacunek zachowanych nagród, nie gwarancja wypłaty ani obliczenie zysku pieniężnego.'
  - q: 'Czy odjąć opłatę minera od hashrate puli?'
    a: 'Najpierw określ znaczenie liczby. Jeśli już szacuje pracę dla twojego adresu po czasie dewelopera, kolejne odjęcie podwaja korektę. Kalkulator pozwala wpisać zero dla korekty już uwzględnionej. Nie odejmuj opłat ponownie od potwierdzonego salda lub płatności.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Prześledź dwa poziomy opłat w szacunku Blocknet. Poradnik używa seine v0.2.36 i ustawień bntpool sprawdzonych 26 października 2026, aby pomóc w [kalkulatorze](/blog/pl/estimate-blocknet-mining-rewards/#mining-calculator) i [My Stats](/stats).

## Czym różnią się opłaty puli i minera?

<Answer>
Opłata puli jest częścią podziału nagród, a dewelopera — harmonogramu kopania programu. Różnią się właścicielem i etapem. Sprawdź obie, ustal, czy wejście jest brutto czy już skorygowane, i zastosuj każdą właściwą korektę raz.
</Answer>

*Praca przechodzi przez korektę czasu minera oraz osobną korektę nagrody puli przed oszacowaniem kwoty zachowanej na koncie.*

## Do czego stosuje się opłatę puli?

Pula zbiera kwalifikującą się pracę i dzieli nagrodę według zasad księgowania. bntpool oblicza opłatę od zapisanej nagrody bloku przed przydzieleniem reszty przez PPLNS. [Poradnik PPLNS](/blog/pl/how-pplns-payouts-work/) opisuje udział i uznania.

26 października 2026 `https://bntpool.com/api/info` publikuje `pool_fee_pct` równe 0,0, czyli 0%. Przy aktualizacji sprawdzaj [Get Started](/start) lub API. Zero opisuje to ustawienie puli; polityka programu pozostaje osobnym wejściem.

## Jak działa opłata dewelopera seine?

v0.2.36 dokumentuje 2% na każdej puli i solo. Jest to czas kopania dla adresu dewelopera, nie późniejsza wypłata z portfela. Pierwsze 10 minut jest bez opłaty; śledzony jest czas późniejszy. Zobacz [konfigurację wydania](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md).

Harmonogram działa rundami, więc krótki okres nie musi dawać dokładnie 2%. To cel długoterminowy po okresie początkowym, nie obietnica na minutę czy blok. Zapisz wersję: starsza dokumentacja lub inny program mogą się różnić. Opłata puli nie zastępuje tej informacji.

## Jak łączyć procenty?

Użyj zachowanych ułamków. Dla ułamka minera m i puli p pozostaje `(1 - m) * (1 - p)` kwoty brutto. Tak działa [estymator](/blog/pl/estimate-blocknet-mining-rewards/). Dodawanie procentów nie jest ogólną regułą, bo druga korekta dotyczy pozostałej kwoty.

Przykład wyłącznie arytmetyczny: hipotetyczne 100 BNT brutto, miner 2% i sprawdzona pula 0% dają 100 × 0,98 × 1,00 = 98 BNT. To stan ustalony, nie pomiar sprzętu, obietnica ani symulacja krótkiej sesji z okresem początkowym.

## Wybierz właściwy hashrate

Wynik lokalny lub benchmark opisuje pracę maszyny. Przed korektą ustal, czy obejmuje cały czas, czy tylko pracę dla twojego adresu. Sprawdź źródło i okno. [Poradnik hashrate](/blog/pl/what-is-hashrate/) odróżnia pomiary lokalne, puli i sieci.

Hashrate puli dla adresu może już uwzględniać czas pracy przypisanej gdzie indziej. Wtedy wpisz zero dla dodatkowej opłaty minera w kalkulatorze. Zmienia to rachunek, nie politykę programu. Opłata puli nadal dotyczy szacowania kwalifikujących się nagród brutto; sprawdź zawartość liczb.

## Dlaczego nie odejmować opłat ponownie od salda?

Potwierdzone saldo jest wynikiem księgowania, nie nienaruszonym szacunkiem brutto. Pula już przypisała uznania. Kolejne odjęcie od salda czy płatności zmienia liczbę bez nowego naliczenia. Uzgadniaj konto na podstawie wyświetlonej kwoty i jej etapu.

Oczekująca nagroda, potwierdzone saldo i wysłana płatność różnią się też czasem. Dojrzałość bloku i próg wypłaty wyjaśniają przejścia, nie dodatkowe procenty. Zobacz [potwierdzenia](/blog/pl/block-confirmations-mining-rewards/) i [historię](/payouts). Ewentualna opłata transakcji portfela jest osobnym polem.

## Zachowaj źródła szacunku

Zapisz datę, wersję, opłaty, źródło hashrate i okres. Kalkulator łączy aktualne dane puli/sieci z twoim wejściem; odśwież sieć przy powrocie. Rzeczywiste uznania zależą też od udziałów, bloków i PPLNS. Wynik wzoru i późniejszy zapis odpowiadają na różne pytania.

Używaj [My Stats](/stats) i [poradnika śledzenia](/blog/pl/track-your-miner-on-bntpool/). Porównuj ten sam adres, okres i etap przed zmianą opłat. Kalkulator szacuje BNT; energia, sprzęt i cena pieniężna są poza nim. Nie trzeba zmieniać adresu portfela.

<CTA target="pool" title="Śledź nagrody na bntpool.com" text="Używaj kalkulatora do szacunków, a My Stats do zapisanej pracy, sald i płatności." />
