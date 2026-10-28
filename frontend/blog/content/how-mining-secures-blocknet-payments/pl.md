---
sourceHash: 77522359a409
translation: draft
title: Jak kopanie zabezpiecza płatności Blocknet
description: Dowiedz się, jak Blocknet wybiera historię płatności przez proof of work, co dają potwierdzenia i co sprawdzają węzły przed zaakceptowaniem nowego bloku.
tags: [Blocknet, Bezpieczeństwo płatności, Proof of work]
faq:
  - q: Czy większa moc kopania może autoryzować płatność z mojego portfela?
    a: Moc kopania pomaga tworzyć konkurencyjne bloki. Wydawanie środków nadal wymaga prawidłowej autoryzacji, a węzły sprawdzają dowody i bilans transakcji przed akceptacją bloku. Cięższa gałąź również musi spełniać te zasady. Proof of work nie zastępuje kluczy prywatnych potrzebnych do wydania Twoich wyjść.
  - q: Czy Blocknet wybiera łańcuch z największą liczbą bloków?
    a: Wydany core v0.22.0 oblicza skumulowaną pracę, dodając trudność każdego bloku. Prawidłowa gałąź z większą pracą może zostać wybranym łańcuchem, zgodnie z zasadami reorganizacji. Sama liczba bloków nie wystarcza, bo mogą mieć różną trudność. Liczą się praca i poprawność.
  - q: Czy potwierdzenia płatności są tym samym co udziały puli?
    a: Udział zapisuje pracę wysłaną do puli. Potwierdzenie płatności dotyczy umieszczenia transakcji w wybranym łańcuchu oraz bloków zbudowanych później. Udziały pomagają dzielić nagrody, a bloki sieci rozwijają historię płatności. Zwiększenie liczby udziałów nie dodaje bezpośrednio potwierdzeń konkretnej płatności.
---

Prześledź płatność BNT od umieszczenia w bloku do historii wspieranej przez późniejszą pracę górników. Poradnik wyjaśnia, dlaczego sieć wybiera między konkurencyjnymi historiami, co mówią potwierdzenia i jak ten wybór współdziała z weryfikacją prywatnych transakcji. Opis dotyczy wydanego core v0.22.0, sprawdzonego 28 października 2026 roku.

## Co chroni kopanie, gdy wysyłasz BNT?

<Answer>
Kopanie pomaga sieci wybrać wspólną, uporządkowaną historię płatności. Każdy zaakceptowany blok łączy się z poprzednikiem i wnosi proof of work. Węzły sprawdzają proponowane bloki i transakcje, następnie porównują prawidłowe gałęzie według skumulowanej pracy i zasad reorganizacji. Kolejne bloki dodają pracę za uwzględnioną płatnością, nadając jej potwierdzeniom praktyczne znaczenie.
</Answer>

Prawidłowa płatność potrzebuje autoryzacji i miejsca w rejestrze. Portfel zapewnia autoryzację wydania środków. Kopanie oraz wybór łańcucha pomagają węzłom uzgodnić, które poprawne transakcje należą do bieżącej historii. Te zadania się uzupełniają: proof of work bloku nie sprawia, że nieprawidłowa płatność staje się dopuszczalna.

[Poradnik egzekwowania zasad](https://seineminer.com/pl/blog/who-controls-blocknet/) wyjaśnia, kto rozwija, proponuje i weryfikuje. Tutaj skupiamy się na płatności: jak jej uwzględnienie staje się częścią historii śledzonej przez portfel i inne węzły.

## Jak bloki wiążą historię płatności?

Nagłówek bloku zawiera hash poprzedniego bloku i korzeń Merkle’a wyprowadzony z transakcji. Korzeń kryptograficznie wiąże zawartość, a hash poprzednika łączy blok z wcześniejszą historią. Zmiana transakcji zmienia korzeń oraz nagłówek poddawany kopaniu. Bloki potomne muszą następnie odwoływać się do zmienionej historii przez własne połączenia.

Rozważ płatność w bloku, po którym zaakceptowano kilka kolejnych. Zastąpienie tego fragmentu historii wymaga konkurencyjnej gałęzi prawidłowych, połączonych bloków z dostateczną pracą, w granicach zasad reorganizacji węzła. Edycja transakcji w lokalnym pliku nie tworzy takiej gałęzi. Praca należy do konkretnej sekwencji nagłówków i transakcji.

[Opublikowany kod bloków](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) definiuje te pola, kontrole Merkle’a i ścieżki wyboru łańcucha. [Poradnik trudności](/blog/pl/mining-difficulty-explained/) wyjaśnia, jak sieć ustala pracę wymaganą dla nowego bloku.

## Dlaczego skumulowana praca jest ważniejsza od liczby bloków?

Dwaj górnicy mogą znaleźć różne prawidłowe bloki rozwijające tego samego rodzica. Każda propozycja ma własny hash, a węzły mogą otrzymać je w różnej kolejności. Może to chwilowo utworzyć konkurencyjne gałęzie. Sieć potrzebuje reguły śledzącej pracę wspierającą historię, a nie kolejność ogłoszeń konkretnego uczestnika.

Core v0.22.0 dodaje trudność bloku do skumulowanej pracy rodzica. Po weryfikacji gałąź o ściśle większej pracy może wywołać zmianę wybranego łańcucha. Alternatywa o równej pracy nie powoduje zmiany przez to porównanie. Sama trudność musi być prawidłowa w kontekście rodzica gałęzi; górnik nie może po prostu wpisać większej liczby do nagłówka.

Porównaj dwie gałęzie ze wspólnym początkiem. Wspólna część wnosi identyczną pracę do obu sum. Decyduje praca zgromadzona po rozdzieleniu wraz z kontrolami poprawności i reorganizacji. Liczenie bloków bez uwzględnienia trudności może pominąć tę różnicę. To porównanie pojęciowe, nie relacja z bieżącego rozwidlenia.

## Jak potwierdzenia pomagają śledzić płatność?

Po uwzględnieniu płatności w wybranym łańcuchu kolejne bloki budują na tej historii. Jej głębokość rośnie z nadejściem bloków. Zastąpienie uwzględnienia wymagałoby dopuszczalnej historii alternatywnej sprzed odpowiedniego bloku, podczas gdy bieżąca gałąź nadal gromadzi pracę. Dlatego uwzględnienie i późniejsza głębokość mówią więcej niż samo rozgłoszenie transakcji.

Wysokość wskazuje pozycję, a hash konkretny blok. Sprawdź zapis transakcji i blok, który ją zawiera, a następnie upewnij się, że nadal należy on do wybranego łańcucha. Zmiana wybranej historii może zmienić uwzględnienie i głębokość. Liczniki mogą wliczać blok z transakcją albo tylko późniejsze; sprawdź konwencję interfejsu.

Core v0.22.0 narzuca także granicę reorganizacji: odrzuca przełączenie, jeśli wspólny przodek leży poniżej obecnej wysokości końcówki minus 100. To reguła działającej implementacji, a nie uniwersalna obietnica związana z licznikiem. [Poradnik czasu przelewu](https://seineminer.com/pl/blog/blocknet-transfer-time/) omawia zwykłą możliwość wydawania z portfela, a [potwierdzenia nagród](/blog/pl/block-confirmations-mining-rewards/) — dojrzewanie coinbase i księgowość puli.

## Jak sprawdzić ponowne wydanie w prywatnej płatności?

Wydany format transakcji Blocknet używa obrazu klucza związanego z dowodem wydania. Weryfikacja sprawdza to powiązanie i odrzuca obraz już wydany we właściwym stanie łańcucha. Węzły wykrywają w ten sposób sprzeczne wykorzystanie wyjścia, podczas gdy podpis pierścieniowy ukrywa, który dopuszczalny członek pierścienia jest rzeczywistym wejściem. Prywatne kwoty nadal przechodzą kontrole bilansu i zakresu.

Załóżmy, że właściciel autoryzuje sprzeczne transakcje wydające to samo wyjście. Prawidłowa zaakceptowana historia nie może zawierać obu wydań. Wybór łańcucha ma znaczenie, bo konkurencyjne historie mogą uwzględniać różne wybory; weryfikacja ma znaczenie, bo każda musi przestrzegać zasad wydawania. Większa praca nie zastąpi podpisu ani nie przepuści niezbilansowanej transakcji przez kontrolę rachunkową.

Kontrole te znajdują się w [opublikowanym walidatorze transakcji](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). Szersze mechanizmy opisuje [poradnik prywatności Blocknet](https://seineminer.com/pl/blog/blocknet-privacy-features/). Bezpieczeństwo płatności łączy prawidłowe wydawanie, spójną historię i kryptografię chroniącą ujawniane przez nią informacje.

## Jak kopanie w puli wpływa na bezpieczeństwo płatności?

Pula koordynuje poszukiwanie bloku i przesyła rozwiązany kandydat do węzła nadrzędnego. Udział spełniający łatwiejszy cel puli zapisuje wkład pracy; tylko rozwiązanie spełniające cel sieci może po weryfikacji zostać blokiem sieciowym. [Wyjaśnienie puli](/blog/pl/what-is-a-mining-pool/) rozdziela te dwa cele i ich zadania.

Twój miner może więc wydłużać historię płatności, podczas gdy pula osobno rozlicza pracę. Prywatne saldo puli, liczba udziałów i potwierdzona płatność w portfelu oznaczają różne rzeczy. Zacznij od [poradnika połączenia z pulą](/blog/pl/mine-blocknet-on-bntpool/), aby dołączyć, a konkretną płatność BNT śledź w zapisie transakcji portfela.

<CTA target="pool" title="Wnieś pracę do Blocknet" text="Połącz seine z bntpool.com i śledź pracę, nagrody oraz płatności w publicznych narzędziach puli." />
