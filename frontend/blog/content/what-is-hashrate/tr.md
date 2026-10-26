---
title: Kripto madenciliğinde hashrate nedir?
description: Hashrate, madencinin saniyede hesapladığı hash sayısıdır. Birimleri, sizin, havuzun ve ağın hashrate farklarını ve neden değiştiklerini öğrenin.
tags: [Hashrate, Madencilik temelleri, Blocknet, bntpool]
sourceHash: 78ea554d4001
translation: draft
faq:
  - q: 'Bir bilgisayar Blocknet madenciliğinde ne kadar hashrate sağlar?'
    a: 'seine ölçümlerinde her donanım neslinin en iyi sonucu AMD EPYC Zen 5 sunucu için 49,4 H/s, AMD EPYC Zen 4 için 35,7 H/s, Apple M4 Max dizüstü için 30,9 H/s ve AWS Graviton4 Arm sunucu için 21,5 H/s''dir. Kendi sonucunuz çoğunlukla bellek bant genişliğine ve RAM''in kaç 2 GB iş parçacığı barındırdığına bağlıdır.'
  - q: 'Blocknet hashrate''i neden MH/s veya TH/s yerine H/s ile ölçülür?'
    a: 'Her Blocknet hash''i yaklaşık 2 GiB belleği yazıp geri okuyan bir Argon2id hesabıdır; büyük sunucu bile saniyede yalnızca onlarca hash tamamlar. SHA-256 gibi hızlı hash''lere dayalı iş kanıtları çok daha büyük sayılara ulaşır, bu yüzden büyük birimler kullanırlar. Hashrate''leri yalnızca aynı algoritma içinde karşılaştırın.'
  - q: 'Havuz neden madencimden farklı hashrate gösteriyor?'
    a: 'Madenciniz tamamladığı her hash''i sayar. Havuz yalnızca gönderdiğiniz share''leri görür ve hashrate''inizi son bir saatteki zorluklarından tahmin eder. Share''ler rastgele anlarda gelir; havuz değeri daha çok dalgalanır ve zamanla oturur. Adil karşılaştırma için My Stats''taki 24 saatlik grafikle madencinizin ortalamasını karşılaştırın.'
  - q: 'Daha yüksek hashrate daha fazla madencilik ödülü demek mi?'
    a: 'Evet, orantılı olarak. Her hash aynı blok bulma şansına sahiptir; ağ bloklarındaki beklenen payınız ağ hashrate''indeki payınıza eşittir. Hashrate''inizi ikiye katlamak beklenen ödülleri ikiye katlar; ağ hashrate''inin yükselmesi payınızı küçültür. bntpool.com''da bu pay, havuzun bulduğu her bloktan bir parça olarak gelir.'
---

Hashrate her madencinin, havuzun ve blok gezgininin ilk gösterdiği sayıdır. Bu rehber Blocknet ve bntpool.com panelini örnek alarak neyi ölçtüğünü, H/s'den yukarı birimlerin nasıl okunduğunu, kendi hashrate'inizin havuz ve ağla ilişkisini ve her değerin dakikadan dakikaya neden değiştiğini açıklar.

## Hashrate nedir?

<Answer>
Hashrate, madencinin saniyede kaç hash hesapladığıdır. İş kanıtı madenciliğinde her hash bir blok çözümü denemesidir ve her denemenin aynı küçük kazanma şansı vardır. Yüksek hashrate saniyede daha fazla deneme demektir; beklenen blok payınız toplam hashrate'teki payınızla orantılı büyür.
</Answer>

İş kanıtı zinciri madencilerden hash'i bir hedef değerin altına düşen blok başlığı bulmalarını ister. Kestirme yol yoktur: madenci başlıktaki nonce adlı sayıyı değiştirir, sonucu hash'ler, hedefle karşılaştırır ve tekrarlar. Hashrate makinenin saniyede bu denemelerden kaçını tamamladığını sayar.

Blocknet'te bir deneme bir Argon2id hash'idir. Nonce parola olarak, başlığın geri kalanı tuz olarak girer; fonksiyon 32 baytlık yanıtını üretmeden önce 2 GiB belleği işler. seine ve havuz hashrate'i tamamlanan hash/saniye olarak bildirir; 1 H/s saniyede bir tam Argon2id hesabıdır.

## H/s, kH/s ve MH/s ne demek?

Hashrate birimleri standart metrik ön ekleri izler; her basamak öncekinden bin kat büyüktür:

| Birim | Saniyedeki hash | Tipik kullanım |
|---|---|---|
| H/s | 1 | Blocknet'te tek bilgisayar |
| kH/s (KH/s) | 1.000 | Blocknet havuz ve ağ toplamları |
| MH/s | 1.000.000 | Daha hızlı iş kanıtı algoritmaları |
| GH/s | 1.000.000.000 | SHA-256 gibi hızlı hash'ler |
| TH/s ve üzeri | 1.000.000.000.000 ve üzeri | Hızlı hash'lerin ağ toplamları |

bntpool.com paneli binlik birimi KH/s olarak yazar ve değer 1.000'i her geçtiğinde üst birime geçer; 44,44 KH/s, 44.440 H/s demektir. Bir hashrate'i diğerine bölerken önce ikisini de H/s'ye çevirin.

## Blocknet hashrate değerleri neden küçük?

Blocknet'in iş kanıtı bellek yoğundur. Her hash yaklaşık 2 GiB belleği yazar ve yaklaşık 2 GiB'ı geri okur; bu, herhangi bir işlemci önbelleğinden çok fazladır. Dolayısıyla tempoyu bellek sisteminin hızı belirler. Her madencilik iş parçacığının kendi 2 GiB'ı gerekir; bu yüzden makinenin kaç iş parçacığı çalıştırabileceğini RAM belirler.

seine'in her donanım neslinde ölçtüğü en iyi sonuçlar şöyledir:

| Donanım | İş parçacığı | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Arm sunucu (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

32 iş parçacıklı Zen 5 sunucu bile saniyede iş parçacığı başına yaklaşık 1,5 hash tamamlar. Hızlı hash'lere dayalı algoritmalar çok daha büyük sayılar üretir: Bitcoin wiki'si, çift SHA-256 çalıştıran Bitcoin madencilerinin 2014'te bile saniyede milyarlarca hash ile derecelendirildiğini belirtir. İki sayı hash başına farklı iş miktarlarını ölçer; hashrate ancak aynı algoritmadaki diğer değerlerle karşılaştırılınca anlamlıdır. [Argon2id iş kanıtı açıklaması](https://seineminer.com/tr/blog/argon2id-proof-of-work-explained/) algoritmayı ayrıntılı anlatır; [Blocknet madencilik donanımı rehberi](https://seineminer.com/tr/blog/blocknet-mining-hardware-hashrate/) bellek bant genişliği ve RAM'in sonuçları nasıl belirlediğini gösterir.

## Sizin, havuzun ve ağın hashrate'i

[Havuzda](/blog/tr/what-is-a-mining-pool/) madencilik yapan herkes için üç hashrate önemlidir; her biri farklı ölçülür.

### Sizin hashrate'iniz

seine makinenizin tamamladığı her hash'i sayar ve panelinde iki değer gösterir: **Current**, şu anki hızınız; **Average**, oturum ortalaması. Makinenin gücünü kararlı ölçmek için seine gerçek madencilik işi üzerinde iş parçacığı sayısını ve CPU kernel'ini ayarlamak üzere birkaç dakika çalıştıktan sonraki ortalamayı kullanın.

Havuz hash'lerinizi göremez. Gönderdiğiniz share'leri, yani bağlantınıza belirlenen daha kolay hedefi geçen hash'leri görür. bntpool.com hashrate'inizi son bir saatte kabul edilen share'lerin toplam zorluğundan tahmin eder ve [My Stats](/stats) üzerinde gösterir. [bntpool.com'da madencinizi takip edin](/blog/tr/track-your-miner-on-bntpool/) bu sayfayı anlatır.

### Havuz hashrate'i

Paneldeki **Pool Hashrate** kartı aynı bir saatlik pencerede bağlı tüm madencilerin share işini toplar. bntpool.com'un ağ işinin ne kadarını yaptığını ve dolayısıyla ne sıklıkta blok bulmayı bekleyebileceğini söyler; panel bunu **Expected Block Time** olarak gösterir.

### Ağ hashrate'i

Ağdaki her hash'i kimse sayamaz; ağ hashrate'i bloklardan hesaplanır. Zorluk, bir blok bulmak için gereken beklenen hash sayısıdır; Blocknet 300 saniyede bir blok hedefler.

## Ağ hashrate'i nasıl hesaplanır?

<Answer>
Ağ hashrate'i zorluk ve blok sürelerinden tahmin edilir. Blocknet'te zorluk yaklaşık blok başına beklenen hash sayısıdır; hedef süre 300 saniye olduğundan ağ hashrate'i yaklaşık zorluk bölü 300'dür. bntpool.com paneli bunu en son on bloğun gerçek işi ve zaman damgalarından hesaplar.
</Answer>

25 Ekim 2026 saat 02:26 EDT'de okunan canlı değerlerle hesap şöyledir. [Blocknet gezgini](https://explorer.blocknetcrypto.com) sonraki zorluğu 16.943.147 gösteriyordu; ağ blok başına yaklaşık 16,9 milyon hash gerektirecek şekilde ayarlanmıştı. 300 saniyeye bölününce yaklaşık 56.477 H/s eder.

Aynı anda bntpool.com paneli ağ hashrate'ini 44,44 KH/s, yani 44.440 H/s gösteriyordu. İkisi aynı şeyin farklı açılardan tahminidir. Zorluk değeri, blokları tam beş dakikalık hedefte üretecek hızdır. Panel değeri son on bloğun gerçek işini gerçekten geçen süreye böler; yakın bloklar beş dakikadan uzun sürdüğünde daha düşük çıkar. Blocknet zorluğu her bloktan sonra ayarlar; bu, iki değeri zaman içinde yakın tutar.

## Hashrate neden dalgalanır?

Üç değerin her biri kendi nedenleriyle değişir.

- **Makineniz.** İlk dakikalarda seine madencilik sırasında iş parçacığı sayılarını ve kernel'leri karşılaştırıp en hızlısında karar kılar; ayarladıkça hız yükselir. Sonrasında bilgisayarın belleğini veya çekirdeklerini kullanan diğer işler hash süresinden pay alabilir.
- **Havuzun sizi görüşü.** Share'ler rastgele olaylardır. bntpool.com her bağlantının share zorluğunu beş dakikada yaklaşık 10 share hedefleyecek şekilde ayarlar; bir saatte yaklaşık 120 share olur. Şanslı veya sakin dönem tahmini değiştirir. Yeni bağlanan rig'in ortalaması, tam saat dolana kadar madencilik yaptığı dakikalar üzerinden alınır.
- **Ağ.** Blok süreleri de rastgeledir ve on blokluk örnek kısadır. 25 Ekim 2026 tarihinde panelin ağ hashrate'i 00:26 EDT'de yaklaşık 50.057 H/s, 02:03'te 62.329 H/s ve 02:26'da 44.440 H/s idi. Katılan ve ayrılan madenciler bu gürültünün üzerine gerçek değişimler ekler.

Kararlar için ortalamaları kullanın: seine'in oturum ortalaması, My Stats'taki 24 saatlik grafik ve gün içinde birkaç kez kontrol edilen ağ değeri.

## Hashrate'iniz ödüller açısından ne demek?

Her hash aynı şansa sahip olduğu için ağ hashrate'indeki payınız beklenen blok payınızdır. Blocknet günde 288 blok üretir; ağın %0,1'ine sahip madenci zaman içinde günde yaklaşık 0,29 blok değerinde ödül bekleyebilir. [Blocknet madencilik ödüllerinizi nasıl tahmin edersiniz?](/blog/tr/estimate-blocknet-mining-rewards/) bunu [panelden](/) alınan girdilerle günlük BNT'ye çevirir.

bntpool.com'da beklenen payınız, kabul edilen share'lerinize göre havuzun bulduğu her bloktan bir pay olarak ulaşır; ödüller gün boyu küçük adımlarla gelir. Henüz başlamadıysanız [seine kurulum rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) indirmeden ilk share'lere birkaç dakikada ulaştırır.

<Note>
Bu rehberdeki hashrate değerleri Blocknet'in mevcut Argon2id iş kanıtını anlatır. [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) sitesine göre Blocknet, 16 Şubat 2027 civarında Faraday adlı ağ yükseltmesini planlıyor; iş kanıtını değiştirmesi planlanıyor. seine'i güncel tutun: başlangıçta ve her 12 saatte bir yeni sürümleri kontrol eder.
</Note>

<CTA target="seine" title="Kendi hashrate'inizi görün" text="seine'i indirin, birkaç dakika kendini ayarlamasına izin verin ve hashrate'inizi panelinde ve My Stats'ta izleyin." />
