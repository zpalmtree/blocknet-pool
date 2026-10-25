---
title: Blocknet madencilik ödüllerinizi nasıl tahmin edersiniz?
description: Tek formülle donanımınızın günde ne kadar BNT üreteceğini hesaplayın. bntpool.com panelindeki girdileri ve tarihli hesaplama örneğini öğrenin.
tags: [Madencilik ödülleri, Madencilik rehberi, Hashrate, bntpool]
sourceHash: 28d46138019e
translation: draft
faq:
  - q: 'Blocknet madencilik ödülleri nasıl hesaplanır?'
    a: 'Hashrate''inizi ağ hashrate''ine bölün, Blocknet''in günlük blok sayısı olan 288 ile ve güncel blok ödülüyle çarpın. Sonuç günlük beklenen brüt BNT''dir. Ağ hashrate''ini bntpool.com panelinden, kendi hashrate''inizi seine veya My Stats''tan okuyun.'
  - q: 'Blocknet ağ hashrate''ini nerede görebilirim?'
    a: 'bntpool.com paneli Network Hashrate kartında, genellikle KH/s olarak gösterir; H/s için 1.000 ile çarpın. Aynı değer bntpool.com/api/stats adresindeki havuz API''sinin network_hashrate alanında H/s cinsinden bulunur. En son on blok üzerinden ölçülür ve her blokla güncellenir.'
  - q: 'Blocknet blok ödülü zamanla değişir mi?'
    a: 'Evet. Ödül 8.640 blokluk her 30 günlük ay içinde sabittir ve sonraki ayın başında azalır; 48. ayda kalıcı olarak blok başına 2,0 BNT''ye ulaşır. Güncel ödül için blok yüksekliğini 8.640''a bölüp aşağı yuvarlayın ve ödül takviminde o aya bakın.'
  - q: 'Havuzdaki günlük ödüllerim neden tahminden farklı?'
    a: 'Tahmin bir ortalamadır. Havuz herhangi bir gün beklenenden fazla veya az blok bulabilir; madenciler katıldıkça ağ hashrate''i değişir. Bir hafta veya daha uzun düzenli madencilikte günlük dalgalanmalar ortalamaya yaklaşır; size yazılan ödülleri My Stats sayfasından izleyebilirsiniz.'
  - q: 'Havuzda madencilik beklediğim BNT miktarını değiştirir mi?'
    a: 'Ağın ödüllerindeki beklenen payınız aynı kalır, çünkü yalnızca ağ hashrate''indeki payınıza bağlıdır. Havuzun değiştirdiği zamanlamadır. Tam bir blok için günlerce beklemek yerine bntpool.com''un bulduğu her bloktan pay alırsınız; ödüller küçük, düzenli tutarlarda gelir.'
---

Bu rehber donanımınızın günde ne kadar BNT üreteceğini tahmin etmeyi gösterir. Formülü, her girdinin [bntpool.com panelinde](/) nereden okunacağını ve havuzun canlı verileriyle seine'in en iyi ölçülmüş hashrate'lerinden oluşturulmuş tarihli bir örnekle hesabı öğreneceksiniz.

## Donanımınız günde ne kadar BNT üretir?

<Answer>
Günlük beklenen BNT, hashrate'inizin ağ hashrate'ine bölünüp günde 288 blokla ve güncel blok ödülüyle çarpılmasına eşittir. Kendi hashrate'iniz seine veya My Stats'tan, ağ hashrate'i bntpool.com panelinden, ödül ise güncel blok yüksekliği ve Blocknet'in ödül takviminden gelir.
</Answer>

Formül olarak:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Her parçanın basit bir nedeni vardır:

- **Günde 288 blok.** Blocknet beş dakikada bir blok hedefler; bir gündeki 86.400 saniyenin blok başına 300 saniyeye bölünmesi 288 eder. Zorluk her bloktan sonra ayarlanır; madenciler katıldıkça zincir bu tempoya yakın kalır.
- **Hashrate payınız blok payınızdır.** Blocknet'in zorluğu yaklaşık bir blok bulmak için gereken beklenen hash sayısıdır. Her hash aynı çözüm şansına sahip olduğundan ağ hash'lerinin %1'ini yapan makine zaman içinde blokların %1'ini bekleyebilir.
- **Blok ödülünü yükseklik belirler.** Aynı 8.640 blokluk 30 günlük ay içindeki her blok aynı ödülü öder; ay içindeki herhangi bir günde ödül bilinen sabittir.

Sonuç, zaman içinde ortalanmış beklenen brüt ödülünüzdür.

## Her girdiyi nereden bulursunuz?

### Sizin hashrate'iniz

seine başlar başlamaz terminal panelinde hashrate'inizi gösterir. Birkaç dakika tanıyın: seine gerçek madencilik işi üzerinde iş parçacığı sayısını ve CPU kernel'ini ayarlar; makineniz için en iyi ayarları bulunca değer oturur.

Havuzda madencilik yaparken [My Stats](/stats) üzerindeki **Hashrate** kartı, havuzun gördüğü hashrate'inizi son bir saatin ortalaması olarak gösterir. Tahmin için seine'in kararlı değerini veya havuzun 24 saatlik grafiğini kullanın.

### Ağ hashrate'i

Panelin **Network Hashrate** kartı tüm ağın hashrate'ini gösterir. KH/s olarak görüntülenir; 62,33 KH/s, 62.330 H/s demektir. Bölmeden önce H/s'ye çevirin, aksi hâlde sonuç bin kat hatalı olur.

Havuz bu değeri en son on bloğun işi ve zaman damgalarından ölçer; bloktan bloğa değişir. [Hashrate nedir?](/blog/tr/what-is-hashrate/) bu hesabı ve değişimin nedenini açıklar. Daha kararlı girdi için gün içinde birkaç kez bakıp ortalamasını kullanın. API'den H/s olarak da okuyabilirsiniz:

```sh
curl -s https://bntpool.com/api/stats
```

Yanıtın `chain` bölümü, H/s cinsinden `network_hashrate` ile havuzun üzerinde çalıştığı blok olan `current_job_height` değerlerini içerir.

### Blok ödülü

Panelin **Current Block** kartı ağın çalıştığı yüksekliği gösterir. Aya ulaşmak için 8.640'a bölüp aşağı yuvarlayın; sonra [Blocknet blok ödülü takviminde](/blog/tr/blocknet-block-reward-schedule/) o ayı bulun. [Bloklar sayfası](/blocks) hızlı çapraz kontroldür: Reward sütunu havuzun bulduğu her bloğun tam ödülünü gösterir.

## 25 Ekim 2026 tarihinden hesaplama örneği

<Note>
Bu örnek 25 Ekim 2026 saat 02:03 EDT'de https://bntpool.com/api/stats üzerinden alınan canlı değerlerin anlık görüntüsüdür. Ağ hashrate'i ve blok ödülü değişir; sonuca güvenmeden önce panelden bugünkü değerleri yerleştirin.
</Note>

| Girdi | Görüntü anındaki değer |
|---|---|
| Güncel blok | 69.020 (7. ay, çünkü 69.020 ÷ 8.640 = 7,99) |
| Blok ödülü | 467,67 BNT |
| Ağ hashrate'i | 62.329 H/s (62,33 KH/s gösterilir) |
| bntpool.com hashrate'i | 5.240 H/s |

seine testlerindeki 49,4 H/s hızında AMD EPYC Zen 5 sunucuyu ele alalım:

1. Ağ payı: 49,4 ÷ 62.329 = %0,0793.
2. Günlük beklenen blok: 0,000793 × 288 = 0,2283.
3. Günlük beklenen BNT: 0,2283 × 467,67 = yaklaşık 106,8 BNT.

8. ay görüntüden yaklaşık 100 blok sonra, 69.120 yüksekliğinde başlar ve blok başına 439,46 BNT öder. Aynı ağ hashrate'inde aynı sunucunun tahmini 0,2283 × 439,46 = günde yaklaşık 100,3 BNT olur. Bu nedenle her ay başında ödülü kontrol etmek faydalıdır.

Görüntüdeki ağ hashrate'iyle, seine'in her donanım neslinde ölçtüğü en iyi sonuç için aynı örnek hesap şöyledir:

| Donanım | İş parçacığı | Hashrate | Ağ payı | 467,67 ile BNT/gün | 439,46 ile BNT/gün |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | %0,0793 | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | %0,0573 | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | %0,0496 | 66,8 | 62,7 |
| Arm sunucu (AWS Graviton4) | 16 | 21,5 H/s | %0,0345 | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | %0,0210 | 28,3 | 26,6 |

Önemli olan kendi makinenizin değeridir. seine'i bir süre çalıştırın, kararlı hashrate'ini alın ve tablonun yerine kullanın. [Blocknet madencilik donanımı ve hashrate](https://seineminer.com/tr/blog/blocknet-mining-hardware-hashrate/) rehberi bellek bant genişliği ve RAM'in bu değerleri neden belirlediğini açıklar; tam sonuçlar seine'in [performans sayfasındadır](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md).

## bntpool.com'da madencilik tahmini nasıl değiştirir?

Havuzda beklenen payınız aynıdır. Değişen ödülün size ulaşma biçimidir. bntpool.com PPLNS ile öder: her blok ödülü, önceki altı saatte madencilerin gönderdiği share'ler arasında zorluklarıyla orantılı paylaşılır. [PPLNS ödemeleri nasıl çalışır?](/blog/tr/how-pplns-payouts-work/) paylaşımı anlatır.

Aynı tahmini havuz tarafından da yapabilir ve aynı yanıtı alabilirsiniz. Görüntü anında havuzun 5.240 H/s hızı ağın %8,4'üydü; günde 288 × 5.240 ÷ 62.329 = yaklaşık 24,2 blok, kabaca saatte bir blok bekleyebilirdi. Panel bunu Round Progress bölümünde **Expected Block Time** olarak gösterir. 49,4 H/s sunucu havuz hashrate'inin %0,94'üdür; 467,67 BNT değerinde 24,2 bloğun %0,94'ü yine günde yaklaşık 106,8 BNT'dir. Havuzun hashrate'i hesapta sadeleşir.

Fark zamanlamadır. Tek başına madencilikte o Zen 5 sunucu yaklaşık 4,4 günde bir tam blok beklerdi; [Blocknet için havuz ve solo madencilik](/blog/tr/pool-vs-solo-mining-blocknet/) ikisini ayrıntılı karşılaştırır. bntpool.com'da havuzun günde bulduğu yaklaşık 24 bloğun her birinden pay alır; bakiyesi gün boyu küçük adımlarla büyür. Havuz ücreti ve seine'in geliştirici ücreti, formülün verdiği brüt tutardan düşülür.

İlk gününüzü iki havuz ayrıntısı şekillendirir:

- **Pencere altı saatte dolar.** Yeni madencinin her bloktaki payı, share'leri altı saatlik PPLNS penceresini doldurdukça büyür; madenciliği sürdürdüğü sürece sonra tam oranda kalır.
- **Ödüller ödemeden önce olgunlaşır.** Her blok ödülü 60 onaydan, yaklaşık beş saatten sonra Confirmed Rewards'a geçer; havuz bakiyenizi 0,1 BNT'ye ulaşınca otomatik gönderir.

## Tahmini gerçek sonuçlarla nasıl karşılaştırırsınız?

Tahmin ortalamadır; havuzun şansı turdan tura değiştikçe tek günlerin sonuçları çevresinde dalgalanır. Çok sayıda turda efor ortalaması %100 civarına gelir; bir haftalık düzenli madencilik iyi karşılaştırma dönemidir. Haftanın başında ve sonunda [My Stats](/stats) üzerindeki Confirmed Rewards ile Paid Balance toplamını not edin. Artış, o hafta olgunlaşan bloklardan havuzun size yazdığı tutardır. Aynı günlerin ödül ve ağ hashrate'ini kullanarak yaptığınız tahminle karşılaştırın.

Girdilerden biri değiştiğinde formülü yeniden hesaplayın:

- **Her yeni ayda**, ödül azaldığında. Ay sınırları her 8.640 blokta, yaklaşık 30 günde birdir.
- **Ağ hashrate'i değiştiğinde**, madenciler ağa katıldıkça.
- **Sizin hashrate'iniz değiştiğinde**, yeni makine ekledikten, Linux'ta huge pages ayırdıktan veya seine'in CPU yanında kullandığı NVIDIA GPU ekledikten sonra.

<Note>
Bu rehberdeki hashrate değerleri Blocknet'in mevcut Argon2id iş kanıtını anlatır. [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) sitesine göre Blocknet, 16 Şubat 2027 civarında Faraday adlı ağ yükseltmesini planlıyor; iş kanıtını değiştirmesi planlanıyor. seine'i güncel tutun: başlangıçta ve her 12 saatte bir yeni sürümleri kontrol eder.
</Note>

<CTA target="seine" title="Kendi hashrate'inizi ölçün" text="seine'i indirin, birkaç dakika kendini ayarlamasına izin verin ve hashrate'ini bugünkü panel değerleriyle formülde kullanın." />
