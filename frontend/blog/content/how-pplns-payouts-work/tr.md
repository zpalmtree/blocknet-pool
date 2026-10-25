---
title: bntpool.com'da PPLNS ödemeleri nasıl çalışır?
description: bntpool.com Blocknet madencilerine altı saatlik share penceresiyle PPLNS öder. Ödül paylaşımını, 60 onayı ve 0,1 BNT ödeme alt sınırını öğrenin.
tags: [Ödemeler, PPLNS, bntpool, Madencilik rehberi]
sourceHash: bd204b6650c5
translation: draft
faq:
  - q: 'bntpool.com''da PPLNS penceresi ne kadar uzun?'
    a: 'Pencere altı saattir. Havuz bir blok bulduğunda o bloktan önceki altı saate bakar ve bu sürede share gönderen herkes arasında ödülü share zorluklarıyla orantılı paylaşır. Güncel değer havuzun Get Started sayfasında listelenir.'
  - q: 'bntpool.com''da en düşük ödeme tutarı nedir?'
    a: 'En düşük ödeme 0,1 BNT''dir. Onaylanmış bakiyeniz bu tutara ulaştığında havuz, talep veya alma işlemi gerektirmeden Blocknet adresinize otomatik gönderir. Daha küçük bakiye hesabınızda kalır ve share katkınız olan her onaylanmış blokla eşiği geçene kadar büyür.'
  - q: 'Estimated Rewards neden Confirmed Rewards değerinden farklı?'
    a: 'Estimated Rewards, havuzun bulduğu ve hâlâ 60 onayını toplayan bloklardaki payınızdır; bu yüzden tutar değişebilir. Confirmed Rewards, zincirde 60 blok derine ulaşmış bloklardan gelir ve ödenmeyi bekler. Her blok olgunlaştığında tutarı ilk karttan ikinciye geçer.'
  - q: 'Madenciliği durdurduğum anda ödüllerim kesilir mi?'
    a: 'Hayır. Her share gönderilmesinden sonra altı saat PPLNS penceresinde kalır; havuzun bu sürede bulduğu bloklar yine size pay kazandırır. Önceden onaylanmış ödüller bakiyenizde kalır ve havuz, bakiye 0,1 BNT alt sınırına ulaşınca ödeme yapar.'
  - q: 'Şans sayfasında %100 üzerindeki efor ne anlama gelir?'
    a: 'Efor, havuzun bir turda yaptığı işi, beklenen iş miktarı olan blok zorluğuyla karşılaştırır. %100 üzeri turun ortalamadan uzun, %100 altı daha kısa sürdüğünü gösterir. İkisi de normaldir ve çok sayıda turda efor ortalaması %100 civarına gelir.'
---

Bu rehber, bntpool.com'un madencinizin gönderdiği share'leri Blocknet ödemelerine nasıl dönüştürdüğünü açıklar. Sonunda bir blok ödülünün nasıl bölündüğünü, ödenebilmek için neden yaklaşık beş saat beklediğini, bakiyenizin ne zaman gönderildiğini ve [istatistikler sayfasındaki](/stats) bakiye kartlarıyla [şans sayfasını](/luck) nasıl okuyacağınızı öğreneceksiniz.

Havuz madenciliğinde yeniyseniz [madencilik havuzu nedir?](/blog/tr/what-is-a-mining-pool/) temelleri anlatır. Henüz başlamadıysanız [seine madencilik rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) indirmeden ilk share'lere birkaç dakikada ulaştırır.

## PPLNS nedir?

<Answer>
PPLNS, “pay per last N shares”, yani son N share için ödeme anlamına gelir. bntpool.com bir blok bulduğunda ödülü, o bloktan önceki altı saatte gönderilen share'ler arasında her share'in zorluğuyla orantılı paylaşır. Pencere içinde ne kadar düzenli iş yaparsanız her bloktaki payınız o kadar büyük olur.
</Answer>

bntpool.com'da “son N” bir süredir: altı saat. Havuzun [Get Started sayfası](/start) ödeme yöntemini, pencereyi, en düşük ödemeyi ve onay sayısını gösterir. Aynı değerler `https://bntpool.com/api/info` adresinde yayımlanır.

| Ayar | bntpool.com değeri |
|---|---|
| Ödeme yöntemi | PPLNS |
| Share penceresi | 6 saat |
| Ödeme öncesi onaylar | 60 blok |
| En düşük ödeme | 0,1 BNT |

Share'ler zorluğa göre sayılır. Havuz her madenci bağlantısına ayrı share zorluğu verir ve her madencinin düzenli share akışı göndermesi için ayarlar. Daha hızlı makine daha zor share'ler alır; her share'i daha fazla ağırlık taşır. Payınızı belirleyen toplam yaptığınız iştir; bu, dizüstü ve sunucu için eşit derecede adil bir ölçüdür.

## bntpool.com blok ödülünü nasıl böler?

Havuz blok bulduğunda ödül her seferinde aynı aşamalardan geçer:

1. **Blok kaydedilir.** [Bloklar sayfasında](/blocks) bekleyen durumuyla görünür.
2. **Pencere ölçülür.** Havuz blokta biten altı saatlik süreyi alır ve o sürede her adresten kabul edilen share'lerin zorluklarını toplar.
3. **Ödül bölünür.** Her adres, pencerenin toplam işindeki payıyla aynı oranda ödül alır.
4. **Blok olgunlaşır.** Payınız blok 60 blok derine ulaşana kadar tahmin olarak görünür, sonra onaylanmış bakiyeye geçer.
5. **Bakiye ödenir.** Onaylanmış bakiyeniz 0,1 BNT'ye ulaştığında havuz adresinize gönderir.

## Hesaplama örneği

<Note>
Bu örnek hesabı göstermek için uydurma yuvarlak sayılar kullanır. Gerçek toplamlar havuzun hashrate'ine, sizin hashrate'inize ve o andaki [blok ödülüne](/blog/tr/blocknet-block-reward-schedule/) bağlıdır.
</Note>

Bloktan önceki altı saatte havuzdaki tüm madencilerin toplam 1.000.000 zorlukta share gönderdiğini, sizin share'lerinizin ise 20.000 ettiğini varsayalım. Penceredeki payınız 20.000 ÷ 1.000.000 = %2 olur. Havuz o blok için 400 BNT dağıtırsa size 400 × 0,02 = 8 BNT yazılır.

Şimdi sizinle tam aynı hashrate'e sahip, bloktan 30 dakika önce bağlanan ikinci bir madenci düşünün. Otuz dakika altı saatin on ikide biridir; dolayısıyla pencerede yaklaşık 1.667 zorluğu vardır ve bu bloktan yaklaşık 0,67 BNT alır. Ancak işi sayılmaya devam eder: bu share'lerin her biri sonraki altı saat boyunca pencerede kalır ve havuzun bu sürede bulduğu her bloktan pay kazanır.

| Madenci (örnek) | Penceredeki iş | Pay | 400 BNT ödemeden kazanç |
|---|---|---|---|
| Siz, altı saatin tamamında çalışan | 20.000 | %2 | 8 BNT |
| Aynı hashrate, bloktan 30 dakika önce katılan | 1.667 | yaklaşık %0,17 | yaklaşık 0,67 BNT |
| Küçük madenci, altı saatin tamamında çalışan | 100 | %0,01 | 0,04 BNT |

Son satırdaki küçük madenci blok başına 0,04 BNT kazanır; onaylanmış bakiyesi bu tür üç bloktan sonra 0,1 BNT alt sınırını aşar ve ödenir.

## PPLNS neden düzenli çalışan madencileri ödüllendirir?

Gönderdiğiniz her share, havuzun sonraki altı saatte bulduğu her blok için sayılır. Kesintisiz çalışan madencinin arkasında her zaman tam bir iş penceresi vardır; her yeni blokta tam hızına karşılık gelen payı alır. Geliri bloktan bloğa hashrate'ini yakından izler.

Bu, PPLNS'yi kesintilere karşı da esnek kılar. Yeniden başlatma, sürücü güncellemesi veya kısa ağ kesintisi konumunuzu sıfırlamaz; çünkü gönderdiğiniz share'ler altı saat pencerede kalır. Blok bulunduktan sonra eklediğiniz iş sonraki bloklara sayılır; ek hashrate her zaman kullanılır.

En düzenli sonuçlar için madencinizi çalışır bırakın ve kendini ayarlamasına izin verin. seine, havuza yeniden bağlanmadan madencilik sırasında iş parçacığı sayısını ve CPU kernel'ini ayarlamayı sürdürür; share akışınız kesintisiz kalır. Windows, macOS veya Linux için [seine'i indirebilirsiniz](https://seineminer.com/tr/#download).

## Blok ödülü neden 60 onay bekler?

Blocknet yeni üretilen coin'lerin harcanmadan önce 60 blok beklemesini gerektirir. Daemon kuralları buna coinbase olgunlaşması der; Blocknet'in beş dakikalık blok süresinde yaklaşık beş saattir (60 × 5 dakika = 300 dakika). bntpool.com da ödülü onaylanmış saymadan önce aynı 60 bloğu bekler; böylece her ödeme havuzun gerçekten gönderebileceği coin'lerden gelir.

Blok olgunlaşırken havuz hâlâ ana zincirin parçası olduğunu kontrol eder. 60 blok derine ulaştığında blok onaylanmış işaretlenir ve ödülü her madencinin onaylanmış bakiyesine eklenir. Ağın o yükseklikte farklı bir blokta uzlaştığı nadir durumda blok yetim işaretlenir ve tahmini ödülleri kaldırılır.

## bntpool.com ödemeleri ne zaman gelir?

<Answer>
Blok 60 onay aldıktan sonra, Blocknet'in beş dakikalık blok süresinde yaklaşık beş saat içinde, payınız istatistikler sayfasındaki Confirmed Rewards'a geçer. Onaylanmış bakiyeniz 0,1 BNT'ye ulaştığında bntpool.com bunu Blocknet adresinize otomatik gönderir. Talep etmeniz veya almanız gereken bir işlem yoktur.
</Answer>

Yeni madenci için ilk tahmin, share'leri penceredeyken havuz bir blok bulduğunda görünür. İlk ödeme bu blok olgunlaştıktan ve bakiyeniz alt sınıra ulaştıktan sonra gelir. Gönderilen ödemeler [ödemeler sayfasında](/payouts) ve istatistikler sayfasındaki kendi geçmişinizde listelenir.

## Geçici ödüller nasıl gösterilir?

[İstatistikler sayfasında](/stats) adresinizi veya `@name` adınızı aratın; üç bakiye kartı göreceksiniz:

- **Estimated Rewards:** Henüz olgunlaşmamış bloklardaki payınız. Geçicidir ve her blok onaylanana veya yetim olana kadar değişebilir.
- **Confirmed Rewards:** Ödenmeyi bekleyen olgunlaşmış bakiyeniz.
- **Paid Balance:** Adresinize zaten gönderilmiş toplam tutar.

Havuz geçici ödülleri ayrı tutar; böylece onaylanmış bakiyeniz yalnızca kesinleşmiş tutarları içerir. Kartların altında istatistikler sayfası, her onaylanmamış bloğu tahmini payınızla listeler; onaya doğru ilerleyişini izleyebilirsiniz. [Madencinizi takip etme rehberi](/blog/tr/track-your-miner-on-bntpool/) sayfanın kalanını anlatır.

## Şans ve efor sayfaları ne söyler?

Efor, havuzun bir blok bulmak için ne kadar iş yaptığını ölçer. Tur sırasında kabul edilen işin blok zorluğuna bölünmesiyle bulunur ve yüzde olarak gösterilir. Blocknet'in zorluğu blok başına beklenen hash sayısına eşittir; %100 efor, bloğun tam ortalama iş miktarıyla bulunduğunu gösterir. %100 altı ortalamadan hızlı, üstü daha uzun sürdüğü anlamına gelir.

[Şans sayfası](/luck) her turu blok yüksekliği, efor, tur süresi, durum ve bulunma zamanı ile listeler. [Havuz paneli](/) mevcut turun artan eforunu ve havuzun ortalama şansını gösterir. Tek turlar %100'ün altına ve üstüne dalgalanır; bu, her iş kanıtı havuzunda normaldir. Çok sayıda turda efor ortalaması %100 civarına gelir.

Şans, blokların ne sıklıkta geldiğini etkiler. Her bloktaki payınız yalnızca pencerenin işindeki payınıza bağlıdır; düzenli madencilik, tur kısa da uzun da olsa bu payı tam tutar.

<CTA target="pool" title="Havuza katılın" text="bntpool.com'da düzenli madencilik yapın; her bloktaki payınız 0,1 BNT'ye ulaştığında otomatik ödensin." />
