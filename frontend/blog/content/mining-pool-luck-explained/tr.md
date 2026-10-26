---
title: 'Madencilik havuzu şansı: efor ve blok süreleri'
description: 'Havuz şansını, %100 eforu ve uzun turları anlayın. bntpool.com tur geçmişini ve beklenen blok süresini okuyun; blok bulunmasını ödemelerden ayırt edin.'
tags: ['Madencilik havuzları', 'Havuz şansı', 'Blocknet']
faq:
  - q: '%100 efor, havuzun birazdan blok bulacağı anlamına mı gelir?'
    a: 'Hayır. Kaydedilen pay işinin, hesaplamada kullanılan zorluk düzeyinde bir blok için beklenen miktara ulaştığı anlamına gelir. Bu bir ortalamayla karşılaştırmadır, tamamlanma yüzdesi değildir. Her yeni hash yine de ağ hedefini karşılamak zorundadır.'
  - q: 'bntpool.com''da daha düşük havuz şansı yüzdesi daha mı iyidir?'
    a: 'Önce etiketi ve tanımını okuyun. bntpool.com Efor sütunu ve Ortalama Şans kartı, yapılan işi beklenen işe oranlar; düşük değerler blok başına daha az kayıtlı iş anlamına gelir. Başka siteler ters tanımı kullanabilir. Yalnızca yüzdeleri değil, aynı ölçütleri ve örneklem büyüklüklerini karşılaştırın.'
  - q: 'Uzun bir tur sonraki ödememi artırır mı?'
    a: 'Uzun tur kişisel ödemenizin daha yüksek olacağını garanti etmez. Blok bulunması ödülün ne zaman kullanılabilir olduğunu, havuzun PPLNS muhasebesi ise uygun kabul edilmiş işe göre sizin payınızı belirler. Sonrasında onay ve ödeme kuralları uygulanır. Bakiyelerinizi İstatistiklerim''den, aşamaları ödeme rehberinden kontrol edin.'
sourceHash: dfd8c710a719
translation: draft
---

Havuz panosu hem madencilik işini hem de bu işle bulunan blokları gösterir. Bu rehber bntpool.com'daki efor yüzdelerini, beklenen blok sürelerini ve tur geçmişini açıklar. İstatistiksel ortalamayı geri sayımdan ayırabilir, havuzun sonraki bloğunu beklemeden kendi madencilik bağlantınızı kontrol edebilirsiniz.

## Madencilik havuzu şansı nedir?

<Answer>
Havuz şansı, gerçek blok bulma sonuçlarının havuzun işi için beklenen ortalamayla ilişkisini anlatır. Bloklar rastlantısal bulunur: bazıları az, bazıları daha çok iş gerektirir. bntpool.com'da tur eforu, kabul edilen pay işini blok zorluğuyla karşılaştırır; düşük efor, turun bloğunu daha az kayıtlı işle bulduğu anlamına gelir.
</Answer>

Havuz çok sayıda madencinin denemelerini birleştirir ancak yine de ağ hedefini karşılayan bir hash bulmalıdır. Kabul edilen paylar, daha kolay bir hedef üzerinden katkıları ölçer ve iki blok bulunuşu arasında yapılan hash işini tahmin etmeye yardımcı olur. [Madencilik havuzlarına giriş](/blog/tr/what-is-a-mining-pool/) bu ilişkiyi açıklar; şans ise ortaya çıkan bloklardaki değişkenliği anlatır.

## Havuz eforu nasıl hesaplar?

Tamamlanmış turda bntpool.com, kabul edilen pay işini blok zorluğuna böler ve 100 ile çarpar. Pay işi zorlukla ağırlıklandırılır; yalnızca gönderilen pay sayısı değildir. Daha yüksek atanmış zorluğa sahip pay, düşük zorluktaki paydan daha fazla beklenen hash işini temsil eder.

**Efor (%) = kabul edilen pay işi ÷ blok zorluğu × 100**

Örneğin blok zorluğu 10.000 olsun ve havuz tur boyunca 5.000 birim kabul edilen iş kaydetsin. Efor %50 olur. Aynı blok zorluğunda 20.000 birim işle %200 olurdu. Bunlar aritmetik örneklerdir; üretimdeki belirli bir bloğun ölçümleri değildir.

| Efor | Nasıl okunur |
|---|---|
| 50% | Bu zorluk için beklenen işin yarısı |
| 100% | Bir blok için beklenen iş miktarı |
| 200% | Bu zorluk için beklenen işin iki katı |

Güncel turun eforu, zincir ilerledikçe değişebilen güncel zorluğu kullanır. Geçmiş satırlar kendi bloklarının zorluğunu kullanır. Bu nedenle rakam iş karşılaştırmasıdır, %100'de durması gereken ilerleme göstergesi değildir. Tur süresinden de farklıdır: daha hızlı havuz aynı işi daha kısa sürede yapabilir.

## Beklenen blok süresi ne anlama gelir?

Havuzun ağ hash gücündeki tahmini payının gösterdiği ortalama aralıktır. bntpool.com panosu bunu 300 saniye ile ağ hash gücünü çarpıp havuz hash gücüne bölerek hesaplar. İki hız için aynı birimleri kullanın. [Hash gücü rehberi](/blog/tr/what-is-hashrate/), kabul edilen paylar ve yeni bloklar geldikçe bu tahminlerin neden değiştiğini açıklar.

**Beklenen saniye = 300 × ağ H/s ÷ havuz H/s**

Ağ 100.000 H/s, havuz 10.000 H/s olarak tahmin ediliyorsa havuz tahmini toplamın onda birine sahiptir. Beklenen aralığı 3.000 saniye, yani 50 dakikadır. Bu ortalama kısa ve uzun turların karışımını anlatabilir; sonraki bloğu tam 50 dakika sonrasına planlamaz.

Pano geçen süreyi beklenen süreyle de karşılaştırır. Zamana dayalı bu yüzde ile paylara dayalı efor, farklı girdiler kullandıkları için farklı olabilir. İki rakamı karşılaştırmadan önce etiketlerine bakın. Havuz hash gücündeki veya ağ zorluğundaki değişiklikler, aynı tur devam ederken tahmini değiştirebilir.

## %100 efordan sonra blok gelmesi gerekir mi?

<Answer>
Efor %100'e ulaştığında havuza bir blok borçlu olunmaz. Önceki başarısız hash'ler, sonraki hash'in değişmeyen ağ hedefini karşılama ihtimalini artırmaz. Hash gücü ve zorluk sabitken şu andan itibaren beklenen bekleme süresi, uzun turdan sonra bile aynıdır. Daha fazla madencilik işi daha fazla deneme yaratır, son tarih değil.
</Answer>

Basit bir olasılık modeli bunu açıklar. Bağımsız denemeler ve sabit blok bulma hızıyla bekleme süresi yaklaşık üstel dağılımlıdır. Beklenen bir aralık sonrasında hiç blok olmama ihtimali yaklaşık %36,8; iki aralıkta %13,5; üç aralıkta %5,0'dır. Bu yüzdeler sabit koşullardaki çok sayıda varsayımsal turu anlatır, mevcut havuz turunu öngörmez.

| Geçen beklenen aralıklar | Henüz blok bulunmamasının yaklaşık olasılığı |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Hesap `exp(-t / T)` biçimindedir; `T` ortalama aralığı, `t` geçen süreyi gösterir. [NIST üstel dağılım kaynağı](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) bu sağkalım fonksiyonunu verir. Gerçek hash gücü ve zorluk değişir; tabloyu rastlantısallığın açıklaması olarak kullanın. Hedef aynı kaldığında art arda hızlı bloklar da sonraki hash'in olasılığını değiştirmez.

## bntpool.com şans geçmişi nasıl okunur?

Tamamlanan turları karşılaştırmak için [Şans](/luck) sayfasını açın. Her satır blok, efor, tur süresi, durum ve bulunma zamanını gösterir. Olağandışı kısa veya uzun tek bir satır seçmek yerine birkaç turu birlikte inceleyin. Blok bağlantısı ilgili zincir kaydını açar; sayfalama daha geniş geçmişi incelemenizi sağlar.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Efor, tur süresi ve blok durumunu gösteren gerçek bntpool.com şans geçmişi sayfası" width="1200" height="833" loading="lazy" />

Bu ekran görüntüsü 26 Ekim 2026'da alındı. Satırlar ve durumlar geçmişin bir anlık kaydıdır; güncel değerler için canlı sayfayı kullanın. Beklemede veya onaylandı durumu, bloğun onay durumunu tanımlar. Kayıtlı efor yüzdesinin anlamını değiştirmez ve adresinize yapılan ödemeden ayrıdır.

Panodaki Ortalama Şans, güncel zincir yüksekliğine kadar yetim olmayan blokların kaydedilmiş efor yüzdelerinin aritmetik ortalamasıdır. Adına rağmen kart efor tanımını izler: düşük değer, blok başına daha az kayıtlı iş demektir. Ortalamalar bu örneklemi özetler. Gelecek turların belirli bir önceki sonucu telafi edeceğini vadetmez.

## Havuz şansı ödüllerinizi nasıl etkiler?

Havuz şansı blokların ne zaman bulunduğunu değiştirir. PPLNS, bulunan bloğun ödülünü uygun kabul edilmiş iş arasında nasıl dağıtacağını belirler. Bunlar ayrı aşamalardır. Tur sınırı, PPLNS penceresini yalnız o tura bağlı ödeme kuralına dönüştürmez; gerçek pencere, onaylar ve ödeme eşiği için [PPLNS ödemelerinin işleyişini](/blog/tr/how-pplns-payouts-work/) okuyun.

Bu nedenle madenciniz kabul edilen pay göndermeye devam ederken bakiyeniz basamaklar halinde değişebilir. [Ödül hesaplayıcı](/blog/tr/estimate-blocknet-mining-rewards/#mining-calculator) hash gücü ve ağ girdilerinden üretimi tahmin eder. Zaman içindeki beklentiler için kullanışlıdır; bloğu veya ödemenin cüzdanınıza varacağı anı tahmin etmez.

## Uzun tur sırasında neyi kontrol etmelisiniz?

Doğrudan gözlemleyebildiklerinizi kontrol edin: madenciniz çalışıyor mu, paylar kabul edilmeye devam ediyor mu ve [İstatistiklerim](/stats) üzerinde doğru adres görünüyor mu? Hizmetin genel sağlığı için [Durum](/status) sayfasını kullanın. Kendi kabul edilen paylarınız durursa [madenci istatistikleri sorun giderme rehberini](/blog/tr/track-your-miner-on-bntpool/#zero-hashrate) izleyin; uzun havuz turu ile bağlantısı kopmuş sistem farklı kontroller gerektirir.

Bağlantınız sağlıklıysa şansı anlamlı bir geçmiş üzerinden, aynı tanımlarla okuyun. Böylece her pano rakamının rolü netleşir: hash gücü saniyedeki işi tahmin eder, efor işi bir blok için beklenen işle karşılaştırır, bakiyeler ödül muhasebesini gösterir. Bunların hiçbiri sonraki kazanan hash'in ne zaman geleceğini tahmin etmeyi gerektirmez.

<CTA target="pool" title="bntpool.com'da madenciliğe başlayın" text="seine'ı bağlayın, kabul edilen paylarınızı doğrulayın, tur geçmişini ve ödülleri havuz panosundan izleyin." />
