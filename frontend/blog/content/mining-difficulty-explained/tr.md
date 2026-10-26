---
title: 'Madencilik zorluğu: ağ ve paylar'
description: 'Madencilik zorluğunun hedefi nasıl belirlediğini, Blocknet ayarlamasını ve ağ ile pay zorluğunun neden farklı madencilik ölçülerini etkilediğini öğrenin.'
tags: ['Madencilik temelleri', 'Zorluk', 'Blocknet']
faq:
  - q: 'Yüksek zorluk hashrate''i düşürür mü?'
    a: 'Zorluk sonucun karşılaması gereken hedefi değiştirir; donanımın saniyedeki hash sayısını doğrudan değiştirmez. Aynı hashrate ile daha yüksek ağ zorluğu, bloklar arasındaki beklenen ortalama süreyi uzatır. Performansı, blok veya pay sıklığından ayrı kontrol edin.'
  - q: 'Daha zor pay daha çok ödeme getirir mi?'
    a: 'Daha fazla beklenen işi temsil eder, fakat aynı hashrate ile daha seyrek gönderilir. Aynı dönemdeki zorluk ağırlıklı işi karşılaştırın. bntpool.com ödülleri ayrıca kabul edilen bloklara ve PPLNS hesabına bağlıdır; yalnızca ham pay sayısına değil.'
  - q: 'Blocknet zorluğu ne sıklıkla ayarlar?'
    a: 'Her bloktan sonra yakın zincir geçmişinden sonraki zorluğu hesaplar. Mevcut kural, 60 aralık üzerinden doğrusal ağırlıklı hareketli ortalama kullanır ve blok başına 300 saniyeyi hedefler. Yeni aralıkların ağırlığı fazladır; tek blok daha erken veya geç gelebilir.'
sourceHash: '5c203e8c7523'
translation: draft
---

Zorluk, donanımın çalışmasını ağın kabul ettiği bloklarla ilişkilendirir. Bu rehber hedefi, Blocknet ayarlamasını ve ayrı pay zorluğunu açıklar. Hashrate, beklenen blok süresi veya [ödül hesaplayıcısını](/blog/tr/estimate-blocknet-mining-rewards/#mining-calculator) karşılaştırmadan değişen değeri anlayın.

## Madencilik zorluğu nedir?

<Answer>
Bir hedefi karşılayan sonucu bulmak için beklenen hash işini tanımlar. Yüksek zorluk aynı hashrate ile uygun sonuçları seyrekleştirir. Blocknet ağ zorluğu yaklaşık olarak blok başına beklenen hash sayısıdır; pay zorluğu ayrı bir gönderim hedefini ölçer.
</Answer>

Her hash bir sayı üretir ve bu sayı hedef eşiğini karşılamalıdır. Küçük hedef daha az olası sonucu kabul eder, dolayısıyla daha zordur. Blocknet en büyük 256 bit değeri zorluğa bölerek 256 bit hedefi bulur. Zorluğu ikiye katlamak hash işlemini değiştirmeden beklenen işi yaklaşık ikiye katlar.

## Blocknet ağ zorluğunu nasıl ayarlar?

Her bloktan sonra hesaplar. Mevcut ana ağ kuralı, son 60 aralık için doğrusal ağırlıklı hareketli ortalama, LWMA, kullanır; yenilere daha fazla ağırlık verir. Hedef blok başına 300 saniyedir. Önceki zorluk ve ağırlıklı süreler birlikte, sürekli hızlı bloklarda zorluğu artırma, yavaşlarda azaltma eğilimindedir.

Kod kullanılan süreleri sınırlar ve asgari zorluk uygular. 60 aralık için 61 zaman damgası gerekir; ayarlama yalnızca 60 blokta bir yapılmaz. Kurallar güç değişirken zinciri hedef temposuna yaklaştırır. Beş dakika ortalamadır; tek blok çok daha erken veya geç gelebilir.

## Ağ zorluğu ve pay zorluğu

Havuz, blok keşifleri arasındaki katkıyı gözlemek için pay hedefi atar. Bu genellikle ağ hedefinden kolaydır. İki değer beklenen işi anlatır ama farklı soruları yanıtlar ve sistemin farklı bölümlerince belirlenir:

| Zorluk | Neyi belirler |
|---|---|
| Ağ | Konsensüs kurallarından türetilen aday blok iş eşiği. |
| Pay | Havuzun bağlantıya veya madencilik işine atadığı gönderim eşiği. |

bntpool.com gönderim sıklığını değişken zorluk, vardiff, ile ayarlar. Aynı hashrate ile daha yüksek pay zorluğu genellikle daha az, fakat her biri daha fazla beklenen iş taşıyan pay üretir. Ağ zorluğunu değiştirmez. Ağ hedefini de karşılayan sonuç blok adayı olabilir; daemon yine doğrular. [Pay sonuçları rehberi](/blog/tr/accepted-rejected-stale-shares/) kabul ve retleri açıklar.

## Beklenen blok süresini nasıl etkiler?

Blocknet'te saniye cinsinden ortalama aralığı tahmin etmek için ağ zorluğunu işi yapan hashrate'e bölün. H/s ve aynı dönemin değerlerini kullanın. Örnek, hesabı göstermek için ikisini sabit tutar; güncel ölçüm veya belirli madenci için tahmin değildir.

**Blok başına beklenen saniye ≈ ağ zorluğu ÷ H/s cinsinden hashrate**

| Ağ zorluğu | Hashrate | Beklenen aralık |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

30.000 zorluk ve 100 H/s ile ortalama 300 saniyedir. Aynı hashrate ile 60.000 zorlukta 600 saniye olur. Hashrate de 200 H/s'ye çıkarsa tekrar 300 saniyedir. Bunlar tekrarlanan denemelerin ortalamalarıdır. [Havuz şansı rehberi](/blog/tr/mining-pool-luck-explained/) gerçek turların sapmalarını açıklar.

## Hashrate sabitken ödüller neden değişir?

Kendi hashrate'iniz sabitken yüksek ağ zorluğu, sürdüğü müddetçe zamana göre beklenen blok bulma sayısını azaltır. Toplam hashrate artıp zorluk izlerse katkı payınız küçülür. Blok ödülü ayrı değişkendir: [ödül takvimi](/blog/tr/blocknet-block-reward-schedule/) miktarı bağımsız değiştirir. Zorluk, planlı blok ödülündeki BNT sayısını belirlemez.

Beklenen BNT için hesaplayıcıya güncel ağ verilerini girin. Hashrate'in madenciden mi havuzdan mı geldiğini kontrol ederek madenci ücretini doğru uygulayın; sürümünüzün oranını kullanın. Tahminler varsayımlara bağlı ortalamalardır. Ödeme ayrıca bulunan bloklara, onaylara ve [PPLNS](/blog/tr/how-pplns-payouts-work/) hesabına bağlıdır; zorluk tek başına zamanını söylemez.

## Hangi zorluğu kontrol etmelisiniz?

Sorunuzla başlayın. Bloklar için ağ zorluğunu ve ilgili madenci veya havuzun hashrate'ini kullanın. Pay sıklığı için o işlerin zorluğunu kullanın. Bakiye ve ödemeler için [My Stats](/stats) ve [ödeme geçmişini](/payouts) açın. Karşılaştırırken zaman, kapsam ve birimleri değerle birlikte tutun.

[Havuz paneli](/) ve [blok geçmişi](/blocks) güncel bağlam sağlar. Ağ hashrate'i yakın iş ve geçen blok süresinden tahmin edilir; sonraki zorluğun 300 saniyeye bölümünden farklı olabilir. [Hashrate rehberi](/blog/tr/what-is-hashrate/) farkı açıklar. Değişimi incelerken zorluk, hashrate ve şansı ayırın: farklı yönleri anlatırlar.

<CTA target="pool" title="Madencinizi bntpool.com&#x27;a bağlayın" text="Kurulum sayfasını kullanın, ardından payları ve etkinliği My Stats üzerinden izleyin." />
