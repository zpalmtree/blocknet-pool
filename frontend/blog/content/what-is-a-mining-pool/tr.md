---
title: Madencilik havuzu nedir?
description: Madencilik havuzu birçok madencinin hashrate'ini birleştirip her blok ödülünü paylaşır. Havuzları, PPLNS, PPS ve solo madenciliği öğrenin.
tags: [Madencilik temelleri, Madencilik havuzları, PPLNS, bntpool]
sourceHash: 'a17a47ab1d73'
translation: draft
faq:
  - q: 'Madencilik havuzuna katılmak ürettiğim miktarı artırır mı?'
    a: 'Havuz, hashrate''inizi ve ağın işindeki payınızı aynen bırakır. Değiştirdiği şey ödüllerin size ulaşma biçimidir. Tek başınıza rastgele aralıklarla tam blok beklersiniz. Havuzda, havuzun bulduğu her bloktan küçük bir pay kazanırsınız; ödemeler sık gelir ve tutarları hashrate''inizi izler.'
  - q: 'PPLNS ile PPS arasındaki fark nedir?'
    a: 'PPLNS, havuzun gerçekten bulduğu her bloktan pay öder; son zaman penceresinde madencilerin gönderdiği işe göre böler. PPS, havuz blok bulsa da bulmasa da kabul edilen her share için beklenen değerinden hesaplanan sabit tutarı öder. bntpool.com altı saatlik pencereyle PPLNS kullanır.'
  - q: 'Havuzda madencilik için kendi düğümümü çalıştırmalı mıyım?'
    a: 'Hayır. Havuz düğümü çalıştırır, blok şablonlarını oluşturur, share''leri kontrol eder ve bulduğu blokları gönderir. bntpool.com''da yalnızca seine madencisi ve Blocknet adresi veya Blocknet ID adı gerekir. seine''i çalıştırın, adresinizi yapıştırın ve varsayılan havuzu kabul edin; her bloktaki payınız o adrese ödenir.'
  - q: 'bntpool.com''da madenciliğe nasıl başlarım?'
    a: 'seineminer.com üzerinden Windows, macOS veya Linux için seine''i indirin, çalıştırın ve sorulduğunda Blocknet adresinizi veya @name adınızı yapıştırın. bntpool.com seine''in varsayılan havuzudur; kabul ederek madenciliğe başlayabilirsiniz. Hashrate, share ve bakiyenizi görmek için havuzun istatistikler sayfasında adresinizi aratın.'
---

Bu rehber madencilik havuzunun ne olduğunu, çoğu madencinin neden birine katıldığını ve havuzun her madencinin kazancını nasıl belirlediğini açıklar. Yaygın ödeme yöntemlerini, PPLNS, PPS ve solo madenciliği ve madencinizi havuza yönlendirmeden önce kontrol edilecekleri öğreneceksiniz. Blocknet madencilik havuzu bntpool.com, yazı boyunca uygulamalı örnektir.

## Madencilik havuzu nedir?

<Answer>
Madencilik havuzu, birlikte blok bulmaları için birçok madencinin [hashrate'ini](/blog/tr/what-is-hashrate/) birleştiren sunucudur. Her madenciye aynı blok üzerinde iş verir, her birinin gönderdiği işi sayar ve her blok ödülünü bu işle orantılı paylaşır. Madenciler, nadir tam bloklar yerine küçük ve sık ödemeler kazanır.
</Answer>

İş kanıtı kullanan bir coin'de madenciler, sonuçlardan biri ağın hedef değerinin altına düşene kadar blok başlığını farklı nonce değerleriyle hash'leyerek sonraki bloğu bulmak için yarışır. Her hash aynı küçük başarı şansına sahip bağımsız bir denemedir; kazanan hash'i bulan blok ödülünü alır.

Havuz bu yarışı ekip işine dönüştürür. bntpool.com'a bağlı herkes havuzun oluşturduğu bloklar üzerinde çalışır; içlerinden biri kazanan hash'i bulduğunda ödül, bulunmasına iş katkısı yapan tüm madenciler arasında paylaşılır.

## Madenciler neden havuza katılır?

<Answer>
Madenciler düzenli ödeme almak için havuza katılır. Tek başına tipik madenci ağ hashrate'inin küçük bir kısmına sahiptir ve bloklar arasında rastgele aralıklarla günlerce bekleyebilir. Havuzun birleşik hashrate'i sık blok bulur; her madenci her bloktan pay kazandığından gelir düzenli gelir ve hashrate'ini izler.
</Answer>

Blocknet'te hesabı kontrol etmek kolaydır. Ağ beş dakikada bir blok, yani günde 288 blok hedefler. Dolayısıyla tek başına madenci günde 288 × (kendi hashrate'i ÷ ağ hashrate'i) blok bekleyebilir.

Yuvarlak, varsayımsal sayılar alalım: 50.000 H/s ağdaki 50 H/s rig, ağın %0,1'ine sahiptir ve ortalama günde 0,288 blok, yaklaşık üç buçuk günde bir blok bulur. Bu yalnızca ortalamadır. Blok bulma rastgeledir; rig'in blokları arasındaki gerçek süre çok daha kısa veya uzun olabilir.

Aynı rig'i toplam 5.000 H/s çalışan bir havuza koyarsanız havuz günde yaklaşık 28,8, ortalama 50 dakikada bir blok bulur. Rig havuz işinin %1'ini yaptığından her bloktan %1 kazanır. Katkıda bulunduğu iş aynıdır; değişen, ödülün günde birçok kez küçük paylar hâlinde gelmesidir. [Blocknet için havuz ve solo madencilik](/blog/tr/pool-vs-solo-mining-blocknet/) rehberi bu karşılaştırmayı daha büyük işletmeler için işler.

## Madencilik havuzu nasıl çalışır?

Her havuz aynı temel döngüyü izler. bntpool.com'da şöyle işler:

1. **Havuz blok şablonu oluşturur.** bntpool.com kendi Blocknet düğümünü çalıştırıp ondan blok şablonu alır; madencilerin düğüm çalıştırması veya eşitlemesi gerekmez.
2. **Madencilere iş gönderir.** Madenciler, havuzların iş göndermek için kullandığı Stratum protokolüyle `stratum+tcp://bntpool.com:3333` adresine bağlanır. Havuz her bağlantıya güncel şablona dayalı görev gönderir.
3. **Madenciler hash hesaplayıp share gönderir.** Share, havuzun belirlediği daha kolay hedefi karşılayan hash'tir. Her madencinin ne kadar iş yaptığını kanıtlar.
4. **Ağ hedefini de karşılayan share bir bloktur.** Havuz her share'i iki hedefe karşı kontrol eder. Biri ağ için yeterli olduğunda bloğu düğümüne gönderir.
5. **Ödül bölünüp ödenir.** Havuz ödülü ödeme yöntemine göre böler; her madencinin payı onaylandığında ve en düşük ödeme tutarına ulaştığında gönderilir.

## Madencilik havuzunda share nedir?

Share, gerçek bir bloktan daha düşük eşiği geçen hash'tir. Blok için hash ağ hedefinin altında olmalıdır; share için havuzun çok daha kolay share hedefinin altında olması yeterlidir. Çoğu share asla blok olmaz, ancak her madencinin ne kadar hash hesapladığını havuza düzenli gösterir.

Share'ler zorlukla ağırlıklandırılır. Blocknet'te D zorluğu, hedefi karşılamak için yaklaşık D hash beklenmesi demektir; 60 zorluklu share yaklaşık 60 hash'lik işi temsil eder. bntpool.com her bağlantıya share zorluğu belirler ve ilerledikçe ayarlar; beş dakikada yaklaşık 10 share hedefler ve yeni bağlantıları 60 zorlukta başlatır. Hızlı sunucu daha zor, dizüstü daha kolay share'ler alır ve ikisine de gerçekten yaptıkları iş yazılır. Havuz her rig'in zorluğunu bir saat hatırlar; hızlı yeniden başlatma kaldığı yerden devam eder.

## Yaygın havuz ödeme yöntemleri

Ödeme yöntemi, havuzun share'leri kazanca dönüştürme kuralıdır. En sık görecekleriniz şunlardır.

| Yöntem | Ne için ödeme alırsınız? | Nasıl çalışır? |
|---|---|---|
| PPLNS (pay per last N shares) | Havuzun bulduğu bloklar | Her blok ödülü son zaman penceresinde gönderilen share'ler arasında zorluklarına göre bölünür |
| PPS (pay per share) | Kabul edilen her share | Havuz blok bulsa da bulmasa da share'in beklenen değerine göre sabit tutar öder |
| Solo | Kendiniz bulduğunuz bloklar | Kendi düğümünüzle madencilik yapar, bulduğunuz her bloğun tüm ödülünü tutarsınız |

**PPLNS**, havuzun bulduğu her bloktan geriye bakar. bntpool.com'da “son N share” altı saatlik penceredir: havuz bloktan önceki altı saatte kabul edilen her share'in zorluğunu toplar ve her madenciye işteki payıyla aynı ödül oranını verir. Her share penceredeyken bulunan tüm bloklara sayılır; düzenli çalışan madencinin arkasında her zaman tam pencere vardır ve yeniden başlatma hiçbir şeyi sıfırlamaz. [bntpool.com'da PPLNS ödemeleri nasıl çalışır?](/blog/tr/how-pplns-payouts-work/) paylaşımı adım adım anlatır.

**PPS**, her share geldiğinde beklenen değerini kullanarak ödeme yapar: share zorluğu ağ zorluğuna bölünür, blok ödülüyle çarpılır. Havuz işletmecisi blok bulmanın iniş çıkışlarını üstlenir ve arada madencilere kendi bakiyesinden öder.

**Solo madencilik** havuzu atlar. Kendi düğümünüz şablonları oluşturur ve bulduğunuz her blok tüm ödülü cüzdanınıza öder. Tek başına sık blok bulacak kadar büyük işletmelere uygundur. seine madencisi ikisini de destekler: varsayılan olarak bntpool.com'da çalışır; `./seine --mode daemon` ise kendi eşitlenmiş Blocknet düğümünüzle madencilik yapar.

## Madencilik havuzunda nelere bakmalısınız?

Havuzları karşılaştırırken nasıl ödeme alacağınızı ve bunu ne kadar kolay denetleyebileceğinizi belirleyen ayrıntılar şunlardır:

- **Açık ödeme yöntemi.** Havuz, yöntemini ve PPLNS penceresi gibi ayarlarını belirtmelidir. bntpool.com bunları [Get Started sayfasında](/start) listeler ve `https://bntpool.com/api/info` adresinde yayımlar.
- **Onaylar ve en düşük ödeme.** Ödüller genelde blok zincirde yeterince derine ulaşana kadar bekler; bakiye alt sınırı geçtiğinde gönderilir. bntpool.com'da blok ödülü, Blocknet'in coinbase olgunlaşmasıyla uyumlu olarak 60 blok sonra onaylanır; 0,1 BNT ve üzeri bakiyeler otomatik ödenir.
- **Herkese açık, kontrol edilebilir sayılar.** Havuzun bulduğu her bloğu ve gönderdiği her ödemeyi görebilmelisiniz. bntpool.com bunları [bloklar](/blocks) ve [ödemeler](/payouts) sayfalarında, tur bazında eforu [şans sayfasında](/luck), hizmet sağlığını [durum sayfasında](/status) gösterir.
- **Kendi istatistikleriniz.** Hashrate, share ve bakiyeleri görmek için adresinizi aratın. bntpool.com'da bu, [istatistikler sayfasıdır](/stats); [madencinizi takip etme](/blog/tr/track-your-miner-on-bntpool/) rehberi her kartı açıklar.
- **Kolay kurulum.** Ne kadar az ayar gerekirse o kadar çabuk başlarsınız. bntpool.com, iş parçacıkları, kernel'ler ve belleği kendi ayarlayan seine'in varsayılan havuzudur. Tam adres yerine `@name` gibi bir [Blocknet ID adıyla](https://seineminer.com/tr/blog/blocknet-id-pay-by-name/) giriş yapabilirsiniz.

## Bir bakışta bntpool.com

| Ayar | bntpool.com |
|---|---|
| Ödeme yöntemi | PPLNS |
| Share penceresi | 6 saat |
| Ödeme öncesi onaylar | 60 blok |
| En düşük ödeme | 0,1 BNT |
| Stratum adresi | `stratum+tcp://bntpool.com:3333` |

[Havuz paneli](/) canlı durumu gösterir: bağlı madenciler, havuz ve ağ hashrate'i, bulunan bloklar ve mevcut turun eforu. 25 Ekim 2026 saat 02:25 EDT'de alınan görüntüde havuz, yaklaşık 44.400 H/s ağ içinde 5.200 H/s hızında çalışıyor ve 246 madenci bağlı bulunuyordu. Bu, ağın yaklaşık %12'sidir; günde yaklaşık 34, kabaca 42 dakikada bir blok beklemek için yeterlidir. Rakamlar sürekli değişir; güncel değerleri panelden kontrol edin ve kendi hashrate'iniz için BNT'ye çevirmek üzere [ödül tahmini rehberini](/blog/tr/estimate-blocknet-mining-rewards/) kullanın.

Katılmak için [seine'i indirin](https://seineminer.com/tr/#download), çalıştırın, Blocknet adresinizi veya `@name` adınızı yapıştırın ve varsayılan havuzu kabul edin. [seine madencilik rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) her adımı, [bntpool.com'da Blocknet madenciliği](/blog/tr/mine-blocknet-on-bntpool/) ise rig adlarını ve ilk share'lerin kontrolünü anlatır.

<CTA target="pool" title="bntpool.com'a katılın" text="seine'i havuza yönlendirin ve bulduğu her bloktan pay kazanın; 0,1 BNT'ye ulaştığınızda adresinize otomatik gönderilsin." />
