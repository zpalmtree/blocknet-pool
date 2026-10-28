---
title: Blocknet blok ödülü takvimi
description: Blocknet'in aylık blok ödülü; başlangıçta 723,25 BNT, 48. ayda kalıcı 2,0 BNT kuyruk ödülü, günde 288 blok ve kuyruk öncesinde 98,6 milyon BNT.
tags: [Blocknet, Blok ödülü, Emisyon, Madencilik]
sourceHash: 6c38930dfa09
translation: draft
faq:
  - q: 'Blocknet''in güncel blok ödülü nedir?'
    a: 'Ödül her 8.640 blokta azaldığı için zincirin hangi ayda olduğuna bağlıdır. 25 Ekim 2026 tarihinde zincir 7. aydaydı ve blok başına 467,67 BNT ödüyordu; 8. ay 69.120 yüksekliğinde 439,46 BNT ile başlar. bntpool.com panelinden güncel blok yüksekliğini bulun ve tablodan bakın.'
  - q: 'Günde kaç Blocknet bloğu üretilir?'
    a: 'Blocknet her beş dakikada bir blok hedefler; bu, günde 288 ve her 30 günlük ayda 8.640 blok demektir. Zorluk, son 60 blok süresinin ağırlıklı ortalamasıyla her blokta ayarlanır; böylece madenciler ağa katılıp ayrılırken ortalama beş dakikaya yakın kalır.'
  - q: 'Blocknet''in toplam arzı nedir?'
    a: '48. ayda kuyruk emisyonu başlamadan önce yaklaşık 98,56 milyon BNT üretilir. Kod ve resmî site bunu yaklaşık 100 milyona yuvarlar. Sonrasında her blok sonsuza dek 2,0 BNT öder; bu, günde 576 veya yılda yaklaşık 210.240 BNT ekler, dolayısıyla arz yavaşça büyümeyi sürdürür.'
  - q: 'Blocknet''in kuyruk emisyonu ne zaman başlar?'
    a: 'Kuyruk, 48. ayda, 414.720 blok yüksekliğinde başlar. Bu, başlangıçtan sonra 30 günlük 48 ay, yani beş dakikalık bloklarla 1.440 gün veya yaklaşık dört yıldır. Kesin takvim tarihi gerçek blok sürelerine bağlıdır. Bu yükseklikten itibaren her blok 2,0 BNT öder.'
  - q: 'Madencilikle kazanılan Blocknet coin''leri ne zaman harcanabilir?'
    a: 'Bir blok ödülü 60 blok sonra harcanabilir; Blocknet''in beş dakikalık blok süresinde bu yaklaşık beş saattir. Buna coinbase olgunlaşması denir. bntpool.com havuzu da blok ödülünü ödemeden önce 60 blok bekler, böylece yalnızca olgunlaşmış blokların ödüllerini öder.'
---

Yeni çıkarılan BNT'yi blok ödülünden harcanabilir coinlere ve sonraki ödemelere kadar izleyin. Bu sayfa para akışını açıklar, ardından ödül takviminin her ayını listeler ve rakamları madenci veya coin sahibi olarak nasıl okuyacağınızı gösterir.

## Blocknet blok ödülü nedir?

<Answer>
Blocknet blok ödülü blok başına 723,25 BNT ile başladı ve düzgün bir azalma eğrisini izleyerek 8.640 bloktan oluşan her 30 günlük ayda bir düşer. Bir ay içindeki her blok için aynıdır. 48. aydan, yani 414.720 yüksekliğinden itibaren her blok kalıcı olarak 2,0 BNT kuyruk ödülü öder.
</Answer>

Blocknet her beş dakikada (300 saniyede) bir blok hedefler. Bu, günde 288 ve her 30 günlük ayda 8.640 blok eder. Zorluk, son 60 blok süresinin ağırlıklı ortalamasına göre her bloktan sonra ayarlanır; böylece ağın hashrate'i değişirken tempo beş dakikaya yakın kalır.

Bir bloğun ödülü yalnızca yüksekliğinin hangi aya denk geldiğine bağlıdır. Ay, blok yüksekliğinin 8.640'a bölünüp aşağı yuvarlanmasıyla bulunur: 0–8.639 yükseklikleri 0. ay, 8.640–17.279 yükseklikleri 1. aydır ve böyle devam eder.

## Blok ödülü kullanılabilir BNT'ye nasıl dönüşür?

Kazılan blok, planlanan ödülü bir coinbase çıktısında oluşturur. Bu çıktı blok için seçilen hedefe aittir ve önce coinbase olgunlaşma süresini tamamlar. Harcanabilir olduğunda sahibi bunu normal işlemlerde kullanabilir. Bloğu havuz kazdıysa muhasebesi uygun ödülleri katılımcılar arasında paylaştırır; sonraki ödeme işlemleri mevcut BNT'yi onların cüzdanlarına gönderir.

Dolayısıyla havuz ödemesi ikinci bir ödül yaratmaz. Transfer almak için yeni coin kazmanız da gerekmez. İhraç BNT'nin kökenini, transferler ise sonraki sahiplik değişimlerini açıklar. [Onay rehberi](/blog/tr/block-confirmations-mining-rewards/) havuz aşamalarını, [Blocknet arz rehberi](https://seineminer.com/tr/blog/blocknet-supply/) ise genel parasal takvimi anlatır.

## Ödül nasıl hesaplanır?

Daemon her ayın ödülünü tek formülle hesaplar:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

İlk terim, ödülün 2,0 BNT kuyruk ödülünün üzerindeki kısmıdır. Her yıl e^-0,75 çarpanı kadar küçülür; on iki ay sonra başlangıç değerinin yaklaşık %47'sine iner. Aylara dağıtıldığında, her ayın kuyruk üzerindeki ödül kısmı bir önceki ayın yaklaşık %93,9'u olur; ödül böylece küçük, düzenli adımlarla azalır.

Sonuç tam atomik birimlere aşağı yuvarlanır (1 BNT, 100.000.000 atomik birimdir). Aşağıdaki tablo her ödülü iki ondalık basamağa yuvarlanmış olarak gösterir.

## Aylara göre Blocknet ödül tablosu

| Ay | İlk blok | Blok başına ödül (BNT) | Ay sonunda toplam üretilen (milyon BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| 48 ve sonrası | 414.720 | 2,00 | +0,017 aylık |

0 yüksekliğindeki genesis bloğunun ödülü yoktur, bu nedenle 0. ayda ödül ödenen 8.639 blok bulunur. [Blocknet'in adil başlangıcı](/blog/tr/blocknet-fair-launch/) sayfasının açıkladığı gibi her BNT bu blok ödüllerinden gelir; premine yoktur. 25 Ekim 2026 tarihinde havuz, 7. ayın sonuna yakın olan 69.016 numaralı blok üzerinde çalışıyordu. [bntpool.com paneli](/) güncel blok yüksekliğini gösterir; böylece yukarıda ilgili satırı bulabilirsiniz. [Bloklar sayfası](/blocks) ise havuzun bulduğu blokları listeler.

## 48. ayda ne olur?

<Answer>
48. ay, 414.720 blok yüksekliğinde başlar ve o noktadan itibaren her Blocknet bloğu sonsuza dek sabit 2,0 BNT öder. Buna kuyruk emisyonu denir. Zincir çalıştığı sürece her blokta bir ödül olmasını sağlar; böylece madencilere ağın güvenliğini sağladıkları için her zaman ödeme yapılır.
</Answer>

Azalma formülü 0–47. aylar için geçerlidir ve 47. ayda blok başına 40,22 BNT ile sona erer. 48. aydan itibaren daemon kuyruk değerini doğrudan kullanır; ödül 2,0 BNT olur ve öyle kalır. Günde 288 blokla kuyruk, günde 576 veya yılda yaklaşık 210.240 BNT ekler.

48. ay, başlangıçtan sonra beş dakikalık bloklarla 1.440 gün, yani yaklaşık dört yıl sonra başlar. Takvim tarihi gerçek blok sürelerine bağlı olduğundan güvenilir gösterge blok yüksekliğidir.

## Kuyruk başlamadan önce ne kadar BNT üretilir?

1–414.719 yükseklikleri arasındaki tüm blokları toplamak 98.561.905,08 BNT, yaklaşık 98,6 milyon verir. Kod ve resmî site, takvimi dört yılda yaklaşık 100 milyon coin artı kuyruk olarak tanımlar; kesin toplam bu yuvarlak sayının biraz altında kalır.

Aşağıdaki birikimli toplamlar kabul edilen blokların planlanan ihracını ölçer. Bunlar şu anda harcanabilir cüzdan bakiyelerinin toplamı veya alım satım faaliyetinin ölçüsü değildir. Mevcut BNT'nin sahipler arasında taşınması bu ihraç toplamlarını artırmaz.

Çoğu erken dönemde üretilir. Takvimin yıllarına göre:

| Aylar | Üretilen BNT | Kuyruk öncesi toplamın payı |
|---|---|---|
| 0-11 (1. yıl) | 54,48 milyon | %55,3 |
| 12-23 (2. yıl) | 25,84 milyon | %26,2 |
| 24-35 (3. yıl) | 12,32 milyon | %12,5 |
| 36-47 (4. yıl) | 5,93 milyon | %6,0 |

## Üretilen BNT'yi ne zaman harcayabilirsiniz?

Bir bloğun ödülü, üzerine 60 blok daha eklendiğinde, yaklaşık beş saat sonra harcanabilir. Bu kurala coinbase olgunlaşması denir. bntpool.com havuzu da blok ödülünü ödemeden önce aynı 60 bloğu bekler; [ödemeler sayfası](/payouts) gönderilenleri listeler.

## Takvim madenciler için ne anlama gelir?

Ödüller ilk aylarda daha büyüktür. 12. ayda bulunan bir blok 342,69 BNT, yani 0. ayda ödenen 723,25 BNT'nin yaklaşık %47'sini öder; 24. ayda ise 162,93 BNT öder. Kuyruğa kadar her yeni ay, bir öncekinden daha az blok ödülü verir.

Bu ödüllerdeki payınız, ağın hashrate'indeki payınızı izler. Solo madencilikte günde bulmanız beklenen blok sayısı 288 × (sizin hashrate'iniz ÷ ağ hashrate'i) olur ve her blok kendi ayının ödülünü öder. Havuzda aynı hesap havuzun hashrate'ine uygulanır; her blok ödülü katkıda bulunan madenciler arasında paylaşılır. Böylece tek başınıza tam bir blok bulmayı beklemek yerine havuzun bulduğu her bloktan kazanırsınız. [bntpool.com paneli](/) güncel ağ ve havuz hashrate'ini, [istatistikler sayfası](/stats) ise madenciliğe başladıktan sonra kendi madencinizin hashrate'ini gösterir.

Şimdi madenciliğe başlamak için:

1. [seineminer.com](https://seineminer.com/tr/#download) üzerinden seine'i indirin. Windows, macOS ve Linux'ta çalışır ve donanımınıza göre kendini ayarlar.
2. Çalıştırın ve Blocknet adresinizi veya `@name` adınızı yapıştırın.
3. Varsayılan havuz `stratum+tcp://bntpool.com:3333` adresini kabul edin.

[bntpool.com'da madencilik rehberi](/blog/tr/mine-blocknet-on-bntpool/) havuz tarafındaki her adımı, [Get Started sayfası](/start) havuz ayarlarını, [seine madencilik rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) ise kurulumu adım adım anlatır. Farklı işlemcilerin sonuçları için [Blocknet madencilik donanımı ve hashrate](https://seineminer.com/tr/blog/blocknet-mining-hardware-hashrate/) yazısını okuyun.

<Note>
Bu sayfadaki rakamlar, 25 Ekim 2026 tarihinde kontrol edilen Blocknet daemon'undaki ödül kodundan gelir. [blocknetcrypto.com](https://blocknetcrypto.com/#faraday) sitesine göre Blocknet, 16 Şubat 2027 civarında Faraday adlı bir ağ yükseltmesi planlıyor. seine'i güncel tutun: başlangıçta ve her 12 saatte bir yeni sürümleri kontrol eder.
</Note>

<CTA target="seine" title="seine ile Blocknet madenciliği yapın" text="seine Windows, macOS ve Linux için ücretsizdir ve varsayılan olarak bntpool.com'da madencilik yapar." />
