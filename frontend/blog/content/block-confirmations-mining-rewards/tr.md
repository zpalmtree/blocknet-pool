---
title: 'Blok onayları ve olgunlaşmamış madencilik ödülleri'
description: 'Blocknet havuz ödülünü beklemeden olgunlaşmaya ve ödemeye kadar izleyin. bntpool blok derinliğini nasıl sayar, onaylar neden bloklarla ölçülür öğrenin.'
tags: ['Madencilik ödülleri', 'Onaylar', 'Blocknet']
faq:
  - q: 'bntpool ödül için kaç onay ister?'
    a: '26 Ekim 2026 kontrolünde yayımlanan gereksinim 60 bloktur. Havuz mevcut zincir yüksekliğinden bulunan blok yüksekliğini çıkarır ve karmayı düğümle karşılaştırır. Güncel ayarı Get Started veya açık bilgi uç noktasından kontrol edin.'
  - q: '60 blok tam beş saat mi demektir?'
    a: 'Hayır. Beş dakikalık hedefle 60 aralık, planlama için 300 dakika yani beş saattir. Gerçek ilerleme değişken zamanlarda gelen bloklara bağlıdır. Olgunlaşma, ödeme eşiği ve ödeme işlemi ayrı aşamalardır.'
  - q: 'Mevcut ödül olgunlaşana kadar kazmalı mıyım?'
    a: 'Bulunan blok, madenciniz bağlı olmasa da ağ ilerledikçe derinleşir. Ödül hakkınız havuzun muhasebe kurallarına bağlıdır. Durmak olgunlaşmayı hızlandırmaz veya sıfırlamaz; My Stats üzerinden izleyin.'
sourceHash: '7383b256ba1a'
translation: draft
---

Bulunan Blocknet bloğunu onaydan olgunlaşmaya ve cüzdan ödemesine kadar takip edin. Bu rehber bntpool ödül kartlarının arkasındaki zincir derinliğini açıklar. Paylaştırma için [PPLNS rehberine](/blog/tr/how-pplns-payouts-work/), iş göndermek için [kurulum rehberine](/blog/tr/mine-blocknet-on-bntpool/) bakın.

## Onay ve olgunlaşmamış ödül nedir?

<Answer>
Onaylar bir bloğun zincir ucunun altındaki konumunu anlatır. Olgunlaşmamış ödül, gereken derinliğe henüz ulaşmamış yeni bloğa aittir. bntpool ödülü onaylamadan önce derinliği ve blok kimliğini kontrol eder; bakiye uygunluğu ve sonraki ödeme ayrıdır.
</Answer>

*Yeni bloklar derinliği artırır; ödül olgunlaşma noktasını geçer, bakiyeye uygun hale gelir ve ayrı işlemle cüzdana ödenir.*

## bntpool derinliği nasıl sayar?

Havuz mevcut zincir yüksekliğinden bulunan blok yüksekliğini çıkarır. 26 Ekim 2026 tarihinde gereksinim 60 bloktu. Bu kuralla bulunan blok sıfır derinlikten başlar. Başka sayfalar bloğun kendisini ilk onay sayabilir; sayı kadar sayım kuralını da karşılaştırın.

Örneğin 70.000 yüksekliğindeki blok, uç 70.000 iken sıfır, 70.001 iken bir, 70.060 iken 60 derinliktedir. Bunlar canlı veri değil örnek sayılardır. Havuz ayrıca düğümdeki karmayı doğrular. [Uygulama kodu](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) bu kontrolleri gösterir.

## Beş saat neden yalnız tahmindir?

Blocknet ağ blokları arasında beş dakika hedefler. Altmış aralık çarpı beş dakika, 300 dakika veya beş saattir. Bu planlamaya yardımcı olur; koşul ise blok sayısıdır. Sabit bir saatin dolmasını değil, zincirin gereken derinliğe ilerlemesini izleyin.

Kabul edilen zincirdeki bütün bloklar ilerlemeye katkı verir; hepsini bntpool bulmak zorunda değildir. Madencinizin hızı özel bir onay saati oluşturmaz. Blok gelişleri değişkendir. [Zorluk rehberi](/blog/tr/mining-difficulty-explained/) ayarlamayı, [şans rehberi](/blog/tr/mining-pool-luck-explained/) havuz turlarındaki değişkenliği açıklar.

## Bekleyen, onaylanan ve ödenen ayrı aşamalardır

Yeni havuz bloğu derinlik toplarken bekleyen görünür. Uygun işiniz varsa tahmini payınız [My Stats](/stats) üzerinde görünebilir. Blok ödülü yeni coin üretir; Blocknet cüzdanının coinbase olgunlaşma kuralı harcanabilirlik için 60 blok derinliği kullanır. Havuzun beklemesi bu süreci izler.

Derinlik ve kimlik kontrollerinden sonra havuz bloğu onaylayıp uygun ödülü bakiyelere dağıtabilir. Bu, her adrese anında ödeme demek değildir. Ödeme süreci onaylı bakiyeleri ve gönderim koşullarını ayrıca değerlendirir. Blok durumunu, onaylı bakiyeyi ve ödeme geçmişini ayrı okuyun.

## Havuz blok karmasını neden kontrol eder?

Yükseklik zincirdeki konumu, karma belirli bloğu tanımlar. Düğümler rakip bloklar öğrenebilir; o konumu hangi bloğun tuttuğunu kabul edilen zincir belirler. Havuz düğümünden kayıtlı yükseklikteki bloğu alıp karmayı karşılaştırır. Derinlik tek başına ilk bloğun zincirde kaldığını kanıtlamaz.

Zincirden çıkan blok yetim olarak işaretlenebilir; geçici ödülü kabul edilen zincirin ödülü olmaktan çıkar. Havuz bunu onaydan ayrı işler. [Bloklar sayfasındaki](/blocks) karma bağlantısını kullanın; aynı yükseklikte iki kayıt aynı olay olmayabilir. Tahmini ödül kendi bloğuna bağlıdır.

## Ödeme ile kazılan ödül nasıl farklıdır?

Kazılan ödül önce bloğun coinbase çıktısına aittir. Havuz ödemesi, uygun bakiyeyi adresinize gönderen sonraki işlemdir. Kontrol tarihinde asgari ödeme 0,1 BNT idi. Blok olgunluğu ve bakiye eşiği farklı koşullardır; ayrıntılar [PPLNS rehberindedir](/blog/tr/how-pplns-payouts-work/).

Gönderimden sonra işlemin kaydını ve cüzdanın alım/harcanabilirlik durumunu izleyin. Bu onay geçmişi ilk havuz bloğuna değil ödeme işlemine aittir. Blok olgunlaşıyor diye yeni adres veya yeniden bağlantı gerekmez. [Ödeme geçmişini](/payouts) cüzdanla karşılaştırırken aynı adresi kullanın.

## Ödül baştan sona nasıl izlenir?

[Blocks](/blocks) üzerinde yüksekliği, karmayı ve durumu bulun. Madencinizin ödeme adresiyle [My Stats](/stats) açıp tahmini ve onaylı ödülleri inceleyin. Ödeme oluşunca geçmişten işlem bağlantısını açın. Blok, hesap bakiyesi ve cüzdan ödemesi birbirinin yerine geçmez.

Güncel ayarlar [Get Started](/start) veya `https://bntpool.com/api/info`, hizmet durumu [pool status](/status) üzerindedir. İnceleme için adresi, blok/işlem kimliğini ve gözlem zamanını kaydedin; kurtarma kelimelerini paylaşmayın. [Takip rehberi](/blog/tr/track-your-miner-on-bntpool/) diğer istatistikleri açıklar. Çevrimdışıyken de derinlik artar; hak ediş kayıtlı işe ve zincire bağlıdır.

<CTA target="pool" title="Madencilik ödüllerinizi izleyin" text="bntpool.com’a bağlanın; tahmini ödülleri, onaylı bakiyeleri ve ödemeleri My Stats üzerinden takip edin." />
