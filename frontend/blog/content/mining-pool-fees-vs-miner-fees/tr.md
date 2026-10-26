---
title: 'Havuz ücreti ve madenci geliştirici ücreti'
description: 'Blocknet havuz ve madenci ücretlerinin nerede uygulandığını, nasıl birleştirildiğini ve aynı ücretin iki kez düşülmesini nasıl önleyeceğinizi öğrenin.'
tags: ['Madencilik rehberi', 'Havuz ücretleri', 'Ödüller']
faq:
  - q: 'Bunlar aynı ücret mi?'
    a: 'Hayır. Havuz ücreti ödül dağıtımına, geliştirici ücreti yazılıma aittir. seine v0.2.36 başlangıç muafiyetinden sonra ücret hesabına giren sürenin %2''sini geliştirmeye ayırır. Bntpool''un ayrı havuz ücreti 26 Ekim 2026 kontrolünde %0''dı.'
  - q: 'Nasıl birleştirilir?'
    a: 'Brüt tahmini önce bir eksi madenci oranıyla, sonra bir eksi havuz oranıyla çarpın. Gerçek sürüm ve güncel ayarı kullanın. Bu kalan ödül tahminidir; ödeme garantisi veya nakit kâr hesabı değildir.'
  - q: 'Madenci ücretini havuz hashrate''inden düşmeli miyim?'
    a: 'Önce sayının anlamını belirleyin. Geliştirici süresi sonrası adresinize yazılan işi zaten tahmin ediyorsa tekrar düşmek çift sayımdır. Hesaplayıcı önceden yansıtılmış düzeltme için sıfır kabul eder. Onaylanmış bakiye veya kaydedilmiş ödemeden tekrar ücret düşmeyin.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Blocknet tahminindeki iki ücret katmanını izleyin. Rehber, seine v0.2.36 ve 26 Ekim 2026'da kontrol edilen bntpool ayarlarıyla [hesaplayıcı](/blog/tr/estimate-blocknet-mining-rewards/#mining-calculator) girdilerini ve [My Stats](/stats) bakiyelerini açıklar.

## Havuz ve madenci ücreti nasıl ayrılır?

<Answer>
Havuz ücreti ödül dağıtımının, geliştirici ücreti yazılımın madencilik takviminin parçasıdır. Sahipleri ve aşamaları farklıdır. İkisini kontrol edin, girdinin brüt mü önceden düzeltilmiş mi olduğunu belirleyin ve ilgili düzeltmeyi bir kez uygulayın.
</Answer>

*İş, tahmini kalan tutara ulaşmadan önce madenci zamanı ve ayrı havuz ödülü düzeltmesinden geçer.*

## Havuz ücreti neye uygulanır?

Havuz uygun işi toplar ve dağıtılabilir ödülü kurallarıyla paylaştırır. bntpool, kaydedilmiş blok ödülünden ücreti hesaplar, kalanı PPLNS ile dağıtır. [PPLNS rehberi](/blog/tr/how-pplns-payouts-work/) katılım ve alacakları açıklar.

26 Ekim 2026'da `https://bntpool.com/api/info`, `pool_fee_pct` değerini 0,0 yayımlar; bu %0 demektir. Tahmini yenilerken [Get Started](/start) veya API'den güncel değeri okuyun. Sıfır yalnız bu havuz ayarını anlatır; yazılım politikası ayrı girdidir.

## seine geliştirici ücreti nasıl çalışır?

v0.2.36 her havuzda ve solo modda sabit %2 belgeler. Bu geliştirici adresi için madencilik zamanıdır, cüzdandan sonradan çekim değildir. İlk 10 dakika ücretsizdir; izleyici sonraki süreyi sayar. [Sürüm yapılandırma rehberi](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md) politikayı açıklar.

Zamanlama turlarla çalışır; kısa aralık tam %2 olmayabilir. Bu başlangıç sonrası uzun vadeli hedeftir, her dakika veya blok için söz değildir. Sürümü kaydedin: eski rehber veya başka yazılım farklı olabilir. Havuz ücreti bu bilgiyi karşılamaz.

## Yüzdeler nasıl birleştirilir?

Kalan oranları kullanın. Madenci oranı m, havuz oranı p kesir olarak yazıldığında brütün `(1 - m) * (1 - p)` kadarı kalır. [Ödül hesaplayıcı](/blog/tr/estimate-blocknet-mining-rewards/) bunu kullanır. Yüzdeleri toplamak genel kural değildir; ikinci düzeltme kalana uygulanır.

Yalnız aritmetik örnek: varsayımsal 100 BNT brüt, %2 madenci ve kontrol edilen %0 havuz ile 100 × 0,98 × 1,00 = 98 BNT verir. Bu kararlı durum örneğidir; donanım ölçümü, ödül sözü veya başlangıç muafiyetli kısa oturum simülasyonu değildir.

## Doğru hashrate girdisini seçin

Yerel veya benchmark hızı makine işini anlatır. Geliştirici faktöründen önce tüm zamanı mı yalnız alıcı adresinizin işini mi kapsadığını belirleyin. Kaynak ve süreyi okuyun. [Hashrate rehberi](/blog/tr/what-is-hashrate/) yerel, havuz ve ağ gözlemlerini ayırır.

Adresiniz için havuz hızı başka yere yazılan zamanı zaten yansıtabilir. Öyleyse hesaplayıcıda ek madenci ücreti için sıfır girin. Bu aritmetiği değiştirir, yazılımın politikasını değil. Brüt uygun ödül tahmininde havuz ücreti hâlâ geçerlidir; her sayının kapsamını kontrol edin.

## Bakiyeden neden tekrar ücret düşülmez?

Onaylanmış bakiye muhasebe sonucudur, dokunulmamış brüt tahmin değildir. Havuz alacakları zaten atamıştır. Bakiye veya kaydedilmiş ödemeden aynı ücreti yeniden düşmek yeni ücret olayı olmadan sayıyı değiştirir. Mutabakat için gösterilen tutarı ve aşamayı kullanın.

Bekleyen ödül, onaylanmış bakiye ve gönderilmiş ödeme zaman olarak da ayrılır. Blok olgunlaşması ve ödeme eşiği geçişleri açıklar, ek yüzde değildir. [Onay rehberini](/blog/tr/block-confirmations-mining-rewards/) ve [ödeme geçmişini](/payouts) izleyin. Varsa cüzdan işlem ücreti ayrı işlem alanıdır.

## Tahminin girdilerini kaydedin

Tarih, sürüm, ücretler, hashrate kaynağı ve süreyi saklayın. Hesaplayıcı canlı havuz/ağ verilerini girdilerinizle birleştirir; tekrar bakarken ağ verilerini yenileyin. Gerçek alacaklar uygun paylar, bulunan bloklar ve PPLNS'ye de bağlıdır. Formül ve hesap kaydı farklı soruları yanıtlar.

[My Stats](/stats) ve [takip rehberini](/blog/tr/track-your-miner-on-bntpool/) kullanın. Ücret değiştirmeden aynı adres, dönem ve aşamayı karşılaştırın. Hesaplayıcı BNT ödülünü tahmin eder; elektrik, donanım ve nakit fiyat dışarıdadır. Cüzdan adresini değiştirmek gerekmez.

<CTA target="pool" title="Ödülleri bntpool.com&#x27;da takip edin" text="Tahminler için hesaplayıcıyı; kayıtlı iş, bakiye ve ödemeler için My Stats&#x27;ı kullanın." />
