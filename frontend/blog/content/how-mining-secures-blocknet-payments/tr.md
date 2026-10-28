---
sourceHash: 'e64d7723349d'
translation: draft
title: Madencilik Blocknet ödemelerini nasıl korur?
description: Blocknet'in iş kanıtıyla ödeme geçmişini nasıl seçtiğini, onayların nasıl iş eklediğini ve düğümlerin blokları kabul etmeden neleri doğruladığını öğrenin.
tags: [Blocknet, Ödeme güvenliği, İş kanıtı]
faq:
  - q: Daha fazla madencilik gücü cüzdanımdan ödeme yetkilendirebilir mi?
    a: Madencilik gücü rakip bloklar üretmeye yardımcı olur. Fonları harcamak yine geçerli yetkilendirme gerektirir; düğümler bir bloğu kabul etmeden işlem kanıtlarını ve hesap dengesini denetler. Daha ağır dal da bu kurallara uymalıdır. İş kanıtı, çıktılarınızı harcamak için gereken özel anahtarların yerini almaz.
  - q: Blocknet en çok bloğa sahip zinciri mi izler?
    a: Yayımlanan core v0.22.0, her bloğun zorluğunu toplayarak birikimli işi izler. Daha fazla işi olan geçerli dal, yeniden düzenleme kurallarına bağlı olarak seçilen zincir olabilir. Blokların zorlukları farklı olabileceğinden yalnızca sayıları yeterli değildir. Hem iş hem geçerlilik önemlidir.
  - q: Ödeme onayları ile havuz payları aynı şey midir?
    a: Havuz payı, havuza gönderilen madencilik işini kaydeder. Ödeme onayı ise işlemin seçilen zincire dahil edilmesi ve ardından gelen bloklarla ilgilidir. Paylar ödül dağıtımına yardımcı olurken ağ blokları ödeme geçmişini ilerletir. Pay sayınızı artırmak belirli bir ödemeye doğrudan onay eklemez.
---

Bir BNT ödemesini bloğa dahil edilmesinden sonraki madencilik işiyle desteklenen geçmişe kadar izleyin. Bu rehber ağın neden rakip geçmişler arasında seçim yapması gerektiğini, onayların ne anlattığını ve bu seçimin gizli işlem doğrulamasıyla nasıl birlikte çalıştığını açıklar. Anlatılan uygulama, 28 Ekim 2026'da kontrol edilen yayımlanmış core v0.22.0 sürümüdür.

## BNT gönderirken madencilik neyi korur?

<Answer>
Madencilik, ağın ortak ve sıralı bir ödeme geçmişi seçmesine yardımcı olur. Kabul edilen her blok önceki bloğa bağlanır ve iş kanıtı ekler. Düğümler önerilen blokları ve işlemleri denetler, ardından geçerli dalları yeniden düzenleme kuralları içinde birikimli işe göre karşılaştırır. Sonraki bloklar dahil edilmiş ödemenin arkasına iş ekleyerek onay geçmişine pratik anlam kazandırır.
</Answer>

Geçerli bir ödeme hem yetkilendirme hem de defterde bir yer gerektirir. Cüzdanınız fonlarınızı harcamak için yetkilendirme sağlar. Madencilik ve zincir seçimi, düğümlerin hangi geçerli işlemlerin mevcut geçmişe ait olduğu konusunda anlaşmasına yardımcı olur. Bu görevler birlikte çalışır: bloğun iş kanıtı, geçersiz bir ödemeyi kabul edilebilir yapmaz.

[Kural uygulama rehberi](https://seineminer.com/tr/blog/who-controls-blocknet/) kimin geliştirdiğini, önerdiğini ve doğruladığını açıklar. Burada odak ödemenin kendisidir: cüzdanınız ve diğer düğümlerin izlediği geçmişe nasıl dahil olduğu.

## Bloklar bir ödeme geçmişini nasıl bağlar?

Blok başlığı, önceki bloğun özetini ve işlemlerden türetilen Merkle kökünü içerir. Kök, blok içeriğini kriptografik olarak bağlar; önceki özet ise eski geçmişe bağlantı kurar. Bir işlemi değiştirmek, kökü ve üzerinde madencilik yapılan başlığı değiştirir. Sonraki blokların da kendi bağlantılarıyla bu değişen geçmişi referans alması gerekir.

Bir bloğa dahil edilmiş, ardından birkaç kabul edilmiş blok gelmiş bir ödemeyi düşünün. Geçmişin bu kısmını değiştirmek, düğümün yeniden düzenleme kuralları içinde yeterli birikimli işe sahip, birbirine bağlı geçerli bloklardan oluşan rakip dal gerektirir. Yerel dosyada bir işlemi düzenlemek bu dalı üretmez. İlgili iş, belirli başlık ve işlem dizisine aittir.

[Yayımlanan blok kodu](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) bu başlık alanlarını, Merkle kontrollerini ve zincir seçim yollarını tanımlar. [Zorluk rehberi](/blog/tr/mining-difficulty-explained/) ağın her yeni blok için gereken işi nasıl belirlediğini açıklar.

## Birikimli iş neden blok sayısından daha önemlidir?

İki madenci aynı üst bloğu uzatan farklı geçerli bloklar bulabilir. Her önerinin ayrı özeti vardır ve düğümler bunları farklı sıralarda duyabilir. Böylece kısa süreli rakip dallar oluşabilir. Ağ, belirli bir eşin duyuru sırasını değil, geçmişi destekleyen işi izleyen bir seçim kuralına ihtiyaç duyar.

Core v0.22.0, bloğun zorluğunu üst bloğun birikimli işine ekler. Doğrulamadan sonra kesin olarak daha fazla işi olan dal, seçilen zincirin değişmesini tetikleyebilir. Eşit işli alternatif bu karşılaştırmayla değişim başlatmaz. Zorluk da dalın üst blok bağlamında geçerli olmalıdır; madenci başlığa yalnızca daha büyük sayı yazamaz.

Örneğin ortak başlangıç bölümü olan iki dalı karşılaştırın. Ortak bölüm iki toplama aynı işi ekler. Belirleyici fark, ayrıldıktan sonra biriken iş ile geçerlilik ve yeniden düzenleme kontrolleridir. Zorluğu dikkate almadan blokları saymak bu farkı gözden kaçırabilir. Bu kavramsal bir karşılaştırmadır, canlı bir çatallanmanın raporu değildir.

## Onaylar ödemenin ilerleyişini nasıl gösterir?

Ödeme seçilen zincire dahil olduktan sonra yeni bloklar bu geçmiş üzerine kurulur. Bloklar geldikçe derinliği artar. Dahil edilmesini değiştirmek, ilgili bloktan önce başlayan uygun alternatif geçmiş gerektirir; mevcut dal da iş biriktirmeye devam eder. Bu yüzden dahil edilme ve sonraki derinliği kontrol etmek, işlemin yayınlandığını görmekten daha anlamlıdır.

Yükseklik bir konumu, özet belirli bir bloğu tanımlar. Ödeme kaydını ve bulunduğu bloğu kontrol edip bloğun seçilen zincirde kaldığını doğrulayın. Seçilen geçmiş değişirse dahil edilme ve derinlik de değişebilir. Onay sayaçları işlemi içeren bloğu dahil edebilir veya yalnızca sonraki blokları sayabilir; arayüzün yöntemini okuyun.

Core v0.22.0 ayrıca yeniden düzenleme sınırı uygular: ortak atası mevcut uç yüksekliğinin 100 eksiğinin altında olan geçişi reddeder. Bu, çalışan uygulamanın kuralıdır; görüntülenen sayaca bağlı evrensel bir vaat değildir. [Transfer süresi rehberi](https://seineminer.com/tr/blog/blocknet-transfer-time/) normal cüzdan harcanabilirliğini, [madencilik ödülü onayları](/blog/tr/block-confirmations-mining-rewards/) coinbase olgunlaşmasını ve havuz muhasebesini açıklar.

## Gizli ödemede tekrar harcama nasıl denetlenir?

Blocknet'in yayımlanan işlem biçimi, harcama kanıtına bağlı bir anahtar imgesi kullanır. Doğrulama bu bağı denetler ve ilgili zincir durumunda zaten harcanmış imgeyi reddeder. Böylece düğümler bir çıktının çelişen kullanımını algılar; halka imzası ise uygun halka üyelerinden hangisinin gerçek girdi olduğunu gizler. Gizli tutarlar yine denge ve aralık kontrollerinden geçer.

Bir sahibin aynı çıktıyı harcayan çelişkili işlemler yetkilendirdiğini düşünün. Kabul edilen geçerli geçmiş iki harcamayı birden içeremez. Zincir seçimi önemlidir çünkü rakip geçmişler farklı seçimleri içerebilir; işlem doğrulaması önemlidir çünkü her geçmiş harcama kurallarına uymalıdır. Daha fazla iş, geçerli imza yerine geçmez ve dengesiz işlemi muhasebe kontrolünden geçirmez.

Bu kontroller [yayımlanan işlem doğrulayıcısında](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go) bulunur. Daha geniş gizlilik mekanizmaları için [Blocknet gizlilik rehberini](https://seineminer.com/tr/blog/blocknet-privacy-features/) kullanın. Ödeme güvenliği geçerli harcamayı, tutarlı geçmişi ve geçmişin açığa vurduklarını koruyan kriptografiyi birleştirir.

## Havuz madenciliği ödeme güvenliğinde nereye oturur?

Havuz, madencilerin blok aramasını koordine eder ve çözülen adayı bağlı olduğu düğüme gönderir. Havuzun daha kolay hedefini karşılayan pay katkı işini kaydeder; yalnızca ağ hedefini karşılayan çözüm doğrulamadan sonra ağ bloğu olabilir. [Havuz açıklaması](/blog/tr/what-is-a-mining-pool/) bu iki hedefi ve amaçlarını ayırır.

Madenciniz ödeme geçmişini uzatan sürece katkı sağlayabilir; havuz ise işinizi ayrı hesaplar. Özel havuz bakiyesi, pay sayısı ve cüzdanda onaylanmış ödeme farklı şeyleri anlatır. Katılmak için [havuz bağlantı rehberiyle](/blog/tr/mine-blocknet-on-bntpool/) başlayın; belirli bir BNT ödemesini izlerken cüzdanınızdaki işlem kaydını kullanın.

<CTA target="pool" title="Blocknet'e iş katkısı sağlayın" text="seine'ı bntpool.com'a bağlayın; madencilik işinizi, ödüllerinizi ve ödemelerinizi havuzun herkese açık araçlarıyla izleyin." />
