---
title: "Mal ve hizmetler için Blocknet ödemesi kabul edin"
description: "Siparişler için BNT kabul edin, Blocknet cüzdanındaki ödemeleri eşleştirin, onayları kontrol edin ve kısmi ödemelerle iadeleri açık bir süreçle yönetin."
tags: [Blocknet, BNT]
faq:
  - q: "Müşterinin Blocknet ile ödeme yaptığını nasıl anlarım?"
    a: "Kendi eşitlenmiş cüzdanınızda gelen ödemeyi bulun, tutarı ve sipariş referansını karşılaştırıp onay durumunu kontrol edin. Tam işlem kimliğini siparişle saklayın. Müşteri ekran görüntüsü veya ödeme bağlantısı tek başına alındığını kanıtlamaz; gezgin de adresinize ödenen özel tutarı göstermez."
  - q: "Müşteri istenenden az BNT gönderirse ne yapmalıyım?"
    a: "Siparişi kısmen ödenmiş bırakıp kalan tutarı doğruladığınız ödemelerden hesaplayın. Aynı referansla farkı isteyin ve iki işlem kimliğini de kaydedin. Her alımı bir kez sayın. İptal konusunda anlaşırsanız iade adresini mevcut sipariş görüşmesinden doğrulayın."
  - q: "Memo siparişi otomatik olarak ödenmiş işaretler mi?"
    a: "Hayır. Memo ödemeyi eşleştirmeye yardım eder, ancak ödeyen değiştirebilir veya atlayabilir. Tutarı ve işlem durumunu kendiniz kontrol edin. Cüzdan talepler oluşturup gelen ödemeleri gösterir; bu manuel süreç otomatik ödeme sayfası veya sipariş yönetimi eklemez."
sourceHash: '3309a069e5de'
translation: draft
---

Müşteriye net bir BNT tutarı vererek, ödemeyi kendi cüzdanınızda kontrol ederek ve siparişle kaydederek Blocknet kabul edin. Örneğimiz **order-1042** referanslı, **1.25 BNT** tutarında hayali bir sipariş. Yayımlanmış masaüstü cüzdan v0.22.0 kullanılıyor; ekranlar işaretli demo verileri içeriyor.

## Blocknet ödemesini nasıl kabul edersiniz?

<Answer>
BNT tutarı ve sipariş referansı üzerinde anlaşın, kontrol ettiğiniz adrese ödeme talebi gönderin ve eşitlenmiş cüzdanınızda gelmesini bekleyin. Tutarla referansı eşleştirin, onayları kontrol edin, işlem kimliğini kaydedin ve müşteriyle kararlaştırdığınız onay politikasına göre siparişi ödenmiş işaretleyin.
</Answer>

Cüzdan oluşturmak veya açmak için [masaüstü cüzdan rehberini](https://seineminer.com/tr/blog/blocknet-desktop-wallet/) kullanın. Kurtarma yedeğini sipariş kayıtlarından ayrı tutun. Müşteri ödemesi almak için madenci çalıştırmak veya havuza katılmak gerekmez.

## Tutarı belirleyip siparişi tanımlayın

Referansı, BNT tutarını, alıcı adresini ve alınan ürünü yazın. Başka para biriminde fiyat veriyorsanız ödeme öncesinde BNT tutarıyla teklif süresini kararlaştırın. Bağlantı istenen tutarı taşır; kuru güncellemez veya son tarihi zorunlu kılmaz.

Örnekte **1.25 BNT** isteyin ve memo olarak **order-1042** kullanın. Sipariş numarası yeterlidir; teslimat adresi ve müşteri bilgileri özel sisteminizde kalsın. [Şifreli memo](https://seineminer.com/tr/blog/blocknet-encrypted-memos/) ödemeyle gider; paylaşılan bağlantıya yazılan memo ise bağlantıyı alan herkesçe okunur.

**Receive → Payment Request** içinde tutar ve memoyu yazıp **Generate Payment Link** seçin. Paylaşmadan önce alıcı ve tutarı kontrol edin. [Ödeme bağlantısı rehberi](https://seineminer.com/tr/blog/blocknet-payment-links/) biçimleri ve isimle alıcıları açıklar. Sabit bir fatura için tam alıcı adresini taleple saklayın.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Yayımlanmış Blocknet cüzdanında 1.25 BNT talebi, order-1042 memosu ve demo adresi." loading="lazy" decoding="async"></a>
<figcaption>Demo verilerle gerçek İngilizce arayüz. Talep oluşturmak para taşımaz. Büyütmek için görseli açın.</figcaption>
</figure>

## Kendi cüzdanınızdaki alımı kontrol edin

Müşteriden referansı eklemesini ve işlem kimliğini saklamasını isteyin. Düğüm ve cüzdan güncele geldiğinde **History** üzerinden gelen ödemeyi bulun. Tutar ve memoyu siparişle karşılaştırın. Ödeyen iki alanı da değiştirebilir; talep ne gönderildiğini kanıtlamaz.

Tam işlem kimliğiyle alınan tutarı kaydedin. İşlem Dashboard ve History içinde göründüğü veya müşteri aynı kimliği tekrar gönderdiği için iki kere saymayın. Genel bakiye değişimini değil belirli alımları eşleştirin: bakiyede madencilik, diğer müşteriler ve kendi transferleriniz olabilir.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="Blocknet cüzdanının History sayfasında order-1042 memolu 1.25 BNT demo alımı." loading="lazy" decoding="async"></a>
<figcaption>Örnek alım tutar ve referansla eşleşiyor. Siparişi yerine getirmeden önce işlem durumunu kontrol edin.</figcaption>
</figure>

Alım yoksa müşteriyle tam hedef adresi ve işlem kimliğini karşılaştırın, ardından eşitlemeyi kontrol edin. Gönderim ekran görüntüsü bağlam sağlar; alınan tutarı sizin alıcı cüzdanınız belirler.

## Sipariş ne zaman ödenmiş sayılmalı?

Onaysız gelen ödeme, bir bloğa giren işlem ve harcanabilir fonları ayırın. Yayımlanmış cüzdan, normal ödemenin bulunduğu bloktan sonra on blok daha bekleyerek çıktıları harcanabilir yapar. [Transfer süresi rehberi](https://seineminer.com/tr/blog/blocknet-transfer-time/) sayaçları ve değişken beklemeyi açıklar.

Onay politikasını ödemeden önce bildirin. Basit manuel akış, siparişi tamamlamak için eşleşen alımın harcanabilir olmasını bekler. Sadece gelen tutarın görünmesi kontrolü tamamlamaz. Cüzdan sayacına göre kesin teslimat saati vermeyin; blok aralıkları değişir. [Madenciliğin ödemeleri nasıl koruduğu](/blog/tr/how-mining-secures-blocknet-payments/) onay derinliğini açıklar.

## Kısmi, fazla veya eşleşmeyen ödemeleri yönetin

| Doğruladığınız alım | Sonraki adım |
|---|---|
| 1.25 BNT siparişi için 1.00 BNT | Kısmi ödeme kaydedip kalan 0.25 BNT’yi isteyin. |
| Sonradan gelen 0.25 BNT | İki kimliği saklayın, toplam 1.25 BNT’yi bir kez sayın ve ikisinin onaylarını kontrol edin. |
| 1.25 BNT’den fazla | İade veya kabul edilmiş alacak konusunda anlaşıp kararı kaydedin. |
| Doğru tutar, eksik memo | Atamadan önce tam kimlik ve hedefi müşteriyle doğrulayın. |
| Bekleyen ödeme | Siparişi onay bekliyor tutup durumu yeniden kontrol edin. |

İade için mevcut sipariş görüşmesinde alıcı adresini onaylatın. Açık işlemden geri dönüş adresi çıkarmayın: Blocknet girdileri geleneksel gönderici adresi sunmaz. Yeni gönderimi onaylamadan önce tutar, hedef ve ücreti inceleyin; kimliğini ilk ödemeyle saklayın.

## Yalnızca gereken ödeme kanıtını paylaşın

Gezgin işlemin bloğa alınmasını kontrol etmeye yardım eder, fakat adresinize gelen özel tutarı açığa çıkarmaz. Alım tartışılırsa gönderici [ödeme kanıtı rehberine](https://seineminer.com/tr/blog/blocknet-payment-proof/) göre işleme özel kanıt sunabilir. Alıcı eşleşmelerini, tutarı ve zincir durumunu kontrol edin; anahtarın geçerli olması tek başına yetmez.

Sipariş, talep, gelen kimlikler, doğrulanmış tutarlar, onay kararı ve iadeyi özel kayıtlarınızda birlikte tutun. Yalnız ilgili makbuz veya kanıtı paylaşın. Kurtarma kelimelerini, parolaları veya tüm cüzdanın özel görüntüleme anahtarını ödeme kanıtı olarak asla göndermeyin.

BNT de kazıyorsanız aynı cüzdanda bile [havuz ödeme kayıtlarını](/blog/tr/track-your-miner-on-bntpool/) siparişlerden ayırın. Hangi transferin hangi faturayı ödediği böylece anlaşılır.


<CTA target="pool" title="Madencilik ödüllerini de alın" text="Havuz rehberi bir madenciyi bağlamayı ve ödemelerini diğer Blocknet alımlarıyla birlikte izlemeyi açıklar." />
