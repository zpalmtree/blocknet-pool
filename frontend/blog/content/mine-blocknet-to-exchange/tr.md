---
title: Blocknet'i doğrudan bir borsaya kazın
description: 'bntpool.com''da Blocknet''i doğrudan SafeTrade veya başka bir borsanın yatırma adresine kazın. seine''e yatırma notunuzu ekleyin, havuz hesabınıza ödesin.'
tags: [Madencilik rehberi, bntpool, SafeTrade, Ödemeler]
sourceHash: '8f73aae25c7a'
translation: draft
faq:
  - q: 'Blocknet''i doğrudan bir borsaya kazabilir miyim?'
    a: 'Evet. seine v0.3.1 veya daha yenisini borsa yatırma adresiniz ve yatırma notunuzla (memo) başlatın, örneğin ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com ödüllerinizi notu ekleyerek o adrese öder, böylece borsa onları hesabınıza yazar.'
  - q: 'seine neden ödeme notu istiyor?'
    a: 'SafeTrade gibi borsalar tüm müşterileri için tek bir ortak yatırma adresi kullanır ve yatırmaları notla ayırt eder. seine bu ortak adresleri tanır; birini girdiğinizde notunuzu sorar ve adresle birlikte kaydeder. Notu borsanın yatırma sayfasından aynen kopyalayın.'
  - q: 'bntpool.com''da bir borsaya minimum ödeme nedir?'
    a: 'Havuzun 4 Kasım 2026 tarihli bildirimine göre notlu ödemeler 10 BNT''den başlar. Onaylanmış bakiyeniz bu tutara ulaşana kadar havuzda birikir, ardından havuz onu notunuzla borsa adresine gönderir. Kişisel bir cüzdana ödemeler 1 BNT''den başlar.'
  - q: 'Borsaya madencilik bakiyemi nasıl kontrol ederim?'
    a: 'bntpool.com istatistik sayfasında borsa yatırma adresini girin, Not ekle seçeneğini seçin ve notunuzu girin. Her adres ve not çiftinin kendi bakiyesi vardır; böylece aynı borsa adresini kullanan diğer müşterilerin değil, yalnızca kendi hash gücünüzü, cihazlarınızı ve ödemelerinizi görürsünüz.'
---

Bu rehber, bntpool.com'da Blocknet'i doğrudan borsa hesabınıza nasıl kazacağınızı gösterir. seine'e borsanın yatırma adresini ve yatırma notunuzu (memo) verirsiniz; havuz ödüllerinizi notu ekleyerek oraya öder, böylece borsa onları size yazar. Komutu, minimum ödemeyi ve bakiyenizi nasıl kontrol edeceğinizi anlatır.

## Blocknet'i doğrudan bir borsaya kazabilir misiniz?

<Answer>
Evet. bntpool.com madencilik ödüllerini yatırma notunuzu ekleyerek bir borsa yatırma adresine öder. seine v0.3.1 veya daha yenisini borsa adresi ve notunuzla başlatın: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Notlu ödemeler 10 BNT'den başlar ve borsa her birini hesabınıza yazar.
</Answer>

Kazdığınızı alıp satmayı planlıyorsanız bir borsaya kazmak size bir adım kazandırır. Ödüller doğrudan borsa hesabınıza gelir ve arada cüzdan transferi olmadan [BNT/USDT piyasasında](/blog/tr/sell-blocknet-bnt-safetrade/) satışa hazırdır.

SafeTrade gibi borsalar tüm müşterilerine aynı yatırma adresini verir ve yatırmaları, hesabınıza özel kısa bir kod olan notla ayırt eder. Notun önemi buradan gelir: bir ödemeyi size bağlayan odur. Havuz her adres ve not çifti için ayrı bir bakiye tutar, böylece ödülleriniz aynı borsayı kullanan diğer madencilerinkiyle asla karışmaz.

## seine'de borsaya madencilik nasıl kurulur?

<Answer>
BNT yatırma adresini ve yatırma notunuzu borsanın yatırma sayfasından kopyalayın. seine'i ikisiyle başlatın: adres için --address, not için --payout-memo. İlk çalıştırma isteminde bilinen bir borsa adresi girdiğinizde seine notu kendisi sorar ve kaydeder. Havuzun onayını günlükte kontrol edin, ardından her zamanki gibi kazın.
</Answer>

1. **Yatırma bilgilerinizi alın.** Borsada BNT yatırma sayfasını açın ve yatırma adresini ve notu kopyalayın. Notu aynen kopyalayın: büyük harfler ve baştaki sıfırlar önemlidir.
2. **seine'i ikisiyle başlatın.** macOS veya Linux'ta:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Windows'ta, PowerShell'de:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **seine'in kaydetmesine izin verin.** seine notu adresle birlikte kaydeder, böylece sonraki çalıştırmalar borsa hesabınıza ödemeye devam eder. İlk çalıştırma isteminde bilinen bir borsa adresi yazarsanız seine notu sorar ve ikisini de kaydeder.

Havuzun [Get Started sayfasında](/start) bu komutu sizin için oluşturan bir “Bir borsaya mı madencilik yapıyorsunuz?” bölümü vardır: adresi ve notu yapıştırın, sonucu kopyalayın. seine, SafeTrade'inki gibi bilinen ortak borsa adresleri için not ister. Hizmetler, betikler ve gözetimsiz çalışan diğer madenciler `--payout-memo` değerini komut satırında vermelidir, çünkü yanıtlanacak bir istem yoktur.

seine giriş yaparken notu havuzla kontrol eder. bntpool.com onaylar ve ardından madencilik başlar. Cihazlarınıza ad vermek için kişisel bir adreste olduğu gibi `--pool-worker rig-01` verin.

## Borsa ödemeleri nasıl yapılır?

Ödüller diğer her bntpool.com ödemesiyle aynı yolu izler. Paylarınız, havuzun altı saatlik PPLNS penceresinde bulduğu her bloktan pay kazanır ve her bloğun ödülü 60 bloktan sonra onaylanır. Onaylanmış bakiyeniz ardından havuzda birikir.

Havuzun `/api/info` uç noktası 4 Kasım 2026'da şu minimum ödemeleri bildiriyordu:

| Hedef | Minimum ödeme |
|---|---|
| Notlu borsa adresi | 10 BNT |
| Kişisel cüzdan | 1 BNT |

Onaylanmış bakiyeniz 10 BNT'ye ulaştığında havuz onu notunuzla borsa adresine gönderir. Borsa yatırmayı kendi onay sayısından sonra hesabınıza yazar; kaç onay gerektiği yatırma sayfasında yazar. [PPLNS ödemeleri nasıl çalışır?](/blog/tr/how-pplns-payouts-work/) havuz tarafını ayrıntılı açıklar.

Solo madencilik de notla çalışır. Borsa adresinin önüne `solo:` koyun ve `--payout-memo` değerini koruyun; havuz bulduğunuz her bloğun ödülünün tamamını borsa hesabınıza öder. [bntpool.com'da solo madencilik](/blog/tr/solo-mine-blocknet-on-bntpool/) solo ödülleri açıklar.

## Bakiyenizi nasıl kontrol edersiniz?

[İstatistik sayfasını](/stats) açın, borsa yatırma adresini yapıştırın, **Not ekle** seçeneğini seçin ve notunuzu girin. Sayfa ardından o adres ve not çifti için kendi hash gücünüzü, cihazlarınızı, bakiyenizi ve ödemelerinizi gösterir. Aynı borsanın diğer müşterilerinin kendi çiftleri ve kendi bakiyeleri vardır.

**Son Ödemeler** içindeki her ödeme, gezginde kendi işlemine bağlanır. Coinleri hesabınıza kadar izlemek için bunu borsadaki yatırma geçmişiyle karşılaştırın. [bntpool.com'da madencinizi takip edin](/blog/tr/track-your-miner-on-bntpool/) istatistik sayfasının geri kalanını açıklar.

## Kişisel bir cüzdana nasıl geri dönersiniz?

seine'i yeni adresinizle ve `--no-payout-memo` ile bir kez başlatın:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine yeni adresi notsuz kaydeder ve yeni paylar kişisel cüzdanınıza sayılır. Borsa çiftine zaten yazılmış bir bakiye o çiftte kalır ve minimuma ulaştığında ona ödenir; istatistik sayfasında takip edebilirsiniz. [Madencilik cüzdanı adresinizi değiştirin](/blog/tr/change-mining-wallet-address/) adres değişikliğinden sonra havuzun işi nasıl hesaba yazdığını açıklar.

## Hangi kurulumlar notu destekler?

Notlar, seine'in havuza doğrudan bağlantısını gerektirir; bu varsayılandır. seine'in daemon modu kendi düğümünüze karşı kazar ve düğümün cüzdanına öder, bu yüzden notu yoktur. Çiftlik proxy'si notları taşımaz, bu yüzden borsa cihazlarını havuza doğrudan bağlayın. seine yalnızca notunuzu onaylayan bir havuzda kazar, böylece işiniz her zaman seçtiğiniz hesaba ödenir.

<CTA target="pool" title="Borsa hesabınıza kazın" text="seine'i yatırma adresiniz ve notunuzla başlatın; bntpool.com ödüllerinizi 10 BNT'den itibaren doğrudan borsa hesabınıza ödesin." />
