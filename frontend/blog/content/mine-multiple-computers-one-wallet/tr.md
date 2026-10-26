---
title: 'Bir cüzdana birden çok bilgisayarla Blocknet kazın'
description: 'bntpool.com''da aynı cüzdana ikinci Blocknet madencisini ekleyin. Bilgisayarları adlandırın, payları doğrulayın, ortak ödülleri ve yinelenen adları öğrenin.'
tags: ['Madencilik rehberi', 'Bilgisayarlar', 'Kurulum']
faq:
  - q: 'Her bilgisayara ayrı cüzdan gerekir mi?'
    a: 'Hayır. Her bilgisayar bntpool.com''da aynı alım adresini kullanabilir; farklı makine adları kurulumları ayırır. Havuz ödülleri adres bazında hesaplar. Madencilik makinelerine yalnızca alım adresi veya Blocknet ID gerekir, cüzdan kurtarma ifadesi gerekmez.'
  - q: 'İki madenci aynı adı kullanırsa ne olur?'
    a: 'Aynı adres altında pay geçmişleri aynı isimle gruplanır. Bu, ölçümün hangi makineden geldiğini anlamayı zorlaştırır. Her kuruluma farklı ad verip bu adla yeniden başlatın; önceden kaydedilmiş paylar eski etiketlerini korur.'
  - q: 'Yeni makine eklemek ödemeyi böler mi?'
    a: 'Makine adları etkinlik görünümünü düzenler, aynı adres altında ayrı ödül bakiyeleri oluşturmaz. Bilgisayarların geçerli işi o adresin PPLNS katılımına eklenir. Bakiye havuzun normal onay ve ödeme kurallarına tabi olmaya devam eder.'
sourceHash: 'a07a17533ad2'
translation: draft
---

Tek alıcı cüzdanı koruyarak bntpool.com'a ikinci bilgisayar ekleyin. Her kurulumu adlandırın, ayrı doğrulayın, sonra ortak adresi izleyin. Bu rehber yeni makineyi ve yinelenen adları ele alır. İlk madenci için önce [havuz kurulum rehberini](/blog/tr/mine-blocknet-on-bntpool/) tamamlayın.

## Birden çok bilgisayar aynı cüzdana kazabilir mi?

<Answer>
Evet. Her bilgisayarda seine'i aynı alım adresi ve farklı makine adıyla çalıştırın. bntpool.com işi ve ödülleri adres bazında kaydeder; adlar istatistiklerde etkinliği ayırır. Ortak adres görünümüne güvenmeden önce her makineden kabul edilen payları doğrulayın.
</Answer>

*Ayrı bilgisayarlar havuz üzerinden tek alım adresine iş gönderir; her biri farklı adını korur.*

## Alım adresi ve ad düzenini seçin

Cüzdan alım adresini veya onu gösteren Blocknet ID'yi kullanın. Her madencide çözümlenen adresi kontrol edin. Ad girişi kolaylaştırır, ancak hesaplar sonuçtaki adrese bağlıdır. ID hedefini değiştirirseniz çalışan madenci yeniden başlatılana kadar eski çözümlemeyi tutabilir. Önceden kazanılan ödüller ilk adreslerinde kalır.

[Madencilik cüzdanı rehberi](https://seineminer.com/tr/blog/blocknet-wallet-for-mining/) adres edinmeyi açıklar. Makinelere yalnızca bu açık hedef gerekir; kurtarma ifadesini ve özel anahtarları cüzdanda tutun. `desktop-01` ve `server-02` gibi kısa adlar seçin. Havuz dıştaki boşlukları temizler ve en fazla 64 karakter tutar. Ayırt edici kısmı uzun adın başına koyun.

## İkinci madenciyi kendi adıyla ekleyin

İkinci bilgisayara uygun seine sürümünü kurup çıkarılmış klasörden çalıştırın. İlk PowerShell örneği alıcı adını, havuzu ve makineyi açıkça belirtir. Çalıştırmadan önce `@yourname` yerine kendi ID'nizi veya tam adresinizi yazın. Tırnakları koruyun ve bu makinenin önceden denenmiş backend ya da ayarlama seçeneklerini ekleyin.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Linux veya macOS'ta aşağıdaki programı farklı makine adıyla kullanın. İşletim sistemleri farklı olabilir; alım adresi aynı olmalıdır. Her örnek gerçek madencilik başlatır. Uygun örneği doğru bilgisayarda çalıştırıp normal başlangıç ve ayarlamanın bitmesini bekleyin. İki örneği aynı bilgisayarda çalıştırmak gerekmez.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Hangi ayarlar bilgisayarlar arasında ortaktır?

Alım adresini ve havuz uç noktasını paylaşın. Her kurulum için ayrı yerel veri klasörü ve ad tutun. seine bağlantıyı veri dizinindeki `seine.config.json` dosyasına kaydeder; varsayılan `./seine-data` başlangıç klasörüne bağlıdır. Başka klasör farklı yapılandırmayı bulabilir. Sabit klasör bakımı kolaylaştırır.

İlk bilgisayarın yapılandırmasını kopyalarsanız ikincinin başlatma komutunda farklı ad belirleyip başlangıçta doğrulayın. Sonraki komutlarda da açık adı koruyun: komut satırı geçersiz kılması kayıtlı adı yeniden yazmaz. Donanım seçimlerini makineye uyarlayıp yerel ayarlamaya izin verin. Adres argümanı ödeme hedefini, ad argümanı etiketi belirler; örneklerdeki gibi ayrı girin.

## İkisinin de katkısı nasıl doğrulanır?

bntpool.com'da My Stats'i açıp ortak adresi sorgulayın. İki adı ve her birinden yeni payı bulun. Yerel madencilerde kabul edilen gönderimleri de kontrol edin. Bilgisayarları tek tek incelemek süreci havuz satırıyla eşleştirir; yalnızca toplam hash gücüne bakmaktan daha kolay hata yakalarsınız.

Havuz hash gücünü gönderilen paylardan tahmin eder. Yeni makinenin anlamlı okuma için zamana ve kabul edilmiş işe ihtiyacı olabilir. Yerel hız ve havuz tahmini farklı ölçümler ve zaman aralıklarıdır. Önce yeni paylar ve kabul sayaçlarıyla etkinliği doğrulayın, sonra uzun dönemleri karşılaştırın. [İstatistik rehberi](/blog/tr/track-your-miner-on-bntpool/), bağlantı sayısından cihaz sayısı çıkarmadan kartları ve satırları açıklar.

## Makine görünmüyorsa veya adlar aynıysa ne yapılır?

Aynı adres altındaki eşit adlar payları gruplar. Bir madencinin ad seçeneğini değiştirip onu yeniden başlatın. Yeni gönderimler yeni etiketi kullanır; eski kayıtlar önceki adı tutar. Eski satır rapor döneminde kalabilir. Yeniden adlandırma adresin kazanılmış bakiyesini taşımaz.

Görünmeyen makinenin gerçek çözümlenmiş adresini, uç noktasını ve adını açtığınız sayfayla karşılaştırın. Bağlantı ve kabul edilen payları kontrol edin. Aynı seine sürecindeki CPU ve GPU bu kurulumda ayrı ad gerektirmez. Reddedilen veya eski paylarda adresi sürekli değiştirmek yerine [pay durumu rehberini](/blog/tr/accepted-rejected-stale-shares/) izleyin.

## Ortak ödüller ve ödemeler nasıl işler?

Bilgisayarların geçerli işi aynı adresin PPLNS penceresindeki katılımına katkıda bulunur. Adlar katkıyı izletir, ayrı bakiye yaratmaz. Ek makine cihaz veya ad başına sabit ödül değil, gerçekten gönderdiği yararlı işi ekler. Donanım ve çalışma süresi farklı katkılar sağlayabilir; yalnızca daha fazla ad işi artırmaz.

[PPLNS rehberi](/blog/tr/how-pplns-payouts-work/) blok bulunduğundaki dağılımı açıklar. Onay ve ödeme kuralları ortak adres bakiyesinde geçerlidir. Cüzdanları birleştirmek veya adlar arasında bakiye aktarmak gerekmez. Makineleri çalışır bırakmadan her birinde hedefi, farklı adı ve kabul edilen payları doğrulayın. Sonraki bilgisayarda da aynı listeyi kullanın.

<CTA target="pool" title="bntpool.com&#x27;a başka bilgisayar bağlayın" text="Tek alım adresi kullanın, her kuruluma farklı ad verin ve kabul edilen payları doğrulayın." />
