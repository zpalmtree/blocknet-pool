---
title: 'Blocknet madencilik cüzdan adresini değiştirme'
description: 'Blocknet madencilerinizin alıcı adresini değiştirin, bntpool.com’da yeni payları doğrulayın ve eski adreste kalan önceki ödüllerinizi takip edin.'
tags: ['Madencilik rehberi', 'Cüzdan', 'Ödemeler']
faq:
  - q: 'Adres değişikliği eski bakiyeyi taşır mı?'
    a: 'Yeni hedef, gelecekte gönderilen işi yeni adrese yönlendirir. Mevcut paylar, ödül kredileri ve ödemeler özgün adreste kalır. İstatistiklerim bölümünde iki adresi ayrı arayın ve kalan ödülleri takip ederken eski cüzdana erişimi koruyun.'
  - q: 'Her bilgisayarın adresini değiştirmeli miyim?'
    a: 'Evet. Her süreç kendi yapılandırılmış hedefini kullanır. Önce bir bilgisayarı güncelleyip doğrulayın, ardından kayıtlı dosya ve başlangıç argümanlarıyla diğerlerini değiştirin. Farklı cihaz adları, yeni adrese iş göndermeye başlayan makineleri tanımayı kolaylaştırır.'
  - q: 'Blocknet ID değişikliği çalışan madenciyi günceller mi?'
    a: 'seine kayıtlı ID’yi başlangıçta çözümler ve yeniden bağlantılar dahil çalıştırma boyunca adresi korur. Eşlemeyi değiştirdikten sonra her madenciyi yeniden başlatıp tam adresi kontrol edin. Önceki adrese kaydedilmiş iş ve bakiyeler yeniden atanmaz.'
sourceHash: 'fca912d46d55'
translation: draft
---

Önceki ödüllerin kaydını koruyarak gelecekteki işin hedefini değiştirin. Rehber, bntpool.com’a bağlı çalışan seine kurulumunu kullanır. İlk bağlantı için [havuz rehberine](/blog/tr/mine-blocknet-on-bntpool/), yeni cüzdan için [cüzdan rehberine](https://seineminer.com/tr/blog/blocknet-wallet-for-mining/) bakın.

## Madencilik adresi nasıl değiştirilir?

<Answer>
Bir madenciyi durdurun, alıcı adresini kayıtlı dosyada veya normal komutta değiştirip yeniden başlatın. Çözümlenen tam adresi ve kabul edilen payları kontrol edin, ardından adresi bntpool.com’da arayın. Diğer cihazlarda tekrarlayın. Eski paylar ve ödüller kaydedildikleri adreste kalır.
</Answer>

*Yeni iş ikinci cüzdana gider; eski ödül kayıtları ilk cüzdana bağlı kalır.*

## Değişimden önce iki hedefi kaydedin

Yeni adresi doğrudan kullanacağınız cüzdandan kopyalayın. Madenciye genel alıcı adresi veya kayıtlı Blocknet ID gerekir; kurtarma ifadesi ya da özel anahtar gerekmez. Eski tam adresi de saklayın: önceki havuz etkinliğinin arama anahtarıdır.

Eski adresin İstatistiklerim sayfasında bekleyen ödülleri, onaylı bakiyeyi, son ödemeleri ve aktif cihaz adlarını not edin. Bu geçiş için referanstır; yeni sayfa eski geçmişi içermez. Kalan ödülleri ve ödemeleri izlerken eski cüzdana erişimi koruyun.

## Bir madenciyi ve başlangıç komutunu güncelleyin

Madenciyi durdurup normal başlangıcını inceleyin. Mevcut yapılandırmada `--address`, kayıtlı adresi değiştirmeden öncelik alır. PowerShell örneği bu çalıştırmanın hedefini seçer. `@yourname` yerine yeni ID veya tam adresinizi yazın; sınanmış havuz, veri yolu ve donanım argümanlarını koruyun.

```powershell
.\seine.exe --address "@yourname"
```

Linux veya macOS’ta normal dizininizden aşağıdaki sözdizimini kullanın. İki örnek de gerçek madencilik başlatır; sisteminize uygun olanı seçin. Mevcut kurulum diğer bağlantı ayarlarını sağlar. Yeni adreste paylarını tanımak için cihaz adını aynı tutun.

```sh
./seine --address "@yourname"
```

## Sonraki normal başlangıçta yeni adresi kullanın

Madenci durmuşken `seine.config.json` içindeki `address` alanını düzenleyin. Diğer alanları ve geçerli JSON’u koruyun. `./seine-data` çalışma dizinine göredir; `--data-dir` başka konum seçebilir. Normal başlatıcının gerçekten kullandığı dosyayı düzenleyin.

Kısayol, betik, görev veya hizmette eski `--address` argümanını güncelleyin ya da kaldırın. Başarılı elle komut diğer başlatıcıları değiştirmez. Normal yeniden başlatıp doğrulayın. [Yapılandırma rehberi](https://seineminer.com/tr/blog/configure-seine-miner/), öncelikleri ve farklı çalışma dizininin başka dosyayı seçmesini açıklar.

## Blocknet ID kullanırken ne değişir?

seine v0.2.36, `@name` değerini başlangıçta çözümleyip doğrular. Tam adres yeniden bağlantılar dahil bu çalıştırmada sabittir. Ad eşlemesini değiştirirseniz her madenciyi yeniden başlatıp hedefi kontrol edin. Mevcut sürecin yeniden bağlanması başlangıç çözümlemesini tekrarlamaz.

Okunabilir ad aynı kalsa bile eski ve yeni tam adresi not edin. Muhasebe çözümlenen adresi izler; eşleme değişikliği eski payları veya bakiyeleri yönlendirmez. Ad çözümlenemiyorsa eşlemeyi kontrol edin ya da cüzdanda doğruladığınız tam adresi bilinçli olarak kullanın.

## Her bilgisayarı yeni adreste doğrulayın

Hedefi ve kabul edilen gönderimleri kontrol edip tam yeni adresi İstatistiklerim’de arayın. Cihaz adıyla son payları eşleştirin. Yeni hash gücü tahmini iş ve zaman gerektirir; önce payların geldiğini doğrulayın. [İstatistik rehberi](/blog/tr/track-your-miner-on-bntpool/) sayaçları ve görünmeyen cihazları açıklar.

İlk bilgisayardan sonra diğerlerini değiştirin. Benzersiz adlar kontrolü kolaylaştırır. Eski başlatıcının eski hedefi geri getirmemesi için her makinenin normal başlangıcını da deneyin. [Çoklu bilgisayar rehberi](/blog/tr/mine-multiple-computers-one-wallet/), cihazları tek alıcı adresi altında düzenlemeyi açıklar.

## Eski adresteki ödüller ne olur?

Mevcut krediler ve ödemeler eski adreste kalır. Önceki uygun paylar ilgili PPLNS pencerelerine katkı yapabilir; bekleyen blok ödülleri onay kurallarını izler. Hash gücünün sıfır olması muhasebenin bittiğini göstermez. [PPLNS](/blog/tr/how-pplns-payouts-work/) ve [onay rehberleri](/blog/tr/block-confirmations-mining-rewards/), işi, olgunlaşan ödülleri ve ödemeleri ayırır.

Hedef değişikliği bakiyeleri birleştirmez veya ödeme alt sınırını atlamaz. Güncel kuralları ve kalan tutarlar anlaşılana kadar iki sayfayı kontrol edin. Cihazı yeniden adlandırmak da ödül taşımaz. Son olarak tüm hedeflenen madencilerin yeni adrese yeni iş gönderdiğini doğrulayın ve eski cüzdan ile geçmişi koruyun.

<CTA target="pool" title="Kurulumunuzu bntpool.com’da kontrol edin" text="Her cihazın hedefini ve kabul edilen işini doğrulayın; eski ve yeni ödülleri ayrı takip edin." />
