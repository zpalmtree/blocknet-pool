---
title: 'Kabul edilen, reddedilen ve eski madencilik payları'
description: 'bntpool.com’da kabul edilen, reddedilen ve eski payları anlayın; hata mesajlarını okuyun ve pay zorluğunun madencilik istatistiklerini nasıl etkilediğini görün.'
tags: ['Madencilik rehberi', 'Paylar', 'Sorun giderme']
faq:
  - q: 'Kabul edilen pay blok bulduğum anlamına mı gelir?'
    a: 'Havuzun bir sonucu pay işleme kurallarına göre kabul ettiği anlamına gelir. Blok ayrı ağ hedefini sağlamalı ve daemon tarafından kabul edilmelidir. Ödüller blok keşfi, doğrulama ve ödeme sürecini izler. Pay onayı ödeme bildirimi değildir.'
  - q: 'Eski pay neden reddedilir?'
    a: 'Stale job, gönderimin artık uygun olmayan bir atamaya ait olduğunu belirtir; örneğin süresi dolmuş veya eski şablonun tolerans süresi aşılmıştır. Yeni işleri ve kabul edilen payların dönüşünü kontrol edin. Tekrar ediyorsa bağlantıyı ve madenci sürümünü inceleyin.'
  - q: 'Daha yüksek pay zorluğu ödülümü azaltır mı?'
    a: 'Zorluk her uygun payın temsil ettiği işi ve beklenen sıklığı değiştirir. Ham pay sayısı yerine eşdeğer dönemlerde zorluk ağırlıklı işi karşılaştırın. Ödül ayrıca PPLNS penceresine, uygun işe, bulunan bloklara ve doğrulama kurallarına bağlıdır.'
sourceHash: 5be830208e6c
translation: draft
---

Devam etmek, bağlantıyı kontrol etmek veya tekrarlanan retleri araştırmak için madenci günlüğündeki pay sonucunu kullanın. Bu rehber bntpool.com yanıtlarını ve sayaçların arkasındaki zorluğu açıklar. Gerekirse [havuz kurulumuyla](/blog/tr/mine-blocknet-on-bntpool/) başlayın; adres etkinlik göstermiyorsa [takip rehberini](/blog/tr/track-your-miner-on-bntpool/#zero-hashrate) kullanın.

## Kabul edilen madencilik payı nedir?

<Answer>
Pay, havuzun atadığı hedefe göre gönderilen madencilik sonucudur. Kabul yanıtı, gönderimin işleme akışına alındığını ve işin havuza ulaştığını gösterir. Blok keşfi, pay doğrulaması ve ödeme uygunluğu ayrı adımlardır. Tek başına kabul, ödeme anlamına gelmez.
</Answer>

Madenci paylar arasında birçok hash hesaplar. Çoğu pay hedefine ulaşmaz ve gönderilmez. Kabul sayacı tüm CPU veya GPU hash’lerini değil, gönderim sonuçlarını sayar. [Hash gücü rehberi](/blog/tr/what-is-hashrate/) yerel sayımla gönderilen işe dayalı havuz tahminini ayırır.

## Paylarla blokların farkı nedir?

Havuz bağlantıya işler ve pay zorluğu atar. Pay hedefini sağlayan sonuç o zorlukta işi gösterir. Blok adayı ayrıca ağ hedefini sağlamalı ve daemon kontrollerini geçmelidir. Pay bulmak zincire blok eklendiğini göstermez.

bntpool.com doğrulamada doğrulanmış ve geçici pay durumlarını kaydeder. Kabul edilen gönderim geçici olabilir; ödeme ağırlığı doğrulama ve uygunluk kurallarına tabidir. Kabulü bağlantı işareti olarak kullanın, kayıtlı iş ve ödülleri [İstatistiklerim](/stats) sayfasından izleyin. [PPLNS rehberi](/blog/tr/how-pplns-payouts-work/) blok sonrasını açıklar.

## Reddedilen ve eski paylar ne demektir?

Reddedilen gönderim ilgili kontrolden geçmemiştir. Nedeni sayaçtan daha kullanışlıdır. Stale job bir ret kategorisidir: atama artık uygun değildir. Tam mesajı yeniden bağlantı veya yeni iş gibi aynı andaki olaylarla karşılaştırın.

| Mesaj | Anlamı ve sonraki kontrol |
|---|---|
| `stale job` | Atama yok, süresi dolmuş, toleransı aşacak şekilde değiştirilmiş veya eski şablon artık mevcut değil. Yeni işlerin ve kabul edilen payların dönmesini kontrol edin. |
| `duplicate share` | Aynı şablon ve nonce daha önce gönderilmiş. Tekrarlanan gönderimleri araştırın; aynı iş yeniden katkı oluşturmaz. |
| `low difficulty share` | Hash atanan pay hedefini sağlamıyor. Tekrarlanıyorsa madenci sürümünü ve iş/zorluk işlemesini kontrol edin. |
| `nonce out of assigned range` | Nonce atanan aralığın dışında. Madenci sürümünü ve özel proxy veya entegrasyonları kontrol edin. |

Eski pay atanan işle, yetim blok ise bloğun zincirdeki yeriyle ilgilidir. Teşhiste pay günlüğünü [blok listesinden](/blocks) ayrı tutun. Stale job mesajı tek başına havuzun yetim blok bulduğunu göstermez.

## Pay zorluğu neden değişir?

<Answer>
Değişken zorluk, yani vardiff, bağlantı hedefini gözlenen pay etkinliğine uyarlar. Daha yüksek zorluk genellikle daha az ama daha fazla iş temsil eden pay; daha düşük zorluk tersini getirir. Aynı dönemde yalnızca sayı yerine zorluk ağırlıklı uygun işi karşılaştırın.
</Answer>

Hesap örneği: zorluk 100 olan on uygun pay, zorluk 1.000 olan bir payla aynı toplamı, 1.000’i verir. Bu ağırlıklandırmayı açıklar; ödül veya kesin geliş takvimi vaat etmez. Pay aralıkları değişir ve doğrulama hangi işin sayılacağını belirler.

Gönderimde atamanın zorluğu önemlidir. Eski sonucu yalnızca ekrandaki en yeni zorlukla karşılaştırmayın. Havuz kontrol için atama bilgisini saklar. Normal ayarlama hata değildir; kolay payları zorlamak ek hesaplama işi üretmez.

## Ret oranı nasıl okunmalı?

Kabul ve retleri aynı dönem ve kapsamda karşılaştırın. Madenci bir oturumu sayarken havuz başka dönemi veya birkaç cihazı kapsayabilir. Yeniden başlatma yerel pencereyi değiştirir, adresin havuz geçmişini silmez.

99 kabul ve bir ret, adet bakımından %1 ret eder. Bu otomatik olarak kaybedilen ödül yüzdesi değildir: zorluklar ve uygunluk kuralları değişebilir. Ayar değiştirmeden önce nedenleri, zamanlamayı ve kabul edilen işin sürmesini inceleyin.

## Retler tekrarlanıyorsa ne yapılmalı?

Bir etkilenen cihazla başlayın. Sürüm, tam mesaj, başlangıç zamanı ve yeni işlerin gelip gelmediğini kaydedin. [Durum](/status) sayfasını, ardından cihazın bağlantı kesintilerini kontrol edin. [Resmî seine sürümlerinden](https://github.com/zpalmtree/seine/releases) güncelleyin ve her seferinde tek değişiklikle karşılaştırın.

Özel proxy veya entegrasyon, gönderimi aktarırken iş, nonce aralığı ve zorluğu korumalıdır. Sorun sürerse proje destek kanalına kısa günlük kesiti ve cihaz ayrıntılarını verin. Sırları ve özel hesap verilerini çıkarın; pay teşhisinde cüzdan kurtarma ifadesi asla gerekmez.

## Madenciliğin düzeldiği nasıl doğrulanır?

Yeni işlerden sonra kabul edilen payları gözleyin. Madenciyle aynı alım adresiyle [İstatistiklerim](/stats) sayfasını açıp cihazın yeni etkinliğini doğrulayın. Ortalamaları kıyaslamadan önce hash gücü tahmininin daha fazla iş toplamasını bekleyin. Ardından PPLNS rehberinde tahmini ödül, bakiye ve ödemeleri ayrı aşamalar olarak izleyin.

<CTA target="pool" title="bntpool.com’a bağlanın" text="Kurulum rehberini izleyin, kabul edilen payları doğrulayın ve alım adresinizle etkinliği takip edin." />
