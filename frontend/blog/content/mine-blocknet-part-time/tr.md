---
title: 'Blocknet’i yalnızca boş vakitlerde kazabilir misiniz?'
description: 'Blocknet’i kısa oturumlarda kazın: bntpool’un altı saatlik PPLNS penceresinde payları izleyin, aynı adresle dönün ve ödenmemiş bakiyeyi takip edin.'
tags: ['Madencilik rehberi', 'PPLNS', 'Zamanlama']
faq:
  - q: 'Paylarımın sayılması için altı saat kazmam mı gerekiyor?'
    a: 'Hayır. Uygun paylar altı saat beklemeden bir bloğun ödül penceresine katılabilir. Payınız, pencere içindeki zorlukla ağırlıklandırılmış işinizin havuz toplamına oranına bağlıdır; blok ödülü dağıtılabilmesi için blok bulunmalıdır.'
  - q: 'Mevcut ödemeyi almak için madenci bağlı kalmalı mı?'
    a: 'Etkin madencilik bağlantısı ödeme şartı değildir. Onaylanmış bakiyeler, güncel asgari eşik dahil havuz ödeme kurallarına göre işlenir. Bağlantıyı kesmek kendi başına bakiyeyi silmez veya hemen ödeme talep etmez.'
  - q: 'Yeniden bağlanmak eski payların yaşını sıfırlar mı?'
    a: 'Hayır. Mevcut paylar zaman damgalarını korur. Dönüşünüzden sonra kabul edilen yeni iş ilgili pencerelere girer; yeniden bağlanmak eski payları yenilemez veya çevrimdışı saatler için iş eklemez. Yeniden başlatınca yeni kabul edilmiş payları doğrulayın.'
sourceHash: '0a11c393062e'
translation: draft
---

Blocknet madencisini seçtiğiniz saatlerde çalıştırıp daha sonra dönebilirsiniz. Mevcut bağlantı, gönderilmiş paylar ve dağıtılmış ödülleri ayırmak yararlıdır. Bu rehber kısa bir oturumu bu aşamalardan geçirerek durdurma, yeniden başlatma ve bntpool.com hesabını kontrol etmede beklentilerinizi netleştirir.

## Kısa bir oturum havuz ödülü kazanabilir mi?

<Answer>
Evet. Kısa oturumdaki uygun iş, PPLNS penceresi bu payları içeren bloklara katılabilir. Katkıdan önce altı saati tamamlama şartı yoktur. Ödüller havuzun blok bulmasına ve her penceredeki zorlukla ağırlıklandırılmış oranınıza bağlıdır. Durmak yeni iş eklemez; mevcut paylar normal şekilde yaşlanır, bakiyeler ödeme kurallarını izler.
</Answer>

*Zaman çizelgesi bir madencilik oturumu, çevrimdışı aralık ve dönüşü; pay uygunluğu ile mevcut bakiye için ayrı işaretlerle gösterir.*

## Hangi süreyi izlediğinizi bilin

bntpool şu anda [başlangıç sayfasında](/start) altı saatlik PPLNS penceresi, ödeme uygunluğundan önce 60 blok ve 0,1 BNT asgari ödeme yayımlar. Bunlar üç ayrı aşamadır: blok için sayılan iş, o bloğun olgunlaşması ve bakiye eşiği. Altı saatlik pay penceresi altı saat sonra ödeme sözü veren bir sayaç değildir.

Dağıtım formülü için [PPLNS rehberine](/blog/tr/how-pplns-payouts-work/) bakın. Uygun paylarınız zorlukla ağırlıklandırılarak aynı penceredeki tüm uygun işle karşılaştırılır. Yalnızca çevrimiçi dakika sayısı sonraki ödülün yüzdesini belirlemez. Cihazlar, havuz katılımı ve pay sonuçları, eşit uzunluktaki oturumlarda bile ağırlıkları değiştirebilir.

## Bir payı mola boyunca takip edin

Yerel saatle örnek program: 18:00–20:00 arası kazıyorsunuz ve uygun bir pay 19:00’da kaydediliyor. Ödül penceresi 22:00’da biten havuz bloğu altı saat geriye, 16:00’ya bakar; dolayısıyla madenci artık çevrimdışı olsa da bu pay aralık içindedir. Bu bir uygunluk örneğidir, havuzun o saatte blok bulacağı tahmini değildir.

Ertesi gün 02:00’da biten pencere 20:00’da başlar, dolayısıyla 19:00 payı dışarıdadır. 08:00’da yeniden bağlanmak zaman damgasını değiştirmez. Döndükten sonraki yeni iş daha sonraki pencerelere katkı vermeye başlar. [Havuz şansı rehberi](/blog/tr/mining-pool-luck-explained/), blok zamanlarının değişmesini açıklar; programı bir bloğun artık gelmesi gerektiği varsayımıyla seçmeyin.

## Durmadan önce anlaşılır bir kayıt bırakın

Planlı moladan önce alıcı adresini, rig adını, yaklaşık duruş saatini ve son kabul edilmiş pay mesajlarını not edin. [Takip rehberi](/blog/tr/track-your-miner-on-bntpool/) ile ilgili hesabı kontrol edin. Bu, sonraki oturum için referans verir ve durmuş bir rigi farklı adres veya geciken gösterimden ayırır.

seine’den panelde `q` veya terminalde `Ctrl+C` ile normal çıkın. Çalışan başlatma komutunu ve yapılandırmayı sonraki sefer için saklayın. [Bilgisayar kullanım rehberi](https://seineminer.com/tr/blog/mine-while-using-pc/), madenciliği diğer işlerle uyumlu hale getirmeyi anlatır; düzgün durmak ayar silmeyi, cüzdan adresi değiştirmeyi veya havuzdan hesap kapatmasını istemeyi gerektirmez.

## Çevrimdışıyken ödenmemiş bakiyeye ne olur?

Bir pay daha sonraki bloğun penceresinden çıktığında, önceki bloğa zaten ayrılmış ödül silinmez. Tahmini ödüller blok durumunu ve olgunlaşmasını izlemeye devam eder; onaylanmış ödenmemiş ödüller ödeme işlemi ele alana dek hesapta kalır. [Onaylar rehberi](/blog/tr/block-confirmations-mining-rewards/), geçici blok alacağı ile olgun bakiyeyi ayırır.

0,1 BNT altındaki onaylanmış bakiye, ek alacaklar asgari tutara ulaştırana kadar mevcut otomatik ödeme eşiğinin altında kalır. Yeniden bağlanmak tek başına bakiyeyi artırmaz veya ödemeyi zorlamaz. Öte yandan uygun bakiye işlenirken madencinin çevrimiçi kalması gerekmez. Ödenmemiş bakiyeyi gönderilmiş işlemden ayırmak için [ödeme geçmişini](/payouts) kullanın.

## Aynı hesaba dönüp yeni işi doğrulayın

Kaydettiğiniz komutla yeniden başlatın; istatistikleri karşılaştırmadan önce havuzu, alıcı adresini ve rig adını doğrulayın. Adres, bakiyesini izlediğiniz hesabı tanımlar. Bilerek başka hedef seçiyorsanız [adres değiştirme rehberini](/blog/tr/change-mining-wallet-address/) izleyin; eski hesap bakiyesi sessizce yeni hesaba aktarılmaz.

Güncel bir iş ve yeni kabul edilen paylar arayın, sonra havuz göstergesinin ölçüm penceresine güncellenme zamanı tanıyın. Yakın zamandaki yeniden başlatma, madencinin oturum ortalamasıyla havuzun kayan tahmininde farklı görünebilir. [Hashrate rehberi](/blog/tr/what-is-hashrate/) bu zaman pencerelerini açıklar. Kısa farkı kayıp ödül saymadan önce gerçek işi ve doğru hesabı kontrol edin.

## Oturumları kendi düzeninize göre planlayın

Uygun saatleri bilgisayar kullanımınıza ve elektrik bütçenize göre seçin. Gerçek programın tüketimini, gerektiğinde farklı tarife aralıklarıyla birlikte hesaplamak için [elektrik rehberini](https://seineminer.com/tr/blog/mining-electricity-use/) kullanın. Kısa oturum çalışma süresini azaltır; kesin enerji yine ölçtüğünüz ekipman ve yüke bağlıdır.

Birden çok oturum boyunca başlangıç, duruş, kabul edilen iş ve bakiyelerin küçük bir kaydını tutun. [Ödül hesaplayıcı](/blog/tr/estimate-blocknet-mining-rewards/) varsayımları altında beklenti sunar, belirli bir akşam için söz vermez. Düzeni temsili bir dönemde değerlendirin; bağlantı durumu, pay penceresi uygunluğu, blok olgunluğu ve gönderilmiş ödemeleri ayrı gözlemler olarak tutun.

<CTA target="pool" title="Oturumlarınızı bntpool.com’da takip edin" text="Dönerken aynı alıcı adresini kullanın, yeni payları doğrulayın ve bakiyeleri normal ödeme aşamaları boyunca izleyin." />
