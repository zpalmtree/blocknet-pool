---
title: 'Havuz ödemesi gönderildi ama cüzdanda görünmüyor mu?'
description: 'Blocknet ödemesini işlem, alıcı adresi ve cüzdan eşitlemesi üzerinden izleyin; görünmeyen kaydı araştırmak için gereken bilgileri toplayın.'
tags: ['Ödemeler', 'Cüzdan', 'Sorun giderme']
faq:
  - q: 'Kuyruktaki ödeme cüzdana ulaşmış mıdır?'
    a: 'Kuyruk havuz tarafında bir işleme durumudur. Önce kaydın işlem kimliği içerip içermediğini ve güncel durumunu kontrol edin. Ödeme kaydı, işlemin zincire ulaşması ve cüzdanın çıktıyı tanıması ayrı aşamalardır. Kullanılabilir olduğunda belirli işlemi takip edin.'
  - q: 'Gezgin özel cüzdan bakiyemi gösterebilir mi?'
    a: 'Gezgin işlemin herkese açık zincir durumunu kontrol etmeye yardımcı olur. Blocknet özel çıktılar kullanır; sıradan adres araması cüzdan bakiyenizi göstermez veya belirli çıktının size ait olduğunu kanıtlamaz. Cüzdan kendi çıktılarını tanımak için zinciri tarar.'
  - q: 'Görünmeyen ödeme için hangi bilgileri vermeliyim?'
    a: 'Havuz aramasındaki tam alıcı adresini, işlem kimliğini, yaklaşık saati, havuz durumunu ve cüzdan eşitleme ilerlemesini verin. İşlemin geçmişte görünüp görünmediğini açıklayın. Hesap bilgilerini yalnız uygun destek ortamında paylaşın; kurtarma ifadesi, özel anahtar veya cüzdan parolasını asla vermeyin.'
sourceHash: '915075cf4e2b'
translation: draft
---

Blocknet ödemesini havuz kaydından alıcı cüzdana kadar izleyin. Bu rehber bntpool.com'da belirleyebildiğiniz bir ödeme kaydıyla başlar. Yalnızca ödeme bekleyen tahmini veya onaylanmış madencilik ödülleri görüyorsanız önce [PPLNS rehberini](/blog/tr/how-pplns-payouts-work/) kullanın; bu kendi muhasebe ve ödeme kuralları olan önceki aşamadır.

## Cüzdanda görünmeyen ödeme nasıl izlenir?

<Answer>
Tam alıcı adresinin ödeme kaydını bulun, işlem kimliğiyle güncel durumu not edip zincir durumunu kontrol edin. Aynı hedefin cüzdanını açın, eşitleme ve taramanın yetişmesini bekleyin. Ödemenin mi yerel cüzdan görünümünün mü inceleneceğine karar vermeden işlem geçmişini karşılaştırın.
</Answer>

*Havuz ödeme kaydı zincirdeki işleme, ardından kendi gelen çıktılarını tarayan cüzdana bağlanır.*

## Belirli bir ödeme kaydıyla başlayın

My Stats'ta ödüller kazanılırken kullanılan tam adresi arayın. İlgili kaydın tutarını, saatini, durumunu ve varsa işlem kimliğini not edin. Birçok ödemeyi özetleyebilen toplam ödenen tutar yerine bu kaydı kullanın. [İzleme rehberi](/blog/tr/track-your-miner-on-bntpool/) alanların yerini açıklar.

Herkese açık Payouts sayfası birden fazla alıcı ve işlem bağlantısı içerebilen ödeme gruplarını gösterir. Adresinize ait kaydı incelenen işlemle birlikte tutun. Grup toplamı cüzdanınıza ayrılan tutar değildir. Birden fazla kimlik varsa listeyi açıp yalnız kısaltılmış etiketi değil ilgili kimlikleri saklayın.

## Ödeme kuyrukta mı, onaysız mı, onaylı mı?

Listelenen ödemenin tüm aşamaları bitirdiğini varsaymadan güncel durumu okuyun. Kuyruktaki kayıt havuzda işlenmektedir; onaysız işlem zincir onayı bekler. Kaydı yenileyip bağlantı hazır olduğunda işlemi açın. Birkaç gözlem hizmet kesintisine işaret ediyorsa [durum sayfası](/status) işletim bağlamını sağlar.

Madencilik ödülü olgunlaşmasını ödeme işleminin onayından ayırın. Ödülü kazandıran blok ve daha sonra cüzdana ödeme yapan işlem farklı kimlik ve rollere sahiptir. [Onay rehberi](/blog/tr/block-confirmations-mining-rewards/) farkı açıklar. İşlem kimliği somut inceleme noktasıdır, fakat yerel cüzdanın ödemeyi tarayıp tanıdığını tek başına kanıtlamaz.

## İşlemi zincirde kontrol edin

Ödemenin gezgin bağlantısını açıp tam işlem kimliğini notlarınızla karşılaştırın. Zincir durumunu ve varsa blok bilgisini okuyun. Gezgin henüz bulamıyorsa sonuç çıkarmadan yenilenmiş havuz kaydı ve hizmet durumunu karşılaştırın. Geçici arama sonucu tek başına nedeni belirlemez.

Blocknet gizliliği nedeniyle normal adres araması alıcının herkese açık bakiyesini göstermez. Gezgin işlem durumu ve cüzdan sahiplik kontrolü farklı soruları yanıtlar. Cüzdan zinciri tararken gelen çıktıları tanır. [Gizli adres rehberi](https://seineminer.com/tr/blog/stealth-addresses-explained/) süreci açıklar; sıradan adres listesi özel işlem geçmişini yeniden üretmez.

## Doğru alıcı cüzdana mı bakıyorsunuz?

Havuz arama adresini açık cüzdanın alıcı adresiyle karşılaştırın. Tam değerleri kullanın; benzer kısaltılmış önekler yeterli değildir. Blocknet ID ile kazıyorsanız ilk çalışmanın tam adresini saklayın. Eşleme değişince güncel isim araması başka yere gidebilir, fakat eski ödeme ilk hedefle ilişkili kalır.

Madencilik ayarlarını değiştirmeden önce kullanılan eski cüzdanları kontrol edin. Madenci adresini veya isim eşlemesini değiştirmek önceden atanmış ödemeyi taşımaz. [Adres değiştirme rehberi](/blog/tr/change-mining-wallet-address/) iki geçmişi izlemeyi açıklar. İlk cüzdanı ve yedeğini koruyun; mevcut cüzdanı açmayı farklı adresli yeni cüzdan oluşturmaktan ayırın.

## Cüzdanın eşitlemesini ve taramasını bekleyin

Cüzdan düğümünün güncel zincire ilerlediğini ve cüzdan taramasının ödeme işleminin bloğuna ulaştığını doğrulayın. Düğüm eşitlemesi ve cüzdan taraması ayrı ilerlemelerdir. Yeni açılmış veya geri yüklenmiş cüzdan, gezgin işlemi blokta gösterse bile çıktıları bulmak için zamana ihtiyaç duyabilir.

Cüzdanı bağlı tutup belgelenmiş eşitleme akışını izleyin. Sonraki harcamalar toplamı değiştirebildiği için gelen işlem geçmişini de ana bakiyeyle karşılaştırın. Kayıt varsa onay veya kullanılabilirlik durumunu inceleyin. Kurtarma ya da tam yeniden tarama öncesi bunlarla başlayın, bakım öncesi yedekleri koruyun. [Cüzdan rehberi](https://seineminer.com/tr/blog/blocknet-wallet-for-mining/) erişim ve yedekleri kapsar.

## Kalan farkı çözmeye hangi bilgiler yardımcı olur?

Tam havuz adresini, işlem kimliğini, yaklaşık saati ve güncel havuz durumunu yazın. Cüzdan sürümünü, düğüm ve tarama ilerlemesini, gelen işlemin geçmişte olup olmadığını ekleyin. Tamamlanan kontrolleri belirtin. Bunlar işlem durumu sorusunu adres uyuşmazlığı veya hâlâ yetişen cüzdandan ayırır.

Projenin resmi destek yolunu kullanıp hesap bilgilerini uygun ortamda paylaşın. İşlem incelemesi kurtarma ifadesi, özel anahtar veya parola gerektirmez. Tanılama dosyalarını bütünüyle yayımlamadan içeriklerini inceleyin. Aynı ödeme kaydı, işlem ve alıcı cüzdanı eşleştirin; güncel hashrate veya yeni madenci restartı eski ödemenin alındığını doğrulamaz.

<CTA target="pool" title="Ödemelerinizi bntpool.com&#x27;da izleyin" text="Tam alıcı adresinizle ödeme kaydını bulun ve işlemi eşitlenmiş cüzdan geçmişiyle karşılaştırın." />
