---
title: 'Stratum nedir? Madenciler havuzla nasıl konuşur?'
description: 'seine ile bntpool.com arasındaki Stratum akışını izleyin: giriş, işler, pay hedefleri, gönderimler ve yanıtlarla madenci bağlantısının aşamalarını anlayın.'
tags: ['Madencilik temelleri', 'Stratum', 'Havuz madenciliği']
faq:
  - q: 'Stratum havuz madenciliğinde ne yapar?'
    a: 'Stratum, madenciyle havuz arasındaki oturum tanımlama, iş dağıtımı, pay gönderimi ve yanıt konuşmasını taşır. Madenci hashleri yerel hesaplar. Havuz bu işi üstteki düğümüne bağlar ve ödeme şemasının gerektirdiği muhasebeyi tutar.'
  - q: 'Stratum adresi web adresiyle aynı mı?'
    a: 'Farklı bağlantılara hizmet ederler. bntpool.com sayfa ve API için HTTPS kullanır; belgelenmiş madencilik ucu stratum+tcp://bntpool.com:3333 adresidir. Bunu madencinin havuz ayarına girin. Web sitesini açabilmek tek başına madencilik bağlantısı kurmaz.'
  - q: 'Kabul edilen Stratum payı ödeme gönderildiği anlamına gelir mi?'
    a: 'Hayır. Pay yanıtı oturumdaki gönderimin sonucudur. Doğrulama, ağ açısından geçerli blok, ödüle uygunluk ve tamamlanmış ödeme ayrı aşamalardır. Kabul edilmiş işi My Stats üzerinden izleyin; onaylar, PPLNS ve eşik için ödeme rehberini okuyun.'
sourceHash: '1dba87acd7c0'
translation: draft
---

Bir işi bntpool.com'dan seine'a, bir payı da havuza geri izleyin. Bu konuşma bağlantı mesajlarını, iş güncellemelerini ve yanıtları anlamayı kolaylaştırır. Rehber bntpool.com uygulamasını anlatır; ilk uç nokta ve ödeme adresi ayarı için [havuz kurulum rehberini](/blog/tr/mine-blocknet-on-bntpool/) kullanın.

## Madencilikte Stratum nedir?

<Answer>
Stratum, madencinin havuz işi alıp sonuç göndermesini sağlayan iletişim katmanıdır. seine ve bntpool.com'da kalıcı TCP bağlantısı giriş, iş ve pay için JSON mesajları taşır. Makineniz hashleri yerel hesaplar; havuz düğümünden blok şablonları alır ve gönderilen işi işler.
</Answer>

*Madenci havuza giriş ve pay mesajları gönderir, işler ve yanıtlar alır; havuz düğümüyle şablon ve blok adayı alışverişi yapar.*

## Uç nokta madencilik bağlantısını belirtir

Belgelenmiş uç `stratum+tcp://bntpool.com:3333` adresidir. Şema taşıma biçimini, ana makine adı hizmeti, port da madencilik dinleyicisini seçer. Tarayıcının `https://bntpool.com` bağlantısının amacı farklıdır. Tam ucu [Get Started](/start) sayfasından havuz ayarına kopyalayarak doğru bağlantıyı açın.

Kalıcı bağlantı iş hazırken iki tarafın da mesaj göndermesini sağlar. bntpool yeni satırla ayrılmış JSON kullanır; istek kimlikleri yanıtları eşler. Bunlar bu havuzun biçimidir; madenci biçimi, yeteneklerini ve Blocknet algoritmasını desteklemelidir. [Protokol kaynağı](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) mevcut alanları gösterir.

## Madenci giriş yapınca ne olur?

İstemci ödeme adresi, rig adı, protokol sürümü ve yetenekleri içeren `login` gönderir. Havuz kontrol edip protokol bilgisi ve gerekli yeteneklerle yanıtlar. Bu, oturumun işi nasıl paylaşacağını belirler. TCP bağlantısı ilk adımdır; kabul edilen giriş uygulama konuşmasının da başladığını gösterir.

Rig adı bir adres altındaki makinenin etkinliğini ayırır. Birden fazla rig için farklı adlar ve doğru ödeme adresini kullanın. Giriş alıcı adresi ister; kurtarma ifadesi veya özel harcama anahtarı istemez. Oturum kabul edilip iş hazır olduğunda havuz hashing için `job` gönderir.

## Madencilik işi hangi bilgileri içerir?

İş, atama ve şablon kimliklerini, başlık verisini, blok yüksekliğini, pay ve ağ hedeflerini, atanmış nonce aralığını içerir. Nonce hash sırasında değiştirilen değerdir. Aralık oturumun arama alanını belirler. İş kimliği sonraki gönderimi özgün iş ve koşullarla ilişkilendirir.

Pay hedefi iş kanıtı olarak hangi sonuçların gönderileceğini, ağ hedefi hangi sonuçların blok adayı olabileceğini belirler. Çoğu uygun pay blok olmadan muhasebeye katkı sağlar. [Zorluk rehberi](/blog/tr/mining-difficulty-explained/) hedef boyutu ve beklenen işi açıklar; protokol hedefleri atamanın içinde iletir.

## Bağlantı açıkken işler değişebilir

Yeni iş tek başına bağlantının koptuğu anlamına gelmez. Havuz düğüm şablonu değiştiğinde işi güncelleyebilir veya yeni pay zorluğuyla atama gönderebilir. Değişken zorluk gönderim eşiğini gözlenen işe uyarlar. Kalıcı bağlantı iki yönlü mesaj taşırken madenci yeni atamayı izler.

Teknik günlüklerde iş ve şablon kimliklerini ayırın. Birden çok atama farklı oturum ayrıntılarıyla aynı şablonu kullanabilir. Yükseklik bütün değişiklikleri anlatmaz. Operatör için işlerin gelmesi, hashing ilerlemesi ve gönderimlere yanıt alınması önemlidir; [izleme rehberi](/blog/tr/track-your-miner-on-bntpool/) etkinliğin yerini açıklar.

## Pay gönderimi neler içerir?

seine uygun sonuç bulunca iş, nonce ve iddia edilen hash ile `submit` gönderir. Havuz atama bağlamında işler ve istek kimliğiyle yanıtlar. İddia mesajın parçasıdır; doğrulama ve blok denetimi sonraki aşamayı belirler. Blok adayı doğrulama için daemon'a gönderilir.

Yanıt kabul, doğrulama durumu ve zorluk bilgisi taşıyabilir. Her başarılı yanıtı blok bulma saymak yerine alanları ayrı okuyun. Muhasebe ve ödeme bu alışverişin ötesindedir. [Pay sonuçları rehberi](/blog/tr/accepted-rejected-stale-shares/) yanıtları; [PPLNS rehberi](/blog/tr/how-pplns-payouts-work/) uygun işin ödül ve ödemeyle ilişkisini açıklar.

## Madenciyi kontrol ederken bu sıra nasıl kullanılır?

Aşamaları sırayla okuyun: uca erişildi, giriş kabul edildi, iş alındı, hashing aktif, gönderimler yanıtlandı. Bağlantı kaybolursa istemci yeniden bağlanıp yeni iş öncesi oturum kurar. Tam günlük mesajı ve zamanıyla aşamayı belirleyin. Web sitesinin çalışması, o makineden madencilik portuna erişildiğini kanıtlamaz.

Doğru adres için [My Stats](/stats), hizmet bağlamı için [havuz durumunu](/status) açın. Başlangıç veya yeniden bağlantı sonrası pay tabanlı hash gücü tahminlerine veri toplama zamanı verin. Protokolü ödeme zamanından ayırın: giriş oturumu hazırlar, işler görev verir, paylar sonuç bildirir, muhasebe sonraki ödül aşamalarını belirler.

<CTA target="pool" title="seine&#x27;ı bntpool.com&#x27;a bağlayın" text="Havuz kurulumunu kullanın, sonra işleri, payları ve rig etkinliğini My Stats üzerinden izleyin." />
