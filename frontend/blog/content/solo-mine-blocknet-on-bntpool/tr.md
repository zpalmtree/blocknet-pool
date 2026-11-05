---
title: bntpool.com'da Blocknet solo madenciliği
description: 'Düğüm çalıştırmadan Blocknet solo madenciliği yapın. seine''de adresinizin önüne solo: ekleyin, bntpool.com bulduğunuz her bloğun ödülünün tamamını size ödesin.'
tags: [Madencilik rehberi, Solo madencilik, bntpool, seine]
sourceHash: '623b1e0b356a'
translation: draft
faq:
  - q: 'bntpool.com''da Blocknet solo madenciliği nasıl yapılır?'
    a: 'seine''i v0.3.1 veya daha yeni bir sürüme güncelleyin ve ödeme adresinizin önüne solo: ekleyin, örneğin ./seine --address solo:YOUR_ADDRESS. seine her zamanki gibi bntpool.com''a bağlanır ve havuz, cihazınızın bulduğu her bloğun ödülünün tamamını normal ödemeleriyle size gönderir. Düğüm çalıştırmanız gerekmez.'
  - q: 'Solo madencilik için kendi Blocknet düğümüm gerekir mi?'
    a: 'Hayır. solo: önekiyle bntpool.com düğümü kendisi çalıştırır, blok şablonlarını oluşturur ve bulduğunuz blokları gönderir. Cihazınızda yalnızca seine çalışır. Solo madenciliğin diğer yolu, senkronize kendi düğümünüze karşı çalışan seine''in daemon modudur; zaten düğüm çalıştıranlara uygundur.'
  - q: 'Solo madencilikte Blocknet ID adı kullanabilir miyim?'
    a: 'Evet. solo:ADDRESS yerine solo:@name veya solo:$name kullanın ve kabuğun değeri değiştirmeden iletmesi için komut satırında tırnak içine alın. Havuz adı tam adresine çözümler, ardından solo bloklarınızı hesabınıza yazar ve diğer ödemeler gibi o adrese öder.'
  - q: 'Havuz ödüllerine nasıl geri dönerim?'
    a: 'Adresinizden solo: önekini kaldırın ve seine''i yeniden başlatın. Cihazınız yeniden havuzun bulduğu her bloktan PPLNS ödüllerini paylaşır. Her cihaz modunu bağlanırken seçer; böylece aynı adresle bazı cihazları solo, diğerlerini paylaşılan ödüllerle çalıştırabilirsiniz.'
---

Bu rehber, bntpool.com'da Blocknet solo madenciliğini nasıl yapacağınızı gösterir: seine'de adresinize tek bir önek ekleyin, havuz da cihazlarınızın bulduğu her bloğun ödülünün tamamını size ödesin. Düğüm kurmaz veya senkronize etmezsiniz. Kurulumu yapacak, solo blokların nasıl ödendiğini görecek ve onları havuzda nereden takip edeceğinizi öğreneceksiniz.

## bntpool.com'da solo madencilik nedir?

<Answer>
bntpool.com'da solo madencilik size yalnızca kendi cihazlarınızın bulduğu bloklar için ödeme yapar ve her biri blok ödülünün tamamını adresinize öder. Havuz yine düğümü çalıştırır, blok şablonlarını oluşturur ve bloklarınızı gönderir; böylece havuz madenciliğiyle aynı kurulumla solo ödüller alırsınız. Adresinizin önüne solo: koyarak açarsınız.
</Answer>

Normal havuz madenciliğinde bntpool.com her bloğu, payları altı saatlik PPLNS penceresine düşen madenciler arasında paylaştırır; böylece havuzun bulduğu her bloktan bir pay kazanırsınız. Solo bir cihaz ise kendi bloklarının ödülünün tamamını alır ve başkalarının bulduğu bloklardan bir şey kazanmaz. İkisi de aynı sunucuyu, aynı adresi ve aynı ödemeleri kullanır. Yalnızca giriş değişir.

Havuz üzerinden solo madencilik, düğüm uğraşı olmadan solo ödüller sunar:

- **Çalıştırılacak düğüm yok.** Havuzun düğümü zinciri takip eder, taze şablonlar sunar ve çözdüğünüz blokları gönderir.
- **Otomatik ödemeler.** Bloklarınız, diğer tüm bntpool.com ödemeleri gibi adresinize veya Blocknet ID adınıza ödenir.
- **Canlı istatistikler.** Cihazlarınız, hash gücünüz ve bloklarınız havuzun [istatistik sayfasında](/stats) görünür.
- **Tek önekle geçiş.** Paylaşılan ödüllere dönmek için `solo:` önekini kaldırıp seine'i yeniden başlatın.

Solo madenciliğin diğer yolu, senkronize kendi Blocknet düğümünüze karşı çalışan seine'in daemon modudur. [Blocknet için havuz mu solo madencilik mi](/blog/tr/pool-vs-solo-mining-blocknet/) üç seçeneği karşılaştırır ve hash gücünüzün ne sıklıkla blok bulduğunu gösterir.

## seine ile solo madenciliğe nasıl başlanır?

<Answer>
seine v0.3.1 veya daha yenisini kurun, ardından Blocknet adresinizin önüne solo: koyarak başlatın; örneğin macOS veya Linux'ta ./seine --address solo:YOUR_ADDRESS. seine varsayılan havuzu bntpool.com'a bağlanır ve panel hesabınızı Solo olarak etiketler. seine ilk çalıştırmada ödeme adresi sorduğunda solo: ve adresinizi de yazabilirsiniz.
</Answer>

1. **seine'i güncelleyin.** Solo girişler için seine v0.3.1 veya daha yenisi gerekir. [seine'i indirin](https://seineminer.com/tr/#download) ya da otomatik güncellemelerin kurmasına izin verin.
2. **seine'i önekle başlatın.** macOS veya Linux'ta:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Windows'ta, PowerShell'de:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine varsayılan olarak bntpool.com'da kazı yapar. Havuzun [Get Started](/start) sayfası aynı komutu havuz adresi açıkça yazılmış olarak gösterir.
3. **Paneli kontrol edin.** Bakiye paneli hesabınızı **Solo** olarak etiketler ve günlükte, bu adresin bulduğu bloklar için size ödeme yapıldığına dair havuz onayı görünür.

Önek, seine'in adres aldığı her yerde çalışır: ilk çalıştırma istemi, kayıtlı yapılandırma, yerel kontrol API'si ve çiftlik proxy'si arkasındaki cihazlar. seine öneki adresinizle birlikte kaydeder, böylece sonraki çalıştırmalar da solo kazmaya devam eder.

## Blocknet ID adıyla solo madencilik yapılabilir mi?

Evet. Bir Blocknet ID adı tam adresle aynı şekilde çalışır. `solo:@name` veya `solo:$name` yazın ve kabuğun değeri değiştirmeden iletmesi için tırnak içine alın:

```sh
./seine --address 'solo:@yourname'
```

Havuz adı blocknet.id üzerinden çözümler ve solo bloklarınızı arkasındaki adrese öder. Bir cihaza ad vermek için adı tam adrese nokta ile ekleyin, örneğin `solo:YOUR_ADDRESS.rig1`, ya da herhangi bir adres veya adla `--pool-worker rig1` verin. Önek büyük/küçük harf ayırt etmez, bu yüzden `SOLO:` da çalışır.

## Solo bloklar nasıl ödenir?

Cihazınız bir blok bulduğunda havuz onu solo blok olarak kaydeder. Blok 60 blok derinliğe ulaştığında, yaklaşık beş saat sonra, havuz blok ödülünün tamamını havuz ücreti düşülerek bakiyenize ekler. 4 Kasım 2026 itibarıyla ücret %0'dır. Bakiyeniz ardından havuzun normal ödemeleriyle adresinize gönderilir.

Ödemeler havuz madenciliğiyle aynı kurallara uyar. Kısa bir zincir yeniden düzenlemesinin zaten ödenmiş bir ödülü geri alamaması için her blok 60 onay bekler; [blok onayları ve madencilik ödülleri](/blog/tr/block-confirmations-mining-rewards/) nedenini açıklar. Havuzun `/api/info` uç noktası 4 Kasım 2026'da 1 BNT minimum ödeme bildiriyordu ve tek bir Blocknet blok ödülü bunun çok üzerindedir; bu yüzden onaylanmış bir solo blok havuzun normal ödeme takviminde ödenir. [Ödemeler sayfası](/payouts) havuzun gönderdiği her ödemeyi listeler.

Ödeme memosu olan bir borsa yatırma adresine kazıyorsanız solo da aynı şekilde çalışır. Memo ve onun kendi minimum ödemesi, paylaşılan ödüllerde olduğu gibi solo bloklarınıza da uygulanır.

Cihazınızın işi yine havuzun hash gücüne, tur eforuna ve şansına sayılır; solo bir blok da havuzun turunu diğer her blok gibi bitirir. Solo paylar hiçbir zaman PPLNS penceresine girmez; böylece diğer madenciler sizin bloklarınızdan, siz de onlarınkinden bir şey kazanmazsınız.

## Solo bir cihaz ne sıklıkla blok bulur?

Blocknet her beş dakikada bir blok, yani günde 288 blok hedefler. Solo bir cihaz bunların ortalama 288 × (hash gücünüz ÷ ağ hash gücü) kadarını günde bulur. Ağ hash gücünü [bntpool.com ana sayfasında](/), kendi hash gücünüzü seine'in panelinde okuyun, ardından bir günü sonuca bölerek bloklarınız arasındaki ortalama süreyi bulun.

Solo ödüller bütün bloklar halinde gelir; bu yüzden hash gücü sık blok bulan madencilere uygundur: büyük çiftlikler, sunucu filoları ve ağda büyük payı olan herkes. Daha küçük cihazlar, havuzun bulduğu her bloktan düzenli paylar halinde ödenen paylaşılan PPLNS ödülleriyle ortalamada aynı miktarı kazanır. [Blocknet için havuz mu solo madencilik mi](/blog/tr/pool-vs-solo-mining-blocknet/) üç hash gücü seviyesinde örnekler üzerinden ilerler.

## Solo cihazlarınızı ve bloklarınızı nerede görürsünüz?

Adresinizi [istatistik sayfasında](/stats) arayın. `solo:` önekiyle veya öneksiz yapıştırabilirsiniz. Sayfa ardından solo işinizi işaretler:

- **Workers** her cihazı bir Type sütunuyla listeler; hash gücü penceresinde solo pay gönderen cihazlarda burada **Solo** yazar.
- **Estimated rewards** solo bloklarınızı onay beklerken işaretler.
- **Blocks** ve **Luck** her bloğun durumundan önce Pool veya Solo yazan bir Type sütunu gösterir. [Bloklar sayfası](/blocks) havuzun bulduğu tüm blokları en yeniden başlayarak listeler.

[bntpool.com'da madencinizi takip edin](/blog/tr/track-your-miner-on-bntpool/) istatistik sayfasının geri kalanını anlatır.

## Solo ve havuz cihazlarını birlikte kullanabilir misiniz?

Evet. Her cihaz modunu bağlanırken seçer; böylece tek bir adres aynı anda solo cihazlar ve havuz cihazları çalıştırabilir. Bir çiftlik en büyük sunucularında solo kazıp masaüstü bilgisayarları paylaşılan ödüllerde tutabilir; hepsi tek bir bakiyeye ödenir. Havuz iki tür işi birbirinden ayrı tutar: solo bağlantıda bulunan bir pay yalnızca o adresin solo blokları için sayılır.

Bir cihazı geri almak için komutunuzda veya kayıtlı yapılandırmada adresinden `solo:` önekini kaldırın ve seine'i yeniden başlatın. Cihazın solo modda zaten gönderdiği paylar solo kalır, yeni paylar ise hemen PPLNS penceresine katılır.

<CTA target="pool" title="bntpool.com'da solo kazın" text="seine'de adresinize solo: ekleyin ve cihazlarınızın bulduğu her bloğun ödülünün tamamını düğüm çalıştırmadan alın." />
