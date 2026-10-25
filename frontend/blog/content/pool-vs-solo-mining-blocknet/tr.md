---
title: Blocknet için havuz ve solo madencilik
description: Blocknet için havuz veya solo madencilik seçin. Günlük beklenen blokları hesaplayın, bntpool.com'un düzenli ödemelerini görün ve solo madenciliği kurun.
tags: [Madencilik rehberi, Solo madencilik, bntpool, PPLNS]
sourceHash: 171d240d6c4b
translation: draft
faq:
  - q: 'Solo madencilikte ne sıklıkta Blocknet bloğu bulurum?'
    a: 'Blocknet beş dakikada bir blok hedeflediğinden ortalama günde 288 × (sizin hashrate''iniz ÷ ağ hashrate''i) blok bulursunuz. 50.000 H/s ağda 50 H/s ile bu, günde 0,288 blok veya yaklaşık üç buçuk günde bir bloktur. Canlı ağ hashrate''ini bntpool.com panelinden okuyun.'
  - q: 'bntpool.com''da madencilik için Blocknet düğümü çalıştırmalı mıyım?'
    a: 'Hayır. Havuz kendi Blocknet düğümünü çalıştırır, blok şablonlarını oluşturur ve bulduğu blokları gönderir. Yalnızca seine ve bir Blocknet adresi veya Blocknet ID adı gerekir. seine''i çalıştırın, adresinizi yapıştırın, varsayılan havuzu kabul edin; havuz her bloktaki payınızı o adrese öder.'
  - q: 'seine Blocknet solo madenciliği yapabilir mi?'
    a: 'Evet. seine''i ./seine --mode daemon ile başlatın; düğümün API çerezini kullanarak kendi eşitlenmiş Blocknet düğümünüzle doğrudan madencilik yapar. Bulduğunuz her blok tüm ödülünü düğümün cüzdanına öder. seine deposundaki solo madencilik rehberi özel düğüm düzenlerini ve cüzdan parolalarını anlatır.'
  - q: 'Daha sonra havuz ve solo madencilik arasında geçebilir miyim?'
    a: 'Evet. seine ikisi için de aynı indirmeyi kullanır. bntpool.com''da madencilik için normal çalıştırın veya kendi düğümünüz için --mode daemon ekleyin; değiştirmek istediğinizde yeniden başlatın. Havuza gönderdiğiniz share''ler altı saatlik PPLNS penceresinde kalır ve süreleri dolana kadar kazanmaya devam eder.'
---

Bu rehber, Blocknet madenciliğini bntpool.com'da mı yoksa kendi düğümünüzle solo mu yapacağınıza karar vermenize yardımcı olur. Hashrate'inizin tek başına kaç blok bulduğunu hesaplayacak, havuzun bunu nasıl düzenli ödeme akışına dönüştürdüğünü görecek ve seine ile iki seçeneği de kurmayı öğreneceksiniz.

## Blocknet madenciliğini havuzda mı solo mu yapmalısınız?

<Answer>
İkisi de aynı hesaba dayanır: ağın hashrate'indeki payınız, bloklarındaki payınızı belirler. bntpool.com'da havuzun bulduğu her bloktan pay kazanırsınız; havuz blok buldukça ödemeler düzenli gelir. Solo çalışırken bulduğunuz her bloğun tüm ödülünü tutarsınız; bu, kendi düğümünü çalıştıran büyük işletmelere uygundur.
</Answer>

Çoğu madenci için bntpool.com kolay başlangıç noktasıdır. seine'in varsayılan havuzudur; adresiniz dışında ayar yapmanız, düğüm çalıştırmanız veya eşitlemeniz gerekmez. Solo madencilik de tam desteklenir: seine'in doğrudan kendi Blocknet düğümünüzle çalışan daemon modu vardır. Rehberin kalanı donanımınıza uygun seçeneği belirlemeyi gösterir.

## Solo çalışırken kaç blok bulursunuz?

<Answer>
Blocknet her beş dakikada bir, yani günde 288 blok hedefler. Tek başınıza madencilik yaparken her gün bunların 288 × (sizin hashrate'iniz ÷ ağ hashrate'i) kadarını bulmayı bekleyebilirsiniz. Bloklarınız arasındaki ortalama süre için bir günü bu sayıya bölün. Canlı ağ hashrate'ini bntpool.com panelinden okuyun.
</Answer>

Formül çalışır, çünkü Blocknet zorluğu tüm ağ ortalama her 300 saniyede bir blok bulacak şekilde ayarlar. Blok zorluğu, ağın başarıdan önce denemesi beklenen hash sayısına yaklaşık eşittir. Her hash aynı şansa sahip olduğundan makinenizden gelen ağ hash'lerinin oranı, bulmayı bekleyebileceğiniz blokların oranıdır.

İki sayıya ihtiyacınız var:

- **Sizin hashrate'iniz.** seine madencilik sırasında panelinde gösterir; havuzda çalıştıktan sonra [istatistikler sayfası](/stats) da gösterir.
- **Ağ hashrate'i.** [bntpool.com paneli](/), Pool Hashrate kartının yanında Network Hashrate kartında gösterir. İkisi de zamanla değişir; hesabı yaparken kontrol edin.

Bulduğunuz her blok o ayın blok ödülünü öder. [Blok ödülü takvimi](/blog/tr/blocknet-block-reward-schedule/) ödülü aylara göre listeler.

## Hesaplama örneği

<Note>
Aşağıdaki sayılar hesabı göstermek için seçilmiş varsayımsal, yuvarlak rakamlardır; canlı değerler değildir. Kendi tahmininizde paneldeki güncel ağ hashrate'ini kullanın.
</Note>

Ağın 50.000 H/s hızında çalıştığını varsayalım. 50 H/s hızındaki tek rig, seine'in bir AMD EPYC Zen 5 sunucusunda ölçtüğü değere yakın olarak ağın %0,1'ini oluşturur. Bu rig ve iki daha büyük işletme tek başına madencilik yaparken şunları bulurdu:

| Sizin hashrate'iniz (örnek) | Ağdaki pay | Günlük beklenen blok | Bloklar arası ortalama süre |
|---|---|---|---|
| 50 H/s | %0,1 | 0,288 | yaklaşık 83 saat (3,5 gün) |
| 500 H/s | %1 | 2,88 | yaklaşık 8,3 saat |
| 5.000 H/s | %10 | 28,8 | yaklaşık 50 dakika |

Şimdi 50 H/s rig'i toplam 5.000 H/s hızında çalışan bir havuza koyun. Havuz günde yaklaşık 28,8, ortalama 50 dakikada bir blok bulur; rig'in havuz işindeki %1 payı ona her bloktan %1 kazandırır. Sayılan iş yine günde 0,288 bloktur. Değişen geliş biçimidir: solo çalışmada birkaç günde bir tam blok gelirken havuzda yaklaşık her 50 dakikada küçük bir pay gelir.

Tablo solo madenciliğin büyük işletmelere neden uyduğunu da gösterir. 5.000 H/s ile solo madenci, aynı büyüklükteki havuz kadar sık blok bulur; geliri kendi başına düzenlidir.

Kendi donanımınız için günlük blokları BNT'ye çevirmek ve canlı havuz verilerinden tarihli örnek görmek için [Blocknet madencilik ödüllerinizi nasıl tahmin edersiniz?](/blog/tr/estimate-blocknet-mining-rewards/) yazısına bakın.

## Havuz neden daha düzenli ödeme sağlar?

bntpool.com üzerinde çalışan herkesin hashrate'ini birleştirir; böylece tek bir rig'den çok daha sık blok bulur. Her blok ödülü, bulunmasına iş katkısı yapan madenciler arasında PPLNS ile paylaşılır: havuz bloktan önceki altı saate bakar ve ödülü her madencinin share zorluğuyla orantılı böler.

Bunun sizin için birkaç pratik sonucu vardır:

- **Her bloktan kazanç.** Share'leriniz altı saatlik pencerede olduğu sürece havuzun bulduğu her bloktan pay kazanırsınız. Gelişlerini [bloklar sayfasında](/blocks) izleyebilirsiniz.
- **Otomatik ödemeler.** Blok 60 blok derine ulaştıktan sonra payınız onaylanmış bakiyeye geçer; havuz 0,1 BNT'ye ulaşınca adresinize gönderir. Gönderilen ödemeler [ödemeler sayfasında](/payouts) listelenir.
- **Yeniden başlatmalarda süreklilik.** Önceden gönderdiğiniz share'ler altı saat pencerede kalır; yeniden başlatma veya sürücü güncellemesi konumunuzu sıfırlamaz.
- **Madenci dışında çalıştırılacak bir şey yok.** Havuz düğümü çalıştırır, blok şablonlarını oluşturur ve blokları gönderir. Birden fazla rig'den tek adrese veya Blocknet ID `@name` adına madencilik yapabilirsiniz.

[PPLNS ödemeleri rehberi](/blog/tr/how-pplns-payouts-work/) kendi örneğiyle paylaşımı adım adım anlatır. Havuzun [Get Started sayfası](/start) güncel ödeme ayarlarını listeler.

## Neden Blocknet solo madenciliği yapmalı?

Solo madencilik sürecin her parçasını sizin denetiminize verir. Kendi düğümünüz her blok şablonunu oluşturur ve her bloğu kendisi kontrol eder; bulduğunuz her blok tüm ödülünü doğrudan düğümünüzün cüzdanına öder.

Hashrate'iniz sık blok bulduğunda en uygunudur. Yukarıdaki örnekte ağın %10'una sahip işletme yaklaşık 50 dakikada bir blok bulur; solo ödemeler havuz kadar düzenli gelir. Cüzdanınız veya başka hizmetler için zaten Blocknet düğümü çalıştırıyorsanız solo madencilik bunun üzerine madenciyi ekler.

seine bu mod için de tasarlanmıştır. Düğümün blok şablonlarıyla madencilik yapar; düğüm örneğin yeniden başlatmadan sonra bir şablonu unuttuysa seine çözülmüş bloğun tamamını yeniden gönderir, böylece bulunan blok yine zincire ulaşır.

## seine ile solo madencilik nasıl kurulur?

1. **Blocknet düğümü kurup eşitleyin.** `blocknet` sarmalayıcısıyla çalıştırın:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Bu, düğümün API çerezini `~/.config/bnt/data/mainnet/api.cookie` konumuna yazar. İlk eşitlemeyi kısaltmak için bntpool.com [kontrol noktaları sayfasında](/checkpoints) hızlı ilk eşitlemeye yönelik `checkpoints.dat` dosyası yayımlar. [Blocknet düğümünü kontrol noktalarıyla eşitleme](/blog/tr/blocknet-checkpoints-fast-sync/) düğümün bunu nasıl kullandığını açıklar.
2. **Eşitlemenin bitmesini bekleyin.** Düğümün `[sync] progress` mesajları %100'e ulaşır. Düğüm ancak zincire yetiştikten sonra blok şablonları sunar.
3. **seine'i daemon modunda başlatın.**

   ```sh
   ./seine --mode daemon
   ```

   Özel düğüm düzeninde API ve çerezi seine'e kendiniz gösterin:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

[Solo madencilik rehberi](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) cüzdan parolaları ve özel kurulumları, [seine madencilik rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) ise seine'i indirip ilk share'leri almayı anlatır. Windows, macOS veya Linux için [seine'i indirebilirsiniz](https://seineminer.com/tr/#download).

## Hangisini seçmelisiniz?

Formülü kendi hashrate'iniz ve [paneldeki](/) güncel ağ hashrate'iyle uygulayın, sonra bloklar arası ortalama süreye bakın.

- **bntpool.com'da başlayın:** Masaüstü, dizüstü veya birkaç sunucuda madencilik yapıyorsanız ya da düğüm çalıştırmadan düzenli ödeme istiyorsanız. seine'i çalıştırın, adresinizi veya `@name` adınızı yapıştırın ve varsayılan havuzu kabul edin.
- **Solo madencilik yapın:** İşletmeniz ağın büyük payını elinde tutup tek başına sık blok buluyorsa ve kendi eşitlenmiş düğümünüzü zaten çalıştırıyor veya çalıştırmak istiyorsanız.

seine bu kararı değiştirmeyi kolaylaştırır. Havuz madenciliği ve daemon modu aynı indirmeyi kullanır; donanımınız büyüdükçe tek bayrak ve yeniden başlatmayla geçebilirsiniz. Havuzda madencilik yaparken yerel düğüm çalıştırırsanız seine düğümün cüzdan bakiyesini de havuz bakiyenizin yanında gösterir.

<Note>
[blocknetcrypto.com](https://blocknetcrypto.com/#faraday) sitesine göre Blocknet, 16 Şubat 2027 civarında Faraday adlı bir ağ yükseltmesi planlıyor. İş kanıtını değiştirmesi planlanıyor. seine'i güncel tutun: başlangıçta ve her 12 saatte bir yeni sürümleri kontrol eder.
</Note>

<CTA target="pool" title="bntpool.com'da madenciliğe başlayın" text="seine'i çalıştırın, adresinizi yapıştırın ve havuzun bulduğu her bloktan pay kazanın; 0,1 BNT'den itibaren otomatik ödensin." />
