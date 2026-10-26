---
title: bntpool.com'da Blocknet madenciliği yapın
description: seine'in varsayılan Blocknet havuzu bntpool.com'a katılın. Adresiniz veya @name ile stratum+tcp://bntpool.com:3333'e bağlanın, rig'lerinizi adlandırın.
tags: [Madencilik rehberi, bntpool, Blocknet ID, seine]
sourceHash: '84ce1ed92e20'
translation: draft
faq:
  - q: 'bntpool.com Stratum adresi nedir?'
    a: 'Adres stratum+tcp://bntpool.com:3333''tür; seine ilk çalıştırmada bu havuzu önerir. Havuz sorusunda Enter''a basarak kabul edin veya --pool-url bayrağıyla verin. Blocknet adresiniz veya Blocknet ID adınız giriş bilginizdir; havuz ödülleri bu adrese öder.'
  - q: 'bntpool.com''da Blocknet ID adıyla madencilik yapabilir miyim?'
    a: 'Evet. seine veya havuzun adres istediği her yerde @name veya $name girin. Havuz blocknet.id üzerindeki imzalı eşlemeyi kontrol eder, ardından share ve ödemeleri adın arkasındaki tam adrese kaydeder. seine v0.2.16 veya yenisi gerekir; kabuğun değiştirmemesi için komut satırında adı tırnak içine almalısınız.'
  - q: 'bntpool.com''da madencilik için hesap gerekir mi?'
    a: 'Hayır. Kayıt, parola veya e-posta yoktur. Blocknet adresiniz ya da onu gösteren Blocknet ID adınız hem giriş bilginiz hem ödeme hedefinizdir. seine''i bununla başlatın, sonra hashrate, share, rig, bakiye ve ödemelerinizi görmek için My Stats sayfasında aratın.'
  - q: 'Birden fazla bilgisayardan tek adrese madencilik yapabilir miyim?'
    a: 'Evet. Her bilgisayarda seine''i aynı adres veya adla çalıştırın ve her birine ayrı rig adı verin. Havuz share, bakiye ve ödemeleri adres başına sayar; tüm rig''ler tek bakiyeye ve ödemeye katkı yapar. My Stats her rig''i ayrı satırda listeler.'
---

Bu rehber seine ile bntpool.com havuzuna katılmayı anlatır: havuz adresi, Blocknet adresi veya Blocknet ID `@name` ile giriş, her rig'e ad verme ve share'lerin ulaştığını istatistikler sayfasından kontrol etme. seine henüz kurulu değilse önce [seine kurulum rehberini](https://seineminer.com/tr/blog/how-to-mine-blocknet/) izleyin, sonra buraya dönün.

## bntpool.com havuz adresi nedir?

<Answer>
bntpool.com Stratum adresi `stratum+tcp://bntpool.com:3333`'tür. seine ilk çalıştırmada bunu varsayılan havuz olarak sunar; havuz sorusunda Enter'a basmanız bağlanmak için yeterlidir. Blocknet adresiniz veya Blocknet ID `@name` ile giriş yaparsınız. Oluşturulacak hesap yoktur ve ödüller o adrese gider.
</Answer>

25 Ekim 2026 tarihinde herkese açık `/api/info` uç noktasının bildirdiği havuz ayarları şöyledir:

| Ayar | Değer |
|---|---|
| Stratum adresi | `stratum+tcp://bntpool.com:3333` |
| Giriş | Blocknet adresi, `@name` veya `$name` |
| Ödeme yöntemi | 6 saatlik pencereyle PPLNS |
| Ödeme öncesi onaylar | 60 blok |
| En düşük ödeme | 0,1 BNT |

Aynı değerleri istediğiniz zaman havuzun [Get Started](/start) sayfasında görebilirsiniz.

## bntpool.com neden seine'in varsayılan havuzu?

seine'i ilk çalıştırdığınızda havuzu sorar ve varsayılan olarak `stratum+tcp://bntpool.com:3333` değerini doldurur. Havuz ve madenci birkaç pratik yönden birlikte çalışır:

- **Tek protokol.** bntpool.com seine'in konuştuğu Stratum protokolünü (sürüm 2) çalıştırır; adresiniz dışında yapılandırma gerekmeden bağlanır.
- **Adlar iki tarafta da çalışır.** seine ve havuz Blocknet ID adlarını kabul eder ve blocknet.id üzerindeki aynı imzalı kayıtla doğrular.
- **Sürüm kontrolleri.** Her rig share'leriyle seine sürümünü bildirir; istatistikler sayfası eski seine kullanan rig'i sürümler sayfasına bağlanan “update available” rozetiyle işaretler.
- **seine kurulum rehberi.** Havuzun Get Started sayfası, kopyalayabileceğiniz bir komutla seine'i indirmeyi ve çalıştırmayı anlatır.

## seine bntpool.com'a nasıl bağlanır?

seine'i ilk başlattığınızda iki soru sorar:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Adresinizi yapıştırın veya adınızı yazın; sonra bntpool.com'u kabul etmek için Enter'a basın. seine iki yanıtı da `seine-data/seine.config.json` dosyasına kaydeder, böylece sonraki her başlangıç doğrudan madenciliğe geçer. Ayrıca rig adı seçip aynı dosyaya kaydeder (ayrıntısı aşağıda).

Terminalsiz bir rig'de, hizmette veya betikte soruları yanıtlayacak kimse yoktur. İlk başlangıçta iki değeri de bayrakla verin; Get Started sayfasının verdiği komut da budur:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Daha sonra havuzu veya adresi değiştirmek için `seine-data/seine.config.json` dosyasını düzenleyin veya silin ya da komut satırında `--address` ve `--pool-url` ile kayıtlı değerleri geçersiz kılın.

## Blocknet ID @name adına madencilik yapılabilir mi?

<Answer>
Evet. bntpool.com adres kabul ettiği her yerde `@name` veya `$name` biçiminde Blocknet ID kısa adını kabul eder. Havuz blocknet.id üzerinden imzalı eşlemeyi kontrol eder, adın gösterdiği adresi doğrular ve bağlantı boyunca share, bakiye ve ödemeleri o tam adrese kaydeder.
</Answer>

Adlar büyük-küçük harfe duyarsızdır; `@Alice` ve `@alice` aynı giriş bilgisidir. Ad desteği seine v0.2.16 ile geldi; güncel tüm sürümlerde vardır. Madencilik başlamadan önce seine adın çözümlendiği adresi yazdırır; cüzdanınızla eşleştiğini kontrol edebilirsiniz. Komut satırında adı tek tırnak içine alın. Aksi hâlde kabuk `$name` ifadesini değişken olarak genişletmeye çalışır:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Daha sonra adınızı başka cüzdana yönlendirirseniz seine'i yeniden başlatın. seine kayıtlı adı her başlangıçta çözümler ve adresi tüm çalışma boyunca tutar; havuz da her bağlantı için aynısını yapar. Önceden kazandığınız bakiye, kazanıldığı adreste kalır ve oraya ödenir; adı taşımak coin'lerinizi taşımaz.

[blocknet.id](https://blocknet.id) üzerinden ad kaydedebilirsiniz. [Blocknet ID rehberi](https://seineminer.com/tr/blog/blocknet-id-pay-by-name/) isimle ödemenin nasıl çalıştığını açıklar.

## Rig'lerinizi nasıl adlandırırsınız?

Her bağlantı, adresinizin yanında bir rig adıyla giriş yapar. İlk çalıştırmada seine sizin için `seine-` ve ardından dört rakamdan oluşan bir ad seçip yapılandırma dosyasına kaydeder; aynı makine yeniden başlatmalarda aynı adı korur. Tanıyacağınız bir ad kullanmak için komut satırında ayarlayın:

```sh
./seine --pool-worker rig-01
```

Havuz adın en fazla 64 karakterini saklar. Sabit ad yeniden başlatmadan sonra da yardımcı olur: havuz, adresiniz ve rig adıyla eşleyerek her rig'in share zorluğunu bir saat hatırlar; yeniden bağlanan rig önceden oturmuş zorlukla devam eder.

## Birden fazla rig'den tek adrese madencilik yapılabilir mi?

Evet. Her makinede seine'i aynı adres veya `@name`, ancak farklı rig adıyla başlatın. Havuz share, bakiye ve ödemeleri adres başına sayar; tüm rig'leriniz tek bakiyeye ve ödemeye eklenir. İstatistikler sayfasında her rig'in kendi satırı vardır; böylece tüm makinelerin çalıştığını kolayca görürsünüz.

## Madencinizin çalıştığını nasıl kontrol edersiniz?

seine paneli makinenin üzerinde hashrate ve kabul edilen share'leri gösterir. Havuzun gördüğünü görmek için [My Stats](/stats) sayfasını açın; adresinizi, `@name` veya `$name` adınızı girip **Lookup** düğmesine basın. Ad girerseniz sayfa çözümlenen adresi arama kutusunun yanında gösterir. Ayrıca o tarayıcıdaki son aramanızı hatırlar. [Madencinizi takip etme rehberi](/blog/tr/track-your-miner-on-bntpool/) sayfadaki her kartı anlatır.

Üç şeye bakın:

- **Hashrate ve Shares Accepted.** Hashrate kartı havuzun sizden gördüğünü gösterir; rig'ler iş gönderdikçe kabul sayısı yükselir.
- **Her rig için bir satır.** Her satır rig adı, hashrate, kabul ve ret edilen share'ler ile son share zamanını gösterir. Birkaç saniye önceki share, rig'in bağlı ve çalışıyor olduğunu gösterir.
- **Recent Shares.** Son share'ler, gönderen rig, zorluk ve durumlarıyla listelenir.

Havuz her bağlantı için share zorluğunu ayrı belirler ve madencilik sırasında ayarlar. Böylece küçük bir CPU bile düzenli share gönderir; ilk kabul edilen share'ler bağlantıdan birkaç dakika sonra görünür. Donanımınızdan beklenen hashrate için [Blocknet madencilik donanımı rehberine](https://seineminer.com/tr/blog/blocknet-mining-hardware-hashrate/) bakın.

Ardından ödüller gelir. Share'leriniz altı saatlik pencere içindeyken havuz blok bulunca Estimated Rewards görünür. Blok 60 onay aldığında payınız Confirmed Rewards'a geçer; en az 0,1 BNT olduğunda havuz otomatik gönderir. [PPLNS ödemeleri nasıl çalışır?](/blog/tr/how-pplns-payouts-work/) her blok ödülünün paylaşımını açıklar. Havuzun tamamının geçmişi [Blocks](/blocks) ve [Payouts](/payouts) sayfalarındadır.

## Get Started sayfası neleri kapsar?

Havuzun [Get Started](/start) sayfasında her şey tek ekrandadır:

- **Havuz bilgileri.** Stratum adresi (kopyalamak için tıklayın), ödeme yöntemi, en düşük ödeme ve ödeme öncesi blok onayı sayısı havuzdan canlı okunur.
- **Üç adımda hızlı başlangıç.** [seineminer.com](https://seineminer.com/tr/#download) üzerinden seine'i indirin, bir kez çalıştırıp adres veya adınızı ve havuz URL'sini girin; sonra madenciliğe başlayıp My Stats'ta hashrate ve bakiyenizi izleyin.
- **Kopyalanacak komut.** Havuz URL'si doldurulmuş tam `./seine` komutu. Adresinizi veya tırnak içinde `'@yourname'` adınızı yerleştirin.
- **Kurulum notları.** seine'in yapılandırmayı nereye kaydettiği, nasıl sıfırlanacağı ve geçersiz kılan bayraklar.
- **Ödüllerin sayımı.** Kısa PPLNS açıklaması: blok bulunmadan önce gönderdiğiniz share'ler onun ödülünü kazanır; sonradan eklenen iş sonraki bloklara sayılır. Onaylanmamış blokların ödüllerinin bloklar onaylanana kadar My Stats'ta ayrı gösterildiğini de belirtir.
- **Ekran görüntüleri.** Başlamadan ne bekleyeceğinizi görmeniz için seine'in ilk çalıştırma kurulumu ve madencilik paneli.

<CTA target="pool" title="bntpool.com'da madenciliğe başlayın" text="seine'i adresiniz veya @name ile bntpool.com'a yönlendirin ve share'lerinizin My Stats'a gelişini izleyin." />
