---
title: bntpool.com kontrol noktalarıyla Blocknet düğümünü hızla eşitleyin
description: Blocknet düğümleri bntpool.com checkpoints.dat dosyasıyla hızlı eşitlenir. Kontrol noktalarının zinciri nasıl sabitlediğini öğrenin, dosyayı indirip doğrulayın.
tags: [Blocknet düğümü, Kontrol noktaları, Hızlı eşitleme, bntpool]
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Blocknet düğümü kontrol noktalarını nereden alır?'
    a: 'bntpool.com''dan. İlk başlangıçta veri dizininde checkpoints.dat yoksa Blocknet daemon''ı dosyayı https://bntpool.com/checkpoints.dat adresinden indirir ve hızlı eşitleme için kullanır. Dosyayı Checkpoints sayfasından kendiniz de indirip SHA-256 değerini kontrol edebilir ve düğümü başlatmadan önce veri dizinine koyabilirsiniz.'
  - q: 'bntpool.com checkpoints.dat dosyasını ne sıklıkta günceller?'
    a: 'Havuzun daemon''ı zincir büyüdükçe dosyayı her 100. blok için bir kayıtla yeniden yazar. Her kayıt zincirin ucundan en az 100 blok geridedir; bu, Blocknet''in azami yeniden düzenleme derinliğinin ötesindedir. Böylece kontrol noktaları yalnızca kesinleşmiş geçmişi kapsar. Checkpoints sayfası son yüksekliği ve dosyanın son değişme zamanını gösterir.'
  - q: 'Kontrol noktaları cüzdan bakiyemi veya geçmişimi değiştirir mi?'
    a: 'Hayır. Kontrol noktalarıyla eşitlenen düğüm yine her bloğu saklar ve her çıktı ile anahtar görüntüsünü indekslerine kaydeder; cüzdan taraması ve bakiyeler tam eşitleme sonrasıyla aynıdır. Kontrol noktaları yalnızca hash''lerinin zaten doğruladığı geçmiş için iş kanıtını ve imzaları yeniden kontrol etmeyi atlamaya izin verir.'
  - q: 'Blocknet düğümünü kontrol noktaları olmadan eşitleyebilir miyim?'
    a: 'Evet. Daemon''ı --full-sync bayrağıyla başlatın veya blocknet yapılandırmasında full_sync değerini true yapın. Düğüm kontrol noktası indirmesini atlar ve genesis''ten itibaren her bloğun iş kanıtını ve imzalarını yeniden doğrular. Kontrol noktasıyla eşitlemeden uzun sürer; bu yüzden yeni düğümlerde kontrol noktaları varsayılandır.'
---

Yeni bir Blocknet düğümü, cüzdana veya solo madenciye hizmet vermeden önce genesis'ten bu yana tüm bloklara yetişmelidir. Kontrol noktaları bunu hızlandırır. Bu rehber, bntpool.com'un kontrol noktası dosyasının içeriğini, Blocknet daemon'ının gerçek zincire bağlı kalarak en yavaş kontrolleri nasıl atladığını ve dosyayı kendiniz nasıl indirip doğrulayarak yükleyebileceğinizi açıklar.

## Blocknet kontrol noktaları nedir?

<Answer>
Blocknet kontrol noktaları, her 100. blok için bir tane olmak üzere bilinen blok hash'lerinin listesidir; bntpool.com bunları checkpoints.dat olarak sunar. Eşitlenen düğüm aldığı blokların bu hash'lerle eşleştiğini kontrol eder ve son kontrol noktasına kadar iş kanıtını ve imzaları yeniden doğrulamayı atlar. Böylece ilk eşitleme çok daha hızlı biter.
</Answer>

Dosya, her satırda bir `height:hash` çifti bulunan düz metindir. 25 Ekim 2026 tarihinde ilk satırlar şöyleydi:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Her hash bloğun kimliğidir, yani başlığının SHA3-256 hash'idir. Havuzun kendi daemon'ı dosyayı kendi zincirinden yazar ve yalnızca en az 100 blok derinlikteki blokları ekler. Yüz blok, Blocknet'in kesin finalite sınırı, protokolün izin verdiği en derin yeniden düzenlemedir; dolayısıyla her kontrol noktası kesinleşmiş geçmiştedir.

25 Ekim 2026 tarihinde saat 02:04 EDT'de dosyada 100 yüksekliğinden 68.900'e kadar 689 kontrol noktası vardı ve boyutu 48.811 bayttı. Yaklaşık her 100 blokta, beş dakikalık bloklarla yaklaşık sekiz saatte bir, bir satır büyür. Güncel yükseklik için [Checkpoints sayfasına](/checkpoints) bakın.

## Blocknet düğümü kontrol noktalarını nasıl kullanır?

Kontrol noktalarını yükleyen düğüm, son kontrol noktasının yüksekliğini hızlı eşitleme hedefi sayar. Zinciri o yüksekliğe ulaşana kadar en pahalı iki kontrol için yayımlanan kontrol noktalarına dayanır, diğer tüm kontrolleri normal biçimde yürütür:

- **İş kanıtı kontrol noktalarına dayanır.** Her Blocknet bloğunun iş kanıtı, 2 GiB bellek üzerinde bir Argon2id hash'idir. Her tarihî blok için yeniden hesaplamak pahalıdır ve kontrol noktası hash'leri zaten bu geçmişi doğrular.
- **RingCT imzaları ve aralık kanıtları da bunlara dayanır.** Daemon'ın kendi yorumları, her bloğun coinbase'i dahil her çıktıda bulunduğu için çıktı aralık kanıtı doğrulamasını eşitlemenin en büyük tek maliyeti olarak tanımlar.
- **Diğer tüm kontroller sürer.** Blok bağlantıları, LWMA zorluk kuralı, zaman damgası kuralları, blok boyutu ve yapısı, anahtar görüntülerinde çifte harcama kontrolleri ve her girdiyle imzası arasındaki bağ kontrolleri normal biçimde çalışır. Çıktılar ve anahtar görüntüleri her zamanki gibi düğüm indekslerine girer; cüzdan taraması ve bakiyeler tam eşitleme sonrasıyla aynıdır.

Atlama yalnızca düğümün mevcut zincir ucunu uzatan bloklara ve yalnızca son kontrol noktasının altında uygulanır. Düğüm bu yüksekliği geçtikten sonra her yeni bloğu iş kanıtı dahil tam olarak doğrular.

### Kontrol noktaları güvenilen zinciri nasıl sabitler?

Düğüm son kontrol noktasına doğru eşitlenirken bir kontrol noktası yüksekliğindeki blok tam olarak dosyadaki hash'e sahip olmalıdır. Farklı hash'e sahip blok, kontrol noktası uyuşmazlığı hatasıyla reddedilir. Kontrol noktaları her 100 blokta bir geldiğinden bir eş, hızlı eşitlenen düğümü farklı bir geçmişe yönlendiremez; sonraki kontrol noktası yüksekliği bunu ortaya çıkarır.

Kontrol noktaları her yeniden başlatmayı da hızlandırır. Daemon, diskte bloklar varken başladığında saklanan zinciriyle eşleşen en yüksek kontrol noktasını bulur ve genesis'ten tüm zinciri dolaşmak yerine yalnızca ondan sonraki blokları yeniden kontrol eder.

## Yeni düğüm kontrol noktalarını otomatik indirir mi?

<Answer>
Evet. Blocknet daemon'ı başladığında veri dizininde checkpoints.dat yoksa dosyayı https://bntpool.com/checkpoints.dat adresinden indirir, yükler ve son kontrol noktasına kadar hızlı eşitler. İndirme mümkün olduğunca denenir; tamamlanamazsa düğüm yine başlar ve zinciri tam olarak doğrular.
</Answer>

Bu adres Blocknet çekirdeğine yazılmış varsayılandır; dolayısıyla her yeni düğüm doğrudan bntpool.com'un yayımladığı dosyayla hızlı eşitlenir. Daemon dosyayı yalnızca mevcut değilse alır, isteğe 15 saniye tanır ve indirmeyi 32 MiB ile sınırlar.

## bntpool.com kontrol noktaları adım adım nasıl kullanılır?

Dosyayı kendiniz alıp kontrol etmek veya uzun bir yeniden eşitlemeden önce eski kopyayı yenilemek isterseniz şu adımları uygulayın.

### 1. Veri dizininizi bulun

Kontrol noktası dosyası, zincir veritabanının yanında düğümün veri dizinindedir:

- **`blocknet` yöneticisi:** Yapılandırmada `data_dir` belirlemediyseniz macOS ve Linux'ta `~/.config/bnt/data/mainnet/`, Windows'ta `C:\Users\<you>\.config\bnt\data\mainnet\`.
- **Doğrudan çalıştırılan çekirdek:** Başlattığınız klasördeki `./blocknet-data-mainnet` veya `--data` ile verdiğiniz konum.

### 2. checkpoints.dat dosyasını indirin

[Checkpoints](/checkpoints) sayfasını açıp **Download checkpoints.dat** düğmesine basın veya terminalden alın. Bu örnek çekirdeğin varsayılan veri dizinine yazar:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Mevcut dosyanın üzerine indirmek onu değiştirir; eski kopyayı böyle yenilersiniz.

### 3. SHA-256 değerini kontrol edin

Checkpoints sayfası güncel dosyanın SHA-256 değerini yayımlar. Kopyanızın hash'ini hesaplayıp karşılaştırın:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

macOS'ta bunun yerine `shasum -a 256` kullanın. Havuz her kontrol noktası eklediğinde hash değişir; bu nedenle indirdikten hemen sonra sayfayla karşılaştırın.

### 4. Bir kontrol noktasını gezginde örnek olarak doğrulayın

Herhangi bir satırı herkese açık [Blocknet gezginiyle](https://explorer.blocknetcrypto.com) doğrulayabilirsiniz. Dosyadaki bir yüksekliğin blok sayfasını, örneğin `explorer.blocknetcrypto.com/block/68900`, açın ve hash'ini 68.900 satırıyla karşılaştırın. Bunu 25 Ekim 2026 tarihindeki son kontrol noktası için yaptık ve hash'ler eşleşti.

### 5. Düğümü başlatın

Daemon'ı başlatın veya yeniden başlatın. Başlangıçta `checkpoints.dat` dosyasını okur ve son kontrol noktasına kadar hızlı eşitler. Çekirdeği etkileşimli kabuğuyla (`--cli`) çalıştırıyorsanız `load-checkpoints` komutu dosyayı çalışan düğüme yükler ve hızlı eşitleme yüksekliğini bildirir:

```text
> load-checkpoints
```

Kontrol noktaları düğüm gerideyken yardımcı olur. Zinciriniz son kontrol noktası yüksekliğini geçtiğinde düğüm zaten her bloğu tam doğruluyordur ve daha yeni dosya bir şeyi değiştirmez.

## Kontrol noktası dosyasını bir betikten nasıl denetlersiniz?

Havuz dosyanın ayrıntılarını API anahtarı gerektirmeden `https://bntpool.com/api/checkpoints` adresinde JSON olarak yayımlar:

| Alan | Anlamı |
|---|---|
| `available` | Havuzda sunulacak dosya bulunup bulunmadığı |
| `url` | İndirme adresi |
| `entries` | Dosyadaki kontrol noktası sayısı |
| `latest_height`, `latest_hash` | En yüksek kontrol noktası ve blok hash'i |
| `bytes`, `sha256` | Dosya boyutu ve SHA-256 |
| `updated_at` | Unix zaman damgası olarak dosyanın son değişme zamanı |

İndirme, `X-Checkpoint-Height` ve `X-Checkpoint-Sha256` başlıklarıyla hash'ten oluşturulan bir `ETag` taşır. Böylece betik `If-None-Match` gönderebilir ve hiçbir şey değişmediğinde hızlıca `304 Not Modified` alabilir:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Yanıtlar 60 saniye önbelleğe alınabilir; yaklaşık sekiz saatte bir değişen dosya için bu fazlasıyla yeterlidir.

<Note>
blocknetcrypto.com, planlanan Faraday yükseltmesinin yeni düğümlerin zincire daha hızlı yetişmesi için daha hafif bir iş kanıtı getireceğini söylüyor. O zamana kadar düğümü güncel hâle getirmenin hızlı yolu kontrol noktalarıdır.
</Note>

## Eşitlenmiş düğümle ne yapabilirsiniz?

Eşitlenmiş düğüm Blocknet cüzdanını ve düğüm API'sini destekler; `--mode daemon` ile solo madencilik yaparken seine onunla iletişim kurar. [Blocknet için havuz ve solo madencilik karşılaştırması](/blog/tr/pool-vs-solo-mining-blocknet/), solo madenciliğin donanımınıza uygunluğunu anlamayı gösterir. Düğüm çalıştırmadan başlamak isterseniz seine varsayılan olarak bntpool.com'da madencilik yapar: [seine kurulum rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) ilk share'lerinize ulaştırır, [bntpool.com'da madencilik](/blog/tr/mine-blocknet-on-bntpool/) ise havuz tarafını anlatır.

<CTA target="pool" title="Blocknet kontrol noktalarını sunan havuzda madencilik yapın" text="Birkaç dakika içinde bntpool.com'a katılın; hashrate, share ve ödemelerinizi canlı takip edin." />
