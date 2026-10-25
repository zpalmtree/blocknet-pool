---
title: 'Blocknet’in adil başlangıcı: premine yok, her coin madencilikle üretilir'
description: Blocknet premine, geliştirici fonu veya hazine olmadan başladı. Boş genesis bloğunu ve tek çıktılı coinbase kuralını kendiniz doğrulayın, BNT üretin.
tags: [Blocknet, Adil başlangıç, Genesis bloğu, Madencilik, bntpool]
sourceHash: 814839500c3d
translation: draft
faq:
  - q: 'Blocknet''te premine var mıydı?'
    a: 'Hayır. 15 Şubat 2026 tarihinde oluşturulan Blocknet genesis bloğu hiçbir işlem içermez; dolayısıyla madencilik başlamadan önce coin yoktu. İlk coin''ler 1. bloğun ödülünden geldi ve o zamandan beri her BNT, kaynaklandığı bloğu üreten madenciye ödendi.'
  - q: 'Blocknet''in geliştirici fonu veya hazinesi var mı?'
    a: 'Hayır. Konsensüs her blokta tam bir coinbase işlemine izin verir ve bu işlemde tam olarak blok ödülü değerinde tek çıktı bulunmalıdır. Geliştirici, kurucu veya hazine için ikinci çıktı yoktur; eklemeye çalışan bir blok ağdaki her düğüm tarafından reddedilir.'
  - q: 'Blocknet''in genesis bloğu ne zaman oluşturuldu?'
    a: 'Genesis bloğunun zaman damgası 15 Şubat 2026, 20:58:38 EST, yani 16 Şubat 01:58:38 UTC''dir. "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" mesajını taşır; önceki blok hash''i bu mesajın SHA3-256 hash''idir.'
  - q: 'Blocknet''in adil başlangıcını kendim nasıl doğrulayabilirim?'
    a: 'Blocknet gezgininde 0. bloğu açın; sıfır işlem ve tümü sıfır olan bir Merkle kökü göreceksiniz. Sonra açık kaynak daemon kodunu okuyun: genesis bloğu boş işlem listesiyle sabitlenmiştir ve coinbase kontrolü blok ödülüne eşit tek çıktıyı kabul eder.'
  - q: 'Blocknet madenciliğine nasıl başlarım?'
    a: 'Windows, macOS veya Linux için seine madencisini indirin, çalıştırın ve sorulduğunda Blocknet adresinizi veya Blocknet ID kısa adınızı yapıştırın. Varsayılan havuz bntpool.com''u kabul etmek için Enter''a basın; seine hemen madenciliğe başlar. Ardından bntpool.com istatistikler sayfasında adresinizi aratabilirsiniz.'
---

Blocknet kimseye tahsis yapılmadan başladı. Bu sayfa genesis bloğunun içeriğini, premine, geliştirici fonu veya hazineyi imkânsız kılan konsensüs kurallarını, bunları gezginde ve kaynak kodunda nasıl kontrol edeceğinizi ve bntpool.com'da kendiniz BNT üretmeye nasıl başlayacağınızı gösterir.

## Blocknet'te premine var mıydı?

<Answer>
Hayır. 15 Şubat 2026 tarihinde oluşturulan Blocknet genesis bloğu hiçbir işlem içermez; bu nedenle madencilik başlamadan önce coin yoktu. 1. bloktan bu yana her blok, ödülünü takvimdeki tutara tam olarak eşit tek bir coinbase çıktısında ödedi. Dolayısıyla dolaşımdaki her BNT, iş kanıtını çalıştıran biri tarafından üretildi.
</Answer>

Ne genesis bloğunda ne de sonraki herhangi bir blokta kuruculara, geliştiricilere, yatırımcılara veya bir vakfa tahsis vardır. Coin arzı yalnızca blok ödüllerinden gelir. Bunu uygulayan kurallar konsensüsün parçasıdır; ağdaki her düğüm her blokta bunları denetler.

## Blocknet'in genesis bloğunda ne var?

Genesis bloğu daemon içinde sabit kodlanmıştır; böylece her düğüm aynı 0. bloktan başlar. Alanları sabittir:

| Alan | Değer |
|---|---|
| Yükseklik | 0 |
| Zaman damgası | 15 Şubat 2026, 20:58:38 EST (16 Şubat 01:58:38 UTC) |
| Mesaj | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Önceki hash | Mesajın SHA3-256 hash'i: `a94a7f2b…d909df` |
| Zorluk | 4, ağın alt sınırı |
| Nonce | 0 |
| İşlemler | yok |
| Merkle kökü | tümü sıfır |

Mesaj, 15 Şubat 2026 tarihli bir Wired yazısının başlığıdır ve kod kaynak olarak bu yazıyı gösterir. Önceki hash mesajdan türetildiği için başlık, 0. bloğun ve üzerine kurulan her şeyin kimliğine sabitlenmiştir.

Daemon'daki genesis fonksiyonunun üzerindeki yorum açıkça şöyle der: “Genesis'te işlem yoktur; coinbase yok, yakılmış coin yok. 1 yüksekliğindeki ilk gerçek blok ilk coinbase'i içerecek.” Doğrulama bu yorumu destekler. 0. bloğu kontrol eden bir düğüm, tam sıfır işlem ve tümü sıfır olan Merkle kökü yoksa onu reddeder. Dolayısıyla değiştirilmiş bir düğüm bile diğer herkes tarafından reddedilmeden genesis bloğuna coin ekleyemez.

## Konsensüs geliştirici fonunu veya hazineyi nasıl dışlar?

<Answer>
Her Blocknet bloğu tam bir coinbase işlemiyle başlamalıdır. Bu coinbase'in girdisi olmamalı, ücreti sıfır olmalı ve tutarı o yükseklikteki blok ödülüne eşit tek çıktısı bulunmalıdır. İkinci bir ödeme için yer yoktur; bu yüzden zincir üzerinde geliştirici fonu, kurucu ödülü veya hazine çıktısı bulunamaz.
</Answer>

Daemon'daki birkaç kontrol burada birlikte çalışır:

- **Blok başına tek coinbase.** İlk işlem coinbase olmalıdır; bloktaki başka hiçbir işlem coinbase olamaz.
- **Tek çıktı, sıfır girdi.** Sıfır veya iki çıktısı ya da bir girdisi olan coinbase geçersizdir.
- **Tam ödül tutarı.** Blocknet tutarları Pedersen taahhütleriyle gizler, ancak coinbase özel bir durumdur. Körleme faktörü açık verilerden (işlemin açık anahtarı ve blok yüksekliği) türetilir; böylece her düğüm, takvimdeki ödülün taahhüdünü yeniden oluşturup karşılaştırabilir. Çıktı başka bir tutara taahhüt ediyorsa blok reddedilir.
- **Boş memo.** Coinbase memo'su boş olmalıdır; blok ödülleri ek veri de taşıyamaz.

Normal işlemler coin yaratamaz. Her biri dengelenmelidir: girdiler, çıktılar artı ücrete eşittir ve bu, taahhütlerin kendileri üzerinde kontrol edilir. Her çıktı ayrıca tutarın geçerli bir aralıkta olduğunu ve negatif olamayacağını gösteren bir Bulletproofs aralık kanıtı taşır. Böylece tutarlar gizli kalırken emisyon takvimi tam olarak izler.

## Blocknet'in adil başlangıcını kendiniz nasıl kontrol edersiniz?

Kimsenin sözüne güvenmeniz gerekmez. Hem zincir hem kod herkese açıktır.

**Gezginde.** [Blocknet gezgininde 0. bloğu](https://explorer.blocknetcrypto.com/block/0) açın. 0 işlem, 64 sıfırlı Merkle kökü, 0 nonce ve `a94a7f2b` ile başlayan önceki hash listelenir. Blok ödülü satırı o yükseklik için takvimdeki tutarı gösterir, ancak blokta işlem olmadığından bunu alacak çıktı yoktur. Sonra [1. bloğu](https://explorer.blocknetcrypto.com/block/1) açın: tek işlemi, girdisi olmayan ve tek çıktısı bulunan bir coinbase'dir; zincirdeki ilk coin'lerdir.

Önceki hash'in gerçekten genesis mesajının hash'i olduğunu doğrulayabilirsiniz. OpenSSL 1.1.1 veya daha yenisiyle çalıştırın:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Çıktı `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df` ile biter; gezginin 0. blok için gösterdiği değerle aynıdır.

**Kodda.** Daemon'ın açık kaynak kodu [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core) adresindedir. [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go) içinde şu fonksiyonları arayın:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock`, 0. bloğu boş işlem listesiyle oluşturur; `validateGenesisBlock`, işlem içeren her 0. bloğu reddeder; `validateCoinbaseConsensus` ise blok ödülü değerinde tek çıktı şartını uygular. `transaction.go` içindeki `validateCoinbase` ve `verifyCommitmentBalance`, coinbase yapısını ve normal işlemlerin bakiye kontrolünü kapsar.

## Adil başlangıç madenciler için neden önemlidir?

Madenciler için adil başlangıç, coin'lerin kaynağını blok ödülünün tek başına açıklaması demektir.

- **Tüm yeni coin'ler madencilere gider.** Premine ve hazine çıktısı olmadığından emisyonun %100'ü blokları bulan kişilere ödenir. 1. blok tek coinbase çıktısında 723,25093035 BNT ödedi; sonraki her blok da tüm ödülünü aynı şekilde ödedi.
- **Herkes aynı kurallara uyar.** 1. bloktan beri yeni BNT kazanmanın tek yolu iş kanıtını çalıştırmaktır. [Ödül takvimi](/blog/tr/blocknet-block-reward-schedule/) kodda belirlenmiştir; ilk madenciye ve bugün başlayana eşit uygulanır.
- **İş kanıtı sıradan bilgisayarlara uygundur.** Blocknet hash başına 2 GiB bellekle Argon2id kullanır; bu algoritma ASIC'lere direnmek üzere tasarlanmıştır. Böylece madencilik, insanların zaten sahip olduğu masaüstü, dizüstü ve sunuculara açık kalır. [seineminer.com'daki Argon2id açıklaması](https://seineminer.com/tr/blog/argon2id-proof-of-work-explained/) nedenini anlatır.
- **Emisyonu denetlemek kolaydır.** Her coin, her düğümün kontrol ettiği bir coinbase çıktısına dayanır; dolayısıyla herhangi bir yükseklikte üretilmiş toplam miktar doğrudan ödül takviminden çıkar.

Coin'i yeni tanıyorsanız [Blocknet nedir?](https://seineminer.com/tr/blog/what-is-blocknet/) yazısı gizliliği, isimle ödemeyi ve cüzdanları anlatır.

## Kendiniz nasıl Blocknet madenciliği yaparsınız?

Adil başlangıca katılmak madencilik yapmak demektir. En hızlı yol bntpool.com'da seine madencisidir:

1. **Bir adres edinin.** Resmî Blocknet [komut satırı cüzdanını](https://github.com/blocknetprivacy/blocknet/releases/latest) veya [masaüstü cüzdanını](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) kurup adresinizi kopyalayın ya da `@name` gibi bir Blocknet ID kısa adı kullanın.
2. **seine'i indirin.** [seineminer.com](https://seineminer.com/tr/#download) üzerinden Windows, macOS veya Linux arşivini alıp açın. Kurulacak bir şey yoktur.
3. **Çalıştırın ve varsayılanları kabul edin.** seine'i başlatın, adresinizi veya `@name` adınızı yapıştırın ve varsayılan havuz `stratum+tcp://bntpool.com:3333` için Enter'a basın. seine hemen hash hesaplamaya başlar; çalışırken CPU'nuzda, Windows ve Linux'ta ise NVIDIA grafik kartlarında da iş parçacıklarını ve kernel'leri ayarlar.
4. **Madencinizi izleyin.** Hashrate, share ve bakiyenizi görmek için [istatistikler sayfasında](/stats) adresinizi aratın; havuzun bulduklarını [bloklar sayfasından](/blocks) takip edin.

[bntpool.com'da madencilik rehberi](/blog/tr/mine-blocknet-on-bntpool/) adres veya adla giriş yapmayı ve madencinizi kontrol etmeyi anlatır. [Get Started sayfası](/start) havuzun ödeme ayarlarını listeler; [Blocknet madenciliği rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) ise her sistemde seine kurulumunu adım adım gösterir.

<CTA target="seine" title="Kendi payınızı üretin" text="Her BNT madencilikle üretilir. seine'i indirin ve birkaç dakika içinde bntpool.com'da madenciliğe başlayın." />
