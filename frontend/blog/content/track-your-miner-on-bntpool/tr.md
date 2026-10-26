---
title: bntpool.com'da madencinizi takip edin
description: bntpool.com'da adres veya @name ile hashrate, share, bakiye ve ödemeleri görün; blokları, şansı, çalışma süresini izleyin veya açık API'yi kullanın.
tags: [bntpool, Madencilik istatistikleri, Havuz API’si, Blocknet]
sourceHash: 'f785e00cbe03'
translation: draft
faq:
  - q: 'bntpool.com''daki hashrate''im neden seine''den farklı?'
    a: 'seine makinenizin hesapladığı her hash''i sayar; havuz ise kabul ettiği share''lerin zorluğundan son bir saatin ortalamasıyla hashrate tahmin eder. Share''ler rastgele anlarda geldiğinden havuz değeri daha çok dalgalanır ve zamanla oturur. Adil karşılaştırma için My Stats''taki 24 saatlik grafikle seine ortalamasını karşılaştırın.'
  - q: 'bntpool.com istatistiklerimi @name ile arayabilir miyim?'
    a: 'Evet. My Stats arama kutusuna @name veya $name gibi Blocknet ID kısa adınızı yazın. Sayfa bunu blocknet.id üzerinden çözümler, istatistiklerinizi gösterir ve çözümlenen tam adresi arama kutusuna yerleştirir. Havuz API''sini betikten çağırırken bu tam adresi kullanın.'
  - q: 'bntpool.com API''si anahtar gerektirir mi?'
    a: 'Hayır. Havuz istatistikleri, bloklar, ödemeler, şans, durum ve madenci uç noktaları HTTPS üzerinden herkese açık JSON sunar; curl komutu veya ev paneli kayıt olmadan okuyabilir. Tutarlar atomik birimdedir; BNT için 100.000.000''a bölün ve her 30–60 saniyede bir sorgulayın.'
  - q: 'My Stats neden sıfır Estimated Rewards gösteriyor?'
    a: 'Estimated Rewards, share''leriniz ödeme penceresindeyken havuz blok bulduğunda görünür. Yeni madencide birkaç madencilik turu sürebilir. Önce kabul edilen share''ler ve hashrate, sonra tahmini ödüller görünür; bunlar Confirmed Rewards olarak olgunlaşır ve otomatik ödenir.'
---

seine bntpool.com'da madencilik yapmaya başlayınca havuz gördüğü her şeyi gösterir: hashrate'iniz, kabul edilen her share, kazancınız ve adresinize gönderilen her ödeme. Bu rehber My Stats sayfasını, tüm havuzun herkese açık sayfalarını ve betiklerle ev panellerinden kullanabileceğiniz açık API'yi anlatır.

Henüz madenciliğe başlamadıysanız [seine kurulum rehberi](https://seineminer.com/tr/blog/how-to-mine-blocknet/) birkaç dakikada ilk share'lerinize ulaştırır.

## bntpool.com'da madenci istatistikleri nasıl kontrol edilir?

<Answer>
bntpool.com/stats adresindeki My Stats'ı açın, Blocknet adresinizi veya @name adınızı yapıştırıp Lookup düğmesine basın. Sayfa hashrate, kabul ve ret edilen share'ler, tahmini, onaylanmış ve ödenmiş bakiyeler, hashrate grafiği, her rig için satır ve son ödemelerinizi gösterir. Sekme açıkken kendiliğinden yenilenir.
</Answer>

[My Stats](/stats) sayfasına gidip seine'e verdiğiniz adresi veya [Blocknet ID kısa adını](https://seineminer.com/tr/blog/blocknet-id-pay-by-name/) girin. `@name` veya `$name` gibi bir ad blocknet.id üzerinden çözümlenir; arama kutusu ardından gösterdiği tam adresi içerir. Sayfa o tarayıcıdaki son aramanızı hatırlar; sonraki gelişinizde sayılarınız hazırdır.

## My Stats sayfası neleri gösterir?

Sayfa kart ve tablo gruplarına ayrılır; her biri madencinizle ilgili bir soruyu yanıtlar.

### Bakiye

- **Estimated Rewards:** Havuzun bulduğu, hâlâ olgunlaşan bloklardaki payınız.
- **Confirmed Rewards:** Sonraki ödemeyi bekleyen olgunlaşmış bakiye.
- **Paid Balance:** Adresinize gönderilmiş toplam tutar.

Bekleyen ödülleriniz olduğunda **Estimated From Recent Blocks** tablosu her bloğu yüksekliği, tahmini kazancınız, kalan onay sayısı ve bulunma zamanı ile listeler. Her yükseklik [Blocknet gezginindeki](https://explorer.blocknetcrypto.com) bloğa bağlanır; kendiniz kontrol edebilirsiniz. [PPLNS ödemeleri nasıl çalışır?](/blog/tr/how-pplns-payouts-work/) her blok ödülünün paylaşımını ve bakiyenizin ne zaman ödendiğini açıklar.

### Madencilik ve share'ler

**Mining** kartları güncel **Hashrate**, kendi rig'lerinizin havuz için çözdüğü **Blocks Found** sayısı ve başladığınız tarihi (**Mining Since**) gösterir. Altında **Shares Accepted**, **Shares Rejected**, **Reject Rate** ve **Avg Difficulty** vardır. Sağlıklı rig düzenli kabul edilen share akışı ve sıfıra yakın ret oranı gösterir.

**Hashrate History**, 1h, 24h, 7d veya 30d boyunca hashrate grafiğinizi çizer. Uzun aralıklar, rig'in tüm gece çalışıp çalışmadığını görmenin en iyi yoludur.

### Her rig için bir satır

Birden fazla makine çalıştırıyorsanız her birine ayrı ad verin; havuz hashrate, kabul ve ret edilen share'ler ve son share zamanı ile ayrı listeler. seine ilk çalıştırmada `seine-` ve dört rakamdan oluşan ad seçer; komut satırında kendiniz belirleyebilirsiniz:

```sh
./seine --pool-worker rig-01
```

Eski seine çalıştıran rig, [seine sürümler sayfasına](https://github.com/zpalmtree/seine/releases) bağlanan **update available** rozeti alır; güncellemeyi unuttuğunuz makineyi kolayca fark edersiniz.

### Ödemeler ve son share'ler

**Recent Payouts**, adresinize yapılan her ödemeyi tutarı, gezgindeki işlem bağlantısı ve queued, unconfirmed veya confirmed durumuyla listeler. **Recent Shares**, son 20 share'i görev, rig, zorluk ve durumuyla gösterir; yeni başlattığınız makinenin havuza ulaştığını doğrulamanın en hızlı yoludur.

## Havuz hashrate'i neden seine'den farklı?

seine makinenizin hesapladığı her hash'i sayar. Havuz yalnızca gönderdiğiniz share'leri görür; hashrate'inizi son bir saatte kabul edilen share'lerin toplam zorluğundan tahmin eder. Her share rastgele olaydır; bntpool.com bağlantı başına zorluğu 5 dakikada yaklaşık 10 share hedefleyecek şekilde ayarlar. Kısa pencerede bu nedenle biraz gürültü bulunur.

İki alışkanlık sayıları karşılaştırılabilir kılar:

- Ortalamaları karşılaştırın: My Stats'taki 24h grafiğini seine'in aynı dönemdeki ortalamasıyla.
- Yeni rig'e zaman tanıyın. Yeni başlayan rig için havuz madencilik yaptığı dakikalar üzerinden ortalama alır; ilk okuma birkaç share'e dayanır. Bir saat sonra tam bir saatlik pencere uygulanır.

Recent Shares tablosu, hashrate değeri oturmadan önce bile share'lerin geldiğini hemen gösterir.

<span id="zero-hashrate"></span>

## Seine madencilik yaparken My Stats neden sıfır hashrate gösteriyor?

Önce ödeme adresini, havuz bağlantısını ve kabul edilen payları kontrol edin. Yerel hashrate, makinenin hash hesapladığını gösterir; havuz tahmini, sorguladığınız adres için kabul edilmiş paylara ihtiyaç duyar.

1. **Adresi eşleştirin.** Seine’in kullandığı alıcı adresini veya kayıtlı `@name` adını sorgulayın. Kısa adın adresini değiştirdiyseniz yeniden çözümlemesi için seine’i yeniden başlatın, ardından çözümlenen adresi My Stats’ta kontrol edin.
2. **Havuzu eşleştirin.** Seine’in `stratum+tcp://bntpool.com:3333` kullandığını kontrol edin. Başka yerde veya solo modunda çalışan bir makine buraya pay eklemez. [Havuz kurulum rehberi](/blog/tr/mine-blocknet-on-bntpool/) bağlantıyı açıklar.
3. **Recent Shares bölümünü kontrol edin.** Kabul edilen payları ve sayfanın yenilenmesini bekleyin. Paylar değişken aralıklarla gelir; yerel hashrate’in çalışması anında havuz tahmini garantilemez. Kabul edilen paylar geliyorsa zaman içindeki ortalamaları karşılaştırın.
4. **Bağlantı mesajını okuyun.** Madenci henüz kabul edilen pay göndermiyorsa tabloyu kullanın. Güncel olaylar için [Status](/status) sayfasını kontrol edin.

| Seine mesajı | Sonraki kontrol |
|---|---|
| `failed to connect to pool at` | Sunucu adını ve portu, bağlantınızı ve 3333 portuna giden TCP trafiğine izin verilip verilmediğini kontrol edin. |
| `login rejected:` | Devamındaki nedeni okuyun; alıcı adresini veya kısa adı kontrol edin ve seine’in güncel sürümünü kullanın. |
| `stale job` | Pay eski bir göreve ait. Yeni görevleri ve kabul edilen payları izleyin; sürerse bağlantıyı ve madenci sürümünü kontrol edin. |
| `submit interrupted during reconnect` | Seine yeniden bağlanıyor. Bağlantı geri geldiğinde girişin kabul edilmesini ve yeni payların kabulünü izleyin. |

**Estimated Rewards** değerinin sıfır olması ayrı bir konudur: havuz bloğu ödülleri hesaba geçirmeden önce paylar kabul edilebilir. [PPLNS rehberi](/blog/tr/how-pplns-payouts-work/) bu sırayı açıklar.

## Hangi açık sayfalar tüm havuzu gösterir?

Bu sayfalar arama gerektirmez ve havuzun yaptığı her şeyi açıkça gösterir. [Panel](/) başlıca sayıları toplar: bağlı madenciler, havuz ve ağ hashrate'i, bulunan bloklar ve ortalama şans.

### Bloklar

[Blocks](/blocks), bntpool.com'un bulduğu her bloğu en yeniden başlayarak listeler: yükseklik (gezgin bağlantılı), ödül, efor, tur süresi, durum ve bulunma zamanı. Her bloğun durumunu görmek için Confirmed, Pending veya Orphaned ile filtreleyin.

### Ödemeler

[Payouts](/payouts), her ödeme grubunu toplam tutarı, ödeme yapılan madenci sayısı, işlem bağlantıları ve durumuyla listeler. Grup yayımlanır yayımlanmaz unconfirmed görünür; ilk blok onayından kısa süre sonra confirmed olur. Böylece kendi ödemenizi havuz tarafında ve gezginde izleyebilirsiniz.

### Şans

[Luck](/luck) her turun eforunu gösterir. Efor, havuzun tur sırasında kabul ettiği share işinin blok zorluğuna bölünmesidir: %100 tam beklenen iş miktarı, daha düşük değer daha erken bulunan blok demektir. Tek turlar iki yönde de geniş dalgalanır; bu nedenle izlenecek değer panelin **Average Luck** değeridir. Paneldeki **Pool Luck History** tablosu **View All** ile buraya bağlanır.

### Durum

[Status](/status) havuzun kendi sağlık sayfasıdır. Havuz, genel HTTP, API, Stratum, veritabanı ve daemon'ın çevrimiçi olup olmadığını; güncel görev şablonunun tazeliğini; zincir yüksekliğini; son 10 dakika, 6 saat, 24 saat ve 7 gündeki çalışma süresini ve olay geçmişini gösterir. Çalışma süresi iki kez ölçülür: havuz sunucusundaki izleyici ve Cloudflare üzerinde çalışan dış prob tarafından.

## bntpool.com API'si nasıl kullanılır?

<Answer>
bntpool.com API'si HTTPS üzerinden herkese açık JSON sunar, anahtar gerektirmez. Hashrate, rig, share ve ödemeleriniz için https://bntpool.com/api/miner/ sonuna adresinizi ekleyerek çağırın; bakiyeler için /balance ekleyin, havuz ve ağ hashrate'i için /api/stats kullanın. Tutarlar atomik birimdedir: BNT için 100.000.000'a bölün.
</Answer>

Yukarıdaki her sayfa bu uç noktaları okur; sitede gördüğünüz her şeyi kendiniz alabilirsiniz:

| Uç nokta | Döndürdüğü veriler |
|---|---|
| `/api/stats` | Bağlı madenciler, havuz hashrate'i, bulunan bloklar, ağ hashrate'i, güncel yükseklik |
| `/api/stats/history?range=24h` | Zaman içinde havuz hashrate'i (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Güncel tur, ortalama efor, son turlar |
| `/api/info` | Havuz ayarları: Stratum portu, ödeme yöntemi ve penceresi, en düşük ödeme |
| `/api/blocks?status=confirmed` | Duruma göre filtrelenebilen bulunan bloklar |
| `/api/payouts/recent` | İşlem hash'leriyle ödeme grupları |
| `/api/luck` | Her turun eforu ve süresi |
| `/api/status` | Hizmet sağlığı, çalışma süresi pencereleri, olaylar |
| `/api/miner/{address}` | Hashrate'iniz, toplamlar, rig'ler, son share'ler, ödemeler |
| `/api/miner/{address}/balance` | Tahmini, onaylanmış ve ödenmiş bakiyeler |
| `/api/miner/{address}/hashrate?range=24h` | Hashrate geçmişiniz |

Liste uç noktaları `limit` (varsayılan 25, en fazla 200) ve `offset` alır; `items` ve `total` ile yanıtlar. Madenci uç noktaları tam adres ister; My Stats'ta @name aramasından sonra tam adresi arama kutusundan kopyalayın.

Madencinizi terminalden okumak için:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

`jq` ile tek değerleri, örneğin hashrate ve kabul edilen share'leri ya da BNT cinsinden ödenmiş toplamınızı çıkarabilirsiniz:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Madencinizi ev panelinde gösterin

Aynı uç nokta ev panelinde sensör olarak çalışır. Home Assistant'ta RESTful entegrasyonu birkaç satır `configuration.yaml` ile okur:

```yaml
rest:
  - resource: https://bntpool.com/api/miner/YOUR_ADDRESS
    scan_interval: 60
    sensor:
      - name: "bntpool hashrate"
        value_template: "{{ value_json.hashrate | round(2) }}"
        unit_of_measurement: "H/s"
      - name: "bntpool accepted shares"
        value_template: "{{ value_json.total_accepted }}"
```

Her 30–60 saniyede sorgulamak yeterlidir: hashrate saatlik ortalamadır ve My Stats madenci ayrıntılarınızı her 30 saniyede yeniler. Açık API'de istemci başına hız sınırı vardır. Betik HTTP 429 yanıtı alırsa yavaşlamalı ve yeniden denemeden önce `Retry-After` başlığındaki süreyi beklemelidir.

## Peki seine'in kendi paneli?

Havuz size yazdığı kazancı gösterir. Makinenin canlı görünümü için seine'in terminal paneli hashrate ve share'leri anında gösterir. seine ayrıca REST, sunucu tarafından gönderilen olay akışı ve `/metrics` altında Prometheus metrikleri içeren yerel kontrol API'si sunabilir; varsayılan olarak `127.0.0.1:9977` adresine bağlanır. [Kontrol API'si rehberi](https://github.com/zpalmtree/seine/blob/master/docs/API.md) bayrakları ve uç noktaları anlatır. Birçok madenci ikisini birlikte kullanır: her makine için seine metrikleri, bakiye ve ödemeler için havuz API'si.

seine'de yeni misiniz? [İndirin](https://seineminer.com/tr/#download), varsayılan havuza yönlendirin; ilk kabul edilen share'inizden birkaç dakika sonra adresiniz My Stats'ta görünür.

<CTA target="pool" title="Madenciliğe başlayın, canlı takip edin" text="Birkaç dakika içinde bntpool.com'a katılın; ardından hashrate, share ve bakiyenizi My Stats'ta takip edin." />
