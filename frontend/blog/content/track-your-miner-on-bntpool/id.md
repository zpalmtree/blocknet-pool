---
title: Pantau penambang Anda di bntpool.com
description: Cari alamat atau @name di bntpool.com untuk melihat hashrate, share, saldo dan pembayaran, lalu pantau blok, luck, uptime atau gunakan API publik.
tags: [bntpool, Statistik penambangan, API pool, Blocknet]
faq:
  - q: 'Mengapa hashrate di bntpool.com berbeda dari seine?'
    a: 'seine menghitung setiap hash mesin, sedangkan pool memperkirakan hashrate dari kesulitan share diterima, rata-rata satu jam terakhir. Share tiba secara acak, jadi angka pool lebih berfluktuasi dan stabil seiring waktu. Bandingkan grafik 24 jam My Stats dengan rata-rata seine untuk perbandingan setara.'
  - q: 'Bisakah mencari statistik bntpool.com menggunakan @name?'
    a: 'Ya. Ketik nama pendek Blocknet ID seperti @name atau $name dalam kotak My Stats. Halaman mencari alamat melalui blocknet.id, menunjukkan statistik dan menaruh alamat lengkap hasilnya di kotak pencarian. Gunakan alamat lengkap tersebut ketika memanggil API pool dari skrip.'
  - q: 'Apakah API bntpool.com membutuhkan API key?'
    a: 'Tidak. Endpoint statistik, blok, pembayaran, luck, status dan per penambang menyediakan JSON publik melalui HTTPS, sehingga curl atau dasbor rumah dapat membacanya tanpa mendaftar. Jumlah memakai satuan atomik, jadi bagi 100.000.000 untuk BNT, dan lakukan polling setiap 30 hingga 60 detik.'
  - q: 'Mengapa My Stats menunjukkan Estimated Rewards nol?'
    a: 'Estimated Rewards muncul setelah pool menemukan blok saat share Anda berada dalam jendela pembayaran. Bagi penambang baru mungkin perlu beberapa ronde. Share diterima dan hashrate muncul dahulu, lalu perkiraan imbalan, yang matang menjadi Confirmed Rewards dan dibayar otomatis.'
sourceHash: '8f19279f18f8'
translation: draft
---

Setelah seine menambang di bntpool.com, pool menunjukkan seluruh yang dilihatnya: hashrate, setiap share diterima, penghasilan dan setiap pembayaran ke alamat Anda. Panduan ini membahas My Stats, halaman publik seluruh pool, dan API publik untuk skrip serta dasbor rumah.

Jika belum menambang, [panduan pemasangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) membawa Anda ke share pertama dalam beberapa menit.

## Bagaimana memeriksa statistik penambang di bntpool.com?

<Answer>
Buka My Stats di bntpool.com/stats, tempel alamat Blocknet atau @name, lalu tekan Lookup. Halaman menampilkan hashrate, share diterima dan ditolak, saldo perkiraan, terkonfirmasi dan dibayar, grafik hashrate, satu baris per rig serta pembayaran terkini. Halaman terus memperbarui diri selama tab terbuka.
</Answer>

Buka [My Stats](/stats) dan masukkan alamat atau [nama pendek Blocknet ID](https://seineminer.com/id/blog/blocknet-id-pay-by-name/) yang diberikan ke seine. Nama seperti `@name` atau `$name` dicari melalui blocknet.id, lalu kotak pencarian menunjukkan alamat lengkap tujuannya. Halaman mengingat pencarian terakhir dalam browser itu agar angka Anda sudah tersedia saat kembali.

## Apa yang ditampilkan My Stats?

Halaman terbagi menjadi kelompok kartu dan tabel, masing-masing menjawab satu pertanyaan tentang penambang Anda.

### Saldo

- **Estimated Rewards:** bagian Anda dari blok temuan pool yang masih matang.
- **Confirmed Rewards:** saldo matang yang menunggu pembayaran berikutnya.
- **Paid Balance:** seluruh nilai yang sudah dikirim ke alamat Anda.

Ketika ada imbalan dalam proses, tabel **Estimated From Recent Blocks** mencantumkan ketinggian setiap blok, perkiraan kredit Anda, sisa konfirmasi dan waktu ditemukan. Setiap ketinggian menautkan blok di [Blocknet explorer](https://explorer.blocknetcrypto.com) agar Anda dapat memeriksanya. [Cara kerja pembayaran PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan pembagian imbalan dan waktu saldo dibayar.

### Penambangan dan share

Kartu **Mining** menampilkan **Hashrate** terkini, **Blocks Found** yang dipecahkan rig Anda untuk pool, dan tanggal mulai (**Mining Since**). Di bawahnya ada **Shares Accepted**, **Shares Rejected**, **Reject Rate** dan **Avg Difficulty**. Rig sehat menunjukkan aliran share diterima yang teratur dan tingkat penolakan mendekati nol.

**Hashrate History** memetakan hashrate selama 1h, 24h, 7d atau 30d. Rentang panjang paling baik untuk memastikan rig berjalan sepanjang malam.

### Satu baris per rig

Jika memakai lebih dari satu mesin, beri nama masing-masing agar pool mencantumkannya terpisah, dengan hashrate, share diterima dan ditolak serta waktu share terakhir. seine memilih nama saat pertama mulai, `seine-` diikuti empat digit; Anda dapat mengatur sendiri lewat baris perintah:

```sh
./seine --pool-worker rig-01
```

Rig dengan seine lama mendapat label **update available** yang menautkan [halaman rilis seine](https://github.com/zpalmtree/seine/releases), sehingga mudah menemukan mesin yang lupa diperbarui.

### Pembayaran dan share terbaru

**Recent Payouts** mencantumkan tiap pembayaran ke alamat Anda beserta jumlah, tautan transaksi explorer dan status queued, unconfirmed atau confirmed. **Recent Shares** menampilkan 20 share terbaru beserta job, rig, kesulitan dan status, cara tercepat memastikan mesin yang baru dijalankan mencapai pool.

## Mengapa hashrate pool berbeda dari seine?

seine menghitung setiap hash yang dikerjakan mesin. Pool hanya melihat share terkirim, jadi memperkirakan hashrate dari total kesulitan share diterima selama satu jam terakhir. Setiap share adalah peristiwa acak, dan kesulitan per koneksi bntpool.com menargetkan sekitar 10 share setiap 5 menit, sehingga jendela pendek mengandung variasi.

Dua kebiasaan membantu menyelaraskan angka:

- Bandingkan rata-rata: grafik 24h My Stats dengan rata-rata seine selama rentang sama.
- Beri rig baru waktu. Untuk rig yang baru mulai, pool merata-ratakan selama menit yang sudah ditambang, sehingga bacaan awal hanya dari beberapa share. Setelah satu jam, jendela satu jam penuh berlaku.

Tabel Recent Shares langsung menunjukkan kedatangan share, bahkan sebelum hashrate stabil.

<span id="zero-hashrate"></span>

## Mengapa seine menambang tetapi My Stats menunjukkan hashrate nol?

Mulai dari alamat pembayaran, koneksi pool, dan share yang diterima. Hashrate lokal menunjukkan mesin sedang menghitung hash; estimasi pool memerlukan share yang diterima untuk alamat yang Anda cari.

1. **Cocokkan alamat.** Cari alamat penerima atau `@name` terdaftar yang sama dengan yang digunakan seine. Jika alamat nama singkat berubah, mulai ulang seine agar nama diresolusikan lagi, lalu periksa alamat hasil resolusi di My Stats.
2. **Cocokkan pool.** Pastikan seine menggunakan `stratum+tcp://bntpool.com:3333`. Mesin yang menambang di tempat lain atau dalam mode solo tidak menambahkan share di sini. [Panduan pengaturan pool](/blog/id/mine-blocknet-on-bntpool/) menjelaskan koneksinya.
3. **Periksa Recent Shares.** Tunggu share diterima, lalu biarkan halaman diperbarui. Share datang pada interval yang bervariasi; hashrate lokal yang berjalan tidak menjamin estimasi pool langsung muncul. Jika share diterima terus berdatangan, bandingkan rata-rata selama periode yang lebih panjang.
4. **Baca pesan koneksi.** Gunakan tabel jika penambang belum mengirim share yang diterima. Periksa [Status](/status) untuk insiden yang sedang terjadi.

| Pesan di seine | Yang perlu diperiksa |
|---|---|
| `failed to connect to pool at` | Periksa nama host dan port, koneksi Anda, serta apakah koneksi TCP keluar ke port 3333 diizinkan. |
| `login rejected:` | Baca alasan setelah pesan tersebut; periksa alamat penerima atau nama singkat dan gunakan versi seine terbaru. |
| `stale job` | Share mengacu pada tugas lama. Pantau tugas baru dan share yang diterima; jika berlanjut, periksa koneksi dan versi penambang. |
| `submit interrupted during reconnect` | Seine sedang menyambung ulang. Pantau penerimaan login dan share baru yang diterima setelah koneksi kembali. |

Nilai nol pada **Estimated Rewards** adalah persoalan terpisah: share bisa diterima sebelum blok pool mengkreditkan imbalan. [Panduan PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan urutannya.

## Halaman publik mana yang menunjukkan seluruh pool?

Halaman ini tidak perlu pencarian dan menunjukkan semua kegiatan pool secara terbuka. [Dasbor](/) mengumpulkan angka utama: penambang terhubung, hashrate pool dan jaringan, blok ditemukan serta rata-rata luck.

### Blok

[Blocks](/blocks) mencantumkan semua blok bntpool.com, terbaru dahulu: ketinggian (tautan explorer), imbalan, effort, waktu ronde, status dan waktu temuan. Filter Confirmed, Pending atau Orphaned menunjukkan keadaan tiap blok.

### Pembayaran

[Payouts](/payouts) mencantumkan tiap batch pembayaran dengan jumlah total, jumlah penambang dibayar, tautan transaksi dan status. Batch muncul sebagai unconfirmed segera setelah disiarkan dan menjadi confirmed tak lama setelah konfirmasi blok pertama, sehingga pembayaran Anda dapat diikuti dari sisi pool maupun explorer.

### Luck

[Luck](/luck) menampilkan effort tiap ronde. Effort adalah pekerjaan share diterima selama ronde dibagi kesulitan blok: 100% berarti tepat jumlah pekerjaan yang diharapkan, dan lebih rendah berarti pool menemukannya lebih cepat. Ronde tunggal berfluktuasi jauh ke dua arah, jadi **Average Luck** dasbor adalah angka yang perlu dipantau. Tabel **Pool Luck History** menautkan halaman ini lewat **View All**.

### Status

[Status](/status) adalah halaman kesehatan pool. Halaman menunjukkan apakah pool, HTTP publik, API, Stratum, database dan daemon online, kebaruan template job terkini, ketinggian blockchain, uptime selama 10 menit, 6 jam, 24 jam dan 7 hari terakhir, serta riwayat insiden. Uptime diukur dua kali: monitor pada server pool dan probe luar di Cloudflare.

## Bagaimana menggunakan API bntpool.com?

<Answer>
API bntpool.com menyediakan JSON publik melalui HTTPS tanpa key. Panggil https://bntpool.com/api/miner/ diikuti alamat untuk hashrate, rig, share dan pembayaran, tambah /balance untuk saldo, serta gunakan /api/stats untuk hashrate pool dan jaringan. Jumlah dalam satuan atomik: bagi 100.000.000 untuk BNT.
</Answer>

Semua halaman di atas membaca endpoint berikut, sehingga apa pun yang terlihat dapat Anda ambil sendiri:

| Endpoint | Hasil |
|---|---|
| `/api/stats` | Penambang terhubung, hashrate pool, blok ditemukan, hashrate jaringan, ketinggian terkini |
| `/api/stats/history?range=24h` | Riwayat hashrate pool (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Ronde terkini, rata-rata effort, ronde terbaru |
| `/api/info` | Pengaturan: port Stratum, skema dan jendela pembayaran, minimum pembayaran |
| `/api/blocks?status=confirmed` | Blok temuan, dapat difilter menurut status |
| `/api/payouts/recent` | Batch pembayaran dengan hash transaksi |
| `/api/luck` | Effort dan durasi tiap ronde |
| `/api/status` | Kesehatan layanan, jendela uptime, insiden |
| `/api/miner/{address}` | Hashrate, total, rig, share terbaru dan pembayaran Anda |
| `/api/miner/{address}/balance` | Saldo perkiraan, terkonfirmasi dan dibayar |
| `/api/miner/{address}/hashrate?range=24h` | Riwayat hashrate Anda |

Endpoint daftar menerima `limit` (default 25, maksimum 200) dan `offset`, serta menjawab dengan `items` dan `total`. Endpoint penambang menerima alamat lengkap, jadi setelah mencari @name di My Stats, salin alamat lengkap dari kotak pencarian.

Untuk membaca data penambang dari terminal:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Dengan `jq`, ambil nilai tertentu seperti hashrate dan share diterima, atau total pembayaran dalam BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

### Tampilkan penambang pada dasbor rumah

Endpoint sama dapat menjadi sensor dasbor rumah. Pada Home Assistant, integrasi RESTful membacanya dengan beberapa baris `configuration.yaml`:

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

Polling tiap 30 hingga 60 detik sudah cukup: hashrate adalah rata-rata per jam, dan My Stats sendiri menyegarkan detail penambang tiap 30 detik. API publik dibatasi lajunya per klien. Jika skrip menerima HTTP 429, perlambat dan tunggu durasi pada header `Retry-After` sebelum mencoba lagi.

## Bagaimana dengan dasbor seine sendiri?

Pool menunjukkan kredit yang diberikan. Untuk pandangan langsung mesin, dasbor terminal seine menampilkan hashrate dan share saat terjadi. seine juga dapat menyediakan API kontrol lokal dengan REST, aliran server-sent event dan metrik Prometheus di `/metrics`, terikat ke `127.0.0.1:9977` secara default. [Panduan API kontrol](https://github.com/zpalmtree/seine/blob/master/docs/API.md) membahas flag dan endpoint. Banyak penambang memakai keduanya: metrik seine per mesin dan API pool untuk saldo serta pembayaran.

Baru menggunakan seine? [Unduh](https://seineminer.com/id/#download), arahkan ke pool default, dan alamat akan muncul pada My Stats dalam beberapa menit setelah share diterima pertama.

<CTA target="pool" title="Mulai menambang dan pantau langsung" text="Bergabung ke bntpool.com dalam beberapa menit, lalu pantau hashrate, share dan saldo pada My Stats." />
