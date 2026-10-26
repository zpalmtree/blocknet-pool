---
title: Menambang Blocknet di bntpool.com
description: Bergabung ke bntpool.com, pool Blocknet default seine. Hubungkan alamat atau @name ke stratum+tcp://bntpool.com:3333, beri nama rig dan periksa statistik.
tags: [Panduan penambangan, bntpool, Blocknet ID, seine]
faq:
  - q: 'Apa alamat Stratum bntpool.com?'
    a: 'Alamatnya stratum+tcp://bntpool.com:3333, pool yang disarankan seine saat pertama dijalankan. Tekan Enter pada pertanyaan pool untuk menerimanya, atau gunakan flag --pool-url. Alamat Blocknet atau nama Blocknet ID Anda menjadi login, dan pool membayar imbalan ke alamat tersebut.'
  - q: 'Bisakah saya menambang di bntpool.com dengan nama Blocknet ID?'
    a: 'Ya. Masukkan @name atau $name di mana pun seine atau pool meminta alamat. Pool memeriksa pemetaan bertanda tangan di blocknet.id, lalu mengkreditkan share dan pembayaran ke alamat lengkap di balik nama itu. Anda memerlukan seine v0.2.16 atau lebih baru, dan sebaiknya mengapit nama dengan tanda kutip pada baris perintah agar shell tidak mengubahnya.'
  - q: 'Apakah saya perlu akun untuk menambang di bntpool.com?'
    a: 'Tidak. Tidak ada pendaftaran, kata sandi atau email. Alamat Blocknet, atau nama Blocknet ID yang mengarah ke sana, menjadi login dan tujuan pembayaran. Jalankan seine dengannya, lalu cari di halaman My Stats untuk melihat hashrate, share, rig, saldo dan pembayaran.'
  - q: 'Bisakah beberapa komputer menambang ke satu alamat?'
    a: 'Ya. Jalankan seine pada setiap komputer dengan alamat atau nama yang sama dan beri masing-masing nama rig sendiri. Pool menghitung share, saldo dan pembayaran per alamat, sehingga semua rig menambah satu saldo dan satu pembayaran, dan My Stats menampilkan setiap rig pada baris tersendiri.'
sourceHash: '84ce1ed92e20'
translation: draft
---

Panduan ini membahas cara bergabung ke pool bntpool.com dengan seine: alamat pool, login menggunakan alamat Blocknet atau Blocknet ID `@name`, penamaan setiap rig, serta pemeriksaan kedatangan share di halaman statistik. Jika seine belum terpasang, ikuti [panduan pemasangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) dahulu, lalu kembali ke sini.

## Apa alamat pool bntpool.com?

<Answer>
Alamat Stratum bntpool.com adalah `stratum+tcp://bntpool.com:3333`. seine menawarkannya sebagai pool default saat pertama dijalankan, jadi tekan Enter pada pertanyaan pool untuk terhubung. Login menggunakan alamat Blocknet atau Blocknet ID `@name`. Tidak perlu membuat akun, dan imbalan masuk ke alamat itu.
</Answer>

Berikut pengaturan pool sebagaimana dilaporkan endpoint publik `/api/info` pada 25 Oktober 2026:

| Pengaturan | Nilai |
|---|---|
| Alamat Stratum | `stratum+tcp://bntpool.com:3333` |
| Login | Alamat Blocknet, `@name` atau `$name` |
| Skema pembayaran | PPLNS dengan jendela 6 jam |
| Konfirmasi sebelum pembayaran | 60 blok |
| Pembayaran minimum | 0,1 BNT |

Nilai yang sama selalu tersedia di halaman [Get Started](/start) milik pool.

## Mengapa bntpool.com menjadi pool default seine?

Saat pertama menjalankan seine, program meminta pool dan mengisi `stratum+tcp://bntpool.com:3333` sebagai default. Pool dan penambang saling cocok dalam beberapa hal praktis:

- **Satu protokol.** bntpool.com menjalankan protokol Stratum yang digunakan seine (versi 2), sehingga seine terhubung tanpa pengaturan selain alamat Anda.
- **Nama bekerja di kedua sisi.** seine dan pool menerima nama Blocknet ID serta memverifikasinya terhadap catatan bertanda tangan yang sama di blocknet.id.
- **Pemeriksaan versi.** Setiap rig melaporkan versi seine bersama share, dan halaman statistik menandai rig yang memakai versi lama dengan label "update available" yang menautkan halaman rilis seine.
- **Panduan pengaturan seine.** Halaman Get Started menjelaskan pengunduhan dan menjalankan seine, disertai perintah yang bisa disalin.

## Bagaimana menghubungkan seine ke bntpool.com?

Saat pertama dijalankan, seine mengajukan dua pertanyaan:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Tempel alamat atau ketik nama Anda, lalu tekan Enter untuk menerima bntpool.com. seine menyimpan kedua jawaban di `seine-data/seine.config.json`, sehingga setiap peluncuran berikutnya langsung menambang. Program juga memilih nama rig dan menyimpannya dalam file yang sama, dibahas di bawah.

Pada rig headless, layanan atau skrip, tidak ada yang menjawab pertanyaan. Berikan kedua nilai sebagai flag saat pertama dijalankan, menggunakan perintah yang sama seperti halaman Get Started:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Untuk mengganti pool atau alamat kemudian, edit atau hapus `seine-data/seine.config.json`, atau timpa nilai tersimpan dengan `--address` dan `--pool-url` pada baris perintah.

## Bisakah menambang ke Blocknet ID @name?

<Answer>
Ya. bntpool.com menerima nama pendek Blocknet ID, ditulis `@name` atau `$name`, di mana pun alamat diterima. Pool memeriksa pemetaan bertanda tangan dari blocknet.id, memvalidasi alamat tujuan nama, dan mengkreditkan share, saldo serta pembayaran ke alamat lengkap itu sepanjang koneksi.
</Answer>

Nama tidak membedakan huruf besar dan kecil, sehingga `@Alice` dan `@alice` adalah login yang sama. Dukungan nama hadir pada seine v0.2.16; semua rilis terkini memilikinya. Sebelum menambang, seine menampilkan alamat hasil pencarian nama agar Anda dapat mencocokkannya dengan wallet. Pada baris perintah, apit nama dengan tanda kutip tunggal. Tanpanya, shell akan mencoba mengembangkan `$name` sebagai variabel:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Jika kelak nama Anda diarahkan ke wallet berbeda, mulai ulang seine. seine mencari alamat dari nama tersimpan setiap mulai dan mempertahankannya sepanjang sesi, sedangkan pool melakukannya per koneksi. Saldo yang sudah diperoleh tetap pada alamat asal penghasilnya dan dibayar ke sana, jadi memindahkan nama tidak pernah memindahkan koin Anda.

Daftarkan nama di [blocknet.id](https://blocknet.id). [Panduan Blocknet ID](https://seineminer.com/id/blog/blocknet-id-pay-by-name/) menjelaskan pembayaran dengan nama.

## Bagaimana memberi nama rig?

Setiap koneksi login dengan nama rig di samping alamat Anda. Saat pertama dijalankan, seine memilih nama `seine-` diikuti empat digit dan menyimpannya dalam konfigurasi, sehingga mesin yang sama tetap memakai nama yang sama setelah restart. Untuk nama yang mudah dikenali, atur lewat baris perintah:

```sh
./seine --pool-worker rig-01
```

Pool menyimpan hingga 64 karakter nama. Nama stabil juga membantu setelah restart: pool mengingat kesulitan share setiap rig selama satu jam berdasarkan alamat dan nama rig, sehingga rig yang tersambung kembali melanjutkan pada kesulitan yang sudah sesuai sebelumnya.

## Bisakah beberapa rig menambang ke satu alamat?

Ya. Jalankan seine pada setiap mesin dengan alamat atau `@name` yang sama dan nama rig berbeda. Pool menghitung share, saldo dan pembayaran per alamat, sehingga semua rig tergabung dalam satu saldo dan satu pembayaran. Di halaman statistik, setiap rig memiliki baris sendiri agar mudah memastikan semua mesin menambang.

## Bagaimana memeriksa penambang bekerja?

Dasbor seine menunjukkan hashrate dan share diterima pada mesin itu sendiri. Untuk melihat dari sisi pool, buka [My Stats](/stats), masukkan alamat, `@name` atau `$name`, lalu tekan **Lookup**. Jika memasukkan nama, halaman menampilkan alamat hasil pencarian di samping kotak pencarian. Halaman juga mengingat pencarian terakhir dalam browser tersebut, dan [panduan memantau penambang](/blog/id/track-your-miner-on-bntpool/) membahas setiap kartunya.

Perhatikan tiga hal:

- **Hashrate dan Shares Accepted.** Kartu hashrate menunjukkan yang dilihat pool, dan jumlah diterima bertambah saat rig mengirim pekerjaan.
- **Satu baris per rig.** Setiap baris menampilkan nama, hashrate, share diterima dan ditolak, serta waktu share terakhir. Share yang baru saja masuk berarti rig terhubung dan bekerja.
- **Recent Shares.** Share terbaru tercantum bersama rig pengirim, tingkat kesulitan dan statusnya.

Pool menetapkan kesulitan share secara terpisah per koneksi dan menyesuaikannya selama menambang, sehingga CPU kecil pun mengirim share secara teratur dan share diterima pertama muncul dalam beberapa menit setelah terhubung. Untuk hashrate yang dapat diharapkan dari perangkat Anda, lihat [panduan perangkat keras penambangan Blocknet](https://seineminer.com/id/blog/blocknet-mining-hardware-hashrate/).

Imbalan menyusul. Estimated Rewards muncul setelah pool menemukan blok saat share Anda berada dalam jendela enam jam. Setelah blok mendapat 60 konfirmasi, bagian Anda berpindah ke Confirmed Rewards, dan pool mengirimnya otomatis setelah mencapai setidaknya 0,1 BNT. [Cara kerja pembayaran PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan pembagian imbalan tiap blok. Riwayat seluruh pool ada di halaman [Blocks](/blocks) dan [Payouts](/payouts).

## Apa yang dibahas halaman Get Started?

Halaman [Get Started](/start) menyajikan semuanya pada satu layar:

- **Informasi pool.** Alamat Stratum (klik untuk menyalin), skema pembayaran, pembayaran minimum dan jumlah konfirmasi blok sebelum pembayaran, dibaca langsung dari pool.
- **Mulai cepat dalam tiga langkah.** Unduh seine dari [seineminer.com](https://seineminer.com/id/#download), jalankan sekali lalu masukkan alamat atau nama dan URL pool, kemudian mulai menambang dan pantau hashrate serta saldo di My Stats.
- **Perintah untuk disalin.** Perintah `./seine` lengkap dengan URL pool. Ganti dengan alamat Anda, atau `'@yourname'` dalam tanda kutip.
- **Catatan pengaturan.** Lokasi konfigurasi seine, cara meresetnya, dan flag yang menimpanya.
- **Perhitungan imbalan.** Penjelasan singkat PPLNS: share yang dikirim sebelum blok ditemukan mendapat imbalannya, sedangkan pekerjaan sesudahnya diperhitungkan untuk blok berikutnya. Dijelaskan pula bahwa imbalan blok belum terkonfirmasi ditampilkan terpisah di My Stats sampai blok dikonfirmasi.
- **Tangkapan layar** pengaturan awal seine dan dasbor penambangannya, agar Anda tahu tampilannya sebelum mulai.

<CTA target="pool" title="Mulai menambang di bntpool.com" text="Hubungkan seine ke bntpool.com dengan alamat atau @name Anda dan lihat share masuk di My Stats." />
