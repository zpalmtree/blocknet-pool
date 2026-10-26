---
title: Apa itu pool penambangan?
description: Pool penambangan menggabungkan hashrate banyak penambang dan membagi imbalan blok. Pelajari cara kerja pool, PPLNS, PPS, solo dan hal yang perlu diperiksa.
tags: [Dasar penambangan, Pool penambangan, PPLNS, bntpool]
faq:
  - q: 'Apakah bergabung ke pool menambah hasil tambang saya?'
    a: 'Pool tidak mengubah hashrate maupun bagian pekerjaan jaringan Anda. Yang berubah adalah cara imbalan tiba. Solo menunggu blok utuh pada jarak acak. Pada pool Anda mendapat bagian kecil setiap blok temuan pool, sehingga pembayaran sering tiba dan jumlahnya mengikuti hashrate Anda.'
  - q: 'Apa perbedaan PPLNS dan PPS?'
    a: 'PPLNS membayar bagian setiap blok yang benar-benar ditemukan pool, dibagi berdasarkan pekerjaan dalam jendela terbaru. PPS membayar jumlah tetap per share diterima menurut nilai harapannya, baik pool menemukan blok maupun tidak. bntpool.com memakai PPLNS dengan jendela enam jam.'
  - q: 'Perlukah node sendiri untuk menambang di pool?'
    a: 'Tidak. Pool menjalankan node, menyusun template, memeriksa share dan mengirim blok temuan. Di bntpool.com Anda hanya perlu penambang seine dan alamat Blocknet atau nama Blocknet ID. Jalankan seine, tempel alamat, terima pool default, dan bagian setiap blok dibayar ke alamat itu.'
  - q: 'Bagaimana mulai menambang di bntpool.com?'
    a: 'Unduh seine untuk Windows, macOS atau Linux dari seineminer.com, jalankan dan tempel alamat Blocknet atau @name saat diminta. bntpool.com adalah pool default, jadi terima dan mulai menambang. Cari alamat di halaman statistik untuk melihat hashrate, share dan saldo.'
sourceHash: 'a17a47ab1d73'
translation: draft
---

Panduan ini menjelaskan pool penambangan, alasan kebanyakan penambang bergabung, dan cara pool menentukan penghasilan setiap penambang. Anda akan mempelajari skema pembayaran umum PPLNS, PPS dan solo, serta pemeriksaan sebelum mengarahkan penambang ke pool. Pool Blocknet bntpool.com digunakan sebagai contoh sepanjang panduan.

## Apa itu pool penambangan?

<Answer>
Pool penambangan adalah server yang menggabungkan [hashrate](/blog/id/what-is-hashrate/) banyak penambang agar menemukan blok bersama. Pool memberikan pekerjaan pada blok sama, menghitung pekerjaan yang dikirim masing-masing, lalu membagi imbalan sebanding dengan pekerjaan itu. Penambang menerima pembayaran kecil yang sering, menggantikan blok utuh yang jarang.
</Answer>

Pada koin proof of work, penambang bersaing menemukan blok berikut dengan menghitung hash header memakai nonce berbeda hingga hasil berada di bawah target jaringan. Setiap hash adalah percobaan independen dengan peluang kecil yang sama, dan penemu hash pemenang mendapat imbalan blok.

Pool mengubah persaingan menjadi kerja tim. Semua yang terhubung ke bntpool.com mengerjakan blok buatan pool, dan ketika salah satu menemukan hash pemenang, imbalan dibagikan kepada semua penambang yang pekerjaannya membantu pencarian.

## Mengapa penambang bergabung ke pool?

<Answer>
Penambang bergabung agar dibayar teratur. Sendiri, penambang biasa memiliki bagian sangat kecil hashrate jaringan dan mungkin menunggu berhari-hari antarblok, dengan jarak acak. Dalam pool, gabungan hashrate sering menemukan blok dan setiap penambang mendapat bagian, sehingga penghasilan tiba teratur mengikuti hashrate.
</Answer>

Perhitungannya mudah diperiksa pada Blocknet. Jaringan menargetkan satu blok tiap lima menit, 288 sehari, sehingga penambang solo dapat mengharapkan 288 × (hashrate-nya ÷ hashrate jaringan) blok per hari.

Gunakan angka bulat hipotetis: rig 50 H/s di jaringan 50.000 H/s memiliki 0,1% jaringan dan rata-rata menemukan 0,288 blok sehari, sekitar satu setiap tiga setengah hari. Itu hanya rata-rata. Penemuan blok acak, sehingga jarak sebenarnya bisa jauh lebih pendek atau lebih panjang.

Taruh rig sama di pool total 5.000 H/s dan pool menemukan sekitar 28,8 blok sehari, rata-rata satu tiap 50 menit. Rig menyumbang 1% pekerjaan pool sehingga memperoleh 1% tiap blok. Kontribusi kerja sama; yang berubah imbalan tiba dalam bagian kecil berkali-kali sehari. Panduan [pool versus solo Blocknet](/blog/id/pool-vs-solo-mining-blocknet/) membahas perbandingan ini untuk operasi lebih besar.

## Bagaimana pool penambangan bekerja?

Semua pool mengikuti putaran dasar sama. Berikut pada bntpool.com:

1. **Pool menyusun template blok.** bntpool.com menjalankan node Blocknet sendiri dan mengambil template, sehingga penambang tidak perlu menjalankan atau menyinkronkan node.
2. **Pool mengirim pekerjaan.** Penambang terhubung lewat Stratum, protokol pengiriman pekerjaan pool, pada `stratum+tcp://bntpool.com:3333`. Setiap koneksi mendapat job berdasarkan template terkini.
3. **Penambang menghitung hash dan mengirim share.** Share adalah hash yang memenuhi target lebih mudah dari pool. Share membuktikan banyaknya pekerjaan setiap penambang.
4. **Share yang memenuhi target jaringan juga merupakan blok.** Pool memeriksa setiap share terhadap kedua target. Jika cukup baik untuk jaringan, blok dikirim ke node.
5. **Imbalan dibagi dan dibayar.** Pool membagi sesuai skema pembayaran lalu mengirim bagian setiap penambang setelah terkonfirmasi dan melewati minimum.

## Apa itu share dalam pool?

Share adalah hash yang melewati syarat lebih ringan daripada blok nyata. Blok perlu hash di bawah target jaringan; share cukup di bawah target pool yang jauh lebih mudah. Sebagian besar share tidak menjadi blok, tetapi menunjukkan secara teratur berapa banyak hashing dilakukan setiap penambang.

Share dibobot berdasarkan kesulitan. Pada Blocknet, kesulitan D berarti sekitar D hash diharapkan sebelum target terpenuhi, jadi share kesulitan 60 mewakili sekitar 60 hash pekerjaan. bntpool.com menetapkan kesulitan per koneksi dan menyesuaikannya dengan target sekitar 10 share setiap lima menit, memulai koneksi baru pada kesulitan 60. Server cepat mendapat share lebih sulit, laptop lebih mudah, dan keduanya dikreditkan sesuai pekerjaan nyata. Pool juga mengingat kesulitan setiap rig selama satu jam, sehingga restart singkat melanjutkan posisi sebelumnya.

## Skema pembayaran pool yang umum

Skema pembayaran adalah aturan mengubah share menjadi penghasilan. Berikut yang paling sering dijumpai.

| Skema | Dasar pembayaran | Cara kerja |
|---|---|---|
| PPLNS (pay per last N shares) | Blok temuan pool | Imbalan dibagi berdasarkan kesulitan share dalam jendela terbaru |
| PPS (pay per share) | Setiap share diterima | Pool membayar nilai tetap per share berdasarkan nilai harapan, baik menemukan blok maupun tidak |
| Solo | Blok yang ditemukan sendiri | Menambang pada node sendiri dan menyimpan seluruh imbalan tiap blok temuan |

**PPLNS** melihat ke belakang dari setiap blok pool. Di bntpool.com, "N share terakhir" berupa jendela enam jam: pool menjumlahkan kesulitan setiap share diterima dalam enam jam sebelumnya dan memberi bagian imbalan sesuai proporsi kerja masing-masing. Setiap share berlaku untuk semua blok selama berada dalam jendela, sehingga penambang konsisten selalu memiliki jendela penuh dan restart tidak mereset apa pun. [Cara kerja PPLNS di bntpool.com](/blog/id/how-pplns-payouts-work/) membahas pembagian bertahap.

**PPS** membayar setiap share saat tiba berdasarkan nilai harapannya: kesulitan share dibagi kesulitan jaringan, dikalikan imbalan blok. Operator pool menanggung naik turunnya penemuan blok dan membayar dari saldo sendiri di antaranya.

**Penambangan solo** melewati pool. Node sendiri menyusun template dan tiap blok temuan membayar imbalan penuh ke wallet Anda. Cocok untuk operasi cukup besar yang sering menemukan blok sendiri. seine mendukung keduanya: bntpool.com secara default, dan `./seine --mode daemon` pada node Blocknet Anda yang tersinkron.

## Apa yang perlu diperhatikan saat memilih pool?

Saat membandingkan pool, detail berikut menentukan pembayaran dan kemudahan memeriksanya:

- **Skema jelas.** Pool harus menyebutkan skema dan pengaturan seperti jendela PPLNS. bntpool.com mencantumkannya di [Get Started](/start) dan `https://bntpool.com/api/info`.
- **Konfirmasi dan minimum pembayaran.** Imbalan biasanya menunggu blok cukup dalam, lalu dibayar setelah saldo melewati minimum. Di bntpool.com imbalan terkonfirmasi setelah 60 blok, sesuai coinbase maturity Blocknet, dan saldo setidaknya 0,1 BNT dibayar otomatis.
- **Angka publik yang bisa diperiksa.** Anda harus dapat melihat semua blok temuan dan pembayaran. bntpool.com menyediakannya pada [blok](/blocks) dan [pembayaran](/payouts), effort per ronde di [luck](/luck), dan kesehatan layanan di [status](/status).
- **Statistik sendiri.** Cari alamat untuk hashrate, share dan saldo. Di bntpool.com gunakan [statistik](/stats); [panduan pemantauan](/blog/id/track-your-miner-on-bntpool/) menjelaskan tiap kartu.
- **Pengaturan mudah.** Makin sedikit pengaturan, makin cepat mulai. bntpool.com default seine yang menyetel thread, kernel dan memori sendiri; Anda dapat login dengan [nama Blocknet ID](https://seineminer.com/id/blog/blocknet-id-pay-by-name/) seperti `@name` sebagai pengganti alamat lengkap.

## Sekilas bntpool.com

| Pengaturan | bntpool.com |
|---|---|
| Skema pembayaran | PPLNS |
| Jendela share | 6 jam |
| Konfirmasi sebelum pembayaran | 60 blok |
| Pembayaran minimum | 0,1 BNT |
| Alamat Stratum | `stratum+tcp://bntpool.com:3333` |

[Dasbor pool](/) menunjukkan kondisi langsung: penambang terhubung, hashrate pool dan jaringan, blok temuan serta effort ronde terkini. Pada cuplikan 25 Oktober 2026 pukul 02.25 EDT, pool memiliki sekitar 5.200 H/s dari jaringan sekitar 44.400 H/s, dengan 246 penambang terhubung. Itu sekitar 12% jaringan, cukup untuk perkiraan 34 blok sehari atau satu setiap sekitar 42 menit. Angka terus berubah, jadi periksa dasbor dan gunakan [panduan perkiraan imbalan](/blog/id/estimate-blocknet-mining-rewards/) untuk mengubahnya menjadi BNT sesuai hashrate Anda.

Untuk bergabung, [unduh seine](https://seineminer.com/id/#download), jalankan, tempel alamat Blocknet atau `@name` dan terima pool default. [Panduan penambangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) membahas tiap langkah, dan [menambang Blocknet di bntpool.com](/blog/id/mine-blocknet-on-bntpool/) membahas nama rig serta pemeriksaan share pertama.

<CTA target="pool" title="Bergabung ke bntpool.com" text="Arahkan seine ke pool dan dapatkan bagian tiap blok temuan, dikirim otomatis ke alamat Anda setelah mencapai 0,1 BNT." />
