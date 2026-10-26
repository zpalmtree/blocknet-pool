---
title: "Kalkulator mining Blocknet"
description: "Perkirakan BNT per hari dan minggu berdasarkan hashrate Anda, data jaringan langsung, dan jadwal imbalan, dengan biaya ditampilkan terpisah."
tags: [Imbalan penambangan, Panduan penambangan, Hashrate, bntpool]
faq:
  - q: 'Bagaimana menghitung imbalan penambangan Blocknet?'
    a: 'Bagi hashrate Anda dengan hashrate jaringan, kalikan 288, jumlah blok Blocknet per hari, lalu kalikan imbalan blok terkini. Hasilnya perkiraan BNT bruto per hari. Baca hashrate jaringan di dasbor bntpool.com dan hashrate Anda di seine atau My Stats.'
  - q: 'Di mana melihat hashrate jaringan Blocknet?'
    a: 'Dasbor bntpool.com menampilkannya pada kartu Network Hashrate, biasanya dalam KH/s, jadi kalikan 1.000 untuk mendapat H/s. Nilai sama tersedia dalam H/s pada field network_hashrate API pool di bntpool.com/api/stats. Nilai diukur dari sepuluh blok terbaru dan diperbarui setiap blok.'
  - q: 'Apakah imbalan blok Blocknet berubah seiring waktu?'
    a: 'Ya. Imbalan tetap selama setiap bulan 30 hari berisi 8.640 blok dan turun pada awal bulan berikutnya, hingga menjadi 2,0 BNT permanen per blok pada bulan 48. Untuk imbalan terkini, bagi ketinggian blok dengan 8.640, bulatkan ke bawah dan cari bulan itu dalam jadwal imbalan.'
  - q: 'Mengapa imbalan harian pool berbeda dari perkiraan?'
    a: 'Perkiraan adalah rata-rata. Pada hari tertentu pool dapat menemukan lebih banyak atau lebih sedikit blok dari perkiraan, dan hashrate jaringan berubah saat penambang bergabung. Dalam seminggu atau lebih penambangan konsisten, variasi harian merata, dan Anda dapat memantau imbalan yang dikreditkan di My Stats.'
  - q: 'Apakah menambang di pool mengubah perkiraan jumlah BNT?'
    a: 'Perkiraan bagian imbalan jaringan tetap sama, karena hanya bergantung pada proporsi hashrate jaringan Anda. Pool mengubah waktunya. Alih-alih menunggu berhari-hari untuk satu blok utuh, Anda mendapat bagian setiap blok bntpool.com, sehingga imbalan masuk dalam jumlah kecil teratur.'
sourceHash: c8369c1db7c0
translation: draft
---

<RewardsCalculator />

Panduan ini menunjukkan cara memperkirakan BNT yang ditambang perangkat Anda per hari. Anda akan mempelajari rumus, tempat membaca setiap masukan di [dasbor bntpool.com](/), dan cara menghitung dengan contoh bertanggal dari data langsung pool serta hashrate terbaik terukur seine.

## Berapa BNT yang ditambang perangkat Anda per hari?

<Answer>
Perkiraan BNT per hari adalah hashrate Anda dibagi hashrate jaringan, dikalikan 288 blok sehari dan imbalan blok terkini. Hashrate Anda berasal dari seine atau My Stats, hashrate jaringan dari dasbor bntpool.com, dan imbalan dari ketinggian blok terkini serta jadwal imbalan Blocknet.
</Answer>

Dalam bentuk rumus:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Setiap bagian memiliki alasan sederhana:

- **288 blok sehari.** Blocknet menargetkan satu blok tiap lima menit, dan 86.400 detik sehari dibagi 300 detik per blok menghasilkan 288. Kesulitan disesuaikan setiap blok, sehingga blockchain mendekati laju itu saat penambang bergabung.
- **Bagian hashrate Anda adalah bagian blok Anda.** Kesulitan Blocknet mendekati jumlah hash yang diharapkan untuk menemukan blok. Setiap hash berpeluang sama, sehingga mesin yang mengerjakan 1% hash jaringan dapat mengharapkan 1% blok dalam jangka panjang.
- **Imbalan blok ditentukan ketinggian.** Setiap blok dalam bulan 30 hari berisi 8.640 blok membayar imbalan sama, sehingga imbalan merupakan konstanta diketahui untuk hari mana pun dalam satu bulan.

Hasilnya adalah perkiraan imbalan bruto, dirata-ratakan sepanjang waktu.

## Di mana menemukan setiap masukan?

### Hashrate Anda

seine menampilkan hashrate pada dasbor terminal segera setelah mulai. Beri waktu beberapa menit: program menyetel jumlah thread dan kernel CPU pada pekerjaan penambangan nyata, lalu angka stabil setelah menemukan pengaturan terbaik untuk mesin Anda.

Setelah menambang di pool, kartu **Hashrate** pada [My Stats](/stats) menunjukkan nilai dari sisi pool, rata-rata satu jam terakhir. Untuk perkiraan, gunakan nilai stabil seine atau grafik 24 jam pool.

### Hashrate jaringan

Kartu **Network Hashrate** menampilkan hashrate seluruh jaringan. Satuannya KH/s, sehingga 62,33 KH/s berarti 62.330 H/s. Ubah ke H/s sebelum membagi agar hasil tidak meleset seribu kali.

Pool mengukur nilai dari pekerjaan dan timestamp sepuluh blok terbaru, sehingga berubah tiap blok; [apa itu hashrate?](/blog/id/what-is-hashrate/) menjelaskan perhitungan dan penyebab perubahan. Untuk masukan lebih stabil, periksa beberapa kali sehari lalu gunakan rata-rata. Anda juga dapat membaca H/s dari API:

```sh
curl -s https://bntpool.com/api/stats
```

Bagian `chain` respons memiliki `network_hashrate` dalam H/s dan `current_job_height`, blok yang sedang dikerjakan pool.

### Imbalan blok

Kartu **Current Block** menunjukkan ketinggian yang dikerjakan jaringan. Bagi dengan 8.640 lalu bulatkan ke bawah untuk memperoleh bulan, kemudian cari bulan itu pada [jadwal imbalan blok Blocknet](/blog/id/blocknet-block-reward-schedule/). [Halaman blok](/blocks) menjadi pemeriksaan silang cepat: kolom Reward menampilkan imbalan penuh setiap blok temuan pool.

## Contoh perhitungan dari 25 Oktober 2026

<Note>
Contoh ini menggunakan cuplikan nilai langsung https://bntpool.com/api/stats pada 25 Oktober 2026 pukul 02.03 EDT. Hashrate jaringan dan imbalan blok berubah, jadi masukkan nilai hari ini sebelum mengandalkan hasilnya.
</Note>

| Masukan | Nilai saat cuplikan |
|---|---|
| Blok terkini | 69.020 (bulan 7, karena 69.020 ÷ 8.640 = 7,99) |
| Imbalan blok | 467,67 BNT |
| Hashrate jaringan | 62.329 H/s (ditampilkan 62,33 KH/s) |
| Hashrate bntpool.com | 5.240 H/s |

Ambil server AMD EPYC Zen 5 dari benchmark seine pada 49,4 H/s:

1. Bagian jaringan: 49,4 ÷ 62.329 = 0,0793%.
2. Perkiraan blok sehari: 0,000793 × 288 = 0,2283.
3. Perkiraan BNT sehari: 0,2283 × 467,67 = sekitar 106,8 BNT.

Bulan 8 dimulai pada ketinggian 69.120, sekitar 100 blok setelah cuplikan, dan membayar 439,46 BNT per blok. Dengan hashrate jaringan sama, perkiraan server itu menjadi 0,2283 × 439,46 = sekitar 100,3 BNT sehari. Itulah alasan memeriksa imbalan pada awal setiap bulan.

Berikut perhitungan yang sama pada hashrate jaringan cuplikan, untuk hasil terbaik seine pada setiap generasi perangkat keras:

| Perangkat keras | Thread | Hashrate | Bagian jaringan | BNT/hari pada 467,67 | BNT/hari pada 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Server Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

Angka mesin Anda sendirilah yang penting. Jalankan seine beberapa saat, ambil hashrate stabilnya dan gunakan menggantikan tabel. Panduan [perangkat keras dan hashrate Blocknet](https://seineminer.com/id/blog/blocknet-mining-hardware-hashrate/) menjelaskan peran bandwidth memori dan RAM, dan hasil lengkap ada di [halaman performa](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md) seine.

## Bagaimana penambangan di bntpool.com mengubah perkiraan?

Bagian yang diharapkan tetap sama di pool. Yang berubah adalah cara imbalan tiba. bntpool.com membayar dengan PPLNS: imbalan tiap blok dibagikan berdasarkan share enam jam sebelumnya, sebanding dengan kesulitan. [Cara kerja pembayaran PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan pembagian itu.

Perkiraan yang sama dari sisi pool memberi jawaban sama. Pada cuplikan, 5.240 H/s pool merupakan 8,4% jaringan, sehingga dapat diharapkan 288 × 5.240 ÷ 62.329 = sekitar 24,2 blok sehari, kira-kira satu setiap jam. Dasbor menampilkannya sebagai **Expected Block Time** pada panel Round Progress. Server 49,4 H/s adalah 0,94% hashrate pool, dan 0,94% dari 24,2 blok pada 467,67 BNT kembali menghasilkan sekitar 106,8 BNT sehari. Hashrate pool saling menghapus dalam perhitungan.

Perbedaannya adalah waktu. Menambang sendiri, server Zen 5 itu mengharapkan satu blok utuh sekitar setiap 4,4 hari; [pool versus solo untuk Blocknet](/blog/id/pool-vs-solo-mining-blocknet/) membandingkan lebih rinci. Di bntpool.com server mendapat bagian dari masing-masing sekitar 24 blok harian pool, sehingga saldo bertambah sedikit demi sedikit sepanjang waktu. Biaya pool dan biaya pengembang seine dipotong dari angka bruto rumus.

Dua detail pool memengaruhi hari pertama:

- **Jendela terisi selama enam jam.** Bagian penambang baru bertambah saat share memenuhi jendela PPLNS enam jam, lalu bertahan pada laju penuh selama terus menambang.
- **Imbalan matang sebelum dibayar.** Imbalan setiap blok berpindah ke Confirmed Rewards setelah 60 konfirmasi, sekitar lima jam, dan pool mengirim saldo otomatis setelah mencapai 0,1 BNT.

## Bagaimana membandingkan perkiraan dengan hasil nyata?

Perkiraan adalah rata-rata, sedangkan hasil harian bergerak mengikuti luck pool tiap ronde. Dalam banyak ronde, rata-rata effort mendekati 100%, jadi seminggu penambangan konsisten cocok untuk perbandingan. Catat jumlah Confirmed Rewards dan Paid Balance pada [My Stats](/stats) di awal minggu dan di akhir. Selisihnya adalah kredit dari blok yang matang minggu itu; bandingkan dengan perkiraan untuk hari yang sama menggunakan imbalan dan hashrate jaringan pada hari tersebut.

Hitung ulang saat salah satu masukan berubah:

- **Setiap bulan baru**, ketika imbalan turun. Batas bulan terjadi setiap 8.640 blok, sekitar 30 hari.
- **Saat hashrate jaringan berubah**, seiring penambang bergabung.
- **Saat hashrate Anda berubah**, setelah menambah mesin, menyediakan huge pages di Linux atau menambah GPU NVIDIA, yang ditambang seine bersama CPU.

<Note>
Hashrate panduan ini menggambarkan proof of work Argon2id saat ini. Blocknet merencanakan peningkatan Faraday sekitar 16 Februari 2027 menurut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), dan direncanakan mengubah proof of work. Perbarui seine: program memeriksa rilis baru saat mulai dan setiap 12 jam.
</Note>

<CTA target="seine" title="Ukur hashrate Anda" text="Unduh seine, biarkan menyetel diri beberapa menit, lalu gunakan hashrate-nya dalam rumus bersama nilai dasbor hari ini." />
