---
title: Jadwal imbalan blok Blocknet
description: Imbalan blok Blocknet per bulan, dari 723,25 BNT saat peluncuran ke tail permanen 2,0 BNT pada bulan 48, dengan 288 blok sehari dan 98,6 juta BNT sebelum tail.
tags: [Blocknet, Imbalan blok, Emisi, Penambangan]
faq:
  - q: 'Berapa imbalan blok Blocknet saat ini?'
    a: 'Tergantung bulan blockchain saat ini, karena imbalan turun setiap 8.640 blok. Pada 25 Oktober 2026, blockchain berada pada bulan 7 dengan 467,67 BNT per blok, dan bulan 8 dimulai pada ketinggian 69.120 dengan 439,46 BNT. Cari ketinggian blok terkini di dasbor bntpool.com lalu cocokkan dengan tabel.'
  - q: 'Berapa blok Blocknet ditambang per hari?'
    a: 'Blocknet menargetkan satu blok setiap lima menit, yaitu 288 blok sehari dan 8.640 blok per bulan 30 hari. Kesulitan disesuaikan setiap blok dengan rata-rata tertimbang waktu 60 blok terakhir, sehingga rata-ratanya tetap mendekati lima menit saat penambang bergabung atau meninggalkan jaringan.'
  - q: 'Berapa total pasokan Blocknet?'
    a: 'Sekitar 98,56 juta BNT ditambang sebelum tail dimulai pada bulan 48. Kode dan situs resmi membulatkannya menjadi sekitar 100 juta. Setelah itu setiap blok membayar 2,0 BNT selamanya, menambah 576 BNT sehari atau sekitar 210.240 BNT setahun, sehingga pasokan terus bertambah perlahan.'
  - q: 'Kapan tail emission Blocknet dimulai?'
    a: 'Tail dimulai pada bulan 48, di ketinggian blok 414.720. Itu berarti 48 bulan masing-masing 30 hari, atau 1.440 hari blok lima menit setelah peluncuran, kira-kira empat tahun. Tanggal kalender tepatnya bergantung pada waktu blok sebenarnya. Mulai ketinggian itu, setiap blok membayar 2,0 BNT.'
  - q: 'Berapa lama sebelum koin Blocknet hasil tambang dapat dibelanjakan?'
    a: 'Imbalan blok dapat dibelanjakan setelah 60 blok, sekitar lima jam pada waktu blok lima menit. Ini disebut coinbase maturity. Pool bntpool.com juga menunggu 60 blok sebelum membayar imbalan, sehingga hanya imbalan dari blok matang yang dibayar.'
sourceHash: abea18c2c21b
translation: draft
---

Halaman ini mencantumkan imbalan blok Blocknet untuk setiap bulan jadwal emisi, menjelaskan perhitungannya, serta arti jadwal tersebut bagi Anda sebagai penambang.

## Berapa imbalan blok Blocknet?

<Answer>
Imbalan blok Blocknet dimulai dari 723,25 BNT per blok dan turun sekali setiap bulan 30 hari berisi 8.640 blok, mengikuti kurva penurunan mulus. Nilainya sama untuk setiap blok dalam satu bulan. Mulai bulan 48, pada ketinggian 414.720, setiap blok membayar tail permanen 2,0 BNT.
</Answer>

Blocknet menargetkan satu blok setiap lima menit (300 detik). Hasilnya 288 blok sehari dan 8.640 blok per bulan 30 hari. Kesulitan disesuaikan setiap blok berdasarkan rata-rata tertimbang waktu 60 blok terakhir, agar laju tetap mendekati lima menit saat hashrate jaringan berubah.

Imbalan setiap blok hanya bergantung pada bulan dari ketinggiannya. Bulan dihitung dengan membagi ketinggian blok dengan 8.640 lalu membulatkan ke bawah: ketinggian 0 sampai 8.639 adalah bulan 0, ketinggian 8.640 sampai 17.279 adalah bulan 1, dan seterusnya.

## Bagaimana imbalan dihitung?

Daemon menghitung imbalan bulanan dengan satu rumus:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

Suku pertama adalah bagian imbalan di atas tail 2,0 BNT. Bagian itu menyusut dengan faktor e^-0,75 setiap tahun, sehingga setelah dua belas bulan menjadi sekitar 47% nilai awal. Jika dibagi per bulan, bagian di atas tail setiap bulan sekitar 93,9% bulan sebelumnya, jadi imbalan turun dalam langkah kecil teratur.

Hasil dibulatkan ke bawah ke satuan atomik utuh (1 BNT adalah 100.000.000 satuan atomik). Tabel berikut menampilkan imbalan yang dibulatkan ke dua desimal.

## Tabel imbalan Blocknet per bulan

| Bulan | Blok pertama | Imbalan per blok (BNT) | Total ditambang pada akhir bulan (juta BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| 48 dan seterusnya | 414.720 | 2,00 | +0,017 per bulan |

Blok genesis pada ketinggian 0 tidak membawa imbalan, sehingga bulan 0 memiliki 8.639 blok berbayar. Setiap BNT berasal dari imbalan blok ini, tanpa premine, seperti dijelaskan halaman [fair launch Blocknet](/blog/id/blocknet-fair-launch/). Pada 25 Oktober 2026, pool mengerjakan blok 69.016, mendekati akhir bulan 7. [Dasbor bntpool.com](/) menampilkan ketinggian terkini agar Anda dapat mencari barisnya di atas, dan [halaman blok](/blocks) mencantumkan blok temuan pool.

## Apa yang terjadi pada bulan 48?

<Answer>
Bulan 48 dimulai pada ketinggian 414.720, dan mulai saat itu setiap blok Blocknet membayar 2,0 BNT tetap selamanya. Inilah tail emission. Setiap blok tetap memiliki imbalan selama blockchain berjalan, sehingga penambang selalu dibayar untuk mengamankan jaringan.
</Answer>

Rumus penurunan berlaku untuk bulan 0 hingga 47, berakhir pada 40,22 BNT per blok di bulan 47. Mulai bulan 48 daemon langsung menggunakan nilai tail, jadi imbalan menjadi 2,0 BNT dan tetap di sana. Pada 288 blok sehari, tail menambah 576 BNT sehari atau sekitar 210.240 BNT setahun.

Bulan 48 dimulai setelah 1.440 hari blok lima menit sejak peluncuran, kira-kira empat tahun. Tanggal kalender bergantung pada waktu blok nyata, jadi ketinggian blok adalah penanda andal.

## Berapa BNT ditambang sebelum tail?

Menjumlahkan setiap blok dari ketinggian 1 hingga 414.719 menghasilkan 98.561.905,08 BNT, sekitar 98,6 juta. Kode dan situs resmi menggambarkan jadwal sebagai sekitar 100 juta koin selama empat tahun ditambah tail, dan jumlah tepatnya sedikit di bawah angka bulat tersebut.

Sebagian besar ditambang lebih awal. Dikelompokkan berdasarkan tahun jadwal:

| Bulan | BNT ditambang | Bagian dari total sebelum tail |
|---|---|---|
| 0-11 (tahun 1) | 54,48 juta | 55,3% |
| 12-23 (tahun 2) | 25,84 juta | 26,2% |
| 24-35 (tahun 3) | 12,32 juta | 12,5% |
| 36-47 (tahun 4) | 5,93 juta | 6,0% |

## Kapan BNT hasil tambang dapat dibelanjakan?

Imbalan blok dapat dibelanjakan setelah 60 blok tambahan ditumpuk di atasnya, sekitar lima jam kemudian. Aturan ini disebut coinbase maturity. Pool bntpool.com menunggu 60 blok yang sama sebelum membayar imbalan, dan [halaman pembayaran](/payouts) mencantumkan yang sudah dikirim.

## Apa arti jadwal ini bagi penambang?

Imbalan lebih besar pada bulan-bulan awal. Blok pada bulan 12 membayar 342,69 BNT, sekitar 47% dari 723,25 BNT pada bulan 0, sedangkan blok bulan 24 membayar 162,93 BNT. Setiap bulan baru membayar lebih sedikit per blok daripada sebelumnya hingga tail.

Bagian Anda mengikuti proporsi hashrate jaringan. Untuk penambangan solo, jumlah blok harian yang diharapkan adalah 288 × (hashrate Anda ÷ hashrate jaringan), dan setiap blok membayar imbalan bulannya. Pada pool, perhitungan yang sama berlaku pada hashrate pool, lalu imbalan dibagi kepada penambang yang berkontribusi. Jadi Anda memperoleh bagian setiap blok pool tanpa menunggu menemukan satu blok sendiri. [Dasbor bntpool.com](/) menunjukkan hashrate jaringan dan pool terkini, sedangkan [halaman statistik](/stats) menunjukkan hashrate Anda setelah mulai menambang.

Untuk mulai sekarang:

1. Unduh seine dari [seineminer.com](https://seineminer.com/id/#download). Program berjalan di Windows, macOS dan Linux serta menyetel diri sesuai perangkat keras.
2. Jalankan lalu tempel alamat Blocknet atau `@name` Anda.
3. Terima pool default, `stratum+tcp://bntpool.com:3333`.

[Panduan menambang di bntpool.com](/blog/id/mine-blocknet-on-bntpool/) membahas tiap langkah dari sisi pool, [halaman Get Started](/start) mencantumkan pengaturannya, dan [panduan penambangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) menjelaskan pengaturan langkah demi langkah. Untuk melihat kemampuan berbagai prosesor, baca [perangkat keras dan hashrate penambangan Blocknet](https://seineminer.com/id/blog/blocknet-mining-hardware-hashrate/).

<Note>
Angka di halaman ini berasal dari kode imbalan daemon Blocknet, diperiksa pada 25 Oktober 2026. Blocknet merencanakan peningkatan jaringan Faraday sekitar 16 Februari 2027, menurut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). Perbarui seine: program memeriksa rilis baru saat mulai dan setiap 12 jam.
</Note>

<CTA target="seine" title="Tambang Blocknet dengan seine" text="seine gratis untuk Windows, macOS dan Linux serta menambang di bntpool.com secara default." />
