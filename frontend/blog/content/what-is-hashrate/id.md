---
title: Apa itu hashrate dalam penambangan kripto?
description: Hashrate adalah jumlah hash yang dihitung penambang per detik. Pelajari satuan, perbedaan hashrate Anda, pool dan jaringan, serta penyebab perubahannya.
tags: [Hashrate, Dasar penambangan, Blocknet, bntpool]
faq:
  - q: 'Berapa hashrate komputer saat menambang Blocknet?'
    a: 'Dalam pengukuran seine, hasil terbaik per generasi adalah 49,4 H/s untuk server AMD EPYC Zen 5, 35,7 H/s untuk AMD EPYC Zen 4, 30,9 H/s untuk laptop Apple M4 Max dan 21,5 H/s untuk server Arm AWS Graviton4. Angka Anda terutama bergantung pada bandwidth memori dan jumlah thread 2 GB yang muat di RAM.'
  - q: 'Mengapa hashrate Blocknet diukur dalam H/s, bukan MH/s atau TH/s?'
    a: 'Setiap hash Blocknet adalah komputasi Argon2id yang menulis sekitar 2 GiB memori dan membacanya kembali, sehingga server besar pun hanya menyelesaikan puluhan hash per detik. Proof of work dengan hash cepat seperti SHA-256 menghasilkan angka jauh lebih besar dan memakai satuan lebih besar. Bandingkan hashrate hanya dalam algoritme yang sama.'
  - q: 'Mengapa pool menunjukkan hashrate berbeda dari penambang?'
    a: 'Penambang menghitung setiap hash selesai. Pool hanya melihat share yang dikirim dan memperkirakan hashrate dari kesulitannya selama satu jam terakhir. Share tiba secara acak, jadi angka pool lebih berubah dan stabil seiring waktu. Bandingkan grafik 24 jam My Stats dengan rata-rata penambang agar setara.'
  - q: 'Apakah hashrate lebih tinggi berarti imbalan lebih besar?'
    a: 'Ya, secara proporsional. Setiap hash berpeluang sama menemukan blok, jadi perkiraan bagian blok jaringan setara bagian hashrate jaringan. Menggandakan hashrate menggandakan perkiraan imbalan, sedangkan kenaikan hashrate jaringan mengurangi bagian Anda. Di bntpool.com bagian itu tiba dari setiap blok pool.'
sourceHash: 0bfdcdca348e
translation: draft
---

Hashrate adalah angka pertama yang ditampilkan penambang, pool dan block explorer. Panduan ini menjelaskan ukurannya, cara membaca satuan mulai H/s, hubungan hashrate Anda dengan pool dan jaringan, serta alasan perubahan tiap menit, menggunakan Blocknet dan dasbor bntpool.com sebagai contoh.

## Apa itu hashrate?

<Answer>
Hashrate adalah jumlah hash yang dihitung penambang per detik. Dalam proof of work, setiap hash adalah satu tebakan solusi blok, dengan peluang kecil sama untuk menang. Hashrate lebih tinggi berarti lebih banyak tebakan per detik, sehingga perkiraan bagian blok bertambah sebanding dengan bagian hashrate total Anda.
</Answer>

Blockchain proof of work meminta penambang menemukan header blok dengan hash di bawah target. Tidak ada jalan pintas: penambang mengubah angka nonce pada header, menghitung hash, membandingkan dengan target, lalu mengulang. Hashrate menghitung percobaan yang diselesaikan mesin tiap detik.

Pada Blocknet, satu percobaan adalah satu hash Argon2id. Nonce menjadi password, sisa header menjadi salt, dan fungsi memproses memori 2 GiB sebelum menghasilkan jawaban 32-byte. seine dan pool melaporkan hash selesai per detik, jadi 1 H/s berarti satu komputasi Argon2id penuh tiap detik.

## Apa arti H/s, kH/s dan MH/s?

Satuan hashrate mengikuti awalan metrik biasa, setiap tingkat seribu kali sebelumnya:

| Satuan | Hash per detik | Penggunaan umum |
|---|---|---|
| H/s | 1 | Satu komputer pada Blocknet |
| kH/s (KH/s) | 1.000 | Total pool dan jaringan Blocknet |
| MH/s | 1.000.000 | Algoritme proof of work lebih cepat |
| GH/s | 1.000.000.000 | Hash cepat seperti SHA-256 |
| TH/s dan ke atas | 1.000.000.000.000 dan lebih | Total jaringan untuk hash cepat |

Dasbor bntpool.com menulis satuan ribuan sebagai KH/s dan menaikkan satuan setiap nilai melewati 1.000, sehingga 44,44 KH/s berarti 44.440 H/s. Saat membagi hashrate, ubah keduanya ke H/s dahulu.

## Mengapa hashrate Blocknet kecil?

Proof of work Blocknet bersifat memory-hard. Setiap hash menulis sekitar 2 GiB memori dan membaca kembali sekitar 2 GiB, jauh melampaui kapasitas cache prosesor, sehingga kecepatan memori menentukan laju. Setiap thread perlu 2 GiB sendiri, sehingga RAM menentukan jumlah thread yang bisa dijalankan.

Berikut hasil terbaik seine pada setiap generasi:

| Perangkat keras | Thread | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (48 GB MacBook Pro) | 16 | 30,9 H/s |
| Server Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Server Zen 5 dengan 32 thread pun menyelesaikan sekitar 1,5 hash per thread tiap detik. Algoritme hash cepat memberi angka jauh lebih besar: wiki Bitcoin mencatat penambang Bitcoin dengan SHA-256 ganda sudah diukur dalam miliaran hash per detik pada 2014. Angka itu mewakili pekerjaan berbeda per hash, jadi hashrate hanya bermakna dibanding algoritme sama. [Penjelasan proof of work Argon2id](https://seineminer.com/id/blog/argon2id-proof-of-work-explained/) membahas algoritme mendalam, dan [panduan perangkat keras Blocknet](https://seineminer.com/id/blog/blocknet-mining-hardware-hashrate/) menjelaskan pengaruh bandwidth memori dan RAM.

## Hashrate Anda, pool dan jaringan

Tiga hashrate penting bagi penambang [pool](/blog/id/what-is-a-mining-pool/), dengan pengukuran berbeda.

### Hashrate Anda

seine menghitung setiap hash selesai dan menampilkan **Current**, laju sekarang, serta **Average**, laju seluruh sesi. Untuk bacaan stabil kemampuan mesin, gunakan rata-rata setelah seine menyetel thread dan kernel CPU selama beberapa menit pada pekerjaan nyata.

Pool tidak melihat hash Anda, hanya share yang mengalahkan target lebih mudah per koneksi. bntpool.com memperkirakan hashrate dari total kesulitan share diterima selama satu jam terakhir dan menampilkannya di [My Stats](/stats). [Pantau penambang di bntpool.com](/blog/id/track-your-miner-on-bntpool/) membahas halaman itu.

### Hashrate pool

Kartu **Pool Hashrate** menjumlahkan pekerjaan share semua penambang terhubung dalam jendela satu jam yang sama. Angka menunjukkan bagian pekerjaan jaringan oleh bntpool.com dan perkiraan frekuensi menemukan blok, ditampilkan sebagai **Expected Block Time**.

### Hashrate jaringan

Tak ada yang dapat menghitung semua hash jaringan, sehingga hashrate diturunkan dari blok. Kesulitan adalah perkiraan jumlah hash untuk menemukan blok, dan Blocknet menargetkan satu blok setiap 300 detik.

## Bagaimana hashrate jaringan dihitung?

<Answer>
Hashrate jaringan diperkirakan dari kesulitan dan waktu blok. Pada Blocknet, kesulitan mendekati jumlah hash per blok dengan target 300 detik, jadi hashrate kira-kira kesulitan dibagi 300. Dasbor bntpool.com menghitungnya dari pekerjaan nyata dan timestamp sepuluh blok terbaru.
</Answer>

Berikut perhitungan dengan nilai langsung 25 Oktober 2026 pukul 02.26 EDT. [Blocknet explorer](https://explorer.blocknetcrypto.com) menunjukkan kesulitan berikutnya 16.943.147, sehingga jaringan disetel membutuhkan sekitar 16,9 juta hash per blok. Dibagi 300 detik menghasilkan sekitar 56.477 H/s.

Pada saat sama, dasbor menunjukkan 44,44 KH/s atau 44.440 H/s. Keduanya memperkirakan hal sama dari sudut berbeda. Angka kesulitan adalah laju yang menghasilkan blok tepat pada target lima menit. Angka dasbor membagi pekerjaan nyata sepuluh blok terakhir dengan waktu yang diperlukan, sehingga lebih rendah bila blok terbaru lebih lama dari lima menit. Blocknet menyesuaikan kesulitan setiap blok agar keduanya mendekat seiring waktu.

## Mengapa hashrate berfluktuasi?

Ketiganya berubah dengan alasan berbeda.

- **Mesin Anda.** Pada menit awal seine membandingkan thread dan kernel lalu memilih tercepat, sehingga laju meningkat saat penyetelan. Sesudahnya, penggunaan memori atau core oleh kegiatan lain dapat mengambil waktu hashing.
- **Pandangan pool terhadap Anda.** Share adalah peristiwa acak. bntpool.com mengatur kesulitan agar sekitar 10 share tiap lima menit, jadi satu jam berisi kira-kira 120 share; periode beruntung atau sepi mengubah perkiraan. Rig baru dirata-ratakan selama menit yang ditambang hingga satu jam penuh.
- **Jaringan.** Waktu blok juga acak dan sampel sepuluh blok pendek. Pada 25 Oktober 2026, dasbor menunjukkan sekitar 50.057 H/s pukul 00.26 EDT, 62.329 H/s pukul 02.03 dan 44.440 H/s pukul 02.26. Penambang masuk dan keluar menambahkan perubahan nyata di atas variasi itu.

Untuk keputusan, gunakan rata-rata: rata-rata sesi seine, grafik 24 jam My Stats, dan nilai jaringan yang diperiksa beberapa kali sehari.

## Apa arti hashrate untuk imbalan?

Karena tiap hash berpeluang sama, bagian hashrate jaringan Anda adalah perkiraan bagian blok. Blocknet menghasilkan 288 blok sehari, jadi penambang dengan 0,1% jaringan mengharapkan imbalan setara sekitar 0,29 blok sehari dalam jangka panjang. [Cara memperkirakan imbalan Blocknet](/blog/id/estimate-blocknet-mining-rewards/) mengubahnya menjadi BNT per hari memakai masukan [dasbor](/).

Di bntpool.com, bagian itu tiba sebagai porsi setiap blok pool, dikreditkan menurut share diterima, sehingga imbalan bertambah sedikit demi sedikit sepanjang hari. Jika belum mulai, [panduan pemasangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) membawa Anda dari unduhan ke share pertama dalam beberapa menit.

<Note>
Hashrate panduan ini menggambarkan proof of work Argon2id saat ini. Blocknet merencanakan peningkatan Faraday sekitar 16 Februari 2027 menurut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), dan direncanakan mengubah proof of work. Perbarui seine: program memeriksa rilis baru saat mulai dan setiap 12 jam.
</Note>

<CTA target="seine" title="Lihat hashrate Anda" text="Unduh seine, biarkan menyetel diri beberapa menit, lalu pantau hashrate pada dasbornya dan My Stats." />
