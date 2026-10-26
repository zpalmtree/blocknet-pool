---
title: 'Memahami konfirmasi blok dan imbalan tambang belum matang'
description: 'Ikuti blok Blocknet hingga imbalan matang dan dibayar. Pelajari cara bntpool menghitung kedalaman serta alasan konfirmasi diukur berdasarkan jumlah blok.'
tags: ['Imbalan penambangan', 'Konfirmasi', 'Blocknet']
faq:
  - q: 'Berapa konfirmasi yang diperlukan bntpool?'
    a: 'Pada pemeriksaan 26 Oktober 2026, bntpool.com memublikasikan syarat 60 blok. Pemeriksaan mengurangi tinggi saat ini dengan tinggi blok ditemukan dan membandingkan hash dengan node. Lihat Get Started atau API publik untuk nilai terbaru.'
  - q: 'Apakah 60 blok berarti tepat lima jam?'
    a: 'Tidak. Enam puluh interval target lima menit menghasilkan perkiraan 300 menit atau lima jam. Kemajuan nyata mengikuti blok baru dengan waktu kedatangan bervariasi. Kematangan, ambang saldo, dan transaksi pembayaran berikutnya adalah tahap terpisah.'
  - q: 'Haruskah terus menambang agar imbalan matang?'
    a: 'Blok yang telah ditemukan bertambah dalam saat jaringan menambah blok, meski miner terputus. Hak imbalan mengikuti aturan akuntansi pool. Berhenti tidak mempercepat atau mengulang kematangan; pantau catatannya di My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Ikuti blok Blocknet melalui konfirmasi, kematangan, lalu pembayaran ke dompet. Panduan ini menjelaskan kedalaman rantai di balik kartu imbalan bntpool.com. [Panduan PPLNS](/blog/id/how-pplns-payouts-work/) membahas pembagian; [panduan pengaturan](/blog/id/mine-blocknet-on-bntpool/) membantu memulai.

## Apa itu konfirmasi dan imbalan belum matang?

<Answer>
Konfirmasi menggambarkan posisi blok di bawah ujung rantai saat ini. Imbalan belum matang berasal dari blok baru yang belum mencapai kedalaman wajib. bntpool memeriksa kedalaman dan identitas sebelum mengonfirmasi; kelayakan saldo dan pembayaran menyusul terpisah.
</Answer>

*Blok bertambah dalam, mencapai kematangan, lalu melalui saldo yang memenuhi syarat menuju pembayaran terpisah.*

## Bagaimana bntpool menghitung kedalaman?

Pool mengurangi tinggi rantai saat ini dengan tinggi blok ditemukan. Pada 26 Oktober 2026 syaratnya 60 blok. Dalam konvensi ini blok dimulai dari nol. Halaman lain mungkin menghitung blok itu sendiri sebagai konfirmasi pertama; bandingkan aturan dan angkanya.

Contoh: blok 70.000 berkedalaman nol pada ujung 70.000, satu pada 70.001, dan 60 pada 70.060. Ini tinggi ilustratif, bukan data langsung. Pool juga memeriksa hash dengan node. [Implementasi pool](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) mencatat kedua pemeriksaan.

## Mengapa lima jam hanya perkiraan?

Blocknet menargetkan lima menit antarblok. Enam puluh kali lima menjadi 300 menit atau lima jam. Angka ini membantu perencanaan, tetapi syaratnya jumlah blok. Amati kemajuan rantai dari tinggi ditemukan sampai kedalaman wajib, bukan tenggat jam tetap.

Setiap blok pada rantai diterima menambah kedalaman, tidak harus dari bntpool. Kecepatan miner Anda tidak membuat jam konfirmasi pribadi. Kedatangan bervariasi sehingga waktunya bisa berbeda dari perkiraan. Panduan [kesulitan](/blog/id/mining-difficulty-explained/) dan [keberuntungan](/blog/id/mining-pool-luck-explained/) menjelaskan penyesuaian jaringan serta variasi putaran.

## Bedakan tertunda, terkonfirmasi, dan dibayar

Blok baru tercatat sebagai tertunda sambil bertambah dalam. Bagian perkiraan muncul di [My Stats](/stats) jika pekerjaan memenuhi syarat. Imbalan membuat koin baru; aturan coinbase dompet Blocknet mensyaratkan kedalaman 60 blok sebelum dibelanjakan. Penantian pool mengikuti proses ini.

Setelah kedalaman dan identitas lolos, pool dapat mengonfirmasi serta membagikan imbalan ke saldo yang memenuhi syarat. Tidak semua alamat langsung menerima pembayaran. Proses pembayaran menilai saldo terkonfirmasi dan syarat pengiriman secara terpisah. Status blok, saldo, dan riwayat menunjukkan tahap berbeda.

## Mengapa hash blok diperiksa?

Tinggi menunjukkan posisi, hash menunjukkan blok tertentu. Node dapat mengetahui blok bersaing; rantai diterima menentukan pengisi posisi. Karena itu pool meminta blok pada tinggi tersebut dari node dan membandingkan hash. Kedalaman saja tidak membuktikan blok semula masih di rantai.

Blok yang keluar dapat ditandai orphaned dan imbalan sementaranya tidak lagi memenuhi syarat sebagai imbalan rantai diterima. Status ini ditangani terpisah. Gunakan tautan hash di [Blocks](/blocks): dua entri pada tinggi sama belum tentu peristiwa yang sama. Perkiraan tetap terkait bloknya.

## Apa perbedaan pembayaran dan imbalan tambang?

Imbalan berasal dari output coinbase blok. Pembayaran pool adalah transaksi berikutnya ke alamat penerima. Pada tanggal pemeriksaan, minimum yang dipublikasikan 0,1 BNT. Kematangan dan ambang saldo berbeda; [panduan PPLNS](/blog/id/how-pplns-payouts-work/) merinci pembagian dan pembayaran.

Setelah dikirim, ikuti transaksi serta status penerimaan dan kemampuan belanja di dompet. Konfirmasinya milik pembayaran, bukan blok pool semula. Tidak perlu alamat baru atau menyambung ulang miner selama kematangan. Bandingkan [riwayat pembayaran](/payouts) dan dompet memakai alamat yang sama.

## Bagaimana mengikuti seluruh perjalanan imbalan?

Di [Blocks](/blocks), temukan tinggi, hash, dan status. Buka [My Stats](/stats) dengan alamat miner untuk melihat perkiraan serta imbalan terkonfirmasi. Saat ada pembayaran, ikuti tautan transaksinya. Setiap pengamatan berhubungan dengan blok, saldo akun, atau pembayaran dompet.

Periksa pengaturan di [Get Started](/start) atau `https://bntpool.com/api/info` dan [status pool](/status). Untuk investigasi, catat alamat, ID blok atau transaksi, dan waktu; jangan bagikan kata pemulihan. [Panduan pemantauan](/blog/id/track-your-miner-on-bntpool/) menjelaskan sisanya. Blok tetap bertambah dalam saat offline; kelayakan mengikuti pekerjaan tercatat dan rantai.

<CTA target="pool" title="Pantau imbalan penambangan" text="Hubungkan ke bntpool.com dan ikuti perkiraan, saldo terkonfirmasi, serta pembayaran di My Stats." />
