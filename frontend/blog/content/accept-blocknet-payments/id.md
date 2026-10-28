---
title: "Terima pembayaran Blocknet untuk barang dan jasa"
description: "Terima BNT untuk pesanan, cocokkan pembayaran di dompet Blocknet, periksa konfirmasi, serta tangani pembayaran sebagian dan pengembalian dana dengan jelas."
tags: [Blocknet, BNT]
faq:
  - q: "Bagaimana mengetahui pelanggan sudah membayar dengan Blocknet?"
    a: "Temukan penerimaan di dompet Anda yang tersinkron, cocokkan jumlah dan referensi pesanan, lalu periksa konfirmasinya. Simpan ID transaksi lengkap bersama pesanan. Tangkapan layar pelanggan atau tautan pembayaran saja tidak membuktikan penerimaan; explorer juga tidak menampilkan jumlah privat yang dibayarkan ke alamat Anda."
  - q: "Bagaimana jika pelanggan mengirim BNT kurang dari permintaan?"
    a: "Biarkan pesanan berstatus dibayar sebagian dan hitung sisanya dari penerimaan yang sudah diverifikasi. Minta selisih dengan referensi yang sama dan simpan kedua ID transaksi. Hitung tiap penerimaan sekali. Jika sepakat membatalkan, konfirmasi alamat pengembalian melalui percakapan pesanan yang sudah ada."
  - q: "Apakah memo otomatis menandai pesanan sudah dibayar?"
    a: "Tidak. Memo membantu mengaitkan penerimaan, tetapi pembayar dapat mengubah atau menghilangkannya. Periksa jumlah dan status transaksi sendiri. Dompet membuat permintaan dan menampilkan penerimaan; alur manual ini tidak menambahkan checkout otomatis atau sistem pengelolaan pesanan."
sourceHash: '3309a069e5de'
translation: draft
---

Terima Blocknet dengan menyebutkan jumlah BNT yang jelas, memeriksa penerimaan di dompet, dan menyimpannya bersama pesanan. Contoh fiktif ini meminta **1.25 BNT** dengan referensi **order-1042**. Panduan memakai dompet desktop rilis v0.22.0; tangkapan layar berisi data demo berlabel.

## Bagaimana menerima pembayaran Blocknet?

<Answer>
Sepakati jumlah BNT dan referensi pesanan, kirim permintaan ke alamat yang Anda kendalikan, lalu tunggu penerimaan di dompet tersinkron. Cocokkan jumlah serta referensi, periksa konfirmasi, simpan ID transaksi, dan tandai pesanan dibayar sesuai kebijakan konfirmasi yang disepakati dengan pelanggan.
</Answer>

Gunakan [panduan dompet desktop](https://seineminer.com/id/blog/blocknet-desktop-wallet/) untuk membuat atau membuka dompet. Simpan cadangan pemulihan terpisah dari pesanan. Menerima pembayaran pelanggan tidak membutuhkan miner atau keanggotaan pool.

## Tetapkan jumlah dan identitas pesanan

Catat referensi, jumlah BNT, alamat penerimaan dan barang yang dibeli. Jika harga memakai mata uang lain, sepakati jumlah BNT serta masa berlaku penawaran sebelum pembayaran. Tautan membawa jumlah yang diminta; tidak memperbarui kurs atau memberlakukan tenggat.

Untuk contoh, minta **1.25 BNT** dengan memo **order-1042**. Nomor pesanan sudah cukup; alamat pengiriman dan data pelanggan tetap di sistem pribadi Anda. [Memo terenkripsi](https://seineminer.com/id/blog/blocknet-encrypted-memos/) menyertai pembayaran, tetapi memo dalam tautan yang dibagikan dapat dibaca siapa saja yang menerima tautan itu.

Di **Receive → Payment Request**, isi jumlah dan memo, lalu pilih **Generate Payment Link**. Periksa penerima dan jumlah sebelum membagikannya. [Panduan tautan pembayaran](https://seineminer.com/id/blog/blocknet-payment-links/) menjelaskan format dan penerima berbasis nama. Untuk faktur tetap, simpan alamat lengkap bersama permintaan.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Dompet Blocknet rilis menampilkan permintaan 1.25 BNT, memo order-1042 dan alamat demo." loading="lazy" decoding="async"></a>
<figcaption>Antarmuka Inggris asli dengan data demo. Membuat permintaan belum memindahkan dana. Buka gambar untuk memperbesarnya.</figcaption>
</figure>

## Periksa penerimaan di dompet sendiri

Minta pelanggan menyertakan referensi dan menyimpan ID transaksi. Setelah node dan dompet mengejar ketertinggalan, buka **History** dan temukan pembayaran masuk. Bandingkan jumlah dan memo dengan pesanan. Pembayar dapat mengubah kedua kolom, sehingga permintaan tidak membuktikan apa yang dikirim.

Simpan ID lengkap beserta jumlah diterima. Jangan menghitung dua kali karena transaksi tampil di Dashboard dan History atau pelanggan mengirim ulang ID yang sama. Cocokkan penerimaan tertentu, bukan perubahan saldo keseluruhan yang mungkin mencakup mining, pelanggan lain dan transfer sendiri.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History dompet Blocknet menampilkan penerimaan demo 1.25 BNT dengan memo order-1042." loading="lazy" decoding="async"></a>
<figcaption>Penerimaan contoh sesuai jumlah dan referensi. Periksa status transaksi sebelum memenuhi pesanan.</figcaption>
</figure>

Jika penerimaan belum ada, bandingkan alamat tujuan lengkap dan ID dengan pelanggan, lalu periksa sinkronisasi. Tangkapan layar pengiriman memberi konteks; dompet penerima Anda menetapkan jumlah yang diterima.

## Kapan menandai pesanan sudah dibayar?

Bedakan pembayaran masuk belum terkonfirmasi, transaksi dalam blok, dan dana yang dapat dibelanjakan. Dompet rilis menunggu sepuluh blok berikutnya setelah blok pembayaran biasa sebelum output-nya dapat dibelanjakan. [Panduan waktu transfer](https://seineminer.com/id/blog/blocknet-transfer-time/) menjelaskan penghitung dan waktu tunggu yang bervariasi.

Sampaikan kebijakan konfirmasi sebelum pembayaran. Alur manual sederhana menunggu penerimaan terkait dapat dibelanjakan sebelum memenuhi pesanan. Jumlah masuk yang terlihat belum menyelesaikan pemeriksaan. Jangan menjanjikan waktu pengiriman pasti dari hitung mundur dompet: kedatangan blok bervariasi. [Cara mining mengamankan pembayaran](/blog/id/how-mining-secures-blocknet-payments/) menjelaskan kedalaman konfirmasi.

## Tangani pembayaran sebagian, berlebih atau belum cocok

| Penerimaan terverifikasi | Langkah berikutnya |
|---|---|
| 1.00 BNT untuk pesanan 1.25 BNT | Catat pembayaran sebagian dan minta sisa 0.25 BNT. |
| Tambahan 0.25 BNT kemudian | Simpan kedua ID, hitung total 1.25 BNT sekali dan periksa konfirmasi keduanya. |
| Lebih dari 1.25 BNT | Sepakati pengembalian atau kredit dan catat keputusan. |
| Jumlah benar tanpa memo | Cocokkan ID lengkap dan tujuan dengan pelanggan sebelum mengaitkan. |
| Pembayaran tertunda | Biarkan pesanan menunggu konfirmasi dan periksa status kembali. |

Untuk pengembalian, minta konfirmasi alamat penerima melalui percakapan pesanan yang sama. Jangan menyimpulkan alamat balik dari transaksi publik: input Blocknet tidak menyediakan alamat pengirim konvensional. Tinjau jumlah, tujuan dan biaya sebelum menyetujui pengiriman baru; simpan ID-nya bersama pembayaran awal.

## Bagikan hanya bukti yang diperlukan

Explorer membantu memeriksa penyertaan transaksi, tetapi tidak mengungkap jumlah privat untuk alamat Anda. Jika penerimaan diperselisihkan, pengirim dapat memberikan bukti khusus transaksi melalui [panduan bukti pembayaran](https://seineminer.com/id/blog/blocknet-payment-proof/). Periksa kecocokan penerima, jumlah dan status rantai; validitas kunci saja tidak cukup.

Simpan pesanan, permintaan, ID masuk, jumlah terverifikasi, keputusan konfirmasi dan pengembalian dalam catatan pribadi yang sama. Bagikan hanya tanda terima atau bukti terkait. Jangan pernah membagikan kata pemulihan, kata sandi atau kunci tampilan privat seluruh dompet sebagai bukti.

Jika juga menambang BNT, pisahkan [catatan pembayaran pool](/blog/id/track-your-miner-on-bntpool/) dari pesanan meskipun memakai dompet yang sama. Dengan begitu jelas transfer mana membayar faktur mana.


<CTA target="pool" title="Terima juga imbalan mining" text="Panduan pool menjelaskan cara menghubungkan miner dan mengikuti pembayarannya bersama penerimaan Blocknet lainnya." />
