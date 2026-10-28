---
sourceHash: 'e64d7723349d'
translation: draft
title: Cara penambangan mengamankan pembayaran Blocknet
description: Pelajari cara Blocknet memilih riwayat pembayaran lewat proof of work, peran konfirmasi dalam menambah kerja, dan pemeriksaan node saat menerima blok.
tags: [Blocknet, Keamanan pembayaran, Proof of work]
faq:
  - q: Bisakah daya tambang lebih besar mengizinkan pembayaran dari dompet saya?
    a: Daya tambang membantu menghasilkan blok pesaing. Membelanjakan dana tetap memerlukan otorisasi yang sah, dan node memeriksa bukti serta keseimbangan transaksi sebelum menerima blok. Cabang yang lebih berat harus memenuhi aturan itu. Proof of work tidak menggantikan kunci privat yang diperlukan untuk membelanjakan output Anda.
  - q: Apakah Blocknet mengikuti rantai dengan blok terbanyak?
    a: Core v0.22.0 menghitung kerja kumulatif dengan menjumlahkan difficulty setiap blok. Cabang sah dengan kerja lebih besar dapat menjadi rantai terpilih, sesuai aturan reorganisasi. Jumlah blok saja tidak cukup karena blok dapat memiliki difficulty berbeda. Kerja dan keabsahan sama-sama penting.
  - q: Apakah konfirmasi pembayaran sama dengan share pool?
    a: Share mencatat kerja penambangan yang dikirim ke pool. Konfirmasi pembayaran berkaitan dengan masuknya transaksi ke blockchain terpilih dan blok yang dibangun setelahnya. Share membantu membagi imbalan, sedangkan blok jaringan memajukan riwayat pembayaran. Menambah share Anda tidak langsung menambah konfirmasi pada pembayaran tertentu.
---

Ikuti pembayaran BNT dari saat masuk ke blok hingga menjadi bagian riwayat yang didukung kerja penambangan berikutnya. Panduan ini menjelaskan mengapa jaringan perlu memilih di antara riwayat yang bersaing, arti konfirmasi, serta hubungan pilihan tersebut dengan validasi transaksi privat. Implementasi yang dibahas adalah core v0.22.0 yang telah dirilis, diperiksa pada 28 Oktober 2026.

## Apa yang dilindungi penambangan saat Anda mengirim BNT?

<Answer>
Penambangan membantu jaringan memilih riwayat pembayaran bersama yang berurutan. Setiap blok yang diterima terhubung ke pendahulunya dan menyumbang proof of work. Node memeriksa blok dan transaksi yang diusulkan, lalu membandingkan cabang sah berdasarkan kerja kumulatif sesuai aturan reorganisasi. Blok berikutnya menambahkan kerja di belakang pembayaran yang sudah masuk, memberi makna praktis pada riwayat konfirmasinya.
</Answer>

Pembayaran yang sah memerlukan otorisasi dan tempat dalam buku besar. Dompet menyediakan otorisasi untuk membelanjakan dana Anda. Penambangan dan pemilihan rantai membantu node menyepakati transaksi sah mana yang termasuk riwayat saat ini. Keduanya saling melengkapi: proof of work sebuah blok tidak membuat pembayaran yang tidak sah menjadi dapat diterima.

[Panduan penegakan aturan](https://seineminer.com/id/blog/who-controls-blocknet/) menjelaskan siapa yang mengembangkan, mengusulkan, dan memvalidasi. Di sini fokusnya pembayaran itu sendiri: bagaimana penyertaannya menjadi bagian riwayat yang diikuti dompet dan node lain.

## Bagaimana blok mengikat riwayat pembayaran?

Header blok memuat hash blok sebelumnya dan akar Merkle yang diturunkan dari transaksinya. Akar mengikat isi blok secara kriptografis, sedangkan hash sebelumnya menghubungkannya ke riwayat terdahulu. Mengubah transaksi mengubah akar dan header yang ditambang. Blok turunannya kemudian harus merujuk riwayat yang berubah melalui tautannya sendiri.

Bayangkan pembayaran dalam sebuah blok, diikuti beberapa blok lain yang diterima. Mengganti bagian riwayat itu memerlukan cabang pesaing berisi blok sah yang saling terhubung dan kerja kumulatif yang cukup, dalam batas aturan reorganisasi node. Mengedit transaksi di berkas lokal tidak menghasilkan cabang tersebut. Kerja terkait dengan urutan header dan transaksi tertentu.

[Kode blok yang dirilis](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) menetapkan bidang header, pemeriksaan Merkle, dan jalur pemilihan rantai tersebut. [Panduan difficulty](/blog/id/mining-difficulty-explained/) menjelaskan cara jaringan menentukan kebutuhan kerja setiap blok baru.

## Mengapa kerja kumulatif lebih penting daripada jumlah blok?

Dua penambang dapat menemukan blok sah berbeda yang memperpanjang induk yang sama. Setiap usulan memiliki hash sendiri, dan node dapat mendengarnya dalam urutan berbeda. Hal ini dapat menimbulkan cabang pesaing sementara. Jaringan memerlukan aturan yang mengikuti kerja pendukung riwayat, bukan urutan pengumumannya oleh peer tertentu.

Core v0.22.0 menghitung kerja kumulatif dengan menambahkan difficulty blok ke kerja kumulatif induknya. Setelah validasi, cabang dengan kerja yang benar-benar lebih besar dapat memicu pergantian rantai terpilih. Alternatif dengan kerja setara tidak memicu pergantian melalui perbandingan ini. Difficulty juga harus sah untuk konteks induk cabang; penambang tidak bisa sekadar menulis angka lebih besar di header.

Sebagai contoh, bandingkan dua cabang dengan bagian awal yang sama. Bagian bersama itu menyumbang kerja sama besar ke kedua total. Perbedaan penentunya adalah kerja setelah percabangan, bersama pemeriksaan keabsahan dan reorganisasi. Menghitung blok tanpa mempertimbangkan difficulty dapat melewatkan perbedaan itu. Ini perbandingan konseptual, bukan laporan fork yang sedang terjadi.

## Bagaimana konfirmasi membantu memantau pembayaran?

Setelah pembayaran masuk ke rantai terpilih, blok selanjutnya dibangun di atas riwayat itu. Kedalamannya bertambah seiring datangnya blok. Mengganti penyertaannya memerlukan riwayat alternatif yang memenuhi syarat dari sebelum blok terkait, sementara cabang saat ini terus mengumpulkan kerja. Karena itu, memeriksa penyertaan dan kedalaman lebih bermakna daripada sekadar melihat transaksi disiarkan.

Ketinggian menunjukkan posisi; hash menunjukkan blok tertentu. Periksa catatan transaksi dan blok yang memuatnya, lalu pastikan blok itu masih ada dalam rantai terpilih. Jika riwayat terpilih berubah, penyertaan dan kedalamannya juga dapat berubah. Penghitung konfirmasi bisa menyertakan blok yang memuat transaksi atau hanya menghitung blok sesudahnya. Periksa konvensi antarmukanya.

Core v0.22.0 juga memberlakukan batas reorganisasi: pergantian ditolak jika leluhur bersama berada di bawah ketinggian ujung saat ini dikurangi 100. Ini aturan implementasi yang berjalan, bukan janji universal yang melekat pada penghitung. [Panduan waktu transfer](https://seineminer.com/id/blog/blocknet-transfer-time/) membahas kapan dana biasa dapat dibelanjakan; [konfirmasi imbalan penambangan](/blog/id/block-confirmations-mining-rewards/) membahas pematangan coinbase dan pembukuan pool.

## Bagaimana pembayaran privat diperiksa terhadap pembelanjaan berulang?

Format transaksi Blocknet yang dirilis menggunakan key image yang terikat pada bukti pembelanjaan. Validasi memeriksa ikatan itu dan menolak key image yang sudah dibelanjakan dalam keadaan rantai terkait. Node dapat mendeteksi penggunaan output yang bertentangan, sementara tanda tangan cincin menyembunyikan anggota cincin sah mana yang menjadi input sebenarnya. Jumlah privat tetap menjalani pemeriksaan keseimbangan dan rentang.

Misalkan pemilik mengizinkan transaksi bertentangan yang membelanjakan output sama. Riwayat sah yang diterima tidak dapat memuat keduanya. Pemilihan rantai penting karena riwayat pesaing dapat memuat pilihan berbeda; validasi penting karena setiap riwayat harus menaati aturan pembelanjaan. Kerja lebih besar tidak menggantikan tanda tangan sah atau membuat transaksi tidak seimbang lolos pemeriksaan akuntansi.

Pemeriksaan ini terdapat pada [validator transaksi yang dirilis](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). Untuk mekanisme privasi lebih luas, baca [panduan privasi Blocknet](https://seineminer.com/id/blog/blocknet-privacy-features/). Keamanan pembayaran menggabungkan pembelanjaan sah, riwayat konsisten, dan kriptografi yang melindungi informasi yang terungkap di dalamnya.

## Apa peran penambangan pool dalam keamanan pembayaran?

Pool mengoordinasikan pencarian blok dan mengirim kandidat yang terselesaikan ke node hulunya. Share yang memenuhi target pool yang lebih mudah mencatat kontribusi kerja; hanya solusi yang memenuhi target jaringan dapat menjadi blok setelah validasi. [Penjelasan pool](/blog/id/what-is-a-mining-pool/) membedakan kedua target dan tujuannya.

Miner Anda dapat berkontribusi memperpanjang riwayat pembayaran sementara pool mencatat kerja Anda secara terpisah. Saldo privat pool, jumlah share, dan pembayaran dompet terkonfirmasi adalah hal berbeda. Mulailah dengan [panduan koneksi pool](/blog/id/mine-blocknet-on-bntpool/) untuk berpartisipasi, dan gunakan catatan transaksi dompet untuk melacak pembayaran BNT tertentu.

<CTA target="pool" title="Sumbangkan kerja untuk Blocknet" text="Hubungkan seine ke bntpool.com dan pantau kerja penambangan, imbalan, serta pembayaran melalui alat publik pool." />
