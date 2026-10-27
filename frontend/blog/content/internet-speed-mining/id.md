---
title: 'Apakah kecepatan internet memengaruhi mining Blocknet?'
description: 'Periksa koneksi penambangan Blocknet: bedakan bandwidth dan latensi, bandingkan Wi-Fi dengan Ethernet, lalu verifikasi sambungan ulang dan share diterima.'
tags: ['Panduan penambangan', 'Koneksi', 'Pemecahan masalah']
faq:
  - q: 'Apakah paket internet lebih cepat meningkatkan hashrate?'
    a: 'Penambang menghitung hash pada CPU atau GPU sendiri. Angka unduh yang diiklankan lebih tinggi tidak langsung mempercepat perhitungan. Kualitas koneksi memengaruhi penerimaan job dan pengiriman hasil. Selidiki kemacetan, gangguan, dan balasan pool sebelum mengaitkan perubahan hashrate lokal dengan paket internet.'
  - q: 'Bisakah menambang Blocknet lewat Wi-Fi?'
    a: 'Bisa, jika komunikasi dengan pool stabil. Amati share diterima dan pesan sambungan ulang dalam penggunaan normal. Jika terputus, perbandingan dengan Ethernet dapat mengisolasi bagian nirkabel. Pertahankan pengaturan penambang dan tujuan selama perbandingan.'
  - q: 'Apakah ping berhasil membuktikan koneksi pool berfungsi?'
    a: 'Ping memeriksa pertukaran ICMP dengan host yang menjawab. Sesi penambangan juga memerlukan endpoint, koneksi transport, login, job terkini, dan kiriman diterima. Situs atau ping bisa berfungsi sementara layanan lain tidak terjangkau. Periksa log penambang dan catatan pool.'
sourceHash: 'bf51814bc06d'
translation: draft
---

Periksa koneksi penambangan Blocknet dengan langkah yang dapat diulang: pisahkan hashing lokal dan komunikasi, catat gangguan, lalu ubah satu kondisi jaringan setiap kali. Panduan ini untuk penambang yang terhubung ke bntpool.com. Unduhan program dan sinkronisasi node penuh merupakan beban berbeda yang perlu diukur terpisah.

## Apakah koneksi lebih cepat mempercepat penambangan?

<Answer>
Bandwidth unduh lebih besar tidak langsung membuat CPU atau GPU menghitung lebih banyak hash. Koneksi pool membawa job, kiriman, dan balasan sehingga pengiriman stabil serta tepat waktu penting. Periksa job, sambungan ulang, dan share diterima sebelum mengganti paket. Bedakan hashrate lokal dari perkiraan kerja pool.
</Answer>

*Penambang bertukar job dan share dengan pool melalui jalur jaringan; jam menunjukkan waktu pengiriman.*

## Pisahkan bandwidth, latensi, dan hashrate lokal

Bandwidth menggambarkan kapasitas transfer, sedangkan latensi menggambarkan penundaan. Baca [referensi istilah jaringan](https://www.cloudflare.com/learning/performance/glossary/what-is-latency/) bila tes mencampurkan labelnya. Hasil unduh dan latensi menjawab pertanyaan berbeda. Tujuan tes juga bisa memakai rute lain dari pool; simpan konteks setiap pengukuran.

H/s penambang mengukur aktivitas perhitungan perangkat. Pool memperkirakan kerja dari share sepanjang waktu, dengan variasinya sendiri. Selisih singkat antartampilan bukan pengukuran langsung kehilangan jaringan. [Panduan hashrate](/blog/id/what-is-hashrate/) menjelaskan satuan dan jendela waktu. Kumpulkan hasil kiriman sebelum menjelaskan perbedaan.

## Apa yang dikirim antara penambang dan pool?

Penambang login, menerima job, menghitung secara lokal, lalu mengirim share yang memenuhi syarat. Tidak semua hash percobaan atau arena memorinya diunggah setiap kali. [Panduan Stratum](/blog/id/stratum-mining-protocol/) menguraikan pesan. Penerimaan job, pengiriman share, dan balasan pool adalah pengamatan terpisah meski memakai koneksi sama.

Jumlah pesan bergantung pada job, kesulitan share, kiriman, keepalive, dan sambungan ulang. Anggaran data bulanan universal tidak tepat tanpa mengukur sesi tertentu. Gunakan penghitung per perangkat dari sistem atau router selama periode representatif. Catat interval dan trafik lain; keluarkan unduhan serta sinkronisasi node dari perkiraan penambangan saja.

## Bandingkan Wi-Fi dan Ethernet tanpa mengubah penambang

Jika gangguan bertepatan dengan penggunaan nirkabel, bandingkan komputer yang sama memakai Ethernet bila tersedia. Pertahankan alamat pool, backend, thread, dan pekerjaan latar depan. Catat waktu awal serta akhir dan amati log koneksi. Mengubah setelan hardware bersamaan akan menyulitkan isolasi jalur jaringan.

Periksa apakah unggahan besar, pencadangan cloud, atau transfer rumah tangga bertepatan dengan balasan lambat. Jeda sebentar transfer yang Anda kendalikan, amati perubahan, lalu lanjutkan. Tes kecepatan juga menambah trafik, jadi catat waktunya. Biarkan perlindungan jaringan aktif dan hanya gunakan jaringan yang mengizinkan penambangan.

## Pengamatan apa menunjukkan masalah koneksi pool?

Ikuti koneksi endpoint, login, penerimaan job, kiriman, dan balasan. Jika login ditolak, periksa alamat serta pesan tepatnya sebelum menyalahkan Wi-Fi. Untuk putus berulang, simpan waktu dan baris log di sekitarnya. [Panduan pelacakan](/blog/id/track-your-miner-on-bntpool/) membahas endpoint dan hashrate nol; [halaman status](/status) memberi konteks layanan.

Setelah pulih, cari job baru yang dapat dipakai dan share baru diterima. Situs pulih atau ping berhasil tidak membuktikan sesi mining melewati tahap tersebut. Sebaliknya, galat peramban saja tidak membuktikan Stratum berhenti. Catat apa yang benar-benar diuji agar situs, transport penambang, dan balasan pool tetap terpisah.

## Bagaimana menafsirkan kiriman terlambat atau ditolak?

Penundaan bisa berpengaruh ketika pekerjaan berganti, tetapi share ditolak tidak otomatis membuktikan koneksi lambat. Cocokkan alasannya dengan urutan job dan sambungan ulang. Duplikat, kesulitan, dan validitas job memiliki sebab berbeda. [Panduan hasil share](/blog/id/accepted-rejected-stale-shares/) menjelaskan kategori serta informasi untuk galat berulang.

Masa sepi singkat tidak membuktikan paket hilang. Penemuan share bervariasi dan kesulitan menentukan frekuensinya. Bandingkan periode representatif dengan konfigurasi sama dan balasan aktual. Bila hashing lokal juga turun, selidiki CPU, GPU, dan memori terpisah; [panduan mining desktop](https://seineminer.com/id/blog/mine-while-using-pc/) membantu saat aplikasi lain berbagi komputer.

## Simpan catatan ringkas dan periksa perbaikan

Catat versi penambang, jenis koneksi, endpoint pool, perkiraan waktu, serta pesan sambungan ulang atau penolakan persisnya. Tambahkan perangkat lain yang terkena dan hasil perbandingan Ethernet atau jeda transfer. Lindungi data akun saat berbagi diagnosis. Ini lebih berguna daripada satu angka kecepatan tanpa waktu atau tujuan.

Sesudah satu perubahan terarah, pastikan pool dan alamat benar, job datang, serta share baru diterima. Bandingkan catatan pool setelah jendela tampilannya diperbarui. Pertahankan konfigurasi dengan pengiriman stabil di lingkungan normal. Tujuannya koneksi yang perilakunya dapat dijelaskan; ukur bandwidth untuk trafik nyata yang perlu dianggarkan.

<CTA target="pool" title="Ikuti kerja yang diterima di bntpool.com" text="Bandingkan pesan koneksi penambang dengan catatan pool saat mengevaluasi jaringan sehari-hari." />
