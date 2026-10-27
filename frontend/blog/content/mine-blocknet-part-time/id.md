---
title: 'Bisakah menambang Blocknet paruh waktu?'
description: 'Tambang Blocknet dalam sesi pendek: ikuti share dalam jendela PPLNS enam jam, kembali dengan alamat sama, dan pantau saldo yang belum dibayar.'
tags: ['Panduan mining', 'PPLNS', 'Jadwal']
faq:
  - q: 'Haruskah menambang enam jam sebelum share dihitung?'
    a: 'Tidak. Share yang memenuhi syarat dapat masuk jendela imbalan blok tanpa menunggu enam jam. Bagian Anda bergantung pada pekerjaan berbobot kesulitan dibandingkan total pool dalam jendela itu, dan blok harus ditemukan sebelum imbalannya dialokasikan.'
  - q: 'Haruskah miner tetap terhubung untuk menerima pembayaran yang sudah ada?'
    a: 'Koneksi mining aktif bukan syarat pembayaran. Saldo terkonfirmasi diproses menurut aturan pool, termasuk ambang minimum saat ini. Memutus koneksi tidak menghapus saldo dan tidak dengan sendirinya meminta pembayaran segera.'
  - q: 'Apakah menyambung kembali mengulang usia share lama?'
    a: 'Tidak. Share lama mempertahankan waktu pencatatannya. Pekerjaan baru masuk jendela terkait setelah Anda kembali; koneksi ulang tidak memperbarui share lama atau menambahkan pekerjaan selama offline. Periksa share diterima yang baru setelah restart.'
sourceHash: '0a11c393062e'
translation: draft
---

Anda dapat menjalankan miner Blocknet pada jam tertentu dan kembali nanti. Bedakan koneksi saat ini, share yang telah dikirim, dan imbalan yang sudah dialokasikan. Panduan ini mengikuti sesi pendek agar Anda dapat berhenti, memulai lagi, dan memeriksa akun bntpool.com dengan ekspektasi jelas.

## Bisakah sesi pendek mendapat imbalan pool?

<Answer>
Ya. Pekerjaan yang memenuhi syarat dari sesi pendek dapat ikut dalam blok yang jendela PPLNS-nya mencakup share tersebut. Tidak perlu menyelesaikan enam jam terlebih dahulu. Imbalan bergantung pada blok yang ditemukan dan bagian pekerjaan berbobot Anda. Saat berhenti, tidak ada pekerjaan baru; share menua normal dan saldo mengikuti aturan pembayaran.
</Answer>

*Garis waktu menunjukkan sesi mining, jeda offline, dan kembali, dengan penanda terpisah untuk kelayakan share dan saldo yang sudah ada.*

## Bedakan waktu yang sedang Anda lihat

bntpool saat ini menerbitkan jendela PPLNS enam jam, 60 blok sebelum kelayakan pembayaran, dan minimum 0,1 BNT di [halaman mulai](/start). Ketiganya adalah pekerjaan yang dihitung, pematangan blok, dan ambang saldo. Jendela enam jam bukan penghitung waktu yang menjanjikan pembayaran setelah enam jam.

[Panduan PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan rumus alokasi. Share yang memenuhi syarat dibobot menurut kesulitan terhadap seluruh pekerjaan yang memenuhi syarat dalam jendela sama. Menit online tidak menetapkan persentase imbalan berikutnya. Perangkat, partisipasi pool, dan hasil share dapat mengubah bobot walaupun durasi sesi sama.

## Ikuti satu share selama jeda

Contoh waktu lokal: Anda menambang dari 18:00 sampai 20:00, dengan share memenuhi syarat tercatat 19:00. Blok dengan akhir jendela 22:00 melihat enam jam ke belakang hingga 16:00. Share itu tercakup walaupun miner sudah offline. Ini ilustrasi kelayakan, bukan prediksi bahwa pool menemukan blok pada jam itu.

Jendela yang berakhir 02:00 esok hari dimulai 20:00, sehingga share 19:00 berada di luar. Kembali 08:00 tidak mengubah waktunya. Pekerjaan baru berkontribusi pada jendela selanjutnya. [Panduan keberuntungan](/blog/id/mining-pool-luck-explained/) menjelaskan variasi kedatangan blok; jangan menjadwalkan berdasarkan anggapan bahwa blok sudah waktunya muncul.

## Berhenti dengan catatan yang mudah dikenali

Sebelum jeda, catat alamat penerima, nama rig, perkiraan waktu berhenti, dan pesan share diterima terakhir. Periksa akun melalui [panduan pemantauan](/blog/id/track-your-miner-on-bntpool/). Catatan ini membedakan rig berhenti dari alamat berbeda atau tampilan yang belum diperbarui.

Keluar normal dari seine dengan `q` di dashboard atau `Ctrl+C` di terminal. Simpan perintah dan konfigurasi yang bekerja. [Panduan penggunaan PC](https://seineminer.com/id/blog/mine-while-using-pc/) membahas pembagian dengan tugas lain; berhenti normal tidak memerlukan penghapusan pengaturan, perubahan alamat dompet, atau penutupan akun pool.

## Apa yang terjadi pada saldo belum dibayar saat offline?

Share yang keluar dari jendela blok berikutnya tidak menghapus imbalan yang sudah dialokasikan untuk blok sebelumnya. Estimasi tetap mengikuti status dan pematangan blok; saldo terkonfirmasi yang belum dibayar tetap di akun hingga diproses. [Panduan konfirmasi](/blog/id/block-confirmations-mining-rewards/) membedakan kredit sementara dan saldo matang.

Saldo terkonfirmasi di bawah 0,1 BNT tetap di bawah ambang otomatis saat ini sampai kredit tambahan mencapai minimum. Koneksi ulang tidak menaikkan saldo atau memaksa pembayaran. Sebaliknya, saldo layak tidak membutuhkan miner online saat diproses. [Riwayat pembayaran](/payouts) membedakan saldo belum dibayar dari transaksi yang sudah dikirim.

## Kembali ke akun sama dan periksa pekerjaan baru

Mulai perintah tersimpan dan pastikan pool, alamat, serta nama rig sebelum membandingkan statistik. Alamat mengidentifikasi akun yang Anda pantau. Jika sengaja mengganti tujuan, ikuti [panduan perubahan alamat](/blog/id/change-mining-wallet-address/); saldo akun lama tidak otomatis dipindahkan ke akun baru.

Cari job saat ini dan share diterima yang baru, lalu beri waktu jendela pengukuran pool memperbarui tampilan. Rata-rata sesi yang baru dimulai dapat berbeda dari estimasi bergulir pool. [Panduan hashrate](/blog/id/what-is-hashrate/) menjelaskan interval tersebut. Periksa pekerjaan nyata dan akun yang benar sebelum menganggap selisih singkat sebagai imbalan hilang.

## Atur sesi sesuai rutinitas

Pilih jam menurut penggunaan komputer dan anggaran listrik. [Panduan listrik](https://seineminer.com/id/blog/mining-electricity-use/) menghitung konsumsi jadwal sebenarnya, termasuk tarif berbeda bila relevan. Sesi lebih pendek mengurangi waktu operasi, tetapi energi tepatnya tetap bergantung pada perangkat dan beban yang diukur.

Catat mulai, berhenti, pekerjaan diterima, dan saldo selama beberapa sesi. [Kalkulator imbalan](/blog/id/estimate-blocknet-mining-rewards/) memberi ekspektasi dengan asumsi tertentu, bukan janji untuk satu malam. Nilai periode representatif dan pisahkan koneksi, kelayakan jendela share, pematangan blok, dan pembayaran terkirim.

<CTA target="pool" title="Pantau sesi di bntpool.com" text="Gunakan alamat penerima yang sama saat kembali, periksa share baru, dan ikuti tahap pembayaran saldo yang normal." />
