---
title: 'Apa itu Stratum? Cara penambang berbicara ke pool'
description: 'Ikuti komunikasi Stratum antara seine dan bntpool.com: login, pekerjaan, target share, pengiriman dan balasan, serta cara membaca aktivitas koneksi penambang.'
tags: ['Dasar penambangan', 'Stratum', 'Penambangan pool']
faq:
  - q: 'Apa fungsi Stratum dalam penambangan pool?'
    a: 'Stratum membawa komunikasi antara penambang dan pool: mengenali sesi, mengirim pekerjaan, serta membawa share dan balasannya. Penambang menghitung hash secara lokal. Pool menghubungkan pekerjaan itu ke node upstream dan mengelola pencatatan yang dibutuhkan skema pembayarannya.'
  - q: 'Apakah alamat Stratum sama dengan alamat situs?'
    a: 'Keduanya melayani koneksi berbeda. Situs bntpool.com menggunakan HTTPS untuk halaman dan API; endpoint penambangan resminya adalah stratum+tcp://bntpool.com:3333. Masukkan endpoint itu ke pengaturan pool penambang. Berhasil membuka situs saja belum membentuk koneksi penambangan.'
  - q: 'Apakah share Stratum diterima berarti pembayaran sudah dikirim?'
    a: 'Tidak. Balasan share melaporkan hasil pengiriman dalam sesi. Verifikasi, blok valid di jaringan, kelayakan imbalan, dan pembayaran selesai merupakan tahap berbeda. Ikuti aktivitas diterima di My Stats dan baca panduan pembayaran untuk memahami konfirmasi, PPLNS, dan ambang pembayaran.'
sourceHash: '1dba87acd7c0'
translation: draft
---

Ikuti pekerjaan dari bntpool.com ke seine dan share yang kembali ke pool. Memahami percakapan ini memudahkan membaca pesan koneksi, pembaruan pekerjaan, dan balasan. Panduan ini menjelaskan implementasi bntpool.com; untuk endpoint dan alamat pembayaran awal, gunakan [panduan pengaturan pool](/blog/id/mine-blocknet-on-bntpool/).

## Apa itu Stratum dalam penambangan?

<Answer>
Stratum adalah lapisan komunikasi untuk menerima pekerjaan pool dan mengirim hasil. Pada seine dan bntpool.com, koneksi TCP persisten membawa pesan JSON untuk login, pekerjaan, dan share. Mesin Anda menghitung hash secara lokal, sedangkan pool mengambil templat blok dari node dan memproses pekerjaan yang dikirim.
</Answer>

*Penambang mengirim login dan share ke pool, menerima pekerjaan serta balasan, dan pool bertukar templat serta kandidat blok dengan node.*

## Endpoint menunjukkan koneksi penambangan

Endpoint bntpool.com yang didokumentasikan adalah `stratum+tcp://bntpool.com:3333`. Skema menunjukkan transport, nama host mengenali layanan, dan port memilih listener penambangan. Koneksi browser ke `https://bntpool.com` memiliki fungsi berbeda. Salin endpoint lengkap dari [Get Started](/start) ke pengaturan pool agar klien membuka koneksi yang benar.

Koneksi persisten memungkinkan kedua pihak mengirim pesan saat pekerjaan tersedia. bntpool memakai JSON berpisah baris dengan ID permintaan yang menghubungkan balasan. Detail ini menggambarkan format pool ini; penambang harus mendukung format, kapabilitas, dan algoritma hashing Blocknet. [Sumber protokol](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) mencatat field saat ini.

## Apa yang terjadi ketika penambang login?

Klien mengirim `login` berisi alamat pembayaran, nama rig, versi protokol, dan kapabilitas yang didukung. Pool memeriksa login lalu mengembalikan informasi protokol serta kapabilitas wajib. Ini menentukan pertukaran pekerjaan sesi. Koneksi TCP berhasil adalah langkah pertama; login diterima menunjukkan komunikasi tingkat aplikasi juga dimulai.

Nama rig membantu membedakan aktivitas satu mesin di bawah sebuah alamat. Gunakan nama berbeda untuk beberapa rig dengan alamat pembayaran yang tetap benar. Login membutuhkan alamat penerima, bukan frasa pemulihan atau kunci belanja privat dompet. Setelah sesi diterima dan ada pekerjaan, pool mengirim pesan `job` agar hashing dimulai.

## Apa isi sebuah pekerjaan penambangan?

Pekerjaan mencakup ID penugasan dan templat, data header, tinggi blok, target share, target jaringan, serta rentang nonce. Nonce adalah nilai yang diubah saat hashing. Rentang memberi ruang pencarian terdefinisi bagi sesi. ID pekerjaan menghubungkan pengiriman berikutnya dengan pekerjaan dan kondisi yang semula diberikan.

Target share menentukan hasil yang dikirim sebagai bukti kerja. Target jaringan menentukan hasil yang dapat menjadi kandidat blok. Sebagian besar share memenuhi pencatatan pool tanpa menjadi blok. [Panduan kesulitan](/blog/id/mining-difficulty-explained/) menjelaskan ukuran target dan perkiraan kerja; protokol memasok target itu sebagai bagian penugasan.

## Pekerjaan dapat berubah saat koneksi tetap terbuka

Pekerjaan baru tidak otomatis berarti koneksi terputus. Pool dapat memperbarui pekerjaan ketika templat upstream berubah atau mengirim penugasan dengan kesulitan share baru. Sistem kesulitan variabel menyesuaikan ambang pengiriman dengan aliran kerja teramati. Penambang mengikuti penugasan baru sementara koneksi terus membawa pesan dua arah.

Bedakan ID pekerjaan dan ID templat dalam log teknis. Beberapa penugasan dapat merujuk templat sama dengan detail sesi berbeda. Tinggi blok saja tidak menjelaskan semua perubahan. Bagi operator, periksa pekerjaan masuk, hashing berjalan, dan pengiriman dibalas; [panduan pelacakan](/blog/id/track-your-miner-on-bntpool/) menunjukkan aktivitas hasilnya.

## Apa isi pengiriman share?

Saat menemukan hasil yang memenuhi syarat, seine mengirim `submit` dengan pekerjaan, nonce, dan hash yang diklaim. Pool memproses dalam konteks penugasan lalu membalas sesuai ID permintaan. Klaim merupakan bagian pesan; verifikasi dan validasi blok menentukan tahap lanjut. Kandidat blok dikirim upstream ke daemon untuk validasi.

Balasan dapat memuat penerimaan, status verifikasi, dan informasi kesulitan. Baca field secara terpisah, jangan menganggap setiap balasan berhasil sebagai penemuan blok. Pencatatan dan pembayaran berada setelah pertukaran ini. [Panduan hasil share](/blog/id/accepted-rejected-stale-shares/) menjelaskan hasil pengiriman; [panduan PPLNS](/blog/id/how-pplns-payouts-work/) menghubungkan pekerjaan yang memenuhi syarat dengan imbalan dan pembayaran.

## Bagaimana urutan ini membantu memeriksa penambang?

Baca tahap berurutan: endpoint terjangkau, login diterima, pekerjaan diterima, hashing aktif, dan pengiriman dibalas. Jika koneksi hilang, klien menyambung kembali dan membentuk sesi sebelum menerima pekerjaan baru. Gunakan pesan log tepat beserta waktu untuk menentukan tahapnya. Situs yang dapat dibuka tidak membuktikan port penambangan terjangkau dari mesin itu.

Buka [My Stats](/stats) untuk alamat tujuan dan [status pool](/status) untuk kondisi layanan. Setelah mulai atau menyambung ulang, beri waktu estimasi hashrate berbasis share mengumpulkan pengamatan. Pisahkan komunikasi protokol dari jadwal pembayaran: login menyiapkan sesi, job memberikan pekerjaan, share melaporkan hasil, dan pencatatan menentukan tahap imbalan berikutnya.

<CTA target="pool" title="Hubungkan seine ke bntpool.com" text="Gunakan halaman pengaturan pool, lalu ikuti pekerjaan, share, dan aktivitas rig di My Stats." />
