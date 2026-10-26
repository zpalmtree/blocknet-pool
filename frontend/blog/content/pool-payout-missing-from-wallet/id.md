---
title: 'Pembayaran terkirim, tetapi belum ada di wallet?'
description: 'Telusuri pembayaran Blocknet melalui transaksi, alamat penerima dan sinkronisasi wallet, lalu kumpulkan data untuk memeriksa entri yang belum terlihat.'
tags: ['Pembayaran', 'Wallet', 'Pemecahan masalah']
faq:
  - q: 'Apakah pembayaran dalam antrean berarti sudah diterima?'
    a: 'Antrean adalah status pemrosesan di sisi pool. Periksa dahulu apakah catatan memiliki ID transaksi dan apa status terkininya. Catatan pembayaran, masuknya transaksi ke blockchain dan pengenalan output oleh wallet adalah tahap berbeda. Ikuti transaksi tertentu setelah tersedia.'
  - q: 'Bisakah explorer menampilkan saldo wallet privat saya?'
    a: 'Explorer membantu memeriksa status publik transaksi di blockchain. Blocknet menggunakan output privat, sehingga pencarian alamat biasa bukan tampilan saldo wallet atau bukti bahwa output tertentu milik Anda. Wallet memindai blockchain untuk mengenali outputnya sendiri.'
  - q: 'Data apa yang diperlukan untuk pembayaran yang belum terlihat?'
    a: 'Berikan alamat penerima lengkap untuk pencarian pool, ID transaksi, waktu perkiraan, status pool dan kemajuan sinkronisasi wallet. Jelaskan apakah transaksi terlihat dalam riwayat. Bagikan data akun hanya dalam konteks dukungan yang sesuai, jangan pernah memberikan seed phrase, private key atau kata sandi wallet.'
sourceHash: '915075cf4e2b'
translation: draft
---

Ikuti pembayaran Blocknet dari catatan pool ke wallet penerimanya. Panduan ini dimulai ketika Anda dapat mengenali entri pembayaran di bntpool.com. Jika hanya ada imbalan perkiraan atau terkonfirmasi yang menunggu dibayar, baca [panduan PPLNS](/blog/id/how-pplns-payouts-work/) dahulu; itu tahap lebih awal dengan aturan pembukuan dan pembayaran sendiri.

## Bagaimana menelusuri pembayaran yang belum muncul di wallet?

<Answer>
Temukan catatan untuk alamat penerima lengkap, catat ID transaksi serta status saat ini, lalu periksa statusnya di blockchain. Buka wallet untuk tujuan yang sama dan biarkan sinkronisasi serta pemindaian menyusul. Bandingkan riwayat transaksi sebelum memutuskan apakah pembayaran atau tampilan wallet lokal perlu diperiksa.
</Answer>

*Catatan pembayaran pool terhubung ke transaksi di blockchain, lalu ke wallet yang memindai output masuk miliknya.*

## Mulai dari satu catatan pembayaran

Buka My Stats dan cari alamat lengkap yang digunakan saat imbalan diperoleh. Catat jumlah, waktu, status dan ID transaksi jika ada. Gunakan entri itu, bukan total pembayaran sepanjang waktu yang dapat merangkum beberapa pembayaran. [Panduan pelacakan](/blog/id/track-your-miner-on-bntpool/) menjelaskan lokasi kolom tersebut.

Halaman publik Payouts menampilkan batch yang dapat memuat beberapa penerima dan tautan transaksi. Simpan catatan alamat Anda bersama transaksi yang diperiksa. Total batch bukan jumlah untuk wallet Anda. Jika ada beberapa ID transaksi, perluas daftar dan simpan ID terkait, bukan hanya label singkatnya.

## Apakah pembayaran mengantre, belum terkonfirmasi atau terkonfirmasi?

Baca status terkini sebelum menganggap pembayaran tercantum telah menyelesaikan seluruh tahap. Entri antrean masih diproses pool; transaksi belum terkonfirmasi masih membutuhkan konfirmasi blockchain. Segarkan catatan dan ikuti tautan transaksi ketika tersedia. [Halaman status](/status) memberi konteks operasional jika beberapa pengamatan mengarah pada gangguan layanan.

Pisahkan kematangan imbalan penambangan dari konfirmasi transaksi pembayaran. Blok yang menghasilkan imbalan dan transaksi berikutnya memiliki ID serta peran berbeda. [Panduan konfirmasi](/blog/id/block-confirmations-mining-rewards/) menjelaskan perbedaannya. ID transaksi memberi objek konkret untuk diperiksa, tetapi belum membuktikan wallet lokal sudah memindai dan mengenali pembayaran itu.

## Periksa transaksi di blockchain

Buka tautan explorer pada pembayaran dan bandingkan ID lengkap dengan catatan Anda. Baca status blockchain dan informasi blok jika tersedia. Jika explorer belum menemukannya, bandingkan catatan pool terbaru dan status layanan sebelum menyimpulkan. Hasil pencarian sementara saja tidak menentukan penyebabnya.

Privasi Blocknet berarti pencarian alamat biasa tidak menampilkan saldo publik penerima. Status transaksi di explorer dan pemeriksaan kepemilikan wallet menjawab pertanyaan berbeda. Wallet mengenali output masuk ketika memindai blockchain. [Panduan stealth address](https://seineminer.com/id/blog/stealth-addresses-explained/) menjelaskan prosesnya; daftar alamat publik tidak mereproduksi riwayat transaksi privat wallet.

## Apakah wallet penerima yang Anda buka sudah benar?

Bandingkan alamat pencarian pool dengan alamat penerima wallet yang terbuka. Gunakan nilai lengkap; awalan singkat yang mirip tidak cukup. Jika menggunakan Blocknet ID, simpan alamat lengkap untuk pekerjaan awal. Pencarian nama saat ini dapat menunjuk tempat lain setelah pemetaannya berubah, sementara pembayaran lama tetap terkait tujuan awal.

Periksa wallet lama yang dipakai sebelum mengubah pengaturan penambangan. Mengganti alamat miner atau pemetaan nama tidak memindahkan pembayaran yang sudah ditetapkan. [Panduan perubahan alamat](/blog/id/change-mining-wallet-address/) menjelaskan kedua riwayat. Pertahankan wallet asli dan cadangan, serta bedakan membuka wallet yang ada dari membuat wallet baru dengan alamat berbeda.

## Biarkan wallet menyinkronkan dan memindai

Pastikan node wallet mendekati blockchain terkini dan pemindaian wallet telah mencapai blok transaksi pembayaran. Sinkronisasi node dan pemindaian wallet adalah kemajuan terpisah. Wallet yang baru dibuka atau dipulihkan mungkin perlu waktu menemukan output meskipun explorer sudah menampilkan transaksi dalam blok.

Biarkan wallet tersambung dan ikuti alur sinkronisasi terdokumentasi. Bandingkan riwayat masuk serta saldo utama karena pengeluaran berikutnya dapat mengubah total. Jika entri ada, periksa konfirmasi atau ketersediaannya. Mulai dengan pengamatan ini sebelum pemulihan atau pemindaian penuh, dan simpan cadangan sebelum pemeliharaan. [Panduan wallet](https://seineminer.com/id/blog/blocknet-wallet-for-mining/) mencakup akses dan cadangan.

## Data apa yang membantu menjelaskan perbedaan tersisa?

Catat alamat pool lengkap, ID transaksi, perkiraan waktu dan status pool terkini. Tambahkan versi aplikasi wallet, kemajuan node, kemajuan pemindaian dan apakah transaksi masuk terlihat dalam riwayat. Sebutkan pemeriksaan yang selesai. Ini membedakan status transaksi, alamat tidak cocok dan wallet yang masih menyusul.

Gunakan jalur dukungan resmi proyek dan bagikan data akun hanya di tempat sesuai. Pemeriksaan transaksi tidak memerlukan frasa pemulihan, private key atau kata sandi. Periksa isi berkas diagnostik sebelum memublikasikan semuanya. Cocokkan catatan pembayaran, transaksi dan wallet yang sama; hashrate baru atau restart miner lagi tidak membuktikan penerimaan pembayaran lama.

<CTA target="pool" title="Ikuti pembayaran Anda di bntpool.com" text="Gunakan alamat penerima lengkap untuk melacak catatan dan bandingkan transaksinya dengan riwayat wallet yang telah tersinkron." />
