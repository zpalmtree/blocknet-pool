---
title: 'Biaya pool dan biaya pengembang miner'
description: 'Pahami biaya pool dan miner Blocknet: tahap penerapan, cara menggabungkan, dan cara menghindari pemotongan biaya yang sama dua kali.'
tags: ['Panduan mining', 'Biaya pool', 'Imbalan mining']
faq:
  - q: 'Apakah keduanya biaya yang sama?'
    a: 'Tidak. Biaya pool terkait pembagian imbalan, biaya pengembang terkait perangkat lunak. seine v0.2.36 mengalokasikan 2% waktu mining yang diperhitungkan setelah masa awal untuk pengembangan. Biaya terpisah bntpool adalah 0% saat diperiksa 26 Oktober 2026.'
  - q: 'Bagaimana menggabungkannya?'
    a: 'Kalikan perkiraan bruto dengan satu dikurangi fraksi miner, lalu satu dikurangi fraksi pool. Gunakan versi aktual dan pengaturan terkini. Ini memperkirakan imbalan tersisa, bukan jaminan pembayaran atau laba uang.'
  - q: 'Perlukah biaya miner dipotong dari hashrate pool?'
    a: 'Kenali angka tersebut dahulu. Jika sudah memperkirakan pekerjaan untuk alamat Anda setelah waktu pengembang, pemotongan lagi menghitungnya dua kali. Masukkan nol untuk penyesuaian yang sudah tercermin. Saldo terkonfirmasi dan pembayaran tercatat tidak perlu dipotong lagi.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Ikuti dua lapisan biaya dalam perkiraan mining Blocknet. Panduan memakai seine v0.2.36 dan pengaturan bntpool yang diperiksa 26 Oktober 2026 untuk membantu mengisi [kalkulator](/blog/id/estimate-blocknet-mining-rewards/#mining-calculator) dan membaca [My Stats](/stats).

## Apa perbedaan biaya pool dan miner?

<Answer>
Biaya pool bagian dari distribusi imbalan; biaya pengembang bagian dari jadwal mining perangkat lunak. Pengelola dan tahapnya berbeda. Periksa keduanya, tentukan apakah input bruto atau sudah disesuaikan, lalu terapkan setiap penyesuaian relevan sekali.
</Answer>

*Pekerjaan melalui penyesuaian waktu miner dan penyesuaian imbalan pool terpisah sebelum perkiraan jumlah tersisa masuk akun.*

## Biaya pool diterapkan pada apa?

Pool mengumpulkan pekerjaan yang memenuhi syarat dan membagikan imbalan sesuai aturan. bntpool menghitung biaya dari imbalan blok tercatat sebelum sisanya dialokasikan lewat PPLNS. [Panduan PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan partisipasi dan kredit.

Pada 26 Oktober 2026, `https://bntpool.com/api/info` menerbitkan `pool_fee_pct` sebesar 0,0, berarti 0%. Periksa nilai terkini di [Get Started](/start) atau API ketika memperbarui perkiraan. Nol hanya menjelaskan pengaturan pool ini; kebijakan software tetap input terpisah.

## Bagaimana biaya pengembang seine bekerja?

v0.2.36 mendokumentasikan 2% di semua pool dan mode solo. Biaya berupa waktu mining untuk alamat pengembang, bukan penarikan dompet kemudian. Sebanyak 10 menit pertama bebas biaya; pelacak menghitung waktu sesudahnya. Lihat [panduan konfigurasi](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md).

Penjadwalan memakai putaran, sehingga jendela singkat tidak harus tepat 2%. Persentase itu target jangka panjang setelah masa awal, bukan janji per menit atau blok. Catat versi karena panduan lama atau software lain bisa berbeda. Biaya pool tidak menggantikan informasi ini.

## Bagaimana menggabungkan persentase?

Gunakan fraksi tersisa. Dengan m sebagai fraksi miner dan p fraksi pool, perkiraan mempertahankan `(1 - m) * (1 - p)` dari bruto. Itulah rumus [estimator](/blog/id/estimate-blocknet-mining-rewards/). Menjumlahkan persentase bukan aturan umum karena faktor kedua berlaku pada sisa.

Contoh aritmetika murni: 100 BNT bruto hipotetis, penyesuaian miner 2% dan pool terverifikasi 0% menghasilkan 100 × 0,98 × 1,00 = 98 BNT. Ini contoh kondisi stabil, bukan pengukuran mesin, janji, atau simulasi sesi singkat dengan masa awal.

## Pilih input hashrate yang tepat

Angka lokal atau benchmark menggambarkan pekerjaan mesin. Sebelum faktor pengembang, tentukan apakah mencakup seluruh waktu atau hanya pekerjaan untuk alamat penerima. Periksa sumber dan interval. [Panduan hashrate](/blog/id/what-is-hashrate/) membedakan pengamatan lokal, pool, dan jaringan.

Hashrate pool untuk alamat Anda mungkin sudah mencerminkan waktu yang dikreditkan ke tempat lain. Jika sudah, masukkan nol untuk biaya miner tambahan pada kalkulator. Ini mengubah input hitungan, bukan kebijakan software. Biaya pool tetap berlaku saat memperkirakan imbalan bruto yang memenuhi syarat; periksa isi setiap angka.

## Mengapa jangan memotong saldo lagi?

Saldo terkonfirmasi adalah hasil pembukuan, bukan perkiraan bruto utuh. Pool sudah mengalokasikan kredit. Memotong biaya yang sama dari saldo atau pembayaran hanya mengubah angka tanpa kejadian biaya baru. Gunakan jumlah yang ditampilkan untuk rekonsiliasi dan identifikasi tahapnya.

Imbalan tertunda, saldo terkonfirmasi, dan pembayaran terkirim juga berbeda waktu. Kematangan blok dan ambang pembayaran menjelaskan perpindahan, bukan biaya persentase tambahan. Lihat [panduan konfirmasi](/blog/id/block-confirmations-mining-rewards/) dan [riwayat pembayaran](/payouts). Biaya transaksi dompet, bila ada, merupakan kolom terpisah.

## Simpan asal input perkiraan

Catat tanggal, versi, biaya, sumber hashrate, dan interval. Kalkulator menggabungkan data langsung pool/jaringan dengan input Anda; segarkan jaringan saat meninjau ulang. Kredit aktual mengikuti share layak, blok ditemukan, dan PPLNS. Rumus dan catatan akun menjawab pertanyaan berbeda.

Gunakan [My Stats](/stats) dan [panduan pemantauan](/blog/id/track-your-miner-on-bntpool/). Bandingkan alamat, periode, dan tahap yang sama sebelum mengubah biaya. Kalkulator memperkirakan BNT; listrik, hardware, dan harga uang di luar hitungan. Tidak perlu mengganti alamat dompet.

<CTA target="pool" title="Pantau imbalan di bntpool.com" text="Gunakan kalkulator untuk perkiraan dan My Stats untuk pekerjaan, saldo, serta pembayaran tercatat." />
