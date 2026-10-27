---
title: 'Mining Blocknet dari beberapa komputer ke satu dompet'
description: 'Tambahkan miner Blocknet kedua ke dompet yang sama di bntpool.com. Beri nama tiap komputer, periksa share, dan pahami imbalan gabungan serta nama duplikat.'
tags: ['Panduan mining', 'Komputer', 'Pengaturan']
faq:
  - q: 'Apakah setiap komputer perlu dompet sendiri?'
    a: 'Tidak. Semua komputer dapat memakai alamat penerimaan yang sama di bntpool.com, dengan nama berbeda untuk mengenali tiap instalasi. Pool menghitung imbalan berdasarkan alamat. Mesin mining hanya memerlukan alamat tersebut atau Blocknet ID Anda, bukan frasa pemulihan dompet.'
  - q: 'Apa yang terjadi jika dua miner memakai nama sama?'
    a: 'Di bawah alamat yang sama, riwayat share keduanya dikelompokkan berdasarkan nama itu. Akibatnya mesin di balik suatu pembacaan lebih sulit dikenali. Berikan nama berbeda untuk tiap instalasi dan mulai ulang dengan nama itu; share lama mempertahankan label semula.'
  - q: 'Apakah mesin tambahan membagi pembayaran saya?'
    a: 'Nama mengatur tampilan aktivitas mining dan tidak membuat saldo terpisah di bawah alamat yang sama. Pekerjaan valid dari komputer Anda berkontribusi pada partisipasi PPLNS alamat tersebut. Saldo mengikuti aturan konfirmasi dan pembayaran normal pool.'
sourceHash: '1cfcb79502c0'
translation: draft
---

Tambahkan komputer kedua ke bntpool.com sambil mempertahankan satu dompet penerima. Beri nama tiap instalasi, verifikasi satu per satu, lalu pantau alamat gabungan. Panduan ini membahas penambahan mesin dan nama duplikat. Untuk miner pertama, selesaikan dahulu [panduan pengaturan pool](/blog/id/mine-blocknet-on-bntpool/).

## Bisakah beberapa komputer menambang ke dompet yang sama?

<Answer>
Bisa. Jalankan seine pada tiap komputer dengan alamat penerimaan sama dan nama mesin berbeda. bntpool.com mencatat pekerjaan serta imbalan berdasarkan alamat, sedangkan nama memisahkan aktivitas dalam statistik. Periksa share diterima dari tiap mesin sebelum mengandalkan tampilan gabungan alamat.
</Answer>

*Komputer terpisah mengirim pekerjaan melalui pool ke satu alamat penerimaan; masing-masing mempertahankan nama sendiri.*

## Pilih alamat penerimaan dan pola nama

Gunakan alamat penerimaan dompet atau Blocknet ID yang mengarah ke sana. Periksa alamat hasil resolusi pada tiap miner. Nama memudahkan masukan, tetapi pembukuan mengikuti alamat hasilnya. Jika tujuan ID berubah, proses mining yang sudah berjalan dapat mempertahankan resolusi lama sampai dimulai ulang. Imbalan yang sudah diperoleh tetap berada pada alamat asalnya.

[Panduan dompet mining](https://seineminer.com/id/blog/blocknet-wallet-for-mining/) menjelaskan cara memperoleh alamat. Mesin hanya memerlukan tujuan publik ini; simpan frasa pemulihan dan kunci privat bersama dompet. Pilih nama singkat seperti `desktop-01` dan `server-02`. Pool menghapus spasi di tepi dan menyimpan hingga 64 karakter. Taruh bagian pembeda di awal nama panjang.

## Tambahkan miner kedua dengan nama sendiri

Pasang rilis seine yang sesuai pada komputer kedua dan jalankan dari folder hasil ekstraksi. Contoh PowerShell pertama menetapkan nama penerima, pool, dan mesin secara eksplisit. Ganti `@yourname` dengan ID atau alamat lengkap Anda sebelum menjalankannya. Pertahankan tanda kutip dan tambahkan opsi backend atau penyetelan yang sudah diuji untuk mesin tersebut.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Di Linux atau macOS, gunakan program pada perintah berikut dengan nama mesin berbeda. Sistem operasi boleh berbeda; alamat penerimaan harus sama. Tiap contoh memulai mining sungguhan. Jalankan contoh yang sesuai pada komputer yang tepat dan tunggu startup serta penyetelan normal selesai. Tidak perlu menjalankan kedua contoh pada satu mesin.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Pengaturan apa yang dibagi antar komputer?

Bagikan alamat penerimaan dan endpoint pool. Pertahankan folder data lokal serta nama terpisah untuk tiap instalasi. seine menyimpan koneksi dalam `seine.config.json` di direktori data; lokasi bawaan `./seine-data` bergantung pada folder peluncuran. Folder berbeda dapat menemukan konfigurasi berbeda. Folder tetap memudahkan pemeliharaan.

Jika menyalin konfigurasi komputer pertama, tetapkan nama berbeda pada perintah peluncuran komputer kedua dan periksa saat startup. Pertahankan nama eksplisit itu pada perintah berikutnya: penggantian dari baris perintah tidak menulis ulang nama yang sudah tersimpan. Sesuaikan pilihan perangkat keras dan biarkan komputer menyetel secara lokal. Argumen alamat menentukan pembayaran dan argumen nama menentukan label; masukkan terpisah seperti contoh.

## Bagaimana memeriksa kontribusi kedua komputer?

Buka My Stats di bntpool.com untuk alamat bersama. Cari dua nama dan share terbaru dari masing-masing. Periksa juga kiriman diterima pada miner lokal. Lakukan satu komputer setiap kali untuk menghubungkan proses dengan baris pool; ini mempermudah menemukan kesalahan daripada hanya melihat total hashrate.

Pool memperkirakan hashrate dari share yang dikirim. Mesin baru membutuhkan waktu dan pekerjaan diterima agar pembacaannya bermakna. Laju lokal dan perkiraan pool memakai pengukuran serta jendela waktu berbeda. Pastikan aktivitas melalui share terbaru dan hitungan diterima, lalu bandingkan periode lebih panjang. [Panduan statistik](/blog/id/track-your-miner-on-bntpool/) menjelaskan kartu dan baris tanpa menyimpulkan jumlah perangkat dari total koneksi.

## Bagaimana jika mesin hilang atau namanya sama?

Nama sama pada alamat sama menggabungkan catatan share. Ubah nama satu miner melalui opsi namanya, lalu mulai ulang. Kiriman baru memakai label baru; catatan sebelumnya tetap memakai label lama. Baris lama dapat terlihat selama riwayatnya masuk jendela laporan. Mengganti nama tidak memindahkan saldo alamat yang telah diperoleh.

Untuk mesin yang tidak terlihat, bandingkan alamat hasil resolusi sebenarnya, endpoint, dan nama dengan halaman yang dibuka. Periksa koneksi serta share diterima. CPU dan GPU dalam satu proses seine tidak memerlukan nama terpisah pada alur ini. Untuk share ditolak atau kedaluwarsa, ikuti [panduan status share](/blog/id/accepted-rejected-stale-shares/) daripada terus mengganti alamat dompet.

## Bagaimana imbalan dan pembayaran digabungkan?

Pekerjaan valid dari komputer berkontribusi pada partisipasi alamat yang sama dalam jendela PPLNS. Nama membantu melacak kontribusi, bukan membuat saldo terpisah. Mesin tambahan menyumbang pekerjaan berguna yang benar-benar dikirim, bukan imbalan tetap per perangkat atau nama. Perangkat keras dan waktu aktif berbeda bisa menyumbang berbeda; menambah nama saja tidak menambah pekerjaan.

[Panduan PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan pembagian saat pool menemukan blok. Aturan konfirmasi dan pembayaran tetap berlaku pada saldo gabungan alamat. Tidak perlu menggabungkan dompet atau memindahkan saldo antar nama. Sebelum meninggalkan mesin berjalan, periksa tujuan, nama berbeda, dan share diterima pada tiap komputer. Gunakan daftar singkat itu untuk mesin berikutnya.

<CTA target="pool" title="Hubungkan komputer lain ke bntpool.com" text="Gunakan satu alamat penerimaan, beri tiap instalasi nama berbeda, dan periksa share yang diterima." />
