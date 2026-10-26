---
title: 'Mengganti alamat dompet penambangan Blocknet'
description: 'Ganti alamat penerima penambang Blocknet, periksa share baru di bntpool.com, dan pantau imbalan sebelumnya yang tetap terkait dengan alamat dompet lama.'
tags: ['Panduan penambangan', 'Dompet', 'Pembayaran']
faq:
  - q: 'Apakah mengganti alamat memindahkan saldo lama?'
    a: 'Tujuan baru mengarahkan pekerjaan berikutnya ke alamat baru. Share, kredit, dan pembayaran yang sudah ada tetap terkait alamat awal. Cari kedua alamat secara terpisah di Statistik Saya dan pertahankan akses dompet lama sambil memantau imbalan tersisa.'
  - q: 'Haruskah semua komputer diubah?'
    a: 'Ya. Setiap proses memakai tujuan konfigurasinya sendiri. Perbarui dan periksa satu komputer dahulu, kemudian yang lain beserta berkas dan argumen mulai. Nama rig berbeda membantu mengenali mesin yang sudah mengirim pekerjaan ke alamat baru.'
  - q: 'Apakah perubahan Blocknet ID memperbarui penambang aktif?'
    a: 'seine menyelesaikan ID tersimpan menjadi alamat saat mulai dan mempertahankannya selama sesi, termasuk koneksi ulang. Setelah mengubah pemetaan, mulai ulang setiap penambang dan periksa alamat lengkap. Perubahan tidak mengalihkan pekerjaan atau saldo yang tercatat pada alamat sebelumnya.'
sourceHash: 'fca912d46d55'
translation: draft
---

Ganti tujuan pekerjaan mendatang sambil mempertahankan catatan imbalan lama. Panduan ini memakai seine yang sudah terhubung ke bntpool.com. Untuk koneksi pertama, baca [panduan pool](/blog/id/mine-blocknet-on-bntpool/); untuk dompet penerima baru, baca [panduan dompet](https://seineminer.com/id/blog/blocknet-wallet-for-mining/).

## Bagaimana mengganti alamat penambangan?

<Answer>
Hentikan satu penambang, ubah alamat pada konfigurasi atau perintah biasa, lalu mulai ulang. Periksa alamat lengkap hasil resolusi dan share diterima, kemudian cari alamat itu di bntpool.com. Ulangi untuk rig lain. Share serta imbalan lama tetap terkait alamat yang dipakai saat dicatat.
</Answer>

*Pekerjaan baru menuju dompet kedua; catatan imbalan lama tetap terkait dompet pertama.*

## Catat kedua tujuan sebelum beralih

Salin alamat baru langsung dari dompet tujuan. Penambang membutuhkan alamat publik atau Blocknet ID terdaftar, tanpa frasa pemulihan maupun kunci privat. Simpan juga alamat lengkap lama: alamat itu tetap menjadi kunci pencarian aktivitas pool sebelumnya.

Buka Statistik Saya untuk alamat lama dan catat imbalan tertunda, saldo terkonfirmasi, pembayaran terbaru, serta rig aktif. Ini menjadi acuan transisi; halaman baru tidak memuat riwayat lama. Pertahankan akses dompet awal saat memantau sisa imbalan dan pembayaran.

## Perbarui satu penambang dan perintahnya

Hentikan penambang dan periksa cara mulai biasa. Pada konfigurasi yang ada, `--address` mengungguli alamat tersimpan tanpa menggantinya. Contoh PowerShell memilih tujuan sesi ini. Ganti `@yourname` dengan ID baru atau alamat lengkap dan pertahankan argumen pool, data, serta perangkat keras yang teruji.

```powershell
.\seine.exe --address "@yourname"
```

Pada Linux atau macOS, pakai sintaks berikut dari direktori biasa. Kedua contoh memulai penambangan nyata; pilih sesuai sistem Anda. Instalasi yang ada menyediakan setelan lain. Pertahankan nama rig agar share-nya dikenali pada alamat baru.

```sh
./seine --address "@yourname"
```

## Gunakan alamat baru pada awal normal berikutnya

Saat penambang berhenti, edit `address` di `seine.config.json`. Pertahankan bidang lain dan JSON valid. Periksa folder: `./seine-data` relatif terhadap direktori kerja, sementara `--data-dir` dapat memilih lokasi lain. Edit berkas yang benar-benar digunakan peluncur.

Perbarui atau hapus `--address` lama di pintasan, skrip, tugas, atau layanan. Perintah manual yang berhasil tidak mengubah peluncur lain. Mulai ulang secara normal dan periksa. [Panduan konfigurasi](https://seineminer.com/id/blog/configure-seine-miner/) menjelaskan prioritas serta pemilihan berkas berbeda saat direktori kerja berubah.

## Apa bedanya jika memakai Blocknet ID?

Pada seine v0.2.36, `@name` diresolusi dan divalidasi saat mulai. Alamat lengkap tetap selama sesi, termasuk koneksi pool ulang. Jika pemetaan nama berubah, mulai ulang setiap penambang dan periksa tujuan. Menyambungkan ulang proses lama tidak mengulangi resolusi awal.

Catat kedua alamat lengkap meski nama tetap sama. Akuntansi mengikuti alamat hasil resolusi; perubahan pemetaan tidak mengalihkan share atau saldo lama. Jika nama gagal diresolusi, periksa pemetaan atau pilih alamat lengkap yang sudah Anda verifikasi dari dompet.

## Periksa setiap komputer pada alamat baru

Periksa tujuan dan kiriman diterima, lalu cari alamat baru lengkap di Statistik Saya. Cocokkan nama rig dan share terbaru. Estimasi hashrate baru memerlukan pekerjaan dan waktu; pastikan kiriman tiba dahulu. [Panduan statistik](/blog/id/track-your-miner-on-bntpool/) menjelaskan penghitung dan rig yang belum muncul.

Setelah komputer pertama terkonfirmasi, ulangi pada lainnya. Nama berbeda memudahkan audit. Periksa pula mulai normal tiap mesin agar peluncur lama tidak memakai tujuan sebelumnya lagi. [Panduan banyak komputer](/blog/id/mine-multiple-computers-one-wallet/) menjelaskan penataan rig di bawah satu alamat.

## Bagaimana dengan imbalan alamat lama?

Kredit dan pembayaran tetap pada alamat lama. Share lama yang memenuhi syarat masih dapat berkontribusi dalam jendela PPLNS terkait; imbalan tertunda mengikuti konfirmasi. Hashrate nol tidak berarti akuntansi selesai. Panduan [PPLNS](/blog/id/how-pplns-payouts-work/) dan [konfirmasi](/blog/id/block-confirmations-mining-rewards/) membedakan pekerjaan, pematangan, dan pembayaran.

Pergantian tidak menggabungkan saldo atau melewati minimum pembayaran. Periksa aturan terkini dan kedua halaman sampai jumlah tersisa dipahami. Mengganti nama rig juga tidak memindahkan imbalan. Pastikan semua penambang yang dituju mengirim pekerjaan baru ke alamat baru, dan simpan dompet lama beserta riwayatnya.

<CTA target="pool" title="Periksa pengaturan di bntpool.com" text="Pastikan tujuan dan pekerjaan diterima tiap rig, lalu pantau imbalan lama dan baru secara terpisah." />
