---
title: Menambang Blocknet langsung ke bursa
description: 'Tambang Blocknet langsung ke alamat deposit SafeTrade atau bursa lain di bntpool.com. Tambahkan memo deposit di seine dan pool membayar ke akun Anda.'
tags: [Panduan penambangan, bntpool, SafeTrade, Pembayaran]
faq:
  - q: 'Bisakah saya menambang Blocknet langsung ke bursa?'
    a: 'Ya. Jalankan seine v0.3.1 atau lebih baru dengan alamat deposit bursa dan memo deposit Anda, seperti ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com membayar imbalan Anda ke alamat itu dengan memo terlampir, sehingga bursa mengkreditkannya ke akun Anda.'
  - q: 'Mengapa seine meminta memo pembayaran?'
    a: 'Bursa seperti SafeTrade memakai satu alamat deposit bersama untuk semua pelanggan dan membedakan deposit dari memonya. seine mengenali alamat bersama ini, jadi saat Anda memasukkan salah satunya, seine meminta memo dan menyimpannya bersama alamat. Salin memo persis dari halaman deposit bursa.'
  - q: 'Berapa pembayaran minimum ke bursa di bntpool.com?'
    a: 'Pembayaran dengan memo dimulai dari 10 BNT, sebagaimana dilaporkan pool pada 4 November 2026. Saldo terkonfirmasi Anda terkumpul di pool sampai mencapai jumlah itu, lalu pool mengirimnya ke alamat bursa dengan memo Anda. Pembayaran ke wallet pribadi dimulai dari 1 BNT.'
  - q: 'Bagaimana memeriksa saldo penambangan ke bursa?'
    a: 'Di halaman statistik bntpool.com, masukkan alamat deposit bursa, pilih Tambahkan memo dan masukkan memo Anda. Setiap pasangan alamat dan memo memiliki saldo sendiri, jadi Anda hanya melihat hashrate, rig dan pembayaran Anda sendiri, bukan milik pelanggan lain yang memakai alamat bursa yang sama.'
sourceHash: 8f73aae25c7a
translation: draft
---

Panduan ini menunjukkan cara menambang Blocknet langsung ke akun bursa Anda di bntpool.com. Anda memberi seine alamat deposit bursa dan memo deposit Anda, lalu pool membayar imbalan Anda ke sana dengan memo terlampir, sehingga bursa mengkreditkannya kepada Anda. Panduan ini membahas perintahnya, pembayaran minimum dan cara memeriksa saldo.

## Bisakah menambang Blocknet langsung ke bursa?

<Answer>
Ya. bntpool.com membayar imbalan penambangan ke alamat deposit bursa dengan memo deposit Anda terlampir. Jalankan seine v0.3.1 atau lebih baru dengan alamat bursa dan memo Anda: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Pembayaran dengan memo dimulai dari 10 BNT, dan bursa mengkreditkan setiap pembayaran ke akun Anda.
</Answer>

Menambang ke bursa menghemat satu langkah jika Anda berencana memperdagangkan hasil tambang. Imbalan langsung masuk ke akun bursa, siap dijual di [pasar BNT/USDT](/blog/id/sell-blocknet-bnt-safetrade/), tanpa transfer wallet di antaranya.

Bursa seperti SafeTrade memberi semua pelanggan alamat deposit yang sama dan membedakan deposit dari memo, kode pendek yang unik untuk akun Anda. Itulah mengapa memo penting: memo menghubungkan pembayaran dengan Anda. Pool menyimpan saldo terpisah untuk setiap pasangan alamat dan memo, jadi imbalan Anda tidak pernah tercampur dengan milik penambang lain yang memakai bursa yang sama.

## Bagaimana menyiapkan penambangan ke bursa di seine?

<Answer>
Salin alamat deposit BNT dan memo deposit Anda dari halaman deposit bursa. Jalankan seine dengan keduanya: --address untuk alamat dan --payout-memo untuk memo. Saat Anda memasukkan alamat bursa yang dikenal pada permintaan saat pertama dijalankan, seine otomatis meminta memo dan menyimpannya. Periksa log untuk konfirmasi pool, lalu menambang seperti biasa.
</Answer>

1. **Ambil detail deposit.** Di bursa, buka halaman deposit BNT dan salin alamat deposit serta memonya. Salin memo dengan persis: huruf kapital dan angka nol di depan berpengaruh.
2. **Jalankan seine dengan keduanya.** Di macOS atau Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Di Windows, dalam PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Biarkan seine menyimpannya.** seine menyimpan memo bersama alamat, jadi penambangan berikutnya tetap dibayar ke akun bursa Anda. Jika Anda mengetik alamat bursa yang dikenal pada permintaan saat pertama dijalankan, seine meminta memo dan menyimpan keduanya.

[Halaman Get Started](/start) pool memiliki bagian “Menambang ke bursa?” yang menyusun perintah ini untuk Anda: tempel alamat dan memo, lalu salin hasilnya. seine memerlukan memo untuk alamat bursa bersama yang dikenal seperti milik SafeTrade. Layanan, skrip dan penambang tanpa pengawasan lainnya harus memberikan `--payout-memo` di baris perintah, karena tidak ada permintaan yang bisa dijawab.

seine memeriksa memo dengan pool saat login. bntpool.com mengonfirmasinya, lalu penambangan dimulai. Untuk memberi nama rig, gunakan `--pool-worker rig-01` seperti pada alamat pribadi.

## Bagaimana pembayaran ke bursa dilakukan?

Imbalan mengikuti jalur yang sama dengan pembayaran bntpool.com lainnya. Share Anda memperoleh bagian setiap blok yang ditemukan pool dalam jendela PPLNS enam jam, dan imbalan setiap blok terkonfirmasi setelah 60 blok. Saldo terkonfirmasi Anda kemudian terkumpul di pool.

Endpoint `/api/info` pool melaporkan pembayaran minimum berikut pada 4 November 2026:

| Tujuan | Pembayaran minimum |
|---|---|
| Alamat bursa dengan memo | 10 BNT |
| Wallet pribadi | 1 BNT |

Setelah saldo terkonfirmasi Anda mencapai 10 BNT, pool mengirimnya ke alamat bursa dengan memo Anda. Bursa lalu mengkreditkan deposit setelah jumlah konfirmasinya sendiri; halaman depositnya mencantumkan berapa yang diperlukan. [Cara kerja pembayaran PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan sisi pool secara rinci.

Penambangan solo juga bisa dengan memo. Tambahkan `solo:` di depan alamat bursa dan tetap gunakan `--payout-memo`, maka pool membayar seluruh imbalan setiap blok yang Anda temukan ke akun bursa Anda. [Penambangan solo di bntpool.com](/blog/id/solo-mine-blocknet-on-bntpool/) menjelaskan imbalan solo.

## Bagaimana memeriksa saldo Anda?

Buka [halaman statistik](/stats), tempel alamat deposit bursa, pilih **Tambahkan memo** dan masukkan memo Anda. Halaman itu lalu menampilkan hashrate, rig, saldo dan pembayaran Anda sendiri untuk pasangan alamat dan memo tersebut. Pelanggan lain di bursa yang sama memiliki pasangan dan saldo sendiri.

Setiap pembayaran di **Recent Payouts** menautkan ke transaksinya di explorer. Bandingkan dengan riwayat deposit di bursa untuk mengikuti koin hingga masuk ke akun Anda. [Pantau penambang Anda di bntpool.com](/blog/id/track-your-miner-on-bntpool/) menjelaskan bagian lain halaman statistik.

## Bagaimana kembali ke wallet pribadi?

Jalankan seine sekali dengan alamat baru Anda dan `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine menyimpan alamat baru tanpa memo, dan share baru dihitung untuk wallet pribadi Anda. Saldo yang sudah dikreditkan ke pasangan bursa tetap pada pasangan itu dan dibayar ke sana setelah mencapai minimum; Anda dapat memantaunya di halaman statistik. [Mengubah alamat wallet penambangan](/blog/id/change-mining-wallet-address/) menjelaskan cara pool mengkreditkan setelah alamat berubah.

## Pengaturan mana yang mendukung memo?

Memo memerlukan koneksi langsung seine ke pool, yang merupakan default. Mode daemon seine menambang pada node Anda sendiri dan membayar wallet node, jadi tidak memakai memo. Farm proxy tidak membawa memo, jadi hubungkan rig untuk bursa langsung ke pool. seine hanya menambang di pool yang mengonfirmasi memo Anda, jadi pekerjaan Anda selalu dibayar ke akun yang Anda pilih.

<CTA target="pool" title="Tambang ke akun bursa Anda" text="Jalankan seine dengan alamat deposit dan memo Anda, dan bntpool.com membayar imbalan langsung ke akun bursa Anda mulai 10 BNT." />
