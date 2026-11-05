---
title: Menambang Blocknet solo di bntpool.com
description: 'Tambang Blocknet solo tanpa menjalankan node. Tambahkan solo: di depan alamat di seine, dan bntpool.com membayar seluruh imbalan setiap blok yang Anda temukan.'
tags: [Panduan penambangan, Penambangan solo, bntpool, seine]
faq:
  - q: 'Bagaimana cara menambang Blocknet solo di bntpool.com?'
    a: 'Perbarui seine ke v0.3.1 atau lebih baru dan tambahkan solo: di depan alamat pembayaran, misalnya ./seine --address solo:YOUR_ADDRESS. seine terhubung ke bntpool.com seperti biasa, dan pool membayar Anda seluruh imbalan setiap blok yang ditemukan rig Anda melalui pembayaran normalnya. Anda tidak perlu menjalankan node.'
  - q: 'Perlukah node Blocknet sendiri untuk menambang solo?'
    a: 'Tidak. Dengan awalan solo:, bntpool.com menjalankan node, menyusun template blok dan mengirim blok yang Anda temukan. Rig Anda hanya menjalankan seine. Mode daemon seine adalah cara lain menambang solo, pada node tersinkron milik Anda, dan cocok bagi penambang yang sudah menjalankannya.'
  - q: 'Bisakah memakai nama Blocknet ID untuk menambang solo?'
    a: 'Ya. Gunakan solo:@name atau solo:$name sebagai pengganti solo:ADDRESS, dan beri tanda kutip di baris perintah agar shell meneruskannya tanpa perubahan. Pool mengubah nama itu menjadi alamat lengkapnya, lalu mengkreditkan blok solo Anda dan membayarnya ke alamat tersebut seperti pembayaran lain.'
  - q: 'Bagaimana kembali ke imbalan pool?'
    a: 'Hapus awalan solo: dari alamat dan mulai ulang seine. Rig Anda kembali berbagi imbalan PPLNS dari setiap blok yang ditemukan pool. Setiap rig memilih modenya saat terhubung, jadi Anda dapat menjalankan sebagian rig solo dan sebagian lain dengan imbalan bersama pada alamat yang sama.'
sourceHash: 623b1e0b356a
translation: draft
---

Panduan ini menunjukkan cara menambang Blocknet solo di bntpool.com: tambahkan satu awalan pada alamat di seine, dan pool membayar Anda seluruh imbalan setiap blok yang ditemukan rig Anda. Anda tidak perlu memasang atau menyinkronkan node. Anda akan menyiapkannya, melihat cara blok solo dibayar, dan mempelajari tempat memantaunya di pool.

## Apa itu penambangan solo di bntpool.com?

<Answer>
Penambangan solo di bntpool.com hanya membayar Anda untuk blok yang ditemukan rig Anda sendiri, dan setiap blok membayar seluruh imbalannya ke alamat Anda. Pool tetap menjalankan node, menyusun template blok dan mengirim blok Anda, jadi Anda mendapat imbalan solo dengan pengaturan yang sama seperti penambangan pool. Aktifkan dengan menambahkan solo: di depan alamat Anda.
</Answer>

Pada penambangan pool biasa, bntpool.com membagi setiap blok di antara penambang yang share-nya berada dalam jendela PPLNS enam jam, sehingga Anda mendapat bagian setiap blok yang ditemukan pool. Rig solo justru menyimpan seluruh imbalan bloknya sendiri dan tidak mendapat apa pun dari blok yang ditemukan orang lain. Keduanya memakai server, alamat dan pembayaran yang sama. Hanya login yang berubah.

Menambang solo melalui pool memberi imbalan solo tanpa repot mengurus node:

- **Tanpa node.** Node pool mengikuti blockchain, menyajikan template baru dan mengirim blok yang Anda pecahkan.
- **Pembayaran otomatis.** Blok Anda dibayar ke alamat atau nama Blocknet ID Anda seperti pembayaran bntpool.com lainnya.
- **Statistik langsung.** Rig, hashrate dan blok Anda tampil di [halaman statistik](/stats) pool.
- **Satu awalan untuk beralih.** Hapus `solo:` dan mulai ulang seine untuk kembali ke imbalan bersama.

Mode daemon seine adalah cara lain menambang solo, pada node Blocknet tersinkron milik Anda. [Penambangan pool versus solo untuk Blocknet](/blog/id/pool-vs-solo-mining-blocknet/) membandingkan ketiga pilihan dan menunjukkan seberapa sering hashrate Anda menemukan blok.

## Bagaimana mulai menambang solo dengan seine?

<Answer>
Pasang seine v0.3.1 atau lebih baru, lalu jalankan dengan solo: di depan alamat Blocknet Anda, misalnya ./seine --address solo:YOUR_ADDRESS di macOS atau Linux. seine terhubung ke bntpool.com, pool default-nya, dan dasbor memberi label Solo pada akun Anda. Anda juga dapat mengetik solo: dan alamat Anda saat seine meminta alamat pembayaran pada saat pertama dijalankan.
</Answer>

1. **Perbarui seine.** Login solo memerlukan seine v0.3.1 atau lebih baru. [Unduh seine](https://seineminer.com/id/#download), atau biarkan pembaruan otomatis memasangnya.
2. **Jalankan seine dengan awalan.** Di macOS atau Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Di Windows, dalam PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine menambang di bntpool.com secara default. [Halaman Get Started](/start) pool menampilkan perintah yang sama dengan alamat pool tertulis lengkap.
3. **Periksa dasbor.** Panel saldo memberi label **Solo** pada akun Anda, dan log menampilkan konfirmasi pool bahwa Anda dibayar untuk blok yang ditemukan alamat ini.

Awalan ini berlaku di mana pun seine menerima alamat: permintaan saat pertama dijalankan, konfigurasi tersimpan, API kontrol lokal, dan rig farm proxy. seine menyimpannya bersama alamat Anda, jadi penambangan berikutnya tetap solo.

## Bisakah menambang solo dengan nama Blocknet ID?

Ya. Nama Blocknet ID bekerja sama seperti alamat lengkap. Tulis `solo:@name` atau `solo:$name`, dan beri tanda kutip agar shell meneruskannya tanpa perubahan:

```sh
./seine --address 'solo:@yourname'
```

Pool mencari nama itu di blocknet.id dan membayar blok solo Anda ke alamat di baliknya. Untuk memberi nama rig, tambahkan namanya ke alamat lengkap setelah titik, seperti `solo:YOUR_ADDRESS.rig1`, atau gunakan `--pool-worker rig1` dengan alamat atau nama apa pun. Awalan menerima huruf besar maupun kecil, jadi `SOLO:` juga berfungsi.

## Bagaimana blok solo dibayar?

Saat rig Anda menemukan blok, pool mencatatnya sebagai blok solo. Setelah sedalam 60 blok, sekitar lima jam, pool mengkreditkan seluruh imbalan blok, dikurangi biaya pool, ke saldo Anda. Biayanya 0% per 4 November 2026. Saldo Anda kemudian dibayar ke alamat Anda melalui pembayaran normal pool.

Pembayaran mengikuti aturan yang sama dengan penambangan pool. Setiap blok menunggu 60 konfirmasi agar reorganisasi blockchain singkat tidak dapat membatalkan imbalan yang sudah dibayar, dan [konfirmasi blok dan imbalan penambangan](/blog/id/block-confirmations-mining-rewards/) menjelaskan alasannya. Endpoint `/api/info` pool melaporkan pembayaran minimum 1 BNT pada 4 November 2026, dan satu imbalan blok Blocknet jauh di atas itu, jadi blok solo yang terkonfirmasi dibayar pada jadwal pembayaran normal pool. [Halaman pembayaran](/payouts) mencantumkan setiap pembayaran yang dikirim pool.

Jika Anda menambang ke alamat deposit bursa dengan memo pembayaran, solo bekerja dengan cara yang sama. Memo dan pembayaran minimumnya sendiri berlaku untuk blok solo Anda sama seperti untuk imbalan bersama.

Pekerjaan rig Anda tetap dihitung dalam hashrate pool, effort ronde dan luck, dan blok solo mengakhiri ronde pool seperti blok lainnya. Share solo tidak pernah masuk jendela PPLNS, jadi penambang lain tidak mendapat apa pun dari blok Anda, dan Anda tidak mendapat apa pun dari blok mereka.

## Seberapa sering rig solo menemukan blok?

Blocknet menargetkan satu blok setiap lima menit, atau 288 blok sehari. Rata-rata, rig solo menemukan 288 × (hashrate Anda ÷ hashrate jaringan) blok setiap hari. Baca hashrate jaringan di [halaman depan bntpool.com](/) dan hashrate Anda di dasbor seine, lalu bagi satu hari dengan hasilnya untuk mendapatkan rata-rata waktu antarblok Anda.

Imbalan solo datang sebagai blok utuh, jadi cocok bagi penambang yang hashrate-nya sering menemukan blok: farm besar, armada server, dan siapa pun dengan bagian besar jaringan. Rig yang lebih kecil memperoleh jumlah rata-rata yang sama dengan imbalan PPLNS bersama, dibayar dalam bagian teratur dari setiap blok yang ditemukan pool. [Penambangan pool versus solo untuk Blocknet](/blog/id/pool-vs-solo-mining-blocknet/) membahas contoh pada tiga tingkat hashrate.

## Di mana melihat rig dan blok solo Anda?

Cari alamat Anda di [halaman statistik](/stats). Anda dapat menempelkannya dengan atau tanpa awalan `solo:`. Halaman itu lalu menandai pekerjaan solo Anda:

- **Workers** mencantumkan setiap rig dengan kolom Type yang bertuliskan **Solo** untuk rig yang mengirim share solo dalam jendela hashrate.
- **Estimated rewards** menandai blok solo Anda selama menunggu konfirmasi.
- **Blocks** dan **Luck** menampilkan kolom Type, Pool atau Solo, sebelum status setiap blok. [Halaman blok](/blocks) mencantumkan setiap blok yang ditemukan pool, terbaru dahulu.

[Pantau penambang Anda di bntpool.com](/blog/id/track-your-miner-on-bntpool/) menjelaskan bagian lain halaman statistik.

## Bisakah mencampur rig solo dan pool?

Ya. Setiap rig memilih modenya saat terhubung, jadi satu alamat dapat menjalankan rig solo dan rig pool sekaligus. Sebuah farm dapat menambang solo pada server terbesarnya dan menjaga desktop pada imbalan bersama, semuanya dibayar ke satu saldo. Pool memisahkan kedua jenis pekerjaan: share yang ditemukan pada koneksi solo hanya dihitung untuk blok solo alamat itu.

Untuk mengembalikan rig, hapus `solo:` dari alamatnya, di perintah atau konfigurasi tersimpan, lalu mulai ulang seine. Share yang sudah dikirim rig dalam mode solo tetap solo, dan share baru langsung masuk jendela PPLNS.

<CTA target="pool" title="Tambang solo di bntpool.com" text="Tambahkan solo: ke alamat Anda di seine dan simpan seluruh imbalan setiap blok yang ditemukan rig Anda, tanpa node yang perlu dijalankan." />
