---
title: Sinkronkan node Blocknet cepat dengan checkpoint bntpool.com
description: Node Blocknet melakukan sinkronisasi cepat dari checkpoints.dat bntpool.com. Pelajari penguncian blockchain, lalu unduh, verifikasi dan muat file.
tags: [Node Blocknet, Checkpoint, Sinkronisasi cepat, bntpool]
faq:
  - q: 'Dari mana node Blocknet memperoleh checkpoint?'
    a: 'Dari bntpool.com. Saat pertama mulai, jika direktori data tidak berisi checkpoints.dat, daemon Blocknet mengunduhnya dari https://bntpool.com/checkpoints.dat untuk sinkronisasi cepat. Anda juga dapat mengunduh sendiri lewat halaman Checkpoints, memeriksa SHA-256 dan menaruhnya di direktori data sebelum menjalankan node.'
  - q: 'Seberapa sering bntpool.com memperbarui checkpoints.dat?'
    a: 'Daemon pool menulis ulang file seiring pertumbuhan blockchain, dengan satu entri setiap 100 blok. Setiap entri setidaknya 100 blok di bawah tip, melampaui kedalaman reorganisasi maksimum Blocknet, sehingga checkpoint hanya mencakup riwayat yang sudah menetap. Halaman Checkpoints menampilkan ketinggian terbaru dan waktu perubahan file terakhir.'
  - q: 'Apakah checkpoint mengubah saldo atau riwayat wallet?'
    a: 'Tidak. Node yang sinkron dengan checkpoint tetap menyimpan setiap blok dan mencatat semua output serta key image dalam indeksnya, sehingga pemindaian wallet dan saldo bekerja sama seperti setelah sinkronisasi penuh. Checkpoint hanya memungkinkan node melewati pemeriksaan ulang proof of work dan tanda tangan untuk riwayat yang sudah dijamin hash checkpoint.'
  - q: 'Bisakah node Blocknet disinkronkan tanpa checkpoint?'
    a: 'Ya. Jalankan daemon dengan flag --full-sync, atau atur full_sync menjadi true dalam konfigurasi blocknet. Node melewati pengunduhan checkpoint dan memverifikasi ulang proof of work serta tanda tangan setiap blok sejak genesis. Ini lebih lama daripada sinkronisasi checkpoint, sehingga checkpoint menjadi default bagi node baru.'
sourceHash: 7a3491743f7d
translation: draft
---

Node Blocknet baru harus mengejar semua blok sejak genesis sebelum dapat melayani wallet atau penambang solo. Checkpoint mempercepat proses itu. Panduan ini menjelaskan isi file checkpoint bntpool.com, bagaimana daemon Blocknet menggunakannya untuk melewati pemeriksaan paling lambat sambil tetap terikat pada blockchain sebenarnya, serta cara mengunduh, memverifikasi dan memuatnya sendiri.

## Apa itu checkpoint Blocknet?

<Answer>
Checkpoint Blocknet adalah daftar hash blok yang diketahui, satu setiap 100 blok, disediakan bntpool.com sebagai checkpoints.dat. Node yang sinkron memeriksa kecocokan blok dengan hash tersebut dan melewati verifikasi ulang proof of work serta tanda tangan hingga checkpoint terakhir, sehingga sinkronisasi pertama jauh lebih cepat.
</Answer>

File berupa teks biasa dengan satu pasangan `height:hash` per baris. Baris pertama file pada 25 Oktober 2026 tampak seperti ini:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Setiap hash adalah ID blok, yaitu hash SHA3-256 header-nya. Daemon pool sendiri menulis file dari blockchain miliknya dan hanya menyertakan blok sedalam setidaknya 100 blok. Seratus blok adalah hard finality Blocknet, reorganisasi terdalam yang diizinkan protokol, sehingga setiap checkpoint berada dalam riwayat yang sudah menetap.

Pada 25 Oktober 2026 pukul 02.04 EDT, file memuat 689 checkpoint dari ketinggian 100 sampai 68.900 dalam 48.811 bytes. File bertambah satu baris kira-kira setiap 100 blok, sekitar delapan jam pada blok lima menit, jadi lihat [halaman Checkpoints](/checkpoints) untuk ketinggian terkini.

## Bagaimana node Blocknet menggunakan checkpoint?

Node yang memuat checkpoint menjadikan ketinggian checkpoint terakhir sebagai target sinkronisasi cepat. Sampai blockchain mencapai ketinggian itu, node bergantung pada checkpoint publik untuk dua pemeriksaan paling mahal dan menjalankan semua pemeriksaan lain seperti biasa:

- **Proof of work bergantung pada checkpoint.** Proof of work setiap blok Blocknet berupa hash Argon2id atas 2 GiB memori. Menghitung ulang untuk setiap blok historis mahal, sedangkan hash checkpoint sudah menjamin riwayat tersebut.
- **Tanda tangan RingCT dan range proof juga bergantung padanya.** Komentar daemon menyebut verifikasi range proof output sebagai biaya tunggal terbesar sinkronisasi, karena setiap output memilikinya, termasuk coinbase tiap blok.
- **Semua pemeriksaan lain tetap berjalan.** Keterkaitan blok, aturan kesulitan LWMA, timestamp, ukuran dan bentuk blok, pemeriksaan double spend key image, serta pemeriksaan pengikatan tiap input dengan tanda tangannya tetap normal. Output dan key image masuk indeks seperti biasa, sehingga pemindaian wallet dan saldo sama seperti setelah sinkronisasi penuh.

Pelewatan hanya berlaku pada blok yang memperpanjang tip node saat ini dan hanya di bawah checkpoint terakhir. Setelah melewati ketinggian itu, node memverifikasi penuh setiap blok baru termasuk proof of work.

### Bagaimana checkpoint mengikat blockchain tepercaya

Saat node mengejar checkpoint terakhir, setiap blok pada ketinggian checkpoint harus memiliki hash persis seperti file. Hash berbeda ditolak dengan kesalahan checkpoint mismatch. Karena checkpoint ada setiap 100 blok, peer tidak dapat membawa node fast-sync ke riwayat berbeda: ketinggian checkpoint berikutnya akan mengungkapnya.

Checkpoint juga mempercepat setiap restart. Saat daemon mulai dengan blok yang sudah ada di disk, ia mencari checkpoint tertinggi yang cocok dengan blockchain tersimpan dan memeriksa ulang hanya blok setelahnya, bukan menelusuri seluruh blockchain dari genesis.

## Apakah node baru mengunduh checkpoint otomatis?

<Answer>
Ya. Saat daemon Blocknet mulai dan direktori data tidak memiliki checkpoints.dat, daemon mengunduh dari https://bntpool.com/checkpoints.dat, memuatnya dan melakukan sinkronisasi cepat hingga checkpoint terakhir. Unduhan bersifat best effort: jika gagal selesai, node tetap mulai dan memverifikasi seluruh blockchain.
</Answer>

Alamat itu adalah default dalam Blocknet core sendiri, sehingga setiap node baru langsung melakukan sinkronisasi cepat dari file bntpool.com. Daemon hanya mengambil file jika belum ada, memberi waktu permintaan 15 detik, dan membatasi unduhan 32 MiB.

## Bagaimana menggunakan checkpoint bntpool.com langkah demi langkah?

Jika ingin mengambil dan memeriksa file sendiri, atau memperbarui salinan lama sebelum sinkronisasi ulang panjang, lakukan langkah manual berikut.

### 1. Temukan direktori data

File checkpoint berada dalam direktori data node, di samping database blockchain:

- **Pengelola `blocknet`:** `~/.config/bnt/data/mainnet/` pada macOS dan Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` pada Windows, kecuali Anda mengatur `data_dir` dalam konfigurasinya.
- **Core dijalankan langsung:** `./blocknet-data-mainnet` dalam folder tempat menjalankannya, atau lokasi yang diberikan lewat `--data`.

### 2. Unduh checkpoints.dat

Buka [Checkpoints](/checkpoints) lalu tekan **Download checkpoints.dat**, atau ambil dari terminal. Contoh ini menulis ke direktori data default core:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Mengunduh ke file yang sudah ada akan menggantinya, begitulah memperbarui salinan lama.

### 3. Periksa SHA-256

Halaman Checkpoints memublikasikan SHA-256 file terkini. Hitung hash salinan Anda dan bandingkan:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

Pada macOS gunakan `shasum -a 256`. Hash berubah setiap pool menambah checkpoint, jadi bandingkan dengan halaman segera setelah mengunduh.

### 4. Periksa sampel checkpoint di explorer

Anda dapat mencocokkan baris mana pun dengan [Blocknet explorer](https://explorer.blocknetcrypto.com) publik. Buka halaman blok pada ketinggian dalam file, misalnya `explorer.blocknetcrypto.com/block/68900`, dan bandingkan hash dengan baris 68.900. Kami melakukannya untuk checkpoint terakhir pada 25 Oktober 2026 dan hashnya cocok.

### 5. Jalankan node

Mulai atau mulai ulang daemon. Program membaca `checkpoints.dat` saat mulai dan sinkron cepat hingga checkpoint terakhir. Jika menjalankan core dengan shell interaktif (`--cli`), perintah `load-checkpoints` memuat file ke node berjalan dan melaporkan ketinggian fast-sync:

```text
> load-checkpoints
```

Checkpoint membantu saat node tertinggal. Setelah blockchain melewati checkpoint terakhir, node sudah memverifikasi penuh setiap blok dan file lebih baru tidak mengubah apa pun.

## Bagaimana memeriksa file checkpoint dari skrip?

Pool memublikasikan detail file sebagai JSON di `https://bntpool.com/api/checkpoints`, tanpa API key:

| Field | Arti |
|---|---|
| `available` | Apakah pool memiliki file untuk disajikan |
| `url` | Lokasi mengunduh |
| `entries` | Jumlah checkpoint dalam file |
| `latest_height`, `latest_hash` | Checkpoint tertinggi dan hash bloknya |
| `bytes`, `sha256` | Ukuran file dan SHA-256 |
| `updated_at` | Waktu perubahan terakhir sebagai Unix timestamp |

Unduhan membawa header `X-Checkpoint-Height` dan `X-Checkpoint-Sha256` serta `ETag` dari hash, sehingga skrip dapat mengirim `If-None-Match` dan menerima `304 Not Modified` dengan cepat jika tidak berubah:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Respons dapat di-cache selama 60 detik, cukup untuk file yang berubah sekitar delapan jam sekali.

<Note>
blocknetcrypto.com menyatakan peningkatan Faraday yang direncanakan akan menghadirkan proof of work lebih ringan agar node baru mengejar blockchain lebih cepat. Sampai saat itu, checkpoint adalah cara cepat memperbarui node.
</Note>

## Apa yang dapat dilakukan dengan node tersinkron?

Node tersinkron mendukung wallet Blocknet dan API node, serta dihubungi seine saat menambang solo dengan `--mode daemon`. [Penambangan pool versus solo untuk Blocknet](/blog/id/pool-vs-solo-mining-blocknet/) menunjukkan cara menilai kecocokan solo dengan perangkat Anda. Jika ingin menambang tanpa menjalankan node, seine menambang di bntpool.com secara default: [panduan pemasangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) membawa Anda ke share pertama, dan [menambang di bntpool.com](/blog/id/mine-blocknet-on-bntpool/) membahas sisi pool.

<CTA target="pool" title="Tambang di pool penyedia checkpoint Blocknet" text="Bergabunglah ke bntpool.com dalam beberapa menit dan pantau hashrate, share serta pembayaran secara langsung." />
