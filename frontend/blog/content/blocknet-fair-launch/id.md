---
title: 'Fair launch Blocknet: tanpa premine, setiap koin ditambang'
description: "Periksa genesis kosong Blocknet, imbalan pertama dan aturan penerbitannya. Pahami bagaimana BNT mulai beredar dan koin hasil tambang sampai ke pemilik lain."
tags: [Blocknet, Fair launch, Blok genesis, Penambangan, bntpool]
faq:
  - q: 'Apakah Blocknet memiliki premine?'
    a: 'Tidak. Blok genesis Blocknet, dibuat pada 15 Februari 2026, sama sekali tidak memiliki transaksi, sehingga tidak ada koin sebelum penambangan dimulai. Koin pertama berasal dari imbalan blok 1, dan setiap BNT sejak itu dibayar kepada penambang blok asalnya.'
  - q: 'Apakah Blocknet memiliki dana pengembang atau kas?'
    a: "Aturan yang dirilis tidak mewajibkan alokasi protokol untuk dana pengembang atau kas. Setiap blok setelah genesis memiliki satu output coinbase sebesar imbalan terjadwal. Ini menjelaskan penerbitan baru, bukan identitas pemilik berikutnya. Siapa pun yang memiliki BNT dapat melakukan transfer biasa, termasuk pembayaran sukarela kepada pengembang."
  - q: 'Kapan blok genesis Blocknet dibuat?'
    a: 'Timestamp blok genesis adalah 15 Februari 2026 pukul 20:58:38 EST, yaitu 01:58:38 UTC pada 16 Februari. Blok membawa pesan "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything", dan hash blok sebelumnya adalah hash SHA3-256 pesan tersebut.'
  - q: 'Bagaimana memverifikasi fair launch Blocknet sendiri?'
    a: 'Buka blok 0 pada Blocknet explorer untuk melihat nol transaksi dan merkle root seluruhnya nol. Lalu baca kode daemon open source, tempat blok genesis ditetapkan dengan daftar transaksi kosong dan pemeriksaan coinbase menerima satu output senilai imbalan blok.'
  - q: 'Bagaimana mulai menambang Blocknet?'
    a: 'Unduh penambang seine untuk Windows, macOS atau Linux, jalankan, lalu tempel alamat Blocknet atau nama pendek Blocknet ID saat diminta. Tekan Enter untuk menerima pool default bntpool.com, dan seine langsung menambang. Kemudian cari alamat Anda di halaman statistik bntpool.com.'
sourceHash: a283d654ab64
translation: draft
---

Lihat bagaimana BNT mulai beredar: blok genesis kosong, imbalan pertama yang ditambang, lalu transfer antar pemilik. Panduan ini memeriksa catatan peluncuran dan aturan penerbitan, menjelaskan apa yang dibuktikannya, serta menunjukkan cara menerima BNT baru melalui penambangan di bntpool.com.

## Apakah Blocknet memiliki premine?

<Answer>
Tidak. Blok genesis Blocknet, dibuat pada 15 Februari 2026, tidak memiliki transaksi, jadi tidak ada koin sebelum penambangan dimulai. Sejak blok 1, setiap blok membayar imbalannya dalam satu output coinbase dengan nilai tepat sesuai jadwal, sehingga setiap BNT beredar ditambang oleh seseorang yang menjalankan proof of work.
</Answer>

Blok genesis tidak memberikan cadangan kepada pendiri, pengembang, investor, atau yayasan. Blok berikutnya menciptakan imbalan penambangan terjadwal tanpa output kas tambahan yang diwajibkan protokol. Ini adalah aturan penerbitan: aturan tersebut tidak mengidentifikasi siapa yang menambang imbalan atau siapa yang kemudian menerima koin yang sudah ada melalui transfer.

## Apa isi blok genesis Blocknet?

Blok genesis ditetapkan dalam kode daemon, sehingga setiap node mulai dari blok 0 yang sama. Field-nya tetap:

| Field | Nilai |
|---|---|
| Ketinggian | 0 |
| Timestamp | 15 Februari 2026, 20:58:38 EST (01:58:38 UTC pada 16 Februari) |
| Pesan | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Hash sebelumnya | SHA3-256 pesan: `a94a7f2b…d909df` |
| Kesulitan | 4, minimum jaringan |
| Nonce | 0 |
| Transaksi | tidak ada |
| Merkle root | seluruhnya nol |

Pesan itu adalah judul artikel Wired bertanggal 15 Februari 2026, dan kode merujuk artikel tersebut sebagai sumber. Karena hash sebelumnya diturunkan dari pesan, judul terikat pada identitas blok 0 dan segala yang dibangun di atasnya.

Komentar di atas fungsi genesis menjelaskannya: "Genesis tidak memiliki transaksi — tidak ada coinbase, tidak ada koin dibakar. Blok nyata pertama pada ketinggian 1 akan memiliki coinbase pertama." Validasi menegakkannya. Node menolak blok 0 kecuali memiliki tepat nol transaksi dan merkle root seluruhnya nol. Jadi node yang dimodifikasi sekalipun tidak dapat menyelipkan koin ke genesis tanpa ditolak yang lain.

## Bagaimana konsensus meniadakan dana pengembang atau kas?

<Answer>
Setelah genesis, setiap blok diawali satu transaksi coinbase tanpa input, dengan biaya nol dan tepat satu output sebesar imbalan terjadwal. Aturan yang dirilis tidak menyediakan output terpisah untuk pendiri atau kas dalam transaksi itu. Pemilik tetap dapat melakukan transfer biasa dari BNT yang sudah dimilikinya.
</Answer>

Beberapa pemeriksaan daemon bekerja bersama:

- **Satu coinbase per blok.** Transaksi pertama harus coinbase, dan transaksi lain dalam blok tidak boleh coinbase.
- **Satu output, tanpa input.** Coinbase dengan nol atau dua output, atau memiliki input, tidak valid.
- **Imbalan tepat.** Blocknet menyembunyikan nilai dengan Pedersen commitment, tetapi coinbase khusus. Blinding factor diturunkan dari data publik (public key transaksi dan ketinggian blok), sehingga setiap node dapat membangun kembali commitment imbalan terjadwal untuk dibandingkan. Jika output mengikat nilai lain, blok ditolak.
- **Memo kosong.** Memo coinbase harus kosong, jadi imbalan blok juga tidak dapat membawa data tambahan.

Transaksi biasa tidak dapat menciptakan koin. Setiap transaksi harus seimbang: input sama dengan output ditambah biaya, diperiksa pada commitment itu sendiri, dan setiap output memiliki Bulletproofs range proof yang menunjukkan nilai berada dalam rentang valid dan tidak negatif. Begitulah nilai tetap privat sementara penerbitan mengikuti jadwal dengan tepat.

## Bagaimana memeriksa fair launch Blocknet sendiri?

Anda tidak harus percaya pada ucapan siapa pun. Blockchain dan kode sama-sama publik.

**Pada explorer.** Buka [blok 0 di Blocknet explorer](https://explorer.blocknetcrypto.com/block/0). Tercantum 0 transaksi, merkle root 64 angka nol, nonce 0 dan hash sebelumnya berawalan `a94a7f2b`. Baris imbalan menunjukkan nilai jadwal untuk ketinggian itu, tetapi tanpa transaksi tidak ada output penerimanya. Lalu buka [blok 1](https://explorer.blocknetcrypto.com/block/1): satu-satunya transaksi adalah coinbase tanpa input dengan satu output, koin pertama di blockchain.

Anda dapat memastikan hash sebelumnya memang hash pesan genesis. Dengan OpenSSL 1.1.1 atau lebih baru, jalankan:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Output berakhir dengan `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, sama dengan nilai blok 0 pada explorer.

**Dalam kode.** Daemon tersedia open source di [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). Dalam [`block.go`](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go), cari fungsi berikut:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` membangun blok 0 dengan daftar transaksi kosong, `validateGenesisBlock` menolak blok 0 yang memiliki transaksi, dan `validateCoinbaseConsensus` menegakkan satu output senilai imbalan blok. Dalam `transaction.go`, `validateCoinbase` dan `verifyCommitmentBalance` memeriksa bentuk coinbase serta keseimbangan transaksi biasa.

## Bagaimana BNT yang baru ditambang sampai ke pemilik lain?

Explorer mencatat waktu genesis pada 15 Februari 2026 pukul 20:58:38 EST. Blok 1 bertanggal 20 Februari 2026 pukul 14:29:20 EST dan memuat transaksi coinbase pertama. Keduanya merupakan catatan berbeda: genesis menentukan blok awal bersama, sedangkan blok 1 memulai penerbitan berimbalan. Imbalan yang ditampilkan explorer untuk genesis kosong adalah jumlah menurut jadwal; tidak ada output yang menerimanya.

Setelah imbalan penambangan dapat dibelanjakan, pemiliknya dapat mentransfer BNT kepada orang lain. Pool juga mengirim pembayaran berikutnya dari imbalan yang diterimanya. Transfer ini mendistribusikan ulang koin yang sudah ada; tidak menerbitkannya lagi. Jadi, Anda dapat memiliki dan menggunakan BNT tanpa menambang sendiri blok yang menciptakannya. [Panduan kirim dan terima](https://seineminer.com/id/blog/send-receive-blocknet/) membahas pembayaran biasa, sedangkan [panduan pasokan](https://seineminer.com/id/blog/blocknet-supply/) menjelaskan jadwal moneter secara keseluruhan.

## Mengapa fair launch penting bagi penambang?

Bagi penambang, fair launch berarti imbalan blok adalah seluruh penjelasan asal koin.

- **Semua koin baru masuk ke penambang.** Tanpa premine dan output kas, 100% penerbitan dibayarkan kepada penemu blok. Blok 1 membayar 723,25093035 BNT dalam output coinbase tunggal, dan setiap blok berikutnya membayar imbalan penuh dengan cara sama.
- **Semua mengikuti aturan sama.** Sejak blok 1, satu-satunya cara memperoleh BNT baru adalah menjalankan proof of work. [Jadwal imbalan](/blog/id/blocknet-block-reward-schedule/) ditetapkan dalam kode dan berlaku sama untuk penambang pertama maupun yang mulai hari ini.
- **Proof of work cocok untuk komputer biasa.** Blocknet menggunakan Argon2id dengan memori 2 GiB per hash, dirancang untuk menahan ASIC. Penambangan tetap terbuka bagi desktop, laptop dan server yang sudah dimiliki. [Penjelasan Argon2id di seineminer.com](https://seineminer.com/id/blog/argon2id-proof-of-work-explained/) membahas alasannya.
- **Penerbitan mudah diaudit.** Setiap koin berasal dari output coinbase yang diperiksa tiap node, sehingga total terbit pada ketinggian mana pun langsung mengikuti jadwal imbalan.

Jika baru mengenal koin ini, [Apa itu Blocknet?](https://seineminer.com/id/blog/what-is-blocknet/) membahas privasi, pembayaran dengan nama dan wallet.

## Bagaimana menambang Blocknet sendiri?

Untuk menerima BNT baru melalui penambangan, gunakan miner seine dengan bntpool.com:

1. **Dapatkan alamat.** Pasang [wallet baris perintah](https://github.com/blocknetprivacy/blocknet/releases/latest) atau [wallet desktop](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) resmi Blocknet lalu salin alamat, atau gunakan nama pendek Blocknet ID seperti `@name`.
2. **Unduh seine.** Ambil arsip Windows, macOS atau Linux dari [seineminer.com](https://seineminer.com/id/#download) lalu ekstrak. Tidak perlu instalasi.
3. **Jalankan dan terima default.** Mulai seine, tempel alamat atau `@name`, lalu tekan Enter untuk menerima pool default `stratum+tcp://bntpool.com:3333`. seine langsung menghitung hash dan menyetel thread serta kernel saat berjalan, menggunakan CPU serta kartu NVIDIA pada Windows dan Linux.
4. **Pantau penambang.** Cari alamat di [halaman statistik](/stats) untuk melihat hashrate, share dan saldo, serta ikuti temuan pool di [halaman blok](/blocks).

[Panduan menambang di bntpool.com](/blog/id/mine-blocknet-on-bntpool/) membahas login alamat atau nama dan pemeriksaan penambang, [Get Started](/start) mencantumkan pengaturan pembayaran, dan [panduan menambang Blocknet](https://seineminer.com/id/blog/how-to-mine-blocknet/) menjelaskan pemasangan seine bertahap di setiap sistem.

<CTA target="seine" title="Tambang bagian Anda" text="Setiap BNT ditambang. Unduh seine dan mulai menambang di bntpool.com dalam beberapa menit." />
