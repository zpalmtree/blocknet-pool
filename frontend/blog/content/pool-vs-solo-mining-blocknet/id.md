---
title: Penambangan pool versus solo untuk Blocknet
description: Pilih pool atau solo untuk Blocknet. Hitung perkiraan blok harian, lihat bntpool.com mengubahnya menjadi pembayaran teratur, dan siapkan penambangan solo.
tags: [Panduan penambangan, Penambangan solo, bntpool, PPLNS]
faq:
  - q: 'Seberapa sering saya menemukan blok Blocknet jika menambang solo?'
    a: 'Rata-rata 288 × (hashrate Anda ÷ hashrate jaringan) blok sehari, karena Blocknet menargetkan satu blok tiap lima menit. Dengan 50 H/s pada jaringan 50.000 H/s, hasilnya 0,288 blok sehari, sekitar satu blok setiap tiga setengah hari. Baca hashrate jaringan langsung pada dasbor bntpool.com.'
  - q: 'Perlukah menjalankan node Blocknet untuk menambang di bntpool.com?'
    a: 'Tidak. Pool menjalankan node sendiri, menyusun template blok dan mengirim blok temuan. Anda hanya perlu seine dan alamat Blocknet atau nama Blocknet ID. Jalankan seine, tempel alamat, terima pool default, dan pool membayar bagian tiap blok ke alamat itu.'
  - q: 'Bisakah seine menambang Blocknet solo?'
    a: 'Ya. Jalankan ./seine --mode daemon untuk menambang langsung pada node Blocknet Anda yang tersinkron, menggunakan cookie API node untuk terhubung. Setiap blok temuan membayar imbalan penuh ke wallet node. Panduan solo dalam repositori seine membahas tata letak node khusus dan kata sandi wallet.'
  - q: 'Bisakah berganti antara pool dan solo kemudian?'
    a: 'Ya. seine memakai unduhan sama untuk keduanya. Jalankan biasa untuk bntpool.com, atau tambah --mode daemon untuk node sendiri, lalu mulai ulang saat ingin beralih. Share yang sudah dikirim tetap dalam jendela PPLNS enam jam dan terus memperoleh imbalan selama masih berlaku.'
sourceHash: 171d240d6c4b
translation: draft
---

Panduan ini membantu memilih penambangan Blocknet pada bntpool.com atau solo menggunakan node sendiri. Anda akan menghitung blok yang dapat ditemukan hashrate Anda, melihat bagaimana pool mengubahnya menjadi aliran pembayaran teratur, dan mempelajari pengaturan keduanya dengan seine.

## Sebaiknya menambang Blocknet di pool atau solo?

<Answer>
Keduanya mengikuti perhitungan sama: bagian hashrate jaringan menentukan bagian blok Anda. Di bntpool.com Anda memperoleh bagian setiap blok temuan pool, sehingga pembayaran datang teratur saat pool menemukan blok. Solo memberi imbalan penuh setiap blok yang Anda temukan, cocok untuk operasi besar dengan node sendiri.
</Answer>

Bagi kebanyakan penambang, bntpool.com mudah untuk memulai. Pool ini default seine, jadi tidak perlu pengaturan selain alamat dan tidak perlu menjalankan atau menyinkronkan node. Solo juga didukung penuh: seine memiliki mode daemon yang menambang langsung pada node Blocknet Anda. Bagian berikut membantu menentukan yang sesuai perangkat Anda.

## Berapa blok yang akan Anda temukan secara solo?

<Answer>
Blocknet menargetkan satu blok tiap lima menit, yaitu 288 sehari. Menambang sendiri, perkiraan Anda adalah 288 × (hashrate Anda ÷ hashrate jaringan) blok per hari. Bagi satu hari dengan jumlah itu untuk waktu rata-rata antarblok. Baca hashrate jaringan langsung pada dasbor bntpool.com.
</Answer>

Rumus berlaku karena Blocknet menyesuaikan kesulitan agar seluruh jaringan rata-rata menemukan blok tiap 300 detik, dan kesulitan mendekati jumlah hash yang diperkirakan dicoba sebelum berhasil. Setiap hash berpeluang sama, sehingga proporsi hash jaringan dari mesin Anda adalah proporsi blok yang diharapkan ditemukan.

Anda memerlukan dua angka:

- **Hashrate Anda.** seine menampilkannya pada dasbor saat menambang, dan [halaman statistik](/stats) pool menunjukkannya setelah Anda menambang di sana.
- **Hashrate jaringan.** [Dasbor bntpool.com](/) menampilkannya pada kartu Network Hashrate di samping Pool Hashrate. Keduanya berubah, jadi periksa saat menghitung.

Setiap blok temuan membayar imbalan bulannya. [Jadwal imbalan blok](/blog/id/blocknet-block-reward-schedule/) mencantumkannya per bulan.

## Contoh perhitungan

<Note>
Angka berikut adalah angka bulat hipotetis untuk menunjukkan perhitungan, bukan nilai langsung. Gunakan hashrate jaringan terkini dari dasbor untuk perkiraan sendiri.
</Note>

Misalkan jaringan berjalan pada 50.000 H/s. Rig tunggal 50 H/s, kira-kira hasil seine pada satu server AMD EPYC Zen 5, memiliki 0,1% jaringan. Berikut perkiraan rig itu dan dua operasi lebih besar saat solo:

| Hashrate Anda (contoh) | Bagian jaringan | Perkiraan blok sehari | Rata-rata jarak antarblok |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | sekitar 83 jam (3,5 hari) |
| 500 H/s | 1% | 2,88 | sekitar 8,3 jam |
| 5.000 H/s | 10% | 28,8 | sekitar 50 menit |

Sekarang tempatkan rig 50 H/s pada pool dengan total 5.000 H/s. Pool menemukan sekitar 28,8 blok sehari, rata-rata satu setiap 50 menit, dan bagian rig sebesar 1% pekerjaan pool memberinya 1% setiap blok itu. Pekerjaan terhitung tetap sama, 0,288 blok sehari. Yang berubah cara tiba: bagian kecil sekitar setiap 50 menit, sementara solo memberi satu blok utuh setiap beberapa hari.

Tabel juga menunjukkan mengapa solo cocok untuk operasi besar. Pada 5.000 H/s, penambang solo menemukan blok sesering pool berukuran sama, sehingga penghasilannya stabil sendiri.

Untuk mengubah blok harian menjadi BNT bagi perangkat Anda dengan contoh bertanggal dari data langsung pool, lihat [cara memperkirakan imbalan penambangan Blocknet](/blog/id/estimate-blocknet-mining-rewards/).

## Mengapa pool memberi pembayaran lebih teratur?

bntpool.com menggabungkan hashrate semua penambang, sehingga menemukan blok jauh lebih sering daripada rig tunggal. Imbalan setiap blok dibagi di antara penambang yang membantu menemukannya menggunakan PPLNS: pool melihat enam jam sebelumnya dan membagi berdasarkan kesulitan share masing-masing.

Dampak praktisnya bagi Anda:

- **Penghasilan setiap blok.** Selama share berada dalam jendela enam jam, Anda mendapat bagian setiap blok pool. Pantau kedatangannya pada [halaman blok](/blocks).
- **Pembayaran otomatis.** Setelah blok sedalam 60 blok, bagian Anda masuk saldo terkonfirmasi, dan pool mengirimnya setelah mencapai 0,1 BNT. Pembayaran terkirim tercantum di [halaman pembayaran](/payouts).
- **Berkelanjutan saat restart.** Share yang sudah dikirim tetap dalam jendela enam jam, jadi restart atau pembaruan driver tidak mereset posisi.
- **Hanya perlu menjalankan penambang.** Pool menjalankan node, menyusun template dan mengirim blok. Beberapa rig dapat menambang ke satu alamat atau Blocknet ID `@name`.

[Panduan pembayaran PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan pembagian bertahap dengan contoh sendiri. [Get Started](/start) mencantumkan pengaturan pembayaran terkini.

## Mengapa menambang Blocknet solo?

Solo memberi Anda kendali setiap bagian proses. Node Anda menyusun tiap template dan memeriksa setiap blok sendiri, dan setiap blok temuan membayar imbalan penuh langsung ke wallet node.

Ini paling sesuai ketika hashrate Anda sering menemukan blok. Pada contoh di atas, operasi dengan 10% jaringan menemukan blok sekitar setiap 50 menit, sehingga pembayaran solo seteratur pool. Jika sudah menjalankan node Blocknet untuk wallet atau layanan lain, solo cukup menambahkan penambang di atasnya.

seine juga dibuat untuk mode ini. Program menambang dari template node, dan jika node melupakan template, misalnya setelah restart, seine mengirim ulang blok lengkap yang sudah terpecahkan agar blok temuan tetap sampai ke blockchain.

## Bagaimana menyiapkan penambangan solo dengan seine?

1. **Pasang dan sinkronkan node Blocknet.** Dengan wrapper `blocknet`, jalankan:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Cookie API node ditulis ke `~/.config/bnt/data/mainnet/api.cookie`. Untuk mempercepat sinkronisasi pertama, bntpool.com memublikasikan `checkpoints.dat` pada [halaman checkpoint](/checkpoints); [sinkronisasi node Blocknet dengan checkpoint](/blog/id/blocknet-checkpoints-fast-sync/) menjelaskan penggunaannya.
2. **Tunggu sinkronisasi selesai.** Pesan `[sync] progress` node mencapai 100%. Node hanya menyediakan template setelah mengejar blockchain.
3. **Mulai seine dalam mode daemon.**

   ```sh
   ./seine --mode daemon
   ```

   Untuk tata letak node khusus, arahkan API dan cookie sendiri:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

[Panduan solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) membahas kata sandi wallet dan pengaturan khusus, sedangkan [panduan penambangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) membahas unduhan dan share pertama. Anda dapat [mengunduh seine](https://seineminer.com/id/#download) untuk Windows, macOS atau Linux.

## Mana yang sebaiknya dipilih?

Gunakan rumus dengan hashrate Anda dan hashrate jaringan terkini dari [dasbor](/), lalu lihat rata-rata waktu antarblok.

- **Mulai di bntpool.com** jika menambang pada desktop, laptop atau beberapa server, atau menginginkan pembayaran teratur tanpa node. Jalankan seine, tempel alamat atau `@name` dan terima pool default.
- **Tambang solo** jika operasi Anda memiliki bagian besar jaringan sehingga blok sering datang sendiri, dan Anda sudah atau ingin menjalankan node tersinkron.

seine memudahkan perubahan pilihan. Pool dan mode daemon memakai unduhan sama, jadi beralih cukup satu flag dan restart saat perangkat bertambah. Jika menjalankan node lokal sambil menambang di pool, seine juga menampilkan saldo wallet node di samping saldo pool.

<Note>
Blocknet merencanakan peningkatan Faraday sekitar 16 Februari 2027 menurut [blocknetcrypto.com](https://blocknetcrypto.com/#faraday). Peningkatan direncanakan mengubah proof of work. Perbarui seine: program memeriksa rilis baru saat mulai dan setiap 12 jam.
</Note>

<CTA target="pool" title="Mulai menambang di bntpool.com" text="Jalankan seine, tempel alamat dan dapatkan bagian setiap blok pool, dibayar otomatis mulai 0,1 BNT." />
