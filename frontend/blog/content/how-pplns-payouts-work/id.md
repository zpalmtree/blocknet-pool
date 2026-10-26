---
title: Cara kerja pembayaran PPLNS di bntpool.com
description: bntpool.com membayar penambang Blocknet dengan PPLNS berjendela enam jam. Pelajari pembagian imbalan, 60 konfirmasi blok dan minimum 0,1 BNT.
tags: [Pembayaran, PPLNS, bntpool, Panduan penambangan]
faq:
  - q: 'Berapa lama jendela PPLNS di bntpool.com?'
    a: 'Enam jam. Saat pool menemukan blok, pool melihat enam jam sebelumnya dan membagi imbalan kepada semua pengirim share dalam waktu itu, sebanding dengan kesulitan share mereka. Nilai terkini tercantum di halaman Get Started pool.'
  - q: 'Berapa pembayaran minimum di bntpool.com?'
    a: 'Pembayaran minimum 0,1 BNT. Setelah saldo terkonfirmasi mencapai jumlah itu, pool mengirimnya otomatis ke alamat Blocknet tanpa perlu meminta atau mengklaim. Saldo lebih kecil tetap tersimpan dan bertambah setiap blok terkonfirmasi yang melibatkan share Anda hingga melewati ambang.'
  - q: 'Mengapa Estimated Rewards berbeda dari Confirmed Rewards?'
    a: 'Estimated Rewards adalah bagian Anda dari blok temuan pool yang masih mengumpulkan 60 konfirmasi, sehingga angkanya masih dapat berubah. Confirmed Rewards berasal dari blok sedalam 60 blok dan sedang menunggu pembayaran. Saat setiap blok matang, nilainya berpindah dari kartu pertama ke kedua.'
  - q: 'Apakah imbalan langsung berhenti saat saya berhenti menambang?'
    a: 'Tidak. Setiap share berada dalam jendela PPLNS selama enam jam setelah dikirim, jadi blok yang ditemukan pool dalam waktu itu masih mengkreditkan Anda. Imbalan yang sudah terkonfirmasi tetap dalam saldo dan dibayarkan setelah mencapai minimum 0,1 BNT.'
  - q: 'Apa arti effort di atas 100% pada halaman luck?'
    a: 'Effort membandingkan pekerjaan pool selama satu ronde dengan kesulitan blok, yaitu jumlah pekerjaan yang diharapkan. Effort di atas 100% berarti ronde lebih lama dari rata-rata, dan di bawah 100% berarti lebih cepat. Keduanya normal, dan selama banyak ronde rata-rata effort mendekati 100%.'
sourceHash: 08a99741f46c
translation: draft
---

Panduan ini menjelaskan bagaimana bntpool.com mengubah share dari penambang Anda menjadi pembayaran Blocknet. Anda akan memahami pembagian imbalan blok, alasan menunggu sekitar lima jam sebelum dibayar, kapan saldo dikirim, serta cara membaca kartu saldo di [halaman statistik](/stats) dan [halaman luck](/luck).

Jika baru mengenal penambangan pool, [apa itu pool penambangan?](/blog/id/what-is-a-mining-pool/) membahas dasarnya. Jika belum mulai menambang, [panduan penambangan seine](https://seineminer.com/id/blog/how-to-mine-blocknet/) membawa Anda dari pengunduhan hingga share pertama dalam beberapa menit.

## Apa itu PPLNS?

<Answer>
PPLNS berarti "pay per last N shares", pembayaran berdasarkan N share terakhir. Saat bntpool.com menemukan blok, imbalannya dibagi berdasarkan share yang dikirim dalam enam jam sebelumnya, sebanding dengan kesulitan setiap share. Makin banyak pekerjaan konsisten yang Anda sumbangkan dalam jendela itu, makin besar bagian setiap blok Anda.
</Answer>

Di bntpool.com, "N terakhir" adalah rentang waktu enam jam. [Halaman Get Started](/start) menampilkan skema pembayaran, jendela, minimum pembayaran dan jumlah konfirmasi; nilai yang sama dipublikasikan di `https://bntpool.com/api/info`.

| Pengaturan | Nilai di bntpool.com |
|---|---|
| Skema pembayaran | PPLNS |
| Jendela share | 6 jam |
| Konfirmasi sebelum pembayaran | 60 blok |
| Pembayaran minimum | 0,1 BNT |

Share dihitung berdasarkan tingkat kesulitan. Pool memberi setiap koneksi penambang kesulitan share sendiri lalu menyesuaikannya agar setiap penambang mengirim aliran share teratur. Mesin lebih cepat mendapat share lebih sulit sehingga bobot tiap share lebih besar. Bagian Anda ditentukan oleh total pekerjaan, ukuran yang adil bagi laptop maupun server.

## Bagaimana bntpool.com membagi imbalan blok?

Setiap pool menemukan blok, imbalan melalui langkah yang sama:

1. **Blok dicatat.** Blok muncul di [halaman blok](/blocks) dengan status pending.
2. **Jendela diukur.** Pool mengambil enam jam yang berakhir pada blok itu dan menjumlahkan kesulitan setiap share diterima dari masing-masing alamat.
3. **Imbalan dibagi.** Setiap alamat mendapat proporsi imbalan yang sama dengan proporsi pekerjaannya terhadap total pekerjaan dalam jendela.
4. **Blok matang.** Bagian Anda muncul sebagai perkiraan hingga blok sedalam 60 blok, lalu masuk saldo terkonfirmasi.
5. **Saldo dibayar.** Setelah saldo terkonfirmasi mencapai 0,1 BNT, pool mengirimnya ke alamat Anda.

## Contoh perhitungan

<Note>
Contoh ini memakai angka bulat rekaan untuk memperlihatkan perhitungan. Total sebenarnya bergantung pada hashrate pool, hashrate Anda dan [imbalan blok](/blog/id/blocknet-block-reward-schedule/) saat itu.
</Note>

Misalkan dalam enam jam sebelum suatu blok, semua penambang mengirim share dengan total kesulitan 1.000.000 dan share Anda berjumlah 20.000. Bagian jendela Anda adalah 20.000 ÷ 1.000.000 = 2%. Jika pool membagikan 400 BNT untuk blok tersebut, kredit Anda 400 × 0,02 = 8 BNT.

Sekarang bayangkan penambang kedua dengan hashrate persis sama, tetapi baru terhubung 30 menit sebelum blok. Tiga puluh menit adalah seperdua belas dari enam jam, sehingga kesulitan dalam jendelanya sekitar 1.667 dan ia menerima sekitar 0,67 BNT dari blok ini. Pekerjaannya tetap dihitung: setiap share bertahan enam jam berikutnya dan mendapat bagian setiap blok yang ditemukan pool selama itu.

| Penambang (contoh) | Pekerjaan dalam jendela | Bagian | Kredit dari pembayaran 400 BNT |
|---|---|---|---|
| Anda, menambang penuh enam jam | 20.000 | 2% | 8 BNT |
| Hashrate sama, bergabung 30 menit sebelum blok | 1.667 | sekitar 0,17% | sekitar 0,67 BNT |
| Penambang kecil, menambang penuh enam jam | 100 | 0,01% | 0,04 BNT |

Penambang kecil pada baris terakhir mendapat 0,04 BNT per blok, jadi saldo terkonfirmasinya melewati minimum 0,1 BNT setelah tiga blok seperti itu, lalu dibayar.

## Mengapa PPLNS menguntungkan penambang yang konsisten?

Setiap share yang dikirim diperhitungkan untuk setiap blok yang ditemukan pool selama enam jam berikutnya. Penambang yang berjalan sepanjang waktu selalu memiliki jendela penuh pekerjaan, sehingga setiap blok baru memberinya kredit pada laju penuh. Penghasilannya mengikuti hashrate dengan dekat dari blok ke blok.

Ini juga membuat PPLNS toleran terhadap jeda. Restart, pembaruan driver atau gangguan jaringan singkat tidak mereset posisi Anda karena share yang sudah dikirim bertahan enam jam. Pekerjaan setelah blok ditemukan diperhitungkan untuk blok berikutnya, jadi tambahan hashrate selalu dimanfaatkan.

Untuk hasil paling stabil, biarkan penambang berjalan dan menyetel diri. seine terus menyesuaikan jumlah thread dan kernel CPU saat menambang tanpa menyambung ulang ke pool, sehingga aliran share tidak terputus. Anda dapat [mengunduh seine](https://seineminer.com/id/#download) untuk Windows, macOS atau Linux.

## Mengapa imbalan blok menunggu 60 konfirmasi?

Blocknet mewajibkan koin baru ditambang menunggu 60 blok sebelum dapat dibelanjakan. Aturan daemon menyebutnya coinbase maturity, sekitar lima jam dengan waktu blok lima menit (60 × 5 menit = 300 menit). bntpool.com menggunakan 60 blok yang sama sebelum menganggap imbalan terkonfirmasi, sehingga setiap pembayaran berasal dari koin yang benar-benar dapat dikirim pool.

Selama pematangan, pool memeriksa bahwa blok masih bagian dari blockchain utama. Setelah sedalam 60 blok, statusnya menjadi confirmed dan imbalan ditambahkan ke saldo terkonfirmasi setiap penambang. Pada kasus langka ketika jaringan memilih blok lain pada ketinggian itu, status blok menjadi orphaned dan perkiraan imbalannya dihapus.

## Kapan pembayaran bntpool.com tiba?

<Answer>
Setelah blok mendapat 60 konfirmasi, sekitar lima jam pada waktu blok lima menit, bagian Anda masuk Confirmed Rewards di halaman statistik. Saat saldo terkonfirmasi mencapai 0,1 BNT, bntpool.com otomatis mengirimnya ke alamat Blocknet. Tidak perlu meminta atau mengklaim.
</Answer>

Bagi penambang baru, perkiraan pertama muncul setelah pool menemukan blok saat share Anda ada dalam jendela. Pembayaran pertama menyusul setelah blok matang dan saldo mencapai minimum. Pembayaran terkirim tercantum di [halaman pembayaran](/payouts) dan riwayat Anda sendiri pada halaman statistik.

## Bagaimana imbalan sementara ditampilkan?

Cari alamat atau `@name` di [halaman statistik](/stats) untuk melihat tiga kartu saldo:

- **Estimated Rewards:** bagian Anda dari blok yang belum matang. Nilai sementara ini dapat berubah hingga setiap blok dikonfirmasi atau menjadi orphaned.
- **Confirmed Rewards:** saldo matang yang menunggu pembayaran.
- **Paid Balance:** seluruh jumlah yang sudah dikirim ke alamat Anda.

Pool memisahkan imbalan sementara agar saldo terkonfirmasi hanya berisi jumlah final. Di bawah kartu, halaman statistik mencantumkan setiap blok belum terkonfirmasi beserta perkiraan bagian Anda, sehingga Anda bisa mengikuti kemajuannya menuju konfirmasi. [Panduan memantau penambang](/blog/id/track-your-miner-on-bntpool/) membahas bagian lain halaman statistik.

## Apa yang ditunjukkan halaman luck dan effort?

Effort mengukur pekerjaan pool untuk menemukan blok. Nilainya adalah pekerjaan diterima selama ronde dibagi kesulitan blok, ditampilkan sebagai persentase. Kesulitan Blocknet setara jumlah hash yang diharapkan per blok, jadi effort 100% berarti blok membutuhkan tepat jumlah pekerjaan rata-rata. Di bawah 100% berarti pool menemukan lebih cepat; di atas 100% berarti lebih lama.

[Halaman luck](/luck) mencantumkan tiap ronde dengan ketinggian blok, effort, durasi ronde, status dan waktu ditemukan. [Dasbor pool](/) menampilkan effort ronde berjalan yang terus bertambah serta rata-rata luck pool. Setiap ronde dapat berada di atas atau bawah 100%, normal untuk semua pool proof of work, dan dalam banyak ronde rata-rata effort mendekati 100%.

Luck memengaruhi seberapa sering blok tiba. Bagian setiap blok Anda hanya bergantung pada proporsi pekerjaan dalam jendela, dan penambangan stabil mempertahankan bagian itu tetap penuh, baik ronde cepat maupun lama.

<CTA target="pool" title="Bergabung ke pool" text="Menambanglah secara konsisten di bntpool.com dan bagian setiap blok Anda dibayar otomatis setelah mencapai 0,1 BNT." />
