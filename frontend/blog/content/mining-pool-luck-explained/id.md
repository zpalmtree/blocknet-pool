---
title: 'Memahami keberuntungan pool: effort dan waktu blok'
description: 'Pahami keberuntungan pool, effort 100%, dan putaran panjang. Baca riwayat bntpool.com dan perkiraan waktu blok, lalu bedakan penemuan blok dari pembayaran.'
tags: ['Pool penambangan', 'Keberuntungan pool', 'Blocknet']
faq:
  - q: 'Apakah effort 100% berarti pool akan segera menemukan blok?'
    a: 'Tidak. Artinya, kerja share yang tercatat telah mencapai jumlah yang diharapkan untuk satu blok pada tingkat kesulitan yang digunakan dalam perhitungan itu. Ini perbandingan dengan rata-rata, bukan persentase penyelesaian. Setiap hash baru tetap harus memenuhi target jaringan.'
  - q: 'Apakah persentase keberuntungan lebih rendah lebih baik di bntpool.com?'
    a: 'Baca label dan definisinya terlebih dahulu. Kolom Effort dan kartu Average Luck di bntpool.com menyatakan kerja relatif terhadap kerja yang diharapkan. Nilai lebih rendah berarti lebih sedikit kerja tercatat per blok. Situs lain mungkin memakai konvensi kebalikannya. Bandingkan metrik dan ukuran sampel yang setara, bukan persentase saja.'
  - q: 'Apakah putaran panjang meningkatkan pembayaran berikutnya?'
    a: 'Putaran panjang tidak menjamin pembayaran pribadi yang lebih besar. Penemuan blok menentukan kapan imbalan tersedia, sedangkan perhitungan PPLNS menentukan bagian Anda berdasarkan kerja diterima yang memenuhi syarat. Setelahnya berlaku aturan konfirmasi dan pembayaran. Periksa saldo di Statistik Saya dan urutannya di panduan pembayaran.'
sourceHash: 'f7051fa67591'
translation: draft
---

Dasbor pool menampilkan kerja penambangan serta blok yang ditemukan melalui kerja itu. Panduan ini menjelaskan persentase effort, waktu blok yang diharapkan, dan riwayat putaran di bntpool.com. Anda dapat membedakan rata-rata statistik dari hitung mundur, lalu memeriksa koneksi penambangan sendiri tanpa menunggu blok berikutnya dari pool.

## Apa itu keberuntungan pool penambangan?

<Answer>
Keberuntungan pool menggambarkan perbandingan penemuan blok aktual dengan rata-rata yang diharapkan dari kerja pool. Blok ditemukan secara acak: sebagian memerlukan sedikit kerja, sebagian lebih banyak. Di bntpool.com, effort putaran membandingkan kerja share diterima dengan kesulitan blok; effort lebih rendah berarti putaran menemukan blok dengan lebih sedikit kerja tercatat.
</Answer>

Pool menggabungkan percobaan banyak penambang, tetapi tetap harus menemukan hash yang memenuhi target jaringan. Share diterima mengukur kontribusi dengan target yang lebih mudah. Share membantu pool memperkirakan kerja hashing di antara penemuan blok. [Pengantar pool penambangan](/blog/id/what-is-a-mining-pool/) menjelaskan hubungan itu; keberuntungan menjelaskan variasi blok yang dihasilkan.

## Bagaimana pool menghitung effort?

Untuk putaran selesai, bntpool.com membagi kerja share diterima dengan kesulitan blok lalu mengalikannya dengan 100. Kerja share dibobot berdasarkan kesulitan, bukan sekadar jumlah share yang dikirim. Share dengan kesulitan penugasan lebih tinggi mewakili lebih banyak kerja hashing yang diharapkan daripada share dengan kesulitan lebih rendah.

**Effort (%) = kerja share diterima ÷ kesulitan blok × 100**

Misalnya, sebuah blok memiliki kesulitan 10.000 dan pool mencatat 5.000 unit kerja diterima selama putarannya. Effort-nya 50%. Dengan 20.000 unit kerja pada kesulitan blok yang sama, effort menjadi 200%. Ini contoh aritmetika, bukan pengukuran blok produksi tertentu.

| Effort | Cara membacanya |
|---|---|
| 50% | Setengah kerja yang diharapkan untuk kesulitan tersebut |
| 100% | Jumlah kerja yang diharapkan untuk satu blok |
| 200% | Dua kali kerja yang diharapkan untuk kesulitan tersebut |

Effort putaran saat ini memakai kesulitan saat ini, yang dapat berubah seiring kemajuan rantai. Baris historis memakai kesulitan bloknya masing-masing. Jadi angka ini membandingkan kerja, bukan indikator kemajuan yang harus berhenti di 100%. Angka ini juga berbeda dari durasi putaran: pool lebih cepat dapat mengerjakan jumlah kerja yang sama dalam waktu lebih singkat.

## Apa arti waktu blok yang diharapkan?

Ini adalah interval rata-rata berdasarkan perkiraan bagian pool dari hashrate jaringan. Dasbor bntpool.com menghitungnya sebagai 300 detik dikalikan hashrate jaringan lalu dibagi hashrate pool. Gunakan satuan yang sama untuk keduanya. [Panduan hashrate](/blog/id/what-is-hashrate/) menjelaskan mengapa perkiraan berubah saat share diterima dan blok baru masuk.

**Detik yang diharapkan = 300 × H/s jaringan ÷ H/s pool**

Jika jaringan diperkirakan 100.000 H/s dan pool 10.000 H/s, pool memiliki sepersepuluh dari total perkiraan. Interval yang diharapkan adalah 3.000 detik, atau 50 menit. Rata-rata itu dapat menggambarkan campuran putaran pendek dan panjang; bukan jadwal blok berikutnya tepat 50 menit dari sekarang.

Dasbor juga membandingkan waktu berlalu dengan waktu yang diharapkan. Persentase berbasis waktu ini dapat berbeda dari effort berbasis share karena masukan keduanya berbeda. Periksa label sebelum membandingkan dua angka. Perubahan hashrate pool atau kesulitan jaringan dapat mengubah perkiraan meski putaran yang sama masih berjalan.

## Apakah blok pasti datang setelah effort 100%?

<Answer>
Pool tidak otomatis berhak mendapat blok saat effort mencapai 100%. Hash sebelumnya yang gagal tidak membuat hash berikutnya lebih mungkin memenuhi target jaringan yang tidak berubah. Dengan hashrate dan kesulitan tetap, perkiraan waktu tunggu mulai sekarang tetap sama, bahkan setelah putaran panjang. Lebih banyak kerja menghasilkan lebih banyak percobaan, bukan tenggat.
</Answer>

Model probabilitas sederhana memperjelasnya. Dengan percobaan independen dan laju penemuan blok tetap, waktu tunggu mendekati distribusi eksponensial. Peluang belum mendapat blok setelah satu interval yang diharapkan sekitar 36,8%; setelah dua sekitar 13,5%; setelah tiga sekitar 5,0%. Persentase itu menggambarkan banyak putaran hipotetis dalam kondisi tetap, bukan ramalan putaran pool saat ini.

| Interval yang diharapkan telah berlalu | Perkiraan peluang belum ada blok |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Perhitungannya `exp(-t / T)`, dengan `T` sebagai interval rata-rata dan `t` waktu berlalu. [Referensi distribusi eksponensial NIST](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) memberikan fungsi survival ini. Hashrate dan kesulitan nyata berubah, jadi gunakan tabel sebagai penjelasan keacakan. Rangkaian blok cepat juga tidak mengubah peluang hash berikutnya selama target tetap sama.

## Bagaimana membaca riwayat keberuntungan bntpool.com?

Buka [Keberuntungan](/luck) untuk membandingkan putaran selesai. Tiap baris menampilkan blok, effort, durasi putaran, status, dan waktu penemuan. Amati beberapa putaran bersama, bukan memilih satu baris yang luar biasa pendek atau panjang. Tautan blok membuka catatan rantai terkait, sedangkan paginasi memungkinkan pemeriksaan riwayat lebih luas.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Halaman riwayat keberuntungan bntpool.com asli dengan effort, durasi putaran, dan status blok" width="1200" height="833" loading="lazy" />

Tangkapan layar ini diambil pada 26 Oktober 2026. Baris dan statusnya merupakan rekaman historis; gunakan halaman langsung untuk nilai terkini. Status tertunda atau terkonfirmasi menunjukkan keadaan konfirmasi blok. Status itu tidak mengubah arti persentase effort yang tercatat dan terpisah dari pembayaran ke alamat Anda.

Di dasbor, Average Luck adalah rata-rata aritmetika persentase effort tercatat untuk blok yang bukan orphan hingga ketinggian rantai saat ini. Meski namanya demikian, kartu ini mengikuti konvensi effort: lebih rendah berarti lebih sedikit kerja tercatat per blok. Rata-rata merangkum sampel tersebut. Angka itu tidak menjanjikan putaran mendatang akan mengganti hasil tertentu sebelumnya.

## Bagaimana keberuntungan pool memengaruhi imbalan Anda?

Keberuntungan pool mengubah kapan blok ditemukan. PPLNS menentukan pembagian imbalan blok yang ditemukan di antara kerja diterima yang memenuhi syarat. Keduanya tahap berbeda. Batas putaran tidak mengubah jendela PPLNS menjadi aturan pembayaran khusus satu putaran; baca [cara kerja pembayaran PPLNS](/blog/id/how-pplns-payouts-work/) untuk jendela, konfirmasi, dan ambang pembayaran sebenarnya.

Karena itu, saldo dapat berubah bertahap meskipun penambang terus mengirim share diterima. [Kalkulator imbalan](/blog/id/estimate-blocknet-mining-rewards/#mining-calculator) memperkirakan hasil dari hashrate dan masukan jaringan. Kalkulator berguna untuk ekspektasi dari waktu ke waktu; bukan prediksi blok atau saat pembayaran masuk ke dompet.

## Apa yang perlu diperiksa selama putaran panjang?

Periksa hal yang dapat diamati langsung: penambang berjalan, share terus diterima, dan alamat yang benar muncul di [Statistik Saya](/stats). Gunakan [Status](/status) untuk kesehatan layanan publik. Jika share diterima Anda berhenti, ikuti [panduan pemecahan masalah statistik penambang](/blog/id/track-your-miner-on-bntpool/#zero-hashrate); putaran pool panjang dan rig terputus memerlukan pemeriksaan berbeda.

Setelah koneksi sehat, baca keberuntungan dalam riwayat yang bermakna dengan definisi yang sama. Setiap angka dasbor pun punya peran jelas: hashrate memperkirakan kerja per detik, effort membandingkan kerja dengan kerja yang diharapkan untuk satu blok, dan saldo menunjukkan perhitungan imbalan. Tidak ada yang mengharuskan menebak kapan hash pemenang berikutnya datang.

<CTA target="pool" title="Mulai menambang di bntpool.com" text="Hubungkan seine, pastikan share Anda diterima, lalu pantau riwayat putaran dan imbalan di dasbor pool." />
