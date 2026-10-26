---
title: 'Kesulitan mining: jaringan dan share'
description: 'Pahami target kesulitan mining, penyesuaiannya di Blocknet, serta mengapa kesulitan jaringan dan share memengaruhi angka penambangan yang berbeda.'
tags: ['Dasar penambangan', 'Kesulitan', 'Blocknet']
faq:
  - q: 'Apakah kesulitan lebih tinggi menurunkan hashrate?'
    a: 'Kesulitan mengubah target hasil, bukan langsung jumlah hash yang dihitung per detik. Pada hashrate sama, kesulitan jaringan lebih tinggi memperpanjang interval rata-rata penemuan blok. Periksa performa miner secara terpisah dari frekuensi blok atau share.'
  - q: 'Apakah share lebih sulit membayar lebih banyak?'
    a: 'Share itu mewakili lebih banyak pekerjaan yang diharapkan, tetapi lebih jarang dikirim pada hashrate sama. Bandingkan pekerjaan berbobot kesulitan pada periode yang sama. Di bntpool.com, imbalan juga bergantung pada blok diterima dan PPLNS, bukan jumlah share mentah.'
  - q: 'Seberapa sering Blocknet menyesuaikan kesulitan?'
    a: 'Setelah setiap blok, Blocknet menghitung kesulitan berikutnya dari riwayat terbaru. Aturan saat ini memakai rata-rata bergerak berbobot linear pada 60 interval dengan target 300 detik per blok. Interval terbaru lebih berbobot; setiap blok tetap bisa datang lebih cepat atau lambat.'
sourceHash: '5c203e8c7523'
translation: draft
---

Kesulitan menghubungkan pekerjaan perangkat dengan blok yang diterima jaringan. Panduan ini membahas target, penyesuaian Blocknet, dan kesulitan share yang terpisah. Pahami perubahannya sebelum membandingkan hashrate, waktu blok, atau [kalkulator imbalan](/blog/id/estimate-blocknet-mining-rewards/#mining-calculator).

## Apa itu kesulitan mining?

<Answer>
Kesulitan menggambarkan pekerjaan hashing yang diharapkan untuk menemukan hasil yang memenuhi target. Semakin tinggi, semakin jarang hasil memenuhi syarat pada hashrate sama. Di Blocknet, kesulitan jaringan kira-kira setara hash yang diharapkan per blok; kesulitan share mengukur target pengiriman terpisah.
</Answer>

Setiap hash menghasilkan angka yang harus memenuhi ambang target. Target lebih kecil menerima bagian lebih kecil dari kemungkinan hasil, sehingga kesulitannya lebih tinggi. Blocknet membagi nilai terbesar 256-bit dengan kesulitan untuk memperoleh target 256-bit. Menggandakan kesulitan kira-kira menggandakan pekerjaan yang diharapkan tanpa mengubah operasi hash.

## Bagaimana Blocknet menyesuaikan kesulitan jaringan?

Kesulitan berikutnya dihitung setelah setiap blok. Aturan mainnet saat ini memakai rata-rata bergerak berbobot linear, LWMA, atas 60 interval terbaru dengan bobot lebih besar pada yang baru. Targetnya 300 detik per blok. Gabungan kesulitan sebelumnya dan waktu berbobot membuat kedatangan cepat berkelanjutan cenderung menaikkannya, dan lambat menurunkannya.

Implementasi membatasi interval perhitungan dan menerapkan kesulitan minimum. Jendela 60 interval membutuhkan 61 timestamp; bukan berarti penyesuaian hanya setiap 60 blok. Aturan menjaga laju mendekati target saat daya hashing berubah. Lima menit adalah rata-rata; blok individual dapat datang jauh lebih cepat atau lambat.

## Kesulitan jaringan dan share

Pool menetapkan target share untuk mengamati kontribusi di antara penemuan blok. Target ini umumnya lebih mudah daripada target jaringan. Kedua angka menggambarkan pekerjaan yang diharapkan, tetapi menjawab pertanyaan berbeda dan ditentukan bagian sistem berbeda:

| Kesulitan | Yang dikendalikan |
|---|---|
| Jaringan | Ambang pekerjaan kandidat blok berdasarkan aturan konsensus. |
| Share | Ambang pengiriman yang ditetapkan pool untuk koneksi atau tugas mining. |

bntpool.com memakai kesulitan variabel, vardiff, untuk mengatur frekuensi kiriman. Pada hashrate sama, kesulitan lebih tinggi umumnya menghasilkan lebih sedikit share dengan pekerjaan lebih besar per share. Ini tidak mengubah kesulitan jaringan. Hasil yang juga memenuhi target jaringan dapat menjadi kandidat blok; daemon tetap memvalidasinya. [Panduan hasil share](/blog/id/accepted-rejected-stale-shares/) menjelaskan penerimaan dan penolakan.

## Bagaimana pengaruhnya terhadap waktu blok?

Untuk Blocknet, bagi kesulitan jaringan dengan hashrate yang bekerja dalam H/s untuk memperkirakan interval rata-rata dalam detik. Gunakan nilai dari periode sama. Contoh menjaga kedua variabel tetap hanya untuk menjelaskan aritmetika. Angkanya ilustratif, bukan pengukuran jaringan saat ini atau ramalan bagi miner tertentu.

**Detik yang diharapkan per blok ≈ kesulitan jaringan ÷ hashrate dalam H/s**

| Kesulitan jaringan | Hashrate | Interval yang diharapkan |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Pada 30.000 dan 100 H/s, rata-ratanya 300 detik. Pada 60.000 dengan hashrate sama, menjadi 600 detik. Jika hashrate juga berlipat menjadi 200 H/s, kembali 300 detik. Ini rata-rata percobaan berulang. [Panduan keberuntungan pool](/blog/id/mining-pool-luck-explained/) menjelaskan variasi durasi putaran nyata.

## Mengapa imbalan berubah meski hashrate tetap?

Pada hashrate pribadi tetap, kesulitan lebih tinggi mengurangi penemuan blok yang diharapkan per waktu selama berlaku. Jika hashrate jaringan naik dan kesulitan mengikuti, bagian kontribusi Anda mengecil. Imbalan per blok adalah faktor lain: [jadwal imbalan](/blog/id/blocknet-block-reward-schedule/) mengubahnya secara terpisah. Kesulitan tidak menentukan jumlah BNT dalam imbalan terjadwal.

Gunakan data baru pada kalkulator untuk mengeksplorasi perkiraan BNT. Pastikan hashrate berasal dari miner atau pool agar biaya miner diterapkan dengan benar, memakai tarif versi Anda. Estimasi adalah rata-rata dengan asumsi tertentu. Pembayaran juga bergantung pada blok, konfirmasi, dan [PPLNS](/blog/id/how-pplns-payouts-work/); kesulitan saja tidak menunjukkan kapan pembayaran tiba.

## Kesulitan mana yang perlu diperiksa?

Mulai dari pertanyaannya. Untuk blok, gunakan kesulitan jaringan dan hashrate miner atau pool terkait. Untuk frekuensi share, gunakan kesulitan tugasnya. Untuk saldo atau pembayaran, lihat [My Stats](/stats) dan [riwayat pembayaran](/payouts). Simpan waktu, cakupan, dan satuan setiap nilai yang dibandingkan.

[Dasbor pool](/) dan [riwayat blok](/blocks) memberi konteks terkini. Hashrate jaringan diperkirakan dari pekerjaan terbaru dan waktu blok yang berlalu, sehingga bisa berbeda dari kesulitan berikutnya dibagi target 300 detik. [Panduan hashrate](/blog/id/what-is-hashrate/) menjelaskannya. Pisahkan kesulitan, hashrate, dan keberuntungan saat menyelidiki perubahan: masing-masing menggambarkan aspek berbeda.

<CTA target="pool" title="Hubungkan miner ke bntpool.com" text="Gunakan halaman penyiapan, lalu pantau share dan aktivitas melalui My Stats." />
