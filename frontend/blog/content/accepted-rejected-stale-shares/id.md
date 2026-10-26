---
title: 'Memahami share diterima, ditolak, dan kedaluwarsa'
description: 'Pahami share diterima, ditolak, dan kedaluwarsa di bntpool.com, arti pesan penolakan, serta pengaruh tingkat kesulitan share pada statistik penambangan Anda.'
tags: ['Panduan penambangan', 'Share', 'Pemecahan masalah']
faq:
  - q: 'Apakah share diterima berarti saya menemukan blok?'
    a: 'Artinya pool menerima hasil kiriman sesuai aturan pemrosesan share. Blok harus memenuhi target jaringan terpisah dan diterima daemon. Imbalan mengikuti penemuan blok, validasi, dan pembayaran. Konfirmasi penerimaan share bukan pemberitahuan pembayaran.'
  - q: 'Mengapa share kedaluwarsa ditolak?'
    a: 'Stale job berarti kiriman merujuk pada penugasan yang tidak lagi memenuhi syarat, misalnya kedaluwarsa atau template lama melewati masa tenggang. Periksa job baru dan kembalinya share diterima. Jika berulang, periksa koneksi dan versi penambang.'
  - q: 'Apakah kesulitan share lebih tinggi mengurangi imbalan?'
    a: 'Kesulitan mengubah pekerjaan yang diwakili setiap share layak dan frekuensi kedatangan yang diharapkan. Bandingkan pekerjaan berbobot kesulitan selama periode setara, bukan jumlah mentah share. Imbalan juga bergantung pada jendela PPLNS, pekerjaan layak, blok yang ditemukan, dan aturan validasi.'
sourceHash: 5be830208e6c
translation: draft
---

Gunakan hasil share di log untuk memilih lanjut menambang, memeriksa koneksi, atau menyelidiki penolakan berulang. Panduan ini menjelaskan respons bntpool.com dan kesulitan di balik penghitung. Mulai dengan [panduan koneksi](/blog/id/mine-blocknet-on-bntpool/) jika perlu, atau [panduan pelacakan](/blog/id/track-your-miner-on-bntpool/#zero-hashrate) bila alamat tidak menunjukkan aktivitas.

## Apa itu share penambangan yang diterima?

<Answer>
Share adalah hasil penambangan yang dikirim berdasarkan target dari pool. Respons diterima menegaskan masuknya kiriman ke alur pemrosesan dan menunjukkan pekerjaan mencapai pool. Penemuan blok, verifikasi share, dan kelayakan pembayaran adalah tahap terpisah; penerimaan saja bukan pembayaran.
</Answer>

Penambang menghitung banyak hash di antara kiriman share. Kebanyakan tidak mencapai target share dan tidak dikirim. Penghitung diterima menghitung hasil kiriman, bukan semua hash CPU atau GPU. [Panduan hashrate](/blog/id/what-is-hashrate/) membedakan hitungan lokal dan estimasi pool berdasarkan pekerjaan yang dikirim.

## Apa perbedaan share dan blok?

Pool memberi koneksi Anda job dan kesulitan share. Hasil yang mencapai target membuktikan pekerjaan pada kesulitan itu. Kandidat blok juga harus memenuhi target jaringan dan pemeriksaan daemon. Menemukan share tidak berarti blok baru telah ditambahkan ke rantai.

bntpool.com mencatat share terverifikasi dan sementara dalam validasi. Kiriman diterima dapat berstatus sementara; pembobotan pembayaran mengikuti aturan validasi dan kelayakan. Gunakan penerimaan sebagai tanda koneksi, lalu [Statistik Saya](/stats) untuk pekerjaan dan imbalan tercatat. [Panduan PPLNS](/blog/id/how-pplns-payouts-work/) menjelaskan urutan setelah pool menemukan blok.

## Apa arti share ditolak dan kedaluwarsa?

Kiriman ditolak gagal memenuhi pemeriksaan yang berlaku. Alasannya lebih berguna daripada penghitung saja. Stale job adalah satu kategori penolakan: penugasan tidak lagi layak. Cocokkan pesan persis dengan kejadian bersamaan, seperti koneksi ulang atau job baru.

| Pesan | Makna dan pemeriksaan berikutnya |
|---|---|
| `stale job` | Penugasan hilang, kedaluwarsa, digantikan melewati masa tenggang, atau memakai template lama yang tidak tersedia. Periksa pulihnya job baru dan share diterima. |
| `duplicate share` | Template dan nonce yang sama sudah dikirim. Cari pengiriman berulang; pekerjaan yang sama tidak menambah kontribusi lagi. |
| `low difficulty share` | Hash kiriman tidak mencapai target share yang ditugaskan. Jika berulang, periksa versi penambang dan penanganan job serta kesulitan. |
| `nonce out of assigned range` | Nonce berada di luar rentang job. Periksa versi penambang dan proxy atau integrasi khusus. |

Share kedaluwarsa menyangkut pekerjaan suatu penugasan, sedangkan blok orphan menyangkut posisi blok dalam rantai. Pisahkan log share dari [daftar blok](/blocks) saat diagnosis. Pesan stale job saja tidak berarti pool menemukan blok orphan.

## Mengapa kesulitan share berubah?

<Answer>
Kesulitan variabel, atau vardiff, menyesuaikan target koneksi dengan aktivitas share yang diamati. Kesulitan lebih tinggi biasanya berarti share lebih sedikit dengan lebih banyak pekerjaan per share; lebih rendah berarti kebalikannya. Bandingkan pekerjaan layak berbobot kesulitan selama periode yang sama, bukan hanya jumlah share.
</Answer>

Contoh hitungan: sepuluh share layak pada kesulitan 100 setara totalnya dengan satu share kesulitan 1.000, yaitu 1.000. Ini menjelaskan bobot, bukan janji imbalan atau jadwal pasti. Share datang dengan interval bervariasi; validasi menentukan pekerjaan yang layak dihitung.

Kesulitan penugasan berlaku saat share dikirim. Jangan membandingkan hasil lama hanya dengan kesulitan terbaru di layar. Pool menyimpan penugasan untuk pemeriksaan kiriman. Penyesuaian normal bukan kesalahan; memaksakan share lebih mudah tidak menciptakan pekerjaan komputasi tambahan.

## Bagaimana membaca tingkat penolakan?

Bandingkan kiriman diterima dan ditolak pada periode dan cakupan yang sama. Penambang mungkin menghitung satu sesi, sementara pool memakai periode lain atau menggabungkan beberapa rig. Memulai ulang mengubah jendela lokal tanpa menghapus riwayat alamat di pool.

99 kiriman diterima dan satu ditolak menghasilkan tingkat penolakan 1% menurut jumlah. Ini tidak otomatis sama dengan persentase imbalan hilang: kesulitan dapat berbeda dan akuntansi memakai aturan kelayakan. Periksa alasan, waktu, serta kelanjutan pekerjaan diterima sebelum mengubah pengaturan.

## Apa yang dilakukan bila penolakan berulang?

Mulai dari satu rig terdampak. Catat versi, pesan persis, waktu mulai, dan apakah job baru masih datang. Periksa [Status](/status), lalu gangguan koneksi rig. Perbarui dari [rilis resmi seine](https://github.com/zpalmtree/seine/releases) dan bandingkan setelah satu perubahan setiap kali.

Proxy atau integrasi khusus harus mempertahankan job, rentang nonce, dan kesulitan saat meneruskan kiriman. Jika berlanjut, berikan potongan log singkat serta detail rig melalui kanal dukungan proyek. Hapus rahasia dan data akun pribadi; frasa pemulihan dompet tidak pernah diperlukan untuk diagnosis share.

## Bagaimana memastikan penambangan berjalan lagi?

Cari job baru diikuti share diterima, lalu buka [Statistik Saya](/stats) dengan alamat penerimaan yang sama. Pastikan aktivitas terbaru rig terlihat. Biarkan estimasi hashrate mengumpulkan pekerjaan sebelum membandingkan rata-rata. Setelah normal, ikuti estimasi imbalan, saldo, dan pembayaran sebagai tahap berbeda di panduan PPLNS.

<CTA target="pool" title="Hubungkan ke bntpool.com" text="Ikuti panduan penyiapan, pastikan share diterima, dan pantau aktivitas melalui alamat penerimaan." />
