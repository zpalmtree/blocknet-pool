---
title: Lịch phần thưởng khối Blocknet
description: Phần thưởng Blocknet theo tháng, từ 723,25 BNT lúc ra mắt đến mức đuôi vĩnh viễn 2,0 BNT ở tháng 48, với 288 khối/ngày và 98,6 triệu BNT trước đuôi.
tags: [Blocknet, Phần thưởng khối, Phát hành, Đào coin]
sourceHash: abea18c2c21b
translation: draft
faq:
  - q: 'Phần thưởng khối Blocknet hiện tại là bao nhiêu?'
    a: 'Điều này phụ thuộc chuỗi đang ở tháng nào, vì phần thưởng giảm sau mỗi 8.640 khối. Ngày 25 tháng 10 năm 2026, chuỗi ở tháng 7, trả 467,67 BNT mỗi khối; tháng 8 bắt đầu ở độ cao 69.120 với 439,46 BNT. Tìm độ cao khối hiện tại trên bảng điều khiển bntpool.com rồi tra bảng.'
  - q: 'Mỗi ngày đào được bao nhiêu khối Blocknet?'
    a: 'Blocknet đặt mục tiêu một khối mỗi năm phút, tương đương 288 khối mỗi ngày và 8.640 khối trong mỗi tháng 30 ngày. Độ khó điều chỉnh sau từng khối bằng trung bình có trọng số của thời gian 60 khối gần nhất, nên thời gian trung bình vẫn gần năm phút khi thợ đào tham gia hoặc rời mạng.'
  - q: 'Tổng cung Blocknet là bao nhiêu?'
    a: 'Khoảng 98,56 triệu BNT được đào trước khi phát hành đuôi bắt đầu ở tháng 48. Mã nguồn và trang chính thức làm tròn thành khoảng 100 triệu. Sau đó mỗi khối trả 2,0 BNT mãi mãi, bổ sung 576 BNT mỗi ngày hoặc khoảng 210.240 BNT mỗi năm, nên nguồn cung tiếp tục tăng chậm.'
  - q: 'Khi nào Blocknet bắt đầu phát hành đuôi?'
    a: 'Đuôi bắt đầu ở tháng 48, tại độ cao khối 414.720. Đó là 48 tháng, mỗi tháng 30 ngày, hay 1.440 ngày với khối năm phút sau khi ra mắt, xấp xỉ bốn năm. Ngày chính xác phụ thuộc thời gian khối thực tế. Từ độ cao này, mỗi khối trả 2,0 BNT.'
  - q: 'Bao lâu thì có thể chi tiêu Blocknet vừa đào?'
    a: 'Phần thưởng khối có thể chi tiêu sau 60 khối, khoảng năm giờ với thời gian khối năm phút của Blocknet. Đây gọi là độ trưởng thành coinbase. Pool bntpool.com cũng chờ 60 khối trước khi thanh toán phần thưởng, nên chỉ trả thưởng từ các khối đã trưởng thành.'
---

Trang này liệt kê phần thưởng khối Blocknet cho từng tháng trong lịch phát hành, giải thích cách tính phần thưởng và cho thấy lịch này có ý nghĩa gì với bạn khi đào coin.

## Phần thưởng khối Blocknet là gì?

<Answer>
Phần thưởng khối Blocknet bắt đầu ở 723,25 BNT mỗi khối và giảm một lần trong mỗi tháng 30 ngày gồm 8.640 khối, theo đường cong suy giảm mượt. Mọi khối trong cùng tháng có phần thưởng như nhau. Từ tháng 48, ở độ cao 414.720, mỗi khối trả phần thưởng đuôi cố định 2,0 BNT vĩnh viễn.
</Answer>

Blocknet đặt mục tiêu một khối mỗi năm phút (300 giây). Như vậy có 288 khối mỗi ngày và 8.640 khối trong mỗi tháng 30 ngày. Độ khó điều chỉnh sau mỗi khối dựa trên trung bình có trọng số của thời gian 60 khối gần nhất, nên nhịp độ vẫn gần năm phút khi hashrate mạng thay đổi.

Phần thưởng của một khối chỉ phụ thuộc tháng mà độ cao của nó thuộc về. Tháng bằng độ cao khối chia cho 8.640 rồi làm tròn xuống: độ cao 0 đến 8.639 là tháng 0, độ cao 8.640 đến 17.279 là tháng 1, và cứ thế tiếp tục.

## Phần thưởng được tính thế nào?

Daemon tính phần thưởng mỗi tháng bằng một công thức:

```
reward(month) = (723.25093035 - 2.0) × e^(-0.75 × month / 12) + 2.0
```

Số hạng đầu tiên là phần thưởng vượt mức đuôi 2,0 BNT. Phần này giảm theo hệ số e^-0,75 mỗi năm, nên sau mười hai tháng còn khoảng 47% mức ban đầu. Chia theo tháng, phần vượt đuôi của phần thưởng mỗi tháng bằng khoảng 93,9% tháng trước, nên phần thưởng giảm theo các bước nhỏ, đều đặn.

Kết quả được làm tròn xuống đơn vị nguyên tử nguyên vẹn (1 BNT bằng 100.000.000 đơn vị nguyên tử). Bảng dưới hiển thị mỗi phần thưởng làm tròn đến hai chữ số thập phân.

## Bảng phần thưởng Blocknet theo tháng

| Tháng | Khối đầu tiên | Phần thưởng mỗi khối (BNT) | Tổng đã đào cuối tháng (triệu BNT) |
|---|---|---|---|
| 0 | 0 | 723,25 | 6,25 |
| 1 | 8.640 | 679,55 | 12,12 |
| 2 | 17.280 | 638,50 | 17,64 |
| 3 | 25.920 | 599,94 | 22,82 |
| 4 | 34.560 | 563,71 | 27,69 |
| 5 | 43.200 | 529,68 | 32,27 |
| 6 | 51.840 | 497,71 | 36,57 |
| 7 | 60.480 | 467,67 | 40,61 |
| 8 | 69.120 | 439,46 | 44,40 |
| 9 | 77.760 | 412,96 | 47,97 |
| 10 | 86.400 | 388,06 | 51,33 |
| 11 | 95.040 | 364,67 | 54,48 |
| 12 | 103.680 | 342,69 | 57,44 |
| 13 | 112.320 | 322,05 | 60,22 |
| 14 | 120.960 | 302,66 | 62,83 |
| 15 | 129.600 | 284,45 | 65,29 |
| 16 | 138.240 | 267,33 | 67,60 |
| 17 | 146.880 | 251,26 | 69,77 |
| 18 | 155.520 | 236,16 | 71,81 |
| 19 | 164.160 | 221,97 | 73,73 |
| 20 | 172.800 | 208,64 | 75,53 |
| 21 | 181.440 | 196,12 | 77,23 |
| 22 | 190.080 | 184,36 | 78,82 |
| 23 | 198.720 | 173,31 | 80,32 |
| 24 | 207.360 | 162,93 | 81,73 |
| 25 | 216.000 | 153,18 | 83,05 |
| 26 | 224.640 | 144,02 | 84,29 |
| 27 | 233.280 | 135,42 | 85,46 |
| 28 | 241.920 | 127,33 | 86,56 |
| 29 | 250.560 | 119,74 | 87,60 |
| 30 | 259.200 | 112,61 | 88,57 |
| 31 | 267.840 | 105,91 | 89,49 |
| 32 | 276.480 | 99,61 | 90,35 |
| 33 | 285.120 | 93,70 | 91,16 |
| 34 | 293.760 | 88,14 | 91,92 |
| 35 | 302.400 | 82,92 | 92,63 |
| 36 | 311.040 | 78,02 | 93,31 |
| 37 | 319.680 | 73,41 | 93,94 |
| 38 | 328.320 | 69,09 | 94,54 |
| 39 | 336.960 | 65,02 | 95,10 |
| 40 | 345.600 | 61,20 | 95,63 |
| 41 | 354.240 | 57,62 | 96,13 |
| 42 | 362.880 | 54,25 | 96,60 |
| 43 | 371.520 | 51,08 | 97,04 |
| 44 | 380.160 | 48,11 | 97,45 |
| 45 | 388.800 | 45,31 | 97,85 |
| 46 | 397.440 | 42,69 | 98,21 |
| 47 | 406.080 | 40,22 | 98,56 |
| 48 trở đi | 414.720 | 2,00 | +0,017 mỗi tháng |

Khối genesis ở độ cao 0 không có phần thưởng, nên tháng 0 có 8.639 khối được trả thưởng. Mọi BNT đều đến từ những phần thưởng khối này, không có premine, như trang [Blocknet ra mắt công bằng](/blog/vi/blocknet-fair-launch/) giải thích. Ngày 25 tháng 10 năm 2026, pool đang làm việc trên khối 69.016, gần cuối tháng 7. [Bảng điều khiển bntpool.com](/) hiển thị độ cao khối hiện tại để bạn tìm hàng tương ứng ở trên, còn [trang khối](/blocks) liệt kê các khối pool đã tìm thấy.

## Điều gì xảy ra ở tháng 48?

<Answer>
Tháng 48 bắt đầu ở độ cao khối 414.720; từ đó mỗi khối Blocknet trả cố định 2,0 BNT mãi mãi. Đây là phát hành đuôi. Nó duy trì phần thưởng cho mọi khối trong suốt thời gian chuỗi hoạt động, nên thợ đào luôn được trả công bảo vệ mạng.
</Answer>

Công thức suy giảm áp dụng cho tháng 0 đến 47, kết thúc ở 40,22 BNT mỗi khối trong tháng 47. Từ tháng 48, daemon dùng trực tiếp giá trị đuôi, nên phần thưởng trở thành 2,0 BNT và giữ nguyên. Với 288 khối mỗi ngày, đuôi bổ sung 576 BNT mỗi ngày hoặc khoảng 210.240 BNT mỗi năm.

Tháng 48 bắt đầu sau 1.440 ngày với khối năm phút kể từ khi ra mắt, xấp xỉ bốn năm. Ngày trên lịch phụ thuộc thời gian khối thực tế, nên độ cao khối là mốc đáng tin cậy.

## Có bao nhiêu BNT được đào trước đuôi?

Cộng tất cả các khối từ độ cao 1 đến 414.719 cho kết quả 98.561.905,08 BNT, khoảng 98,6 triệu. Mã nguồn và trang chính thức mô tả lịch là khoảng 100 triệu coin trong bốn năm cộng thêm đuôi; tổng chính xác thấp hơn một chút so với con số tròn đó.

Phần lớn được đào sớm. Nhóm theo năm của lịch:

| Tháng | BNT đã đào | Tỷ lệ trong tổng trước đuôi |
|---|---|---|
| 0-11 (năm 1) | 54,48 triệu | 55,3% |
| 12-23 (năm 2) | 25,84 triệu | 26,2% |
| 24-35 (năm 3) | 12,32 triệu | 12,5% |
| 36-47 (năm 4) | 5,93 triệu | 6,0% |

## Khi nào có thể chi tiêu BNT đã đào?

Phần thưởng khối có thể chi tiêu khi có thêm 60 khối phía trên, khoảng năm giờ sau. Quy tắc này gọi là độ trưởng thành coinbase. Pool bntpool.com chờ cùng 60 khối trước khi thanh toán phần thưởng, và [trang thanh toán](/payouts) liệt kê những gì đã gửi.

## Lịch này có ý nghĩa gì với thợ đào?

Phần thưởng lớn hơn trong các tháng đầu. Khối tìm thấy ở tháng 12 trả 342,69 BNT, khoảng 47% của 723,25 BNT trả ở tháng 0; khối ở tháng 24 trả 162,93 BNT. Mỗi tháng mới trả ít hơn tháng trước cho mỗi khối cho đến khi bắt đầu đuôi.

Phần bạn nhận trong những phần thưởng đó đi theo tỷ lệ hashrate của bạn trong mạng. Khi đào solo, số khối kỳ vọng mỗi ngày là 288 × (hashrate của bạn ÷ hashrate mạng), và mỗi khối trả phần thưởng của tháng tương ứng. Trong pool, cùng phép tính áp dụng cho hashrate pool; phần thưởng mỗi khối chia cho thợ đào đã đóng góp công việc. Nhờ vậy bạn kiếm từ mọi khối pool tìm thấy thay vì chờ tự tìm cả khối. [Bảng điều khiển bntpool.com](/) hiển thị hashrate mạng và pool hiện tại, còn [trang thống kê](/stats) hiển thị hashrate trình đào của bạn sau khi bắt đầu đào.

Để bắt đầu đào ngay:

1. Tải seine từ [seineminer.com](https://seineminer.com/vi/#download). Phần mềm chạy trên Windows, macOS và Linux, tự tinh chỉnh theo phần cứng.
2. Chạy và dán địa chỉ Blocknet hoặc `@name` của bạn.
3. Chấp nhận pool mặc định `stratum+tcp://bntpool.com:3333`.

[Hướng dẫn đào trên bntpool.com](/blog/vi/mine-blocknet-on-bntpool/) trình bày từng bước phía pool, [trang Get Started](/start) liệt kê cài đặt pool và [hướng dẫn đào bằng seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) hướng dẫn thiết lập từng bước. Để xem các bộ xử lý khác nhau đạt kết quả gì, đọc [phần cứng đào Blocknet và hashrate](https://seineminer.com/vi/blog/blocknet-mining-hardware-hashrate/).

<Note>
Số liệu trên trang lấy từ mã phần thưởng của daemon Blocknet, được kiểm tra ngày 25 tháng 10 năm 2026. Theo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), Blocknet dự kiến nâng cấp mạng tên Faraday vào khoảng ngày 16 tháng 2 năm 2027. Hãy cập nhật seine: phần mềm kiểm tra bản phát hành mới khi khởi động và mỗi 12 giờ.
</Note>

<CTA target="seine" title="Đào Blocknet bằng seine" text="seine miễn phí cho Windows, macOS và Linux, mặc định đào trên bntpool.com." />
