---
title: Cách ước tính phần thưởng đào Blocknet
description: Ước tính phần cứng đào được bao nhiêu BNT mỗi ngày bằng một công thức, tìm dữ liệu trên bảng điều khiển bntpool.com và xem ví dụ có ngày cụ thể.
tags: [Phần thưởng đào coin, Hướng dẫn đào coin, Hashrate, bntpool]
sourceHash: 28d46138019e
translation: draft
faq:
  - q: 'Tính phần thưởng đào Blocknet thế nào?'
    a: 'Chia hashrate của bạn cho hashrate mạng, nhân 288 là số khối Blocknet tạo mỗi ngày, rồi nhân phần thưởng khối hiện tại. Kết quả là BNT gộp kỳ vọng mỗi ngày. Xem hashrate mạng trên bảng điều khiển bntpool.com và hashrate riêng trong seine hoặc My Stats.'
  - q: 'Xem hashrate mạng Blocknet ở đâu?'
    a: 'Bảng điều khiển bntpool.com hiển thị trong thẻ Network Hashrate, thường bằng KH/s, nên nhân 1.000 để được H/s. Cùng giá trị có ở đơn vị H/s trong trường network_hashrate của API pool tại bntpool.com/api/stats. Nó được đo trên mười khối gần nhất và cập nhật mỗi khối.'
  - q: 'Phần thưởng khối Blocknet có đổi theo thời gian không?'
    a: 'Có. Phần thưởng cố định trong mỗi tháng 30 ngày gồm 8.640 khối và giảm đầu tháng kế tiếp, cho đến khi đạt mức vĩnh viễn 2,0 BNT mỗi khối ở tháng 48. Để tìm phần thưởng hiện tại, chia độ cao khối cho 8.640, làm tròn xuống rồi tra tháng đó trong lịch phần thưởng.'
  - q: 'Vì sao phần thưởng hàng ngày trong pool khác ước tính?'
    a: 'Ước tính là trung bình. Trong một ngày, pool có thể tìm nhiều hoặc ít khối hơn kỳ vọng; hashrate mạng cũng thay đổi khi thợ đào tham gia. Sau một tuần hoặc lâu hơn đào đều đặn, dao động từng ngày được bình quân; bạn có thể theo dõi phần thưởng đã ghi trên My Stats.'
  - q: 'Đào trong pool có thay đổi lượng BNT kỳ vọng không?'
    a: 'Phần kỳ vọng của bạn trong phần thưởng mạng không đổi, vì chỉ phụ thuộc tỷ lệ hashrate mạng. Pool thay đổi thời điểm nhận. Thay vì chờ nhiều ngày cho cả khối, bạn nhận một phần mỗi khối bntpool.com tìm thấy, nên phần thưởng đến nhỏ và đều.'
---

Hướng dẫn này cho thấy cách ước tính phần cứng đào được bao nhiêu BNT mỗi ngày. Bạn sẽ biết công thức, nơi đọc từng dữ liệu trên [bảng điều khiển bntpool.com](/) và cách tính với ví dụ có ngày cụ thể từ dữ liệu pool trực tiếp cùng hashrate tốt nhất seine đo được.

## Phần cứng của bạn đào được bao nhiêu BNT mỗi ngày?

<Answer>
BNT kỳ vọng mỗi ngày bằng hashrate của bạn chia hashrate mạng, nhân 288 khối mỗi ngày, nhân phần thưởng khối hiện tại. Hashrate riêng lấy từ seine hoặc My Stats, hashrate mạng từ bảng điều khiển bntpool.com, phần thưởng từ độ cao khối hiện tại và lịch phần thưởng Blocknet.
</Answer>

Viết thành công thức:

```
BNT per day = (your H/s ÷ network H/s) × 288 × block reward
```

Mỗi phần có lý do đơn giản:

- **288 khối mỗi ngày.** Blocknet nhắm một khối mỗi năm phút; 86.400 giây trong ngày chia 300 giây mỗi khối bằng 288. Độ khó điều chỉnh sau từng khối, nên chuỗi giữ gần nhịp đó khi thợ đào tham gia.
- **Tỷ lệ hashrate là tỷ lệ khối.** Độ khó Blocknet gần bằng số hash kỳ vọng cần để tìm khối. Mỗi hash có cùng xác suất giải, nên máy làm 1% hash mạng có thể kỳ vọng 1% khối về lâu dài.
- **Phần thưởng khối được đặt theo độ cao.** Mọi khối trong cùng tháng 30 ngày gồm 8.640 khối trả cùng phần thưởng, nên trong bất kỳ ngày nào thuộc tháng đó, phần thưởng là hằng số đã biết.

Kết quả là phần thưởng gộp kỳ vọng, lấy trung bình theo thời gian.

## Tìm từng dữ liệu ở đâu?

### Hashrate của bạn

seine hiển thị hashrate trên bảng điều khiển terminal ngay khi bắt đầu. Hãy cho vài phút: seine tinh chỉnh số luồng và kernel CPU trên công việc đào thật; số liệu ổn định khi tìm được cài đặt tốt nhất cho máy.

Khi đào trong pool, thẻ **Hashrate** trên [My Stats](/stats) hiển thị hashrate pool thấy, trung bình trong giờ qua. Để ước tính, dùng số ổn định của seine hoặc biểu đồ 24 giờ của pool.

### Hashrate mạng

Thẻ **Network Hashrate** hiển thị hashrate cả mạng. Nó dùng KH/s, nên 62,33 KH/s nghĩa là 62.330 H/s. Đổi về H/s trước khi chia, nếu không kết quả sai gấp nghìn lần.

Pool đo giá trị này từ công việc và dấu thời gian mười khối gần nhất, nên đổi từng khối; [hashrate là gì?](/blog/vi/what-is-hashrate/) giải thích phép tính và lý do thay đổi. Để có dữ liệu ổn định hơn, kiểm tra vài lần trong ngày rồi lấy trung bình. Bạn cũng có thể đọc H/s từ API:

```sh
curl -s https://bntpool.com/api/stats
```

Phần `chain` trong phản hồi có `network_hashrate` theo H/s và `current_job_height`, khối pool đang làm việc.

### Phần thưởng khối

Thẻ **Current Block** hiển thị độ cao mạng đang làm việc. Chia cho 8.640, làm tròn xuống để được tháng, rồi tìm trong [lịch phần thưởng khối Blocknet](/blog/vi/blocknet-block-reward-schedule/). [Trang khối](/blocks) là cách kiểm tra chéo nhanh: cột Reward hiển thị toàn bộ phần thưởng mỗi khối pool tìm thấy.

## Ví dụ ngày 25 tháng 10 năm 2026

<Note>
Ví dụ là ảnh chụp giá trị trực tiếp từ https://bntpool.com/api/stats lúc 02:03 EDT ngày 25 tháng 10 năm 2026. Hashrate mạng và phần thưởng khối thay đổi, nên hãy điền giá trị hôm nay từ bảng điều khiển trước khi dựa vào kết quả.
</Note>

| Dữ liệu | Giá trị lúc chụp |
|---|---|
| Khối hiện tại | 69.020 (tháng 7, vì 69.020 ÷ 8.640 = 7,99) |
| Phần thưởng khối | 467,67 BNT |
| Hashrate mạng | 62.329 H/s (hiển thị 62,33 KH/s) |
| Hashrate bntpool.com | 5.240 H/s |

Lấy máy chủ AMD EPYC Zen 5 từ benchmark seine, ở 49,4 H/s:

1. Tỷ lệ mạng: 49,4 ÷ 62.329 = 0,0793%.
2. Khối kỳ vọng mỗi ngày: 0,000793 × 288 = 0,2283.
3. BNT kỳ vọng mỗi ngày: 0,2283 × 467,67 = khoảng 106,8 BNT.

Tháng 8 bắt đầu ở độ cao 69.120, khoảng 100 khối sau ảnh chụp, và trả 439,46 BNT mỗi khối. Với cùng hashrate mạng, ước tính của cùng máy chủ thành 0,2283 × 439,46 = khoảng 100,3 BNT mỗi ngày. Vì vậy nên kiểm tra phần thưởng đầu mỗi tháng.

Đây là cùng phép tính, với hashrate mạng lúc chụp, cho kết quả tốt nhất seine đo ở từng thế hệ phần cứng:

| Phần cứng | Luồng | Hashrate | Tỷ lệ mạng | BNT/ngày ở 467,67 | BNT/ngày ở 439,46 |
|---|---|---|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s | 0,0793% | 106,8 | 100,3 |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s | 0,0573% | 77,1 | 72,5 |
| Apple M4 Max (MacBook Pro 48 GB) | 16 | 30,9 H/s | 0,0496% | 66,8 | 62,7 |
| Máy chủ Arm (AWS Graviton4) | 16 | 21,5 H/s | 0,0345% | 46,5 | 43,7 |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s | 0,0210% | 28,3 | 26,6 |

Con số máy riêng mới quan trọng. Chạy seine một lúc, lấy hashrate ổn định và dùng thay bảng. Hướng dẫn [phần cứng đào Blocknet và hashrate](https://seineminer.com/vi/blog/blocknet-mining-hardware-hashrate/) giải thích vì sao băng thông bộ nhớ và RAM quyết định số liệu; kết quả đầy đủ ở [trang hiệu năng](https://github.com/zpalmtree/seine/blob/master/docs/PERFORMANCE.md) của seine.

## Đào trên bntpool.com thay đổi ước tính thế nào?

Phần kỳ vọng trong pool giữ nguyên. Cách nhận thưởng mới thay đổi. bntpool.com trả theo PPLNS: phần thưởng mỗi khối chia cho share gửi trong sáu giờ trước đó, tỷ lệ theo độ khó. [Thanh toán PPLNS hoạt động thế nào](/blog/vi/how-pplns-payouts-work/) trình bày cách chia.

Bạn có thể tính từ phía pool và nhận cùng kết quả. Lúc chụp, 5.240 H/s của pool bằng 8,4% mạng, nên kỳ vọng 288 × 5.240 ÷ 62.329 = khoảng 24,2 khối mỗi ngày, xấp xỉ một khối mỗi giờ. Bảng điều khiển hiển thị dưới **Expected Block Time** trong Round Progress. Máy chủ 49,4 H/s bằng 0,94% hashrate pool; 0,94% của 24,2 khối ở 467,67 BNT cũng bằng khoảng 106,8 BNT mỗi ngày. Hashrate pool triệt tiêu trong phép tính.

Khác biệt là thời điểm. Đào một mình, máy chủ Zen 5 đó kỳ vọng một khối trọn vẹn khoảng mỗi 4,4 ngày; [đào Blocknet bằng pool hay solo](/blog/vi/pool-vs-solo-mining-blocknet/) so sánh kỹ hơn. Trên bntpool.com, máy nhận phần từ mỗi trong khoảng 24 khối pool tìm hàng ngày, nên số dư tăng từng bước nhỏ suốt ngày đêm. Phí pool và phí nhà phát triển của seine được trừ khỏi số gộp công thức trả về.

Hai chi tiết pool định hình ngày đầu:

- **Cửa sổ đầy dần trong sáu giờ.** Phần của người mới ở mỗi khối tăng khi share lấp đầy cửa sổ PPLNS sáu giờ, rồi giữ ở toàn bộ tỷ lệ miễn là tiếp tục đào.
- **Phần thưởng trưởng thành trước thanh toán.** Mỗi phần thưởng khối chuyển sang Confirmed Rewards sau 60 xác nhận, khoảng năm giờ; pool tự động gửi số dư khi đạt 0,1 BNT.

## Kiểm tra ước tính với kết quả thật thế nào?

Ước tính là trung bình; từng ngày dao động quanh nó khi may mắn của pool thay đổi qua các vòng. Qua nhiều vòng, effort trung bình về khoảng 100%, nên một tuần đào đều là khoảng tốt để so. Ghi tổng Confirmed Rewards và Paid Balance trên [My Stats](/stats) đầu tuần và cuối tuần. Phần tăng là số pool ghi cho bạn từ các khối trưởng thành trong tuần; so với ước tính cùng ngày, dùng phần thưởng và hashrate mạng của những ngày đó.

Tính lại khi một dữ liệu thay đổi:

- **Mỗi tháng mới**, khi phần thưởng giảm. Ranh giới tháng xuất hiện mỗi 8.640 khối, khoảng mỗi 30 ngày.
- **Khi hashrate mạng thay đổi**, do thợ đào tham gia.
- **Khi hashrate riêng thay đổi**, sau khi thêm máy, dành huge pages trên Linux hoặc thêm GPU NVIDIA mà seine đào song song CPU.

<Note>
Hashrate trong hướng dẫn mô tả Proof of Work Argon2id hiện tại của Blocknet. Theo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), Blocknet dự kiến nâng cấp mạng tên Faraday vào khoảng ngày 16 tháng 2 năm 2027 và dự kiến thay đổi Proof of Work. Hãy cập nhật seine: phần mềm kiểm tra bản phát hành mới khi khởi động và mỗi 12 giờ.
</Note>

<CTA target="seine" title="Đo hashrate của bạn" text="Tải seine, để tự tinh chỉnh vài phút và dùng hashrate trong công thức với các giá trị hôm nay từ bảng điều khiển." />
