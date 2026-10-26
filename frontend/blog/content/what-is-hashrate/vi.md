---
title: Hashrate trong đào tiền mã hóa là gì?
description: Hashrate là số hash trình đào tính mỗi giây. Tìm hiểu đơn vị, khác biệt giữa hashrate của bạn, pool và mạng, cùng lý do từng số thay đổi.
tags: [Hashrate, Kiến thức đào coin, Blocknet, bntpool]
sourceHash: '5af3a8b3f18b'
translation: draft
faq:
  - q: 'Máy tính có thể đạt hashrate bao nhiêu khi đào Blocknet?'
    a: 'Trong đo đạc của seine, kết quả tốt nhất mỗi thế hệ phần cứng là 49,4 H/s cho máy chủ AMD EPYC Zen 5, 35,7 H/s cho AMD EPYC Zen 4, 30,9 H/s cho laptop Apple M4 Max và 21,5 H/s cho máy chủ Arm AWS Graviton4. Kết quả riêng phụ thuộc chủ yếu băng thông bộ nhớ và số luồng 2 GB mà RAM chứa được.'
  - q: 'Vì sao hashrate Blocknet đo bằng H/s thay vì MH/s hoặc TH/s?'
    a: 'Mỗi hash Blocknet là một phép tính Argon2id ghi khoảng 2 GiB bộ nhớ rồi đọc lại, nên ngay máy chủ lớn cũng chỉ hoàn tất vài chục hash mỗi giây. Proof of Work dựa trên hash nhanh như SHA-256 đạt số lớn hơn nhiều nên dùng đơn vị lớn hơn. Chỉ so hashrate trong cùng thuật toán.'
  - q: 'Vì sao pool hiển thị hashrate khác trình đào?'
    a: 'Trình đào đếm mọi hash hoàn tất. Pool chỉ thấy share bạn gửi và ước tính hashrate từ độ khó trong giờ qua. Share đến ngẫu nhiên, nên số của pool dao động nhiều hơn rồi ổn định theo thời gian. So biểu đồ 24 giờ trên My Stats với trung bình trình đào để đối chiếu công bằng.'
  - q: 'Hashrate cao hơn có nghĩa nhiều phần thưởng hơn không?'
    a: 'Có, theo tỷ lệ. Mọi hash có cùng xác suất tìm khối, nên phần khối kỳ vọng của bạn bằng tỷ lệ hashrate trong mạng. Gấp đôi hashrate làm phần thưởng kỳ vọng gấp đôi, còn hashrate mạng tăng làm tỷ lệ của bạn nhỏ đi. Trên bntpool.com, phần đó đến dưới dạng một phần mỗi khối pool tìm được.'
---

Hashrate là số mà mọi trình đào, pool và trình khám phá khối hiển thị đầu tiên. Hướng dẫn này giải thích nó đo gì, đọc đơn vị từ H/s trở lên thế nào, hashrate riêng liên hệ với pool và mạng ra sao, và vì sao từng số đổi từng phút, lấy Blocknet và bảng điều khiển bntpool.com làm ví dụ.

## Hashrate là gì?

<Answer>
Hashrate là số hash trình đào tính mỗi giây. Trong đào Proof of Work, mỗi hash là một lần đoán lời giải khối, và mọi lần đoán có cùng cơ hội thắng nhỏ. Hashrate cao hơn nghĩa là nhiều lần đoán hơn mỗi giây, nên phần khối kỳ vọng tăng theo tỷ lệ của bạn trong tổng hashrate.
</Answer>

Chuỗi Proof of Work yêu cầu thợ đào tìm tiêu đề khối có hash thấp hơn mục tiêu. Không có đường tắt: trình đào thay số gọi là nonce trong tiêu đề, băm kết quả, so với mục tiêu rồi lặp lại. Hashrate đếm số lần thử máy hoàn tất mỗi giây.

Trong Blocknet, mỗi lần thử là một hash Argon2id. Nonce được dùng làm mật khẩu, phần còn lại của tiêu đề khối làm salt, và hàm xử lý 2 GiB bộ nhớ trước khi tạo kết quả 32 byte. seine và pool đều báo hashrate bằng hash hoàn tất mỗi giây, nên 1 H/s nghĩa là một phép tính Argon2id đầy đủ mỗi giây.

## H/s, kH/s và MH/s nghĩa là gì?

Đơn vị hashrate theo tiền tố hệ mét thông thường, mỗi bậc gấp nghìn lần bậc trước:

| Đơn vị | Hash mỗi giây | Cách dùng thường gặp |
|---|---|---|
| H/s | 1 | Một máy tính trên Blocknet |
| kH/s (KH/s) | 1.000 | Tổng pool và mạng Blocknet |
| MH/s | 1.000.000 | Thuật toán Proof of Work nhanh hơn |
| GH/s | 1.000.000.000 | Hash nhanh như SHA-256 |
| TH/s trở lên | 1.000.000.000.000 trở lên | Tổng mạng của hash nhanh |

Bảng điều khiển bntpool.com viết đơn vị nghìn là KH/s và chuyển lên một đơn vị khi giá trị vượt 1.000, nên 44,44 KH/s nghĩa là 44.440 H/s. Khi chia hai hashrate, hãy đổi cả hai về H/s trước.

## Vì sao hashrate Blocknet nhỏ như vậy?

Proof of Work của Blocknet là memory-hard. Mỗi hash ghi khoảng 2 GiB bộ nhớ và đọc lại khoảng 2 GiB, lớn hơn nhiều cache của bất kỳ bộ xử lý nào, nên tốc độ hệ thống bộ nhớ quyết định nhịp độ. Mỗi luồng đào cần 2 GiB riêng, vì vậy RAM quyết định máy chạy được bao nhiêu luồng.

Đây là kết quả tốt nhất seine đo ở từng thế hệ phần cứng:

| Phần cứng | Luồng | Hashrate |
|---|---|---|
| AMD EPYC · Zen 5 (EPYC 9R45) | 32 | 49,4 H/s |
| AMD EPYC · Zen 4 (EPYC 9R14) | 24 | 35,7 H/s |
| Apple M4 Max (MacBook Pro 48 GB) | 16 | 30,9 H/s |
| Máy chủ Arm (AWS Graviton4) | 16 | 21,5 H/s |
| Intel Xeon (Sapphire Rapids 8488C) | 12 | 13,1 H/s |

Ngay máy chủ Zen 5 với 32 luồng cũng chỉ hoàn tất khoảng 1,5 hash mỗi luồng mỗi giây. Thuật toán dựa trên hash nhanh tạo số lớn hơn nhiều: wiki Bitcoin ghi nhận máy đào Bitcoin dùng SHA-256 kép đã được xếp hạng hàng tỷ hash mỗi giây từ năm 2014. Hai số đo lượng công việc khác nhau mỗi hash, nên hashrate chỉ có ý nghĩa khi đặt cạnh số của cùng thuật toán. [Giải thích Proof of Work Argon2id](https://seineminer.com/vi/blog/argon2id-proof-of-work-explained/) trình bày sâu thuật toán; [hướng dẫn phần cứng đào Blocknet](https://seineminer.com/vi/blog/blocknet-mining-hardware-hashrate/) cho thấy băng thông bộ nhớ và RAM quyết định kết quả thế nào.

## Hashrate của bạn, pool và mạng

Ba hashrate quan trọng với người đào trong [pool](/blog/vi/what-is-a-mining-pool/), và mỗi số được đo khác nhau.

### Hashrate của bạn

seine đếm mọi hash máy hoàn tất và hiển thị hai số trên bảng điều khiển: **Current**, tốc độ ngay lúc này, và **Average**, tốc độ trong cả phiên. Để đo ổn định khả năng máy, dùng trung bình sau khi seine có vài phút tinh chỉnh số luồng và kernel CPU trên công việc đào thực.

Pool không thấy các hash của bạn. Nó thấy share bạn gửi, tức hash vượt mục tiêu dễ hơn đặt cho kết nối. bntpool.com ước tính hashrate từ tổng độ khó share được chấp nhận trong giờ qua và hiển thị trên [My Stats](/stats). [Theo dõi trình đào trên bntpool.com](/blog/vi/track-your-miner-on-bntpool/) hướng dẫn trang này.

### Hashrate pool

Thẻ **Pool Hashrate** cộng công việc share của mọi thợ đào kết nối trong cùng cửa sổ một giờ. Nó cho biết bntpool.com làm bao nhiêu công việc mạng và từ đó kỳ vọng tìm khối thường xuyên thế nào; bảng điều khiển hiển thị dưới tên **Expected Block Time**.

### Hashrate mạng

Không ai đếm được mọi hash trong mạng, nên hashrate mạng được tính từ khối. Độ khó là số hash kỳ vọng cần để tìm khối, và Blocknet nhắm một khối mỗi 300 giây.

## Hashrate mạng được tính thế nào?

<Answer>
Hashrate mạng được ước tính từ độ khó và thời gian khối. Trong Blocknet, độ khó gần bằng số hash kỳ vọng mỗi khối, còn mục tiêu là 300 giây, nên hashrate mạng xấp xỉ độ khó chia 300. Bảng điều khiển bntpool.com tính từ công việc thực và dấu thời gian mười khối gần nhất.
</Answer>

Đây là phép tính với giá trị trực tiếp đọc ngày 25 tháng 10 năm 2026 lúc 02:26 EDT. [Trình khám phá Blocknet](https://explorer.blocknetcrypto.com) hiển thị độ khó tiếp theo 16.943.147, nên mạng được đặt để cần khoảng 16,9 triệu hash mỗi khối. Chia 300 giây được khoảng 56.477 H/s.

Cùng lúc, bảng điều khiển bntpool.com hiển thị hashrate mạng 44,44 KH/s, tức 44.440 H/s. Cả hai ước tính cùng một thứ từ các góc khác nhau. Giá trị độ khó là tốc độ tạo khối đúng mục tiêu năm phút. Giá trị bảng điều khiển chia công việc thực của mười khối cuối cho thời gian thực đã dùng, nên khi các khối gần đây lâu hơn năm phút thì số đọc thấp hơn. Blocknet điều chỉnh độ khó sau mỗi khối, giúp hai số gần nhau theo thời gian.

## Vì sao hashrate dao động?

Mỗi trong ba số thay đổi vì lý do riêng.

- **Máy của bạn.** Những phút đầu, seine so số luồng và kernel trong lúc đào rồi chọn nhanh nhất; tốc độ tăng khi tinh chỉnh. Sau đó, bất cứ tác vụ nào khác dùng bộ nhớ hoặc nhân máy tính đều có thể lấy bớt thời gian băm.
- **Góc nhìn pool về bạn.** Share là sự kiện ngẫu nhiên. bntpool.com đặt độ khó mỗi kết nối để nhắm khoảng 10 share mỗi năm phút, nên một giờ có khoảng 120 share; giai đoạn may mắn hoặc yên ắng làm ước tính thay đổi. Máy mới kết nối được lấy trung bình theo số phút đã đào cho đến khi đủ một giờ.
- **Mạng.** Thời gian khối cũng ngẫu nhiên, và mẫu mười khối là ngắn. Ngày 25 tháng 10 năm 2026, hashrate mạng trên bảng điều khiển khoảng 50.057 H/s lúc 00:26 EDT, 62.329 H/s lúc 02:03 và 44.440 H/s lúc 02:26. Thợ đào vào và rời mạng thêm thay đổi thực lên những nhiễu này.

Để quyết định, dùng trung bình: trung bình phiên seine, biểu đồ 24 giờ trên My Stats và giá trị mạng kiểm tra vài lần trong ngày.

## Hashrate có ý nghĩa gì với phần thưởng?

Vì mọi hash có cùng cơ hội, tỷ lệ hashrate mạng của bạn là tỷ lệ khối kỳ vọng. Blocknet tạo 288 khối mỗi ngày, nên người có 0,1% mạng có thể kỳ vọng phần thưởng trị giá khoảng 0,29 khối mỗi ngày về lâu dài. [Cách ước tính phần thưởng đào Blocknet](/blog/vi/estimate-blocknet-mining-rewards/) đổi thành BNT mỗi ngày với dữ liệu từ [bảng điều khiển](/).

Trên bntpool.com, phần kỳ vọng đến dưới dạng một phần mỗi khối pool tìm được, ghi theo share được chấp nhận, nên phần thưởng đến thành bước nhỏ suốt ngày đêm. Nếu chưa bắt đầu, [hướng dẫn cài seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) đưa bạn từ tải xuống đến share đầu trong vài phút.

<Note>
Hashrate trong hướng dẫn mô tả Proof of Work Argon2id hiện tại của Blocknet. Theo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), Blocknet dự kiến nâng cấp mạng tên Faraday vào khoảng ngày 16 tháng 2 năm 2027 và dự kiến thay đổi Proof of Work. Hãy cập nhật seine: phần mềm kiểm tra bản phát hành mới khi khởi động và mỗi 12 giờ.
</Note>

<CTA target="seine" title="Xem hashrate của bạn" text="Tải seine, để phần mềm tự tinh chỉnh vài phút và theo dõi hashrate trên bảng điều khiển cùng My Stats." />
