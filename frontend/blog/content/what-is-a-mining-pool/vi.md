---
title: Pool đào là gì?
description: Pool đào kết hợp hashrate nhiều thợ đào và chia phần thưởng mỗi khối. Tìm hiểu cách pool hoạt động, PPLNS, PPS, solo và tiêu chí lựa chọn.
tags: [Kiến thức đào coin, Pool đào, PPLNS, bntpool]
sourceHash: 'a17a47ab1d73'
translation: draft
faq:
  - q: 'Tham gia pool có tăng lượng coin tôi đào được không?'
    a: 'Pool giữ nguyên hashrate và tỷ lệ công việc của bạn trong mạng. Nó thay đổi cách phần thưởng đến với bạn. Đào một mình, bạn chờ cả khối ở các khoảng ngẫu nhiên. Trong pool, bạn nhận phần nhỏ từ mỗi khối pool tìm được, nên thanh toán đến thường xuyên với số tiền bám theo hashrate.'
  - q: 'PPLNS khác PPS thế nào?'
    a: 'PPLNS trả một phần từ mỗi khối pool thực sự tìm thấy, chia theo công việc thợ đào gửi trong cửa sổ gần đây. PPS trả số tiền cố định cho mỗi share được chấp nhận, tính theo giá trị kỳ vọng, dù pool có tìm khối hay không. bntpool.com dùng PPLNS với cửa sổ sáu giờ.'
  - q: 'Có cần chạy node riêng để đào trong pool không?'
    a: 'Không. Pool chạy node, tạo mẫu khối, kiểm tra share và gửi khối tìm được. Trên bntpool.com bạn chỉ cần trình đào seine cùng địa chỉ Blocknet hoặc tên Blocknet ID. Chạy seine, dán địa chỉ, chấp nhận pool mặc định; phần của bạn từ mỗi khối được trả về địa chỉ đó.'
  - q: 'Làm sao bắt đầu đào trên bntpool.com?'
    a: 'Tải seine cho Windows, macOS hoặc Linux từ seineminer.com, chạy và dán địa chỉ Blocknet hoặc @name khi được hỏi. bntpool.com là pool mặc định của seine, nên bạn chỉ cần chấp nhận và bắt đầu đào. Tra địa chỉ trên trang thống kê pool để xem hashrate, share và số dư.'
---

Hướng dẫn này giải thích pool đào là gì, vì sao đa số thợ đào tham gia và cách pool quyết định thu nhập của từng người. Bạn sẽ biết các phương thức trả thưởng phổ biến PPLNS, PPS và đào solo, cùng những điều cần kiểm tra trước khi trỏ trình đào đến pool. bntpool.com, pool đào Blocknet, được dùng làm ví dụ xuyên suốt.

## Pool đào là gì?

<Answer>
Pool đào là máy chủ kết hợp [hashrate](/blog/vi/what-is-hashrate/) của nhiều thợ đào để cùng tìm khối. Pool giao cho mỗi người công việc trên cùng khối, tính công việc từng người gửi và chia phần thưởng mỗi khối tỷ lệ với công việc đó. Thợ đào nhận thanh toán nhỏ, thường xuyên thay vì hiếm khi nhận cả khối.
</Answer>

Trong coin dùng Proof of Work, thợ đào cạnh tranh tìm khối tiếp theo bằng cách băm tiêu đề khối với các nonce khác nhau cho đến khi một kết quả thấp hơn mục tiêu mạng. Mỗi hash là lần thử độc lập với cùng xác suất thành công nhỏ; ai tìm hash thắng sẽ nhận phần thưởng khối.

Pool biến cuộc cạnh tranh thành công việc nhóm. Mọi người kết nối bntpool.com làm việc trên các khối pool tạo; khi một người tìm hash thắng, phần thưởng được chia cho tất cả thợ đào đã đóng góp công việc tìm nó.

## Vì sao thợ đào tham gia pool?

<Answer>
Thợ đào tham gia pool để nhận tiền đều đặn. Đào một mình, thợ đào thông thường chỉ nắm một phần rất nhỏ hashrate mạng và có thể chờ nhiều ngày giữa các khối, với khoảng cách ngẫu nhiên. Trong pool, hashrate kết hợp tìm khối thường xuyên và mỗi người nhận phần từ từng khối, nên thu nhập đều và bám theo hashrate.
</Answer>

Phép tính dễ kiểm tra trên Blocknet. Mạng nhắm một khối mỗi năm phút, tức 288 khối mỗi ngày; thợ đào riêng có thể kỳ vọng 288 × (hashrate của họ ÷ hashrate mạng) khối mỗi ngày.

Lấy số tròn giả định: dàn máy 50 H/s trong mạng 50.000 H/s nắm 0,1% mạng và tìm trung bình 0,288 khối mỗi ngày, hay một khối khoảng mỗi ba ngày rưỡi. Đó chỉ là trung bình. Tìm khối là ngẫu nhiên, nên khoảng cách thực giữa các khối của máy có thể ngắn hoặc dài hơn nhiều.

Đưa cùng máy vào pool tổng 5.000 H/s, pool sẽ tìm khoảng 28,8 khối mỗi ngày, trung bình một khối mỗi 50 phút. Máy làm 1% công việc pool nên nhận 1% mỗi khối đó. Công việc đóng góp không đổi; điều đổi là phần thưởng đến thành nhiều phần nhỏ trong ngày. Hướng dẫn [đào Blocknet bằng pool hay solo](/blog/vi/pool-vs-solo-mining-blocknet/) trình bày so sánh này cho hệ thống lớn hơn.

## Pool đào hoạt động thế nào?

Mọi pool theo cùng vòng lặp cơ bản. Trên bntpool.com, quy trình như sau:

1. **Pool tạo mẫu khối.** bntpool.com chạy node Blocknet riêng và lấy mẫu khối từ đó, nên thợ đào không cần tự chạy hoặc đồng bộ node.
2. **Gửi công việc cho thợ đào.** Thợ đào kết nối qua Stratum, giao thức pool dùng để gửi công việc, tại `stratum+tcp://bntpool.com:3333`. Pool gửi mỗi kết nối một job dựa trên mẫu hiện tại.
3. **Thợ đào băm và gửi share.** Share là hash đạt mục tiêu dễ hơn do pool đặt. Share chứng minh mỗi thợ đào làm bao nhiêu công việc.
4. **Share đạt cả mục tiêu mạng là một khối.** Pool kiểm tra từng share với cả hai mục tiêu. Khi đủ tốt cho mạng, pool gửi khối đến node.
5. **Phần thưởng được chia và trả.** Pool chia phần thưởng theo phương thức trả thưởng và gửi phần của mỗi người khi đã xác nhận và đạt mức thanh toán tối thiểu.

## Share trong pool đào là gì?

Share là hash vượt một ngưỡng dễ hơn khối thực. Khối cần hash thấp hơn mục tiêu mạng; share chỉ cần hash thấp hơn mục tiêu share dễ hơn nhiều của pool. Phần lớn share không bao giờ thành khối, nhưng chúng đều đặn cho pool biết mỗi người đang băm bao nhiêu.

Share được tính trọng số theo độ khó. Trong Blocknet, độ khó D nghĩa là kỳ vọng khoảng D hash trước khi có hash đạt mục tiêu, nên share độ khó 60 đại diện khoảng 60 hash công việc. bntpool.com đặt độ khó share cho từng kết nối và điều chỉnh khi chạy, nhắm khoảng 10 share mỗi năm phút, bắt đầu kết nối mới ở độ khó 60. Máy chủ nhanh nhận share khó hơn, laptop nhận share dễ hơn; cả hai được ghi đúng công việc thực làm. Pool còn nhớ độ khó từng máy một giờ, nên khởi động lại nhanh tiếp tục từ mức trước.

## Các phương thức trả thưởng pool phổ biến

Phương thức trả thưởng là quy tắc pool dùng để chuyển share thành thu nhập. Đây là những phương thức bạn thường gặp nhất.

| Phương thức | Được trả cho | Cách hoạt động |
|---|---|---|
| PPLNS (pay per last N shares) | Khối pool tìm được | Phần thưởng mỗi khối chia cho share gửi trong cửa sổ gần đây, theo độ khó share |
| PPS (pay per share) | Mỗi share được chấp nhận | Pool trả số tiền cố định mỗi share theo giá trị kỳ vọng, dù có tìm khối hay không |
| Solo | Khối bạn tự tìm | Đào với node riêng và giữ toàn bộ phần thưởng mỗi khối tự tìm |

**PPLNS** nhìn lại từ mỗi khối pool tìm thấy. Trên bntpool.com, “N share cuối” là cửa sổ sáu giờ: pool cộng độ khó mọi share được chấp nhận trong sáu giờ trước khối và cho mỗi người tỷ lệ phần thưởng bằng tỷ lệ công việc. Mỗi share được tính vào mọi khối tìm được khi nó còn trong cửa sổ, nên người đào đều luôn có cửa sổ đầy phía sau; khởi động lại không đặt lại gì. [Thanh toán PPLNS trên bntpool.com hoạt động thế nào](/blog/vi/how-pplns-payouts-work/) trình bày cách chia từng bước.

**PPS** trả cho từng share khi đến, dùng giá trị kỳ vọng: độ khó share chia độ khó mạng rồi nhân phần thưởng khối. Người vận hành pool chịu các dao động tìm khối và trả thợ đào từ số dư riêng trong thời gian chờ.

**Đào solo** bỏ qua pool. Node riêng tạo mẫu; mỗi khối tìm thấy trả toàn bộ thưởng về ví. Cách này hợp hệ thống đủ lớn để tự tìm khối thường xuyên. Trình đào seine hỗ trợ cả hai: mặc định đào trên bntpool.com, còn `./seine --mode daemon` đào với node Blocknet đã đồng bộ của bạn.

## Nên tìm gì ở một pool đào?

Khi so pool, những chi tiết sau quyết định cách được trả và mức dễ kiểm tra:

- **Phương thức trả thưởng rõ ràng.** Pool nên nêu phương thức và cài đặt như cửa sổ PPLNS. bntpool.com liệt kê trên [trang Get Started](/start) và công bố tại `https://bntpool.com/api/info`.
- **Xác nhận và mức thanh toán tối thiểu.** Phần thưởng thường chờ khối đủ sâu trong chuỗi; thanh toán gửi khi số dư vượt tối thiểu. Trên bntpool.com, phần thưởng khối xác nhận sau 60 khối, phù hợp độ trưởng thành coinbase của Blocknet; số dư từ 0,1 BNT tự động được trả.
- **Số liệu công khai, kiểm tra được.** Bạn nên thấy mọi khối pool tìm và mọi khoản nó gửi. bntpool.com hiển thị trên các trang [khối](/blocks), [thanh toán](/payouts), effort từng vòng trên [trang may mắn](/luck) và sức khỏe dịch vụ trên [trang trạng thái](/status).
- **Thống kê riêng.** Tra địa chỉ để xem hashrate, share và số dư. Trên bntpool.com đó là [trang thống kê](/stats); hướng dẫn [theo dõi trình đào](/blog/vi/track-your-miner-on-bntpool/) giải thích từng thẻ.
- **Thiết lập dễ dàng.** Cần ít cài đặt thì bắt đầu càng nhanh. bntpool.com là pool mặc định trong seine, vốn tự tinh chỉnh luồng, kernel và bộ nhớ; bạn có thể đăng nhập bằng [tên Blocknet ID](https://seineminer.com/vi/blog/blocknet-id-pay-by-name/) như `@name` thay địa chỉ đầy đủ.

## Tổng quan bntpool.com

| Cài đặt | bntpool.com |
|---|---|
| Phương thức trả thưởng | PPLNS |
| Cửa sổ share | 6 giờ |
| Xác nhận trước thanh toán | 60 khối |
| Thanh toán tối thiểu | 0,1 BNT |
| Địa chỉ Stratum | `stratum+tcp://bntpool.com:3333` |

[Bảng điều khiển pool](/) cho thấy tình hình trực tiếp: thợ đào kết nối, hashrate pool và mạng, khối đã tìm và effort vòng hiện tại. Ở ảnh chụp dữ liệu ngày 25 tháng 10 năm 2026 lúc 02:25 EDT, pool chạy khoảng 5.200 H/s trong mạng khoảng 44.400 H/s, với 246 thợ đào kết nối. Đó là khoảng 12% mạng, đủ kỳ vọng khoảng 34 khối mỗi ngày, một khối khoảng mỗi 42 phút. Số liệu đổi liên tục; xem bảng điều khiển cho giá trị hiện tại và dùng [hướng dẫn ước tính phần thưởng](/blog/vi/estimate-blocknet-mining-rewards/) để đổi thành BNT cho hashrate riêng.

Để tham gia, [tải seine](https://seineminer.com/vi/#download), chạy, dán địa chỉ Blocknet hoặc `@name` và chấp nhận pool mặc định. [Hướng dẫn đào bằng seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) trình bày từng bước; [đào Blocknet trên bntpool.com](/blog/vi/mine-blocknet-on-bntpool/) trình bày tên dàn máy và kiểm tra share đầu tiên.

<CTA target="pool" title="Tham gia bntpool.com" text="Trỏ seine đến pool và kiếm một phần từ mỗi khối pool tìm thấy, tự động gửi về địa chỉ khi đạt 0,1 BNT." />
