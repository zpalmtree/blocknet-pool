---
title: 'Xác nhận khối và phần thưởng đào chưa trưởng thành'
description: 'Theo dõi khối Blocknet từ chờ xác nhận đến phần thưởng trưởng thành và thanh toán. Hiểu cách bntpool đếm độ sâu và vì sao thời gian chỉ là ước tính.'
tags: ['Phần thưởng đào', 'Xác nhận', 'Blocknet']
faq:
  - q: 'bntpool cần bao nhiêu xác nhận?'
    a: 'Khi kiểm tra ngày 26/10/2026, yêu cầu công khai là 60 khối. Pool lấy chiều cao chuỗi hiện tại trừ chiều cao khối đã tìm và đối chiếu hash với node. Xem Get Started hoặc API thông tin để biết thiết lập hiện tại.'
  - q: '60 khối có đúng năm giờ không?'
    a: 'Không. Với mục tiêu năm phút, 60 khoảng là 300 phút, tức năm giờ để lập kế hoạch. Tiến độ thực phụ thuộc các khối đến không đều. Trưởng thành, ngưỡng thanh toán và giao dịch thanh toán là các giai đoạn riêng.'
  - q: 'Có phải tiếp tục đào để thưởng cũ trưởng thành?'
    a: 'Khối đã tìm tăng độ sâu khi mạng thêm khối, dù máy bạn ngắt kết nối. Quyền nhận thưởng theo quy tắc kế toán pool. Dừng không tăng tốc hay đặt lại độ trưởng thành; theo dõi trong My Stats.'
sourceHash: '7383b256ba1a'
translation: draft
---

Theo dõi khối Blocknet qua xác nhận, trưởng thành và thanh toán vào ví. Bài này giải thích độ sâu chuỗi phía sau thẻ thưởng bntpool. Xem [PPLNS](/blog/vi/how-pplns-payouts-work/) để biết chia thưởng, hoặc [thiết lập pool](/blog/vi/mine-blocknet-on-bntpool/) để gửi công việc.

## Xác nhận và phần thưởng chưa trưởng thành là gì?

<Answer>
Xác nhận mô tả vị trí khối bên dưới đầu chuỗi hiện tại. Phần thưởng chưa trưởng thành thuộc khối mới chưa đạt độ sâu cần thiết. bntpool kiểm tra độ sâu và danh tính khối trước khi xác nhận; điều kiện số dư và thanh toán diễn ra riêng.
</Answer>

*Khối tìm được tăng độ sâu nhờ các khối sau, đạt mốc trưởng thành rồi chuyển qua số dư đủ điều kiện đến khoản trả ví riêng.*

## bntpool đếm độ sâu thế nào?

Pool lấy chiều cao chuỗi trừ chiều cao khối đã tìm. Yêu cầu công khai ngày 26/10/2026 là 60 khối. Theo quy ước này, khối bắt đầu ở độ sâu không. Trang khác có thể tính chính khối đó là xác nhận đầu; so sánh cả quy tắc đếm lẫn số hiển thị.

Ví dụ khối ở 70.000 có độ sâu không khi đầu chuỗi là 70.000, một tại 70.001 và 60 tại 70.060. Đây là minh họa, không phải dữ liệu trực tiếp. Pool cũng đối chiếu hash với node. [Mã triển khai](https://github.com/zpalmtree/blocknet-pool/blob/b002c96/crates/pool-runtime/src/payout.rs) ghi rõ hai kiểm tra.

## Tại sao năm giờ chỉ là ước tính?

Blocknet đặt mục tiêu năm phút giữa các khối mạng. Sáu mươi khoảng nhân năm phút bằng 300 phút, hay năm giờ. Phép tính giúp lập kế hoạch, nhưng điều kiện trưởng thành là số khối. Quan sát chuỗi tiến đến độ sâu yêu cầu, không phải đồng hồ đạt hạn cố định.

Mọi khối trên chuỗi được chấp nhận đều tăng độ sâu; không cần tất cả do bntpool tìm. Tốc độ máy bạn không tạo đồng hồ xác nhận riêng. Thời điểm khối đến thay đổi. [Hướng dẫn độ khó](/blog/vi/mining-difficulty-explained/) giải thích điều chỉnh; [hướng dẫn may mắn](/blog/vi/mining-pool-luck-explained/) giải thích biến động vòng đào.

## Phân biệt chờ, xác nhận và đã trả

Khối pool mới hiện chờ khi tích lũy độ sâu. Phần ước tính có thể hiện trong [My Stats](/stats) nếu bạn có công việc đủ điều kiện. Thưởng khối tạo coin mới; quy tắc trưởng thành coinbase của ví Blocknet dùng độ sâu 60 khối trước khi đầu ra có thể chi tiêu. Pool chờ tương ứng để kế toán theo tiến trình này.

Sau kiểm tra độ sâu và danh tính, pool có thể xác nhận và phân phối thưởng đủ điều kiện vào số dư. Không phải mọi địa chỉ được trả ngay lúc đó. Quy trình thanh toán xét số dư đã xác nhận và điều kiện gửi riêng. Phân biệt trạng thái khối, số dư xác nhận và lịch sử trả.

## Tại sao kiểm tra hash khối?

Chiều cao là vị trí trên chuỗi, hash là danh tính khối cụ thể. Node có thể thấy khối cạnh tranh; chuỗi được chấp nhận quyết định khối nào ở vị trí đó. Pool hỏi node về khối ở chiều cao đã ghi rồi so hash. Độ sâu riêng chưa chứng minh khối gốc vẫn thuộc chuỗi.

Khối rời chuỗi có thể bị đánh dấu mồ côi, và thưởng tạm tính không còn đủ điều kiện như thưởng của chuỗi được chấp nhận. Pool xử lý trạng thái này riêng. Dùng liên kết hash trên [trang khối](/blocks); hai mục cùng chiều cao chưa chắc là cùng sự kiện. Thưởng ước tính luôn gắn với khối của nó.

## Thanh toán khác thưởng đào ra sao?

Thưởng đào trước hết thuộc đầu ra coinbase của khối. Thanh toán pool là giao dịch sau, gửi số dư đủ điều kiện đến địa chỉ nhận. Tại ngày kiểm tra, mức tối thiểu là 0,1 BNT. Trưởng thành và đạt ngưỡng số dư là hai điều kiện; [PPLNS](/blog/vi/how-pplns-payouts-work/) giải thích chi tiết.

Sau khi gửi, theo dõi giao dịch và trạng thái nhận/có thể chi tiêu trong ví. Lịch sử xác nhận đó thuộc giao dịch thanh toán, không thuộc khối pool ban đầu. Không cần tạo địa chỉ mới hay kết nối lại chỉ vì khối đang trưởng thành. Dùng đúng địa chỉ khi so [lịch sử thanh toán](/payouts) với ví.

## Theo dõi thưởng từ đầu đến cuối

Trên [Blocks](/blocks), tìm chiều cao, hash và trạng thái. Mở [My Stats](/stats) với địa chỉ máy đào dùng, xem phần thưởng ước tính và đã xác nhận. Khi có thanh toán, mở liên kết giao dịch từ lịch sử. Khối, số dư tài khoản và khoản trả ví là những quan sát khác nhau.

Xem thiết lập hiện tại ở [Get Started](/start) hoặc `https://bntpool.com/api/info`, tình trạng dịch vụ ở [pool status](/status). Khi cần kiểm tra, ghi địa chỉ, mã khối/giao dịch và thời điểm; không chia sẻ từ khôi phục ví. [Hướng dẫn theo dõi](/blog/vi/track-your-miner-on-bntpool/) giải thích số liệu khác. Khối tiếp tục sâu hơn khi máy ngoại tuyến; quyền nhận theo công việc đã ghi và chuỗi.

<CTA target="pool" title="Theo dõi phần thưởng đào" text="Kết nối bntpool.com và dùng My Stats để theo dõi thưởng ước tính, số dư đã xác nhận và thanh toán." />
