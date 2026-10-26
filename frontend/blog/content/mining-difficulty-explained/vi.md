---
title: 'Độ khó đào: mạng và share'
description: 'Hiểu mục tiêu độ khó đào, cách Blocknet điều chỉnh và vì sao độ khó mạng cùng độ khó share tác động đến các số liệu khai thác khác nhau của bạn.'
tags: ['Kiến thức đào', 'Độ khó', 'Blocknet']
faq:
  - q: 'Độ khó cao hơn có làm giảm hashrate không?'
    a: 'Độ khó đổi mục tiêu kết quả phải đạt, không trực tiếp đổi số hash phần cứng tính mỗi giây. Với hashrate giữ nguyên, độ khó mạng cao làm dài khoảng chờ trung bình giữa các khối. Kiểm tra hiệu năng riêng với tần suất khối hoặc share.'
  - q: 'Share khó hơn có trả nhiều hơn không?'
    a: 'Nó đại diện cho nhiều công việc kỳ vọng hơn nhưng được gửi ít thường xuyên hơn ở cùng hashrate. So sánh công việc có trọng số độ khó trong cùng kỳ. Tại bntpool.com, phần thưởng còn phụ thuộc khối được chấp nhận và PPLNS, không chỉ số share.'
  - q: 'Blocknet điều chỉnh độ khó bao lâu một lần?'
    a: 'Sau mỗi khối, Blocknet tính độ khó khối kế tiếp từ lịch sử gần đây. Quy tắc hiện tại dùng trung bình trượt trọng số tuyến tính của 60 khoảng thời gian, nhắm 300 giây mỗi khối. Khoảng mới có trọng số lớn hơn; từng khối vẫn có thể đến sớm hoặc muộn.'
sourceHash: '5c203e8c7523'
translation: draft
---

Độ khó nối công việc của phần cứng với khối được mạng chấp nhận. Hướng dẫn giải thích mục tiêu, điều chỉnh Blocknet và độ khó share riêng. Hiểu thay đổi này trước khi so sánh hashrate, thời gian khối hoặc [công cụ tính thưởng](/blog/vi/estimate-blocknet-mining-rewards/#mining-calculator).

## Độ khó đào là gì?

<Answer>
Độ khó mô tả lượng công việc băm kỳ vọng để tìm kết quả đạt mục tiêu. Càng cao, kết quả đạt chuẩn càng thưa ở cùng hashrate. Với Blocknet, độ khó mạng gần bằng số hash kỳ vọng mỗi khối; độ khó share đo một mục tiêu gửi riêng.
</Answer>

Mỗi hash tạo một số cần đạt ngưỡng mục tiêu. Mục tiêu nhỏ chấp nhận phần ít hơn của các kết quả có thể, nên khó hơn. Blocknet chia giá trị 256 bit lớn nhất cho độ khó để lấy mục tiêu 256 bit. Gấp đôi độ khó gần như gấp đôi công việc kỳ vọng mà không đổi phép băm.

## Blocknet điều chỉnh độ khó mạng thế nào?

Tính sau mỗi khối. Quy tắc mainnet hiện tại dùng trung bình trượt trọng số tuyến tính, LWMA, của 60 khoảng gần nhất, ưu tiên khoảng mới. Mục tiêu là 300 giây mỗi khối. Kết hợp độ khó cũ và thời gian có trọng số khiến khối liên tục nhanh thường tăng độ khó, còn chậm thường giảm.

Mã giới hạn khoảng thời gian dùng trong phép tính và áp dụng độ khó tối thiểu. 60 khoảng cần 61 dấu thời gian; không có nghĩa chỉ điều chỉnh mỗi 60 khối. Quy tắc giữ nhịp chuỗi gần mục tiêu khi sức mạnh băm thay đổi. Năm phút là trung bình; một khối có thể sớm hoặc muộn hơn nhiều.

## Độ khó mạng và độ khó share

Pool giao mục tiêu share để quan sát đóng góp giữa các lần tìm khối. Nó thường dễ hơn mục tiêu mạng. Cả hai mô tả công việc kỳ vọng nhưng trả lời câu hỏi khác nhau và do các phần khác nhau của hệ thống đặt:

| Độ khó | Điều khiển gì |
|---|---|
| Mạng | Ngưỡng công việc của khối ứng viên theo quy tắc đồng thuận. |
| Share | Ngưỡng gửi pool giao cho kết nối hoặc công việc đào. |

bntpool.com dùng độ khó biến đổi, vardiff, để điều chỉnh tần suất gửi. Ở cùng hashrate, độ khó cao thường cho ít share hơn, mỗi share mang nhiều công việc kỳ vọng hơn. Nó không đổi độ khó mạng. Kết quả đạt cả mục tiêu mạng có thể thành khối ứng viên; daemon vẫn xác thực khối. [Hướng dẫn share](/blog/vi/accepted-rejected-stale-shares/) giải thích chấp nhận và từ chối.

## Độ khó ảnh hưởng thời gian khối thế nào?

Với Blocknet, chia độ khó mạng cho hashrate đang làm việc tính bằng H/s để ước tính khoảng trung bình bằng giây. Dùng dữ liệu cùng kỳ. Ví dụ giữ hai biến cố định để minh họa phép tính; không phải số liệu hiện tại hoặc dự báo cho máy cụ thể.

**Số giây kỳ vọng mỗi khối ≈ độ khó mạng ÷ hashrate tính bằng H/s**

| Độ khó mạng | Hashrate | Khoảng kỳ vọng |
|---|---|---|
| 30.000 | 100 H/s | 300 s |
| 60.000 | 100 H/s | 600 s |
| 60.000 | 200 H/s | 300 s |

Ở độ khó 30.000 và 100 H/s, trung bình là 300 giây. Tăng lên 60.000 với cùng hashrate thành 600 giây. Nếu hashrate cũng gấp đôi lên 200 H/s, lại là 300 giây. Đây là trung bình qua nhiều lần thử. [Hướng dẫn may mắn pool](/blog/vi/mining-pool-luck-explained/) giải thích độ lệch của từng vòng.

## Vì sao thưởng đổi khi hashrate ổn định?

Với hashrate riêng cố định, độ khó mạng cao giảm số khối kỳ vọng theo thời gian trong lúc nó duy trì. Nếu hashrate tổng tăng và độ khó theo sau, phần đóng góp của bạn giảm. Thưởng mỗi khối là yếu tố khác: [lịch thưởng](/blog/vi/blocknet-block-reward-schedule/) thay đổi độc lập. Độ khó không đặt số BNT của thưởng khối theo lịch.

Dùng dữ liệu mới trong công cụ tính để xem BNT kỳ vọng. Xác định hashrate từ phần mềm hay pool nhằm xử lý phí miner đúng, theo bản đang dùng. Ước tính là trung bình dưới các giả định. Thanh toán còn tùy khối tìm được, xác nhận và [PPLNS](/blog/vi/how-pplns-payouts-work/); độ khó đơn lẻ không cho biết lúc nhận tiền.

## Cần kiểm tra độ khó nào?

Bắt đầu từ câu hỏi. Với khối, dùng độ khó mạng và hashrate máy hoặc pool tương ứng. Với tần suất share, dùng độ khó công việc đó. Với số dư và thanh toán, xem [My Stats](/stats) và [lịch sử chi trả](/payouts). Giữ thời điểm, phạm vi và đơn vị cùng mỗi giá trị so sánh.

[Bảng pool](/) và [lịch sử khối](/blocks) cho ngữ cảnh hiện tại. Hashrate mạng được ước tính từ công việc gần đây và thời gian khối đã qua nên có thể khác độ khó kế tiếp chia cho 300 giây. [Hướng dẫn hashrate](/blog/vi/what-is-hashrate/) giải thích điểm này. Phân biệt độ khó, hashrate và may mắn khi chẩn đoán: chúng mô tả khía cạnh khác nhau.

<CTA target="pool" title="Kết nối máy đào với bntpool.com" text="Dùng trang thiết lập rồi theo dõi share và hoạt động qua My Stats." />
