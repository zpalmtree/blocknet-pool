---
title: 'Phí pool và phí nhà phát triển trình đào'
description: 'Hiểu phí pool và trình đào Blocknet: nơi áp dụng, cách kết hợp và cách tránh trừ cùng một khoản phí hai lần.'
tags: ['Hướng dẫn đào', 'Phí pool', 'Phần thưởng']
faq:
  - q: 'Đây có phải cùng một khoản phí?'
    a: 'Không. Phí pool thuộc phân phối phần thưởng, phí nhà phát triển thuộc phần mềm. seine v0.2.36 dành 2% thời gian tính phí cho phát triển sau giai đoạn đầu miễn phí. Phí pool riêng của bntpool là 0% khi kiểm tra ngày 26 tháng 10 năm 2026.'
  - q: 'Kết hợp thế nào?'
    a: 'Nhân ước tính gộp với một trừ tỷ lệ trình đào, rồi một trừ tỷ lệ pool. Dùng phiên bản thực tế và thiết lập hiện tại. Đây là ước tính phần thưởng còn lại, không phải bảo đảm thanh toán hay lợi nhuận tiền mặt.'
  - q: 'Có trừ phí trình đào khỏi hashrate pool không?'
    a: 'Trước tiên xác định ý nghĩa số đó. Nếu đã ước tính công việc cho địa chỉ của bạn sau thời gian nhà phát triển, trừ nữa là tính hai lần. Máy tính cho nhập không với điều chỉnh đã phản ánh. Không trừ lại phí từ số dư xác nhận hoặc thanh toán đã ghi.'
sourceHash: '8d54fc4f307d'
translation: draft
---

Theo dõi hai lớp phí trong ước tính Blocknet. Hướng dẫn dùng seine v0.2.36 và thiết lập bntpool kiểm tra ngày 26 tháng 10 năm 2026 để nhập [máy tính](/blog/vi/estimate-blocknet-mining-rewards/#mining-calculator) và đọc [My Stats](/stats).

## Phí pool khác phí trình đào thế nào?

<Answer>
Phí pool thuộc phân phối phần thưởng; phí nhà phát triển thuộc lịch đào của phần mềm. Chủ thể và giai đoạn khác nhau. Kiểm tra cả hai, xác định đầu vào gộp hay đã điều chỉnh, rồi áp dụng mỗi điều chỉnh phù hợp một lần.
</Answer>

*Công việc đi qua điều chỉnh thời gian trình đào và điều chỉnh phần thưởng pool riêng trước số tiền ước tính còn lại.*

## Phí pool áp dụng vào đâu?

Pool tập hợp công việc đủ điều kiện và chia phần thưởng theo quy tắc kế toán. bntpool tính phí từ phần thưởng khối ghi nhận trước khi phân bổ phần còn lại qua PPLNS. [Hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/) giải thích tham gia và ghi có.

Ngày 26 tháng 10 năm 2026, `https://bntpool.com/api/info` công bố `pool_fee_pct` là 0,0, nghĩa là 0%. Kiểm tra giá trị mới trên [Get Started](/start) hoặc API khi cập nhật ước tính. Không phần trăm chỉ mô tả thiết lập pool này; chính sách phần mềm là đầu vào riêng.

## Phí nhà phát triển seine hoạt động thế nào?

v0.2.36 ghi rõ 2% trên mọi pool và chế độ solo. Đó là thời gian đào cho địa chỉ nhà phát triển, không phải rút ví sau đó. 10 phút đầu miễn phí; bộ theo dõi tính thời gian sau đó. Xem [hướng dẫn cấu hình](https://github.com/zpalmtree/seine/blob/v0.2.36/docs/guides/configuration.md).

Lịch chạy theo lượt nên khoảng ngắn không nhất thiết đúng 2%. Đây là mục tiêu dài hạn sau giai đoạn đầu, không phải cam kết mỗi phút hay khối. Ghi phiên bản vì tài liệu cũ hoặc phần mềm khác có thể khác. Phí pool không thay thế thông tin này.

## Kết hợp các phần trăm ra sao?

Dùng tỷ lệ giữ lại. Với m là tỷ lệ trình đào và p là tỷ lệ pool, ước tính giữ `(1 - m) * (1 - p)` phần gộp. [Bộ ước tính](/blog/vi/estimate-blocknet-mining-rewards/) dùng công thức này. Cộng tỷ lệ không phải quy tắc chung vì bước hai áp dụng trên phần còn lại.

Ví dụ thuần số học: 100 BNT gộp giả định, trình đào 2% và pool đã kiểm tra 0% cho 100 × 0,98 × 1,00 = 98 BNT. Đây là trạng thái ổn định, không phải đo máy, hứa phần thưởng hay mô phỏng phiên ngắn có miễn phí đầu kỳ.

## Chọn đúng hashrate đầu vào

Tốc độ cục bộ hoặc benchmark mô tả công việc máy. Trước hệ số nhà phát triển, xác định nó gồm toàn bộ thời gian hay chỉ công việc cho địa chỉ nhận. Kiểm tra nguồn và khoảng thời gian. [Hướng dẫn hashrate](/blog/vi/what-is-hashrate/) phân biệt máy, pool và mạng.

Hashrate pool cho địa chỉ có thể đã phản ánh thời gian công việc được ghi cho nơi khác. Nếu vậy nhập không cho phí trình đào bổ sung trong máy tính. Điều này đổi phép tính, không đổi chính sách phần mềm. Phí pool vẫn áp dụng khi ước tính phần thưởng gộp đủ điều kiện; xem mỗi số đã gồm gì.

## Vì sao không trừ phí từ số dư lần nữa?

Số dư xác nhận là kết quả kế toán, không phải ước tính gộp nguyên vẹn. Pool đã phân bổ ghi có. Trừ cùng phí từ số dư hoặc thanh toán chỉ đổi số mà không có sự kiện phí mới. Dùng số hiển thị để đối chiếu và xác định giai đoạn.

Phần thưởng chờ, số dư xác nhận và khoản đã gửi cũng khác về thời gian. Độ trưởng thành khối và ngưỡng trả giải thích chuyển giai đoạn, không phải phí phần trăm thêm. Xem [hướng dẫn xác nhận](/blog/vi/block-confirmations-mining-rewards/) và [lịch sử trả](/payouts). Phí giao dịch ví nếu có là trường riêng.

## Giữ nguồn gốc đầu vào

Lưu ngày, phiên bản, phí, nguồn hashrate và khoảng đo. Máy tính ghép dữ liệu trực tiếp pool/mạng với đầu vào của bạn; làm mới dữ liệu khi xem lại. Ghi có thực tế còn theo share hợp lệ, khối tìm được và PPLNS. Công thức và bản ghi tài khoản trả lời câu hỏi khác nhau.

Dùng [My Stats](/stats) và [hướng dẫn theo dõi](/blog/vi/track-your-miner-on-bntpool/). So sánh cùng địa chỉ, thời kỳ và giai đoạn trước khi đổi phí. Máy tính ước tính BNT; điện, phần cứng và giá tiền nằm ngoài phép tính. Không cần đổi địa chỉ ví.

<CTA target="pool" title="Theo dõi phần thưởng trên bntpool.com" text="Dùng máy tính cho ước tính và My Stats cho công việc, số dư và thanh toán đã ghi." />
