---
title: 'Share được chấp nhận, bị từ chối và quá hạn'
description: 'Hiểu share được chấp nhận, bị từ chối và quá hạn trên bntpool.com, đọc thông báo lỗi và xem độ khó share ảnh hưởng thế nào đến thống kê đào của bạn.'
tags: ['Hướng dẫn đào', 'Share', 'Xử lý sự cố']
faq:
  - q: 'Share được chấp nhận có nghĩa là tìm thấy khối không?'
    a: 'Nghĩa là pool nhận kết quả theo quy tắc xử lý share. Khối phải đạt mục tiêu mạng riêng và được daemon chấp nhận. Phần thưởng theo sau việc tìm khối, xác thực và thanh toán. Xác nhận share không phải thông báo trả tiền.'
  - q: 'Tại sao share quá hạn bị từ chối?'
    a: 'Stale job nghĩa là kết quả thuộc phân công không còn hợp lệ, chẳng hạn hết hạn hoặc mẫu cũ vượt thời gian gia hạn. Kiểm tra job mới và share được chấp nhận trở lại. Nếu lặp lại, kiểm tra kết nối và phiên bản phần mềm đào.'
  - q: 'Độ khó share cao hơn có giảm thưởng không?'
    a: 'Độ khó thay đổi công việc mỗi share đủ điều kiện đại diện và tần suất kỳ vọng. So sánh công việc có trọng số độ khó trong các khoảng tương đương, không chỉ số share. Thưởng còn phụ thuộc cửa sổ PPLNS, công việc hợp lệ, khối tìm thấy và quy tắc xác thực.'
sourceHash: 5be830208e6c
translation: draft
---

Dùng kết quả share trong log để quyết định tiếp tục, kiểm tra kết nối hay điều tra từ chối lặp lại. Bài này giải thích phản hồi bntpool.com và độ khó đằng sau bộ đếm. Bắt đầu từ [hướng dẫn kết nối](/blog/vi/mine-blocknet-on-bntpool/) nếu cần, hoặc [hướng dẫn theo dõi](/blog/vi/track-your-miner-on-bntpool/#zero-hashrate) khi địa chỉ không có hoạt động.

## Share được chấp nhận là gì?

<Answer>
Share là kết quả đào gửi theo mục tiêu do pool giao. Phản hồi chấp nhận xác nhận kết quả đi vào quy trình xử lý và chứng tỏ công việc đến pool. Tìm khối, xác minh share và đủ điều kiện thanh toán là các bước riêng; chỉ được nhận chưa phải được trả tiền.
</Answer>

Phần mềm tính nhiều hash giữa các share gửi đi. Phần lớn không đạt mục tiêu share nên không gửi. Bộ đếm chấp nhận tính kết quả gửi, không tính mọi hash CPU hoặc GPU. [Hướng dẫn hashrate](/blog/vi/what-is-hashrate/) phân biệt đếm tại máy và ước tính của pool từ công việc gửi lên.

## Share khác khối thế nào?

Pool giao job và độ khó cho kết nối. Kết quả đạt mục tiêu share chứng minh công việc ở độ khó đó. Ứng viên khối còn phải đạt mục tiêu mạng và qua kiểm tra daemon. Tìm share không đồng nghĩa thêm khối vào chuỗi.

bntpool.com ghi trạng thái đã xác minh và tạm thời khi xác thực. Kết quả được nhận có thể là tạm thời; trọng số thanh toán áp dụng quy tắc xác thực và điều kiện. Dùng chấp nhận như tín hiệu kết nối, rồi xem [Thống kê của tôi](/stats) cho công việc và thưởng đã ghi. [Hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/) giải thích sau khi pool tìm khối.

## Share bị từ chối và quá hạn nghĩa là gì?

Kết quả bị từ chối không qua kiểm tra áp dụng. Lý do hữu ích hơn bộ đếm. Stale job là một loại từ chối: phân công không còn hợp lệ. So sánh đúng thông báo với sự kiện cùng lúc như kết nối lại hoặc job mới.

| Thông báo | Ý nghĩa và kiểm tra tiếp theo |
|---|---|
| `stale job` | Phân công không tồn tại, hết hạn, đã thay thế quá thời gian gia hạn hoặc gắn mẫu cũ không còn sẵn. Kiểm tra job mới và share được nhận trở lại. |
| `duplicate share` | Cùng mẫu và nonce đã được gửi. Tìm gửi lặp; cùng công việc không thêm đóng góp lần nữa. |
| `low difficulty share` | Hash không đạt mục tiêu share được giao. Nếu lặp lại, kiểm tra phiên bản và xử lý job/độ khó. |
| `nonce out of assigned range` | Nonce ngoài phạm vi được giao. Kiểm tra phiên bản phần mềm và proxy hay tích hợp tùy chỉnh. |

Share quá hạn nói về công việc của một phân công; khối mồ côi nói về vị trí khối trong chuỗi. Tách log share và [danh sách khối](/blocks) khi chẩn đoán. Riêng thông báo stale job không nói rằng pool tìm thấy khối mồ côi.

## Tại sao độ khó share thay đổi?

<Answer>
Độ khó biến thiên, hay vardiff, điều chỉnh mục tiêu theo hoạt động share quan sát được. Độ khó cao thường cho ít share hơn, mỗi share đại diện nhiều công việc; thấp thì ngược lại. So sánh công việc hợp lệ có trọng số trong cùng kỳ, thay vì chỉ đếm share.
</Answer>

Ví dụ: mười share hợp lệ độ khó 100 bằng một share độ khó 1.000 về tổng độ khó: đều 1.000. Đây là minh họa trọng số, không hứa thưởng hay lịch đến chính xác. Khoảng cách share thay đổi và xác thực quyết định công việc được tính.

Độ khó của phân công quan trọng khi gửi share. Đừng so kết quả cũ chỉ với độ khó mới nhất trên màn hình. Pool lưu phân công để kiểm tra. Điều chỉnh bình thường không phải lỗi; ép share dễ hơn không tạo thêm công việc tính toán.

## Đọc tỷ lệ từ chối thế nào?

So sánh gửi được nhận và từ chối trong cùng kỳ và phạm vi. Bộ đếm máy có thể tính một phiên, pool tính kỳ khác hoặc nhiều máy. Khởi động lại đổi cửa sổ tại máy nhưng không xóa lịch sử địa chỉ trên pool.

99 lượt được nhận và một lượt từ chối cho tỷ lệ 1% theo số lượng. Đây không tự động là phần trăm thưởng mất: độ khó có thể khác và có quy tắc điều kiện. Xem lý do, thời điểm và công việc được nhận có tiếp tục không trước khi đổi thiết lập.

## Làm gì khi từ chối lặp lại?

Bắt đầu với một máy bị ảnh hưởng. Ghi phiên bản, đúng thông báo, lúc bắt đầu và job mới còn đến không. Kiểm tra [Trạng thái](/status), rồi gián đoạn kết nối máy. Cập nhật từ [bản phát hành chính thức của seine](https://github.com/zpalmtree/seine/releases) và so sánh sau từng thay đổi.

Proxy hoặc tích hợp riêng phải giữ nguyên job, phạm vi nonce và độ khó khi chuyển tiếp. Nếu vẫn lỗi, gửi đoạn log ngắn và chi tiết máy qua kênh hỗ trợ dự án. Xóa bí mật và dữ liệu tài khoản riêng; không bao giờ cần cụm từ khôi phục ví để chẩn đoán share.

## Xác nhận đào hoạt động trở lại thế nào?

Tìm job mới rồi share được nhận, mở [Thống kê của tôi](/stats) bằng cùng địa chỉ nhận của máy. Xác nhận hoạt động gần đây. Chờ ước tính hashrate tích lũy thêm công việc trước khi so trung bình. Sau đó theo dõi thưởng ước tính, số dư và thanh toán thành các giai đoạn riêng trong hướng dẫn PPLNS.

<CTA target="pool" title="Kết nối bntpool.com" text="Làm theo hướng dẫn, xác nhận share được chấp nhận và theo dõi hoạt động bằng địa chỉ nhận." />
