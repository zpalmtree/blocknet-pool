---
title: 'Có thể đào Blocknet bán thời gian không?'
description: 'Đào Blocknet theo phiên ngắn: theo dõi share trong cửa sổ PPLNS sáu giờ của bntpool, quay lại cùng địa chỉ và kiểm tra số dư chưa thanh toán.'
tags: ['Hướng dẫn đào', 'PPLNS', 'Lịch chạy']
faq:
  - q: 'Phải đào sáu giờ thì share mới được tính sao?'
    a: 'Không. Share hợp lệ có thể tham gia cửa sổ thưởng block mà không phải đợi sáu giờ. Phần của bạn phụ thuộc công việc có trọng số độ khó trong cửa sổ so với tổng pool, và phải tìm được block để phân bổ thưởng.'
  - q: 'Miner phải kết nối để nhận khoản thanh toán sẵn có không?'
    a: 'Kết nối đào đang hoạt động không phải điều kiện thanh toán. Số dư đã xác nhận được xét theo quy tắc pool, gồm ngưỡng tối thiểu hiện tại. Ngắt kết nối không tự xóa số dư hay yêu cầu trả ngay.'
  - q: 'Kết nối lại có đặt lại tuổi share cũ không?'
    a: 'Không. Share cũ giữ nguyên dấu thời gian. Công việc mới được chấp nhận tham gia cửa sổ tương ứng sau khi quay lại; kết nối lại không gia hạn share cũ hay thêm công việc cho giờ ngoại tuyến. Kiểm tra share mới được chấp nhận sau khi khởi động lại.'
sourceHash: '0a11c393062e'
translation: draft
---

Bạn có thể chạy miner Blocknet vào những giờ đã chọn rồi quay lại sau. Cần phân biệt kết nối hiện tại, share đã gửi và thưởng đã phân bổ. Hướng dẫn này theo một phiên ngắn qua các bước để bạn dừng, chạy lại và kiểm tra tài khoản bntpool.com với kỳ vọng rõ ràng.

## Phiên đào ngắn có thể nhận thưởng pool không?

<Answer>
Có. Công việc hợp lệ của phiên ngắn có thể tham gia block có cửa sổ PPLNS chứa những share đó. Không cần hoàn thành sáu giờ trước khi đóng góp. Thưởng phụ thuộc pool tìm block và phần công việc có trọng số độ khó của bạn trong từng cửa sổ. Dừng không thêm việc mới; share cũ tiếp tục già đi bình thường và số dư tuân theo quy tắc trả thưởng.
</Answer>

*Dòng thời gian thể hiện phiên đào, thời gian ngoại tuyến và lúc quay lại, với dấu riêng cho tính hợp lệ của share và số dư hiện có.*

## Phân biệt các mốc thời gian

bntpool hiện công bố trên [trang Bắt đầu](/start) cửa sổ PPLNS sáu giờ, 60 block trước khi đủ điều kiện thanh toán và mức trả tối thiểu 0,1 BNT. Đó là ba bước khác nhau: công việc tính cho block, độ chín của block và ngưỡng số dư. Cửa sổ share sáu giờ không phải bộ đếm hứa thanh toán sau sáu giờ.

Xem công thức phân bổ trong [hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/). Share hợp lệ được tính trọng số độ khó so với toàn bộ công việc hợp lệ cùng cửa sổ. Chỉ số phút trực tuyến không ấn định phần trăm thưởng tiếp theo. Thiết bị, sự tham gia của pool và kết quả share có thể đổi trọng số dù hai phiên dài bằng nhau.

## Theo một share qua giờ nghỉ

Lịch minh họa theo giờ địa phương: đào từ 18:00 đến 20:00, một share hợp lệ ghi lúc 19:00. Block pool có cửa sổ thưởng kết thúc 22:00 nhìn lại sáu giờ tới 16:00, nên share vẫn nằm trong khoảng dù miner đã ngoại tuyến. Đây là minh họa điều kiện tham gia, không dự đoán pool tìm block lúc đó.

Cửa sổ kết thúc 02:00 hôm sau bắt đầu 20:00, nên share 19:00 nằm ngoài. Kết nối lại lúc 08:00 không đổi dấu thời gian của nó. Việc mới bắt đầu đóng góp vào các cửa sổ sau khi bạn quay lại. [Hướng dẫn may mắn của pool](/blog/vi/mining-pool-luck-explained/) giải thích thời điểm block biến động; đừng chọn lịch vì cho rằng đã đến lượt có block.

## Dừng với bản ghi dễ nhận biết

Trước giờ nghỉ dự kiến, ghi địa chỉ nhận, tên rig, giờ dừng gần đúng và thông báo share chấp nhận gần nhất. Kiểm tra tài khoản tương ứng theo [hướng dẫn theo dõi](/blog/vi/track-your-miner-on-bntpool/). Đây là mốc cho phiên sau và giúp phân biệt rig đã dừng với địa chỉ khác hay hiển thị chậm.

Thoát seine bình thường bằng `q` trong bảng điều khiển hoặc `Ctrl+C` ở terminal. Giữ lệnh chạy và cấu hình đang hoạt động cho lần sau. [Hướng dẫn dùng PC](https://seineminer.com/vi/blog/mine-while-using-pc/) nói về kết hợp đào với việc khác; dừng đúng cách không cần xóa cấu hình, đổi địa chỉ ví hay yêu cầu pool đóng tài khoản.

## Số dư chưa trả ra sao khi bạn ngoại tuyến?

Share ra khỏi cửa sổ của block sau không xóa thưởng đã phân bổ cho block trước. Thưởng ước tính vẫn theo trạng thái và độ chín block; thưởng đã xác nhận nhưng chưa trả nằm trong tài khoản tới khi được xử lý thanh toán. [Hướng dẫn xác nhận](/blog/vi/block-confirmations-mining-rewards/) phân biệt ghi có block tạm thời với số dư đã chín.

Số dư xác nhận dưới 0,1 BNT vẫn dưới ngưỡng trả tự động hiện tại cho đến khi ghi có thêm đạt mức tối thiểu. Kết nối lại không tự tăng số dư hay buộc thanh toán. Ngược lại, số dư đủ điều kiện không cần miner trực tuyến khi pool xử lý. Dùng [lịch sử thanh toán](/payouts) để phân biệt số dư chưa trả với giao dịch đã gửi.

## Quay lại đúng tài khoản và kiểm tra việc mới

Chạy lại lệnh đã lưu, xác nhận pool, địa chỉ nhận và tên rig trước khi so sánh thống kê. Địa chỉ xác định tài khoản có số dư bạn theo dõi. Nếu chủ ý chọn nơi nhận khác, làm theo [hướng dẫn đổi địa chỉ](/blog/vi/change-mining-wallet-address/); số dư tài khoản cũ không tự chuyển sang tài khoản mới.

Tìm job hiện tại và share mới được chấp nhận, rồi cho cửa sổ đo của pool thời gian cập nhật. Lần khởi động gần đây có thể hiện khác nhau trong trung bình phiên miner và ước tính trượt của pool. [Hướng dẫn hashrate](/blog/vi/what-is-hashrate/) giải thích các cửa sổ này. Kiểm tra công việc thật và đúng tài khoản trước khi hiểu chênh lệch ngắn hạn là mất thưởng.

## Xếp phiên theo sinh hoạt của bạn

Chọn giờ rảnh theo việc dùng máy và ngân sách điện. Dùng [hướng dẫn điện năng](https://seineminer.com/vi/blog/mining-electricity-use/) tính theo lịch thực, gồm các khoảng giá khác nhau nếu có. Phiên ngắn giảm thời gian chạy, nhưng năng lượng chính xác vẫn phụ thuộc thiết bị và tải được đo.

Ghi ngắn gọn lần bắt đầu, dừng, việc được chấp nhận và số dư qua nhiều phiên. [Công cụ tính thưởng](/blog/vi/estimate-blocknet-mining-rewards/) đưa kỳ vọng theo giả định, không hứa kết quả cho một tối cụ thể. Đánh giá lịch qua khoảng đại diện và ghi riêng kết nối hiện tại, quyền tham gia cửa sổ share, độ chín block và khoản đã gửi.

<CTA target="pool" title="Theo dõi phiên trên bntpool.com" text="Dùng lại cùng địa chỉ nhận, kiểm tra share mới và theo dõi số dư qua các bước thanh toán thông thường." />
