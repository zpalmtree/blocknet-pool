---
title: 'May mắn của pool đào: effort và thời gian tìm khối'
description: 'Hiểu may mắn của pool, effort 100% và vòng đào dài. Đọc lịch sử bntpool.com, thời gian tìm khối kỳ vọng và phân biệt việc tìm khối với nhận thanh toán.'
tags: ['Pool đào', 'May mắn của pool', 'Blocknet']
faq:
  - q: 'Effort 100% có nghĩa là pool sắp tìm được khối không?'
    a: 'Không. Nó có nghĩa là công việc share được ghi nhận đã đạt lượng kỳ vọng cho một khối ở độ khó dùng trong phép tính đó. Đây là so sánh với giá trị trung bình, không phải phần trăm hoàn thành. Mỗi hash mới vẫn phải đáp ứng mục tiêu của mạng.'
  - q: 'Phần trăm may mắn thấp hơn có tốt hơn trên bntpool.com không?'
    a: 'Hãy đọc nhãn và định nghĩa trước. Cột Effort và thẻ Average Luck của bntpool.com thể hiện công việc so với lượng kỳ vọng, nên giá trị thấp hơn nghĩa là ít công việc được ghi nhận hơn cho mỗi khối. Trang khác có thể dùng quy ước ngược. Hãy so sánh cùng chỉ số và cỡ mẫu, không chỉ phần trăm.'
  - q: 'Vòng đào dài có làm khoản thanh toán tiếp theo lớn hơn không?'
    a: 'Vòng dài không đảm bảo khoản thanh toán cá nhân lớn hơn. Việc tìm khối quyết định khi nào có phần thưởng; kế toán PPLNS của pool quyết định phần của bạn theo công việc được chấp nhận đủ điều kiện. Sau đó mới áp dụng quy tắc xác nhận và thanh toán. Xem số dư trong Thống kê của tôi và trình tự trong hướng dẫn thanh toán.'
sourceHash: dfd8c710a719
translation: draft
---

Bảng điều khiển pool hiển thị cả công việc đào lẫn các khối tìm được nhờ công việc đó. Hướng dẫn này giải thích phần trăm effort, thời gian tìm khối kỳ vọng và lịch sử vòng đào trên bntpool.com. Bạn sẽ phân biệt được trung bình thống kê với đếm ngược, rồi kiểm tra kết nối đào của mình mà không phải chờ khối tiếp theo của pool.

## May mắn của pool đào là gì?

<Answer>
May mắn của pool mô tả việc tìm khối thực tế so với trung bình kỳ vọng từ công việc của pool. Khối được tìm ngẫu nhiên: có khối cần ít công việc, có khối cần nhiều hơn. Trên bntpool.com, effort của vòng so sánh công việc share được chấp nhận với độ khó khối; effort thấp hơn nghĩa là vòng đó tìm được khối với ít công việc ghi nhận hơn.
</Answer>

Pool kết hợp các lần thử của nhiều thợ đào nhưng vẫn phải tìm hash đáp ứng mục tiêu mạng. Share được chấp nhận đo đóng góp ở mục tiêu dễ hơn, giúp pool ước tính lượng công việc băm giữa các lần tìm khối. [Bài giới thiệu pool đào](/blog/vi/what-is-a-mining-pool/) giải thích quan hệ này; may mắn mô tả sự biến thiên của các khối thu được.

## Pool tính effort như thế nào?

Với vòng đã hoàn thành, bntpool.com chia công việc share được chấp nhận cho độ khó khối rồi nhân 100. Công việc share được tính theo trọng số độ khó, không chỉ là số share đã gửi. Share có độ khó được giao cao hơn đại diện cho nhiều công việc băm kỳ vọng hơn share có độ khó thấp.

**Effort (%) = công việc share được chấp nhận ÷ độ khó khối × 100**

Ví dụ, khối có độ khó 10.000 và pool ghi nhận 5.000 đơn vị công việc được chấp nhận trong vòng đó. Effort là 50%. Với 20.000 đơn vị ở cùng độ khó khối, effort sẽ là 200%. Đây là ví dụ số học, không phải số đo của một khối cụ thể trong hệ thống thực.

| Effort | Cách đọc |
|---|---|
| 50% | Một nửa công việc kỳ vọng ở độ khó đó |
| 100% | Lượng công việc kỳ vọng cho một khối |
| 200% | Gấp đôi công việc kỳ vọng ở độ khó đó |

Effort của vòng hiện tại dùng độ khó hiện tại, có thể thay đổi khi chuỗi tiến lên. Các hàng lịch sử dùng độ khó của khối tương ứng. Vì vậy đây là so sánh công việc, không phải thanh tiến độ phải dừng ở 100%. Nó cũng khác thời lượng vòng: pool nhanh hơn có thể làm cùng lượng công việc trong thời gian ngắn hơn.

## Thời gian tìm khối kỳ vọng nghĩa là gì?

Đó là khoảng thời gian trung bình suy ra từ tỷ lệ hashrate ước tính của pool trong mạng. Bảng điều khiển bntpool.com tính bằng 300 giây nhân hashrate mạng rồi chia hashrate pool. Dùng cùng đơn vị cho cả hai. [Hướng dẫn hashrate](/blog/vi/what-is-hashrate/) giải thích vì sao ước tính thay đổi khi có share được chấp nhận và khối mới.

**Số giây kỳ vọng = 300 × H/s của mạng ÷ H/s của pool**

Nếu mạng được ước tính 100.000 H/s và pool 10.000 H/s thì pool chiếm một phần mười tổng ước tính. Khoảng thời gian kỳ vọng là 3.000 giây, tức 50 phút. Trung bình này có thể gồm cả vòng ngắn và dài; nó không hẹn khối tiếp theo xuất hiện chính xác sau 50 phút nữa.

Bảng điều khiển còn so sánh thời gian đã trôi qua với thời gian kỳ vọng. Phần trăm theo thời gian có thể khác effort theo share vì dùng đầu vào khác nhau. Kiểm tra nhãn trước khi so sánh hai số. Thay đổi hashrate pool hoặc độ khó mạng có thể làm ước tính thay đổi dù cùng một vòng vẫn đang chạy.

## Sau effort 100% thì khối có đến hạn không?

<Answer>
Pool không được đảm bảo một khối khi effort đạt 100%. Các hash thất bại trước đó không làm hash tiếp theo có khả năng đáp ứng mục tiêu mạng không đổi cao hơn. Với hashrate và độ khó cố định, thời gian chờ kỳ vọng tính từ lúc này vẫn như cũ, kể cả sau vòng dài. Thêm công việc tạo thêm lần thử, không tạo thời hạn.
</Answer>

Mô hình xác suất đơn giản giúp thấy rõ điều này. Với các lần thử độc lập và tốc độ tìm khối cố định, thời gian chờ gần với phân phối mũ. Xác suất chưa có khối sau một khoảng kỳ vọng là khoảng 36,8%; sau hai là 13,5%; sau ba là 5,0%. Các tỷ lệ này mô tả nhiều vòng giả định trong điều kiện cố định, không dự báo vòng hiện tại của pool.

| Số khoảng kỳ vọng đã trôi qua | Xác suất xấp xỉ vẫn chưa có khối |
|---|---|
| 1 × | 36,8% |
| 2 × | 13,5% |
| 3 × | 5,0% |

Công thức là `exp(-t / T)`, trong đó `T` là khoảng trung bình và `t` là thời gian đã trôi qua. [Tài liệu phân phối mũ của NIST](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3667.htm) nêu hàm sống sót này. Hashrate và độ khó thực thay đổi nên hãy coi bảng là lời giải thích về tính ngẫu nhiên. Một chuỗi khối nhanh cũng không thay đổi xác suất của hash kế tiếp khi mục tiêu giữ nguyên.

## Đọc lịch sử may mắn của bntpool.com như thế nào?

Mở [May mắn](/luck) để so sánh các vòng đã hoàn thành. Mỗi hàng hiển thị khối, effort, thời lượng vòng, trạng thái và thời điểm tìm thấy. Xem nhiều vòng cùng nhau thay vì chọn một hàng ngắn hoặc dài bất thường. Liên kết khối cho phép xem bản ghi tương ứng trên chuỗi; phân trang giúp xem lịch sử rộng hơn.

<img src="/blog/covers/pool-luck-history-20261026.jpg" alt="Trang lịch sử may mắn thực tế của bntpool.com với effort, thời lượng vòng và trạng thái khối" width="1200" height="833" loading="lazy" />

Ảnh này được chụp ngày 26 tháng 10 năm 2026. Các hàng và trạng thái là bản ghi lịch sử tại thời điểm đó; dùng trang trực tiếp để xem giá trị hiện tại. Trạng thái chờ hoặc đã xác nhận mô tả tình trạng xác nhận khối. Nó không đổi ý nghĩa của phần trăm effort ghi nhận và tách biệt với thanh toán đến địa chỉ của bạn.

Trên bảng điều khiển, Average Luck là trung bình cộng của các phần trăm effort ghi nhận cho khối không bị mồ côi đến chiều cao chuỗi hiện tại. Dù tên thẻ là vậy, nó theo quy ước effort: thấp hơn nghĩa là ít công việc ghi nhận trên mỗi khối hơn. Trung bình tóm tắt mẫu đó, không hứa các vòng tương lai sẽ bù cho kết quả cụ thể trước đây.

## May mắn của pool ảnh hưởng phần thưởng thế nào?

May mắn của pool thay đổi thời điểm tìm khối. PPLNS quyết định phân bổ phần thưởng của khối tìm được giữa công việc được chấp nhận đủ điều kiện. Đây là hai bước khác nhau. Ranh giới vòng không biến cửa sổ PPLNS thành quy tắc chỉ trả theo vòng; đọc [cách thanh toán PPLNS hoạt động](/blog/vi/how-pplns-payouts-work/) để biết cửa sổ, xác nhận và ngưỡng thanh toán thực tế.

Vì vậy số dư có thể tăng theo từng đợt dù phần mềm đào vẫn gửi share được chấp nhận. [Máy tính phần thưởng](/blog/vi/estimate-blocknet-mining-rewards/#mining-calculator) ước tính sản lượng từ hashrate và dữ liệu mạng. Nó hữu ích để hình dung kỳ vọng theo thời gian; không dự đoán khối hay thời điểm tiền đến ví.

## Nên kiểm tra gì trong vòng đào dài?

Kiểm tra những gì quan sát được trực tiếp: phần mềm đào đang chạy, share tiếp tục được chấp nhận và đúng địa chỉ xuất hiện tại [Thống kê của tôi](/stats). Dùng [Trạng thái](/status) để xem sức khỏe dịch vụ công khai. Nếu share được chấp nhận của bạn dừng, theo [hướng dẫn xử lý lỗi thống kê thợ đào](/blog/vi/track-your-miner-on-bntpool/#zero-hashrate); vòng pool dài và giàn đào mất kết nối cần kiểm tra khác nhau.

Khi kết nối ổn định, hãy đọc may mắn qua lịch sử đủ ý nghĩa và giữ cùng định nghĩa. Mỗi con số trên bảng điều khiển sẽ có vai trò rõ ràng: hashrate ước tính công việc mỗi giây, effort so sánh công việc với mức kỳ vọng cho khối, số dư thể hiện kế toán phần thưởng. Không chỉ số nào đòi hỏi đoán lúc hash thắng tiếp theo xuất hiện.

<CTA target="pool" title="Bắt đầu đào trên bntpool.com" text="Kết nối seine, xác nhận share được chấp nhận rồi theo dõi lịch sử vòng và phần thưởng trên bảng điều khiển pool." />
