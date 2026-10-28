---
sourceHash: 77522359a409
translation: draft
title: Đào coin bảo vệ thanh toán Blocknet như thế nào?
description: Tìm hiểu cách Blocknet chọn lịch sử thanh toán bằng proof of work, ý nghĩa của xác nhận và những gì nút kiểm tra trước khi chấp nhận một khối mới.
tags: [Blocknet, Bảo mật thanh toán, Proof of work]
faq:
  - q: Nhiều sức đào hơn có thể cho phép chi tiền từ ví của tôi không?
    a: Sức đào giúp tạo khối cạnh tranh. Chi tiêu vẫn cần ủy quyền hợp lệ, và nút kiểm tra bằng chứng cùng cân đối giao dịch trước khi nhận khối. Nhánh có nhiều công hơn vẫn phải tuân thủ các quy tắc đó. Proof of work không thay thế khóa riêng cần để chi đầu ra của bạn.
  - q: Blocknet có theo chuỗi nhiều khối nhất không?
    a: Core v0.22.0 tính công tích lũy bằng cách cộng độ khó từng khối. Nhánh hợp lệ có nhiều công hơn có thể thành chuỗi được chọn, tùy quy tắc tái tổ chức. Chỉ đếm khối không đủ vì độ khó có thể khác nhau. Cả công lẫn tính hợp lệ đều quan trọng.
  - q: Xác nhận thanh toán có giống share của pool không?
    a: Share ghi nhận công đào gửi cho pool. Xác nhận thanh toán nói về giao dịch được đưa vào chuỗi đã chọn và các khối tiếp theo. Share giúp phân chia phần thưởng, còn khối mạng phát triển lịch sử thanh toán. Tăng số share không trực tiếp thêm xác nhận cho một khoản thanh toán cụ thể.
---

Theo dõi khoản thanh toán BNT từ lúc vào khối đến khi thuộc lịch sử được công đào tiếp theo hỗ trợ. Hướng dẫn này giải thích vì sao mạng cần chọn giữa các lịch sử cạnh tranh, xác nhận cho biết gì và lựa chọn ấy phối hợp với xác thực giao dịch riêng tư ra sao. Phần triển khai được mô tả là core v0.22.0 đã phát hành, kiểm tra ngày 28 tháng 10 năm 2026.

## Việc đào bảo vệ điều gì khi bạn gửi BNT?

<Answer>
Việc đào giúp mạng chọn lịch sử thanh toán chung có thứ tự. Mỗi khối được chấp nhận liên kết với khối trước và đóng góp proof of work. Nút kiểm tra khối cùng giao dịch được đề xuất, rồi so sánh các nhánh hợp lệ theo công tích lũy trong quy tắc tái tổ chức. Khối sau thêm công phía sau khoản thanh toán đã được đưa vào, tạo ý nghĩa thực tế cho các xác nhận.
</Answer>

Khoản thanh toán hợp lệ cần cả ủy quyền lẫn vị trí trong sổ cái. Ví cung cấp ủy quyền để chi tiền của bạn. Việc đào và chọn chuỗi giúp các nút thống nhất giao dịch hợp lệ nào thuộc lịch sử hiện tại. Hai nhiệm vụ bổ trợ nhau: proof of work của khối không biến khoản thanh toán không hợp lệ thành chấp nhận được.

[Hướng dẫn thực thi quy tắc](https://seineminer.com/vi/blog/who-controls-blocknet/) giải thích ai phát triển, đề xuất và xác thực. Ở đây trọng tâm là khoản thanh toán: việc nó được đưa vào trở thành một phần lịch sử mà ví và các nút khác theo dõi như thế nào.

## Các khối ràng buộc lịch sử thanh toán như thế nào?

Tiêu đề khối chứa hash khối trước và gốc Merkle suy ra từ các giao dịch. Gốc ràng buộc nội dung khối bằng mật mã, còn hash trước nối nó với lịch sử cũ. Thay đổi giao dịch sẽ đổi gốc và tiêu đề đang được đào. Các khối hậu duệ sau đó phải tham chiếu lịch sử đã đổi bằng liên kết của chính chúng.

Xét một khoản thanh toán trong khối, theo sau bởi vài khối đã được chấp nhận. Thay thế phần lịch sử đó cần nhánh cạnh tranh gồm các khối hợp lệ liên kết với nhau và đủ công tích lũy, trong giới hạn tái tổ chức của nút. Sửa giao dịch trong tệp cục bộ không tạo ra nhánh ấy. Công gắn với một chuỗi tiêu đề và giao dịch cụ thể.

[Mã khối đã phát hành](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/block.go) định nghĩa các trường này, kiểm tra Merkle và đường chọn chuỗi. [Hướng dẫn độ khó](/blog/vi/mining-difficulty-explained/) giải thích cách mạng quyết định yêu cầu công cho mỗi khối mới.

## Vì sao công tích lũy quan trọng hơn số khối?

Hai thợ đào có thể tìm thấy khối hợp lệ khác nhau cùng nối một khối cha. Mỗi đề xuất có hash riêng, và các nút có thể nghe về chúng theo thứ tự khác nhau. Điều này có thể tạo nhánh cạnh tranh trong thời gian ngắn. Mạng cần quy tắc dựa vào công hỗ trợ lịch sử, thay vì thứ tự một nút ngang hàng thông báo.

Core v0.22.0 cộng độ khó của khối vào công tích lũy của khối cha. Sau xác thực, nhánh có công lớn hơn một cách nghiêm ngặt có thể kích hoạt đổi chuỗi được chọn. Nhánh có công bằng nhau không kích hoạt thay đổi qua phép so sánh này. Độ khó phải hợp lệ với ngữ cảnh khối cha của nhánh; thợ đào không thể chỉ ghi số lớn hơn vào tiêu đề.

Ví dụ, so sánh hai nhánh có đoạn đầu chung. Đoạn đó đóng góp công bằng nhau vào hai tổng. Khác biệt quyết định là công tích lũy sau khi tách nhánh, cùng các kiểm tra tính hợp lệ và tái tổ chức. Đếm khối mà bỏ qua độ khó có thể làm mất khác biệt ấy. Đây là so sánh khái niệm, không phải báo cáo về một phân nhánh đang xảy ra.

## Xác nhận giúp theo dõi tiến trình thanh toán thế nào?

Khi khoản thanh toán vào chuỗi được chọn, các khối sau xây trên lịch sử đó. Độ sâu tăng theo số khối đến sau. Thay thế việc đưa nó vào cần một lịch sử thay thế đủ điều kiện từ trước khối liên quan, trong khi nhánh hiện tại vẫn tích lũy công. Vì vậy kiểm tra việc đưa vào và độ sâu hữu ích hơn chỉ thấy giao dịch đã được phát đi.

Chiều cao chỉ vị trí; hash chỉ một khối cụ thể. Kiểm tra bản ghi giao dịch và khối chứa nó, rồi xác nhận khối vẫn thuộc chuỗi được chọn. Nếu lịch sử được chọn thay đổi, việc đưa vào và độ sâu cũng có thể đổi. Bộ đếm xác nhận có thể tính cả khối chứa giao dịch hoặc chỉ các khối sau; hãy đọc quy ước giao diện.

Core v0.22.0 còn áp dụng giới hạn tái tổ chức: từ chối chuyển nhánh nếu tổ tiên chung nằm dưới chiều cao đỉnh hiện tại trừ 100. Đây là quy tắc của phần triển khai đang chạy, không phải lời hứa phổ quát gắn với bộ đếm. [Hướng dẫn thời gian chuyển](https://seineminer.com/vi/blog/blocknet-transfer-time/) nói về khả năng chi tiêu thông thường trong ví; [xác nhận phần thưởng đào](/blog/vi/block-confirmations-mining-rewards/) nói về trưởng thành coinbase và kế toán pool.

## Làm sao kiểm tra chi tiêu lặp lại trong thanh toán riêng tư?

Định dạng giao dịch đã phát hành của Blocknet dùng key image gắn với bằng chứng chi tiêu. Xác thực kiểm tra liên kết này và từ chối key image đã được chi trong trạng thái chuỗi liên quan. Nhờ vậy nút phát hiện việc dùng đầu ra xung đột, trong khi chữ ký vòng che giấu thành viên hợp lệ nào là đầu vào thật. Số tiền riêng tư vẫn chịu kiểm tra cân đối và phạm vi.

Giả sử chủ sở hữu cho phép các giao dịch xung đột chi cùng một đầu ra. Lịch sử hợp lệ được chấp nhận không thể chứa cả hai lần chi. Chọn chuỗi quan trọng vì lịch sử cạnh tranh có thể chứa lựa chọn khác nhau; xác thực quan trọng vì mỗi lịch sử phải tuân thủ quy tắc chi. Nhiều công hơn không thay thế chữ ký hợp lệ hoặc giúp giao dịch mất cân đối vượt kiểm tra kế toán.

Các kiểm tra nằm trong [bộ xác thực giao dịch đã phát hành](https://github.com/blocknetprivacy/core/blob/5a0a5b510c1dce46f4eeb12c33a291052e44e7b3/transaction.go). Với các cơ chế riêng tư rộng hơn, xem [hướng dẫn riêng tư Blocknet](https://seineminer.com/vi/blog/blocknet-privacy-features/). Bảo mật thanh toán kết hợp chi tiêu hợp lệ, lịch sử nhất quán và mật mã bảo vệ thông tin mà lịch sử tiết lộ.

## Đào qua pool đóng vai trò gì trong bảo mật thanh toán?

Pool phối hợp việc tìm khối và gửi ứng viên đã giải được tới nút thượng nguồn. Share đạt mục tiêu dễ hơn của pool ghi nhận công đóng góp; chỉ lời giải đạt mục tiêu mạng mới có thể thành khối mạng sau xác thực. [Bài giải thích pool](/blog/vi/what-is-a-mining-pool/) phân biệt hai mục tiêu và vai trò của chúng.

Máy đào có thể góp phần kéo dài lịch sử thanh toán trong khi pool tính công riêng. Số dư riêng trong pool, số share và khoản thanh toán đã xác nhận trong ví là những thứ khác nhau. Bắt đầu với [hướng dẫn kết nối pool](/blog/vi/mine-blocknet-on-bntpool/) để tham gia, và dùng bản ghi giao dịch của ví khi theo dõi một khoản BNT cụ thể.

<CTA target="pool" title="Đóng góp công cho Blocknet" text="Kết nối seine với bntpool.com và theo dõi công đào, phần thưởng cùng thanh toán qua các công cụ công khai của pool." />
