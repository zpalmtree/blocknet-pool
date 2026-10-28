---
title: "Nhận thanh toán Blocknet cho hàng hóa và dịch vụ"
description: "Nhận BNT cho đơn hàng, đối chiếu khoản nhận trong ví Blocknet, kiểm tra xác nhận và xử lý thanh toán thiếu, thanh toán dư hoặc hoàn tiền rõ ràng."
tags: [Blocknet, BNT]
faq:
  - q: "Làm sao biết khách đã thanh toán bằng Blocknet?"
    a: "Tìm khoản nhận trong ví đã đồng bộ của bạn, đối chiếu số tiền và mã đơn rồi kiểm tra trạng thái xác nhận. Lưu mã giao dịch đầy đủ cùng đơn hàng. Ảnh chụp của khách hoặc liên kết thanh toán không chứng minh đã nhận; trình khám phá cũng không hiện số tiền riêng tư trả cho địa chỉ của bạn."
  - q: "Nếu khách gửi ít BNT hơn yêu cầu thì sao?"
    a: "Giữ đơn ở trạng thái thanh toán một phần và tính số còn thiếu từ những khoản đã xác minh. Yêu cầu phần chênh lệch với cùng mã đơn, rồi lưu cả hai mã giao dịch. Chỉ tính mỗi khoản nhận một lần. Nếu thống nhất hủy, xác nhận địa chỉ hoàn tiền qua cuộc trao đổi hiện có về đơn hàng."
  - q: "Memo có tự đánh dấu đơn hàng đã trả tiền không?"
    a: "Không. Memo giúp liên kết khoản nhận với đơn nhưng người trả có thể sửa hoặc bỏ qua. Hãy tự kiểm tra số tiền và trạng thái giao dịch. Ví tạo yêu cầu và hiển thị khoản nhận; quy trình thủ công này không thêm hệ thống thanh toán hay quản lý đơn tự động."
sourceHash: '3309a069e5de'
translation: draft
---

Nhận Blocknet bằng cách báo số BNT rõ ràng, kiểm tra khoản nhận trong ví của bạn và lưu cùng đơn hàng. Ví dụ giả định yêu cầu **1.25 BNT**, mã **order-1042**. Hướng dẫn dùng ví máy tính đã phát hành v0.22.0; ảnh có dữ liệu minh họa được ghi rõ.

## Nhận thanh toán Blocknet như thế nào?

<Answer>
Thống nhất số BNT và mã đơn, gửi yêu cầu tới địa chỉ bạn kiểm soát rồi chờ khoản nhận trong ví đã đồng bộ. Đối chiếu số tiền và mã tham chiếu, kiểm tra xác nhận, lưu mã giao dịch và đánh dấu đơn đã trả theo chính sách xác nhận đã thống nhất với khách.
</Answer>

Xem [hướng dẫn ví máy tính](https://seineminer.com/vi/blog/blocknet-desktop-wallet/) nếu cần tạo hoặc mở ví. Giữ bản sao khôi phục tách khỏi hồ sơ đơn. Nhận tiền của khách không đòi hỏi chạy miner hay tham gia pool.

## Chốt số tiền và nhận diện đơn hàng

Ghi mã đơn, số BNT, địa chỉ nhận và hàng khách mua. Nếu báo giá bằng tiền khác, thống nhất số BNT và thời hạn báo giá trước khi trả. Liên kết chứa số BNT yêu cầu; nó không cập nhật tỷ giá hoặc cưỡng chế hạn thanh toán.

Trong ví dụ, yêu cầu **1.25 BNT** với memo **order-1042**. Mã đơn là đủ; địa chỉ giao hàng và dữ liệu khách nằm trong hệ thống riêng của bạn. [Memo mã hóa](https://seineminer.com/vi/blog/blocknet-encrypted-memos/) đi cùng khoản trả, nhưng memo viết trong liên kết được chia sẻ vẫn đọc được bởi người nhận liên kết.

Tại **Receive → Payment Request**, điền số tiền và memo rồi chọn **Generate Payment Link**. Kiểm tra người nhận và số tiền trước khi chia sẻ. [Hướng dẫn liên kết thanh toán](https://seineminer.com/vi/blog/blocknet-payment-links/) giải thích định dạng và người nhận theo tên. Với hóa đơn cố định, lưu địa chỉ đầy đủ cùng yêu cầu.

<figure class="payment-figure">
<a href="/blog/accept-payments/request.webp"><img src="/blog/accept-payments/request.webp" width="737" height="442" alt="Ví Blocknet đã phát hành hiển thị yêu cầu 1.25 BNT, memo order-1042 và địa chỉ minh họa." loading="lazy" decoding="async"></a>
<figcaption>Giao diện tiếng Anh thật với dữ liệu minh họa. Tạo yêu cầu chưa chuyển tiền. Mở ảnh để phóng lớn.</figcaption>
</figure>

## Kiểm tra khoản nhận trong ví của bạn

Nhờ khách điền mã đơn và giữ mã giao dịch. Khi node và ví đã đồng bộ, mở **History** để tìm khoản vào. So sánh số tiền và memo với đơn. Người trả có thể sửa cả hai; yêu cầu không chứng minh số đã gửi.

Lưu mã đầy đủ cùng số tiền nhận. Đừng tính hai lần vì giao dịch hiện ở cả Dashboard lẫn History hoặc khách gửi lại cùng mã. Đối chiếu từng khoản nhận thay vì chênh lệch tổng số dư, vốn có thể gồm phần thưởng đào, khách khác và chuyển khoản của chính bạn.

<figure class="payment-figure">
<a href="/blog/accept-payments/receipt.webp"><img src="/blog/accept-payments/receipt.webp" width="1200" height="440" alt="History trong ví Blocknet hiển thị khoản nhận minh họa 1.25 BNT với memo order-1042." loading="lazy" decoding="async"></a>
<figcaption>Khoản nhận ví dụ khớp số tiền và mã yêu cầu. Kiểm tra trạng thái trước khi thực hiện đơn.</figcaption>
</figure>

Nếu chưa thấy khoản nhận, đối chiếu địa chỉ đích đầy đủ và mã giao dịch với khách, sau đó kiểm tra đồng bộ. Ảnh màn hình gửi cung cấp bối cảnh; ví nhận của bạn xác định số đã nhận.

## Khi nào nên đánh dấu đơn đã trả tiền?

Phân biệt khoản vào chưa xác nhận, giao dịch đã nằm trong block và tiền có thể chi tiêu. Ví đã phát hành chờ mười block tiếp sau block chứa khoản thanh toán thông thường trước khi cho chi tiêu các đầu ra. [Hướng dẫn thời gian chuyển](https://seineminer.com/vi/blog/blocknet-transfer-time/) giải thích bộ đếm và thời gian chờ thay đổi.

Công bố chính sách xác nhận trước khi khách trả. Cách thủ công đơn giản là chờ khoản nhận tương ứng có thể chi tiêu rồi mới giao hàng. Chỉ thấy số tiền đến chưa hoàn tất việc kiểm tra. Đừng hứa giờ giao cố định dựa vào bộ đếm của ví vì block đến không đều. [Cách mining bảo vệ thanh toán](/blog/vi/how-mining-secures-blocknet-payments/) giải thích độ sâu xác nhận.

## Xử lý khoản trả thiếu, dư hoặc chưa khớp

| Khoản đã xác minh | Bước tiếp theo |
|---|---|
| 1.00 BNT cho đơn 1.25 BNT | Ghi nhận trả một phần và yêu cầu 0.25 BNT còn lại. |
| Thêm 0.25 BNT sau đó | Lưu cả hai mã, tính tổng 1.25 BNT một lần và kiểm tra xác nhận của cả hai. |
| Hơn 1.25 BNT | Thống nhất hoàn tiền hoặc ghi có rồi lưu quyết định. |
| Đúng số tiền nhưng thiếu memo | Xác nhận mã đầy đủ và đích với khách trước khi gán vào đơn. |
| Khoản đang chờ | Giữ đơn chờ xác nhận và kiểm tra lại trạng thái. |

Để hoàn tiền, yêu cầu khách xác nhận địa chỉ nhận trong cuộc trao đổi hiện có về đơn. Không suy ra địa chỉ trả lại từ giao dịch công khai: đầu vào Blocknet không cho một địa chỉ người gửi thông thường. Kiểm tra số tiền, đích và phí trước khi duyệt lần gửi mới; lưu mã cùng khoản gốc.

## Chỉ chia sẻ bằng chứng cần thiết

Trình khám phá giúp kiểm tra giao dịch vào block nhưng không tiết lộ số tiền riêng tư cho địa chỉ bạn. Nếu có tranh luận, người gửi có thể cung cấp bằng chứng riêng cho giao dịch theo [hướng dẫn bằng chứng thanh toán](https://seineminer.com/vi/blog/blocknet-payment-proof/). Kiểm tra đầu ra khớp người nhận, số tiền và trạng thái chuỗi; khóa hợp lệ thôi chưa đủ.

Lưu riêng tư cùng nhau đơn, yêu cầu, mã giao dịch nhận, số tiền đã kiểm tra, quyết định xác nhận và hoàn tiền. Chỉ chia sẻ biên nhận hoặc bằng chứng liên quan. Không bao giờ gửi từ khôi phục, mật khẩu hay khóa xem riêng tư của cả ví làm bằng chứng.

Nếu cũng đào BNT, tách [hồ sơ thanh toán pool](/blog/vi/track-your-miner-on-bntpool/) khỏi đơn của khách dù dùng chung ví. Nhờ vậy biết rõ chuyển khoản nào trả hóa đơn nào.


<CTA target="pool" title="Nhận thêm phần thưởng đào" text="Hướng dẫn pool chỉ cách kết nối miner và theo dõi khoản trả cùng các khoản nhận Blocknet khác." />
