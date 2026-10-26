---
title: 'Pool đã trả nhưng ví chưa thấy khoản tiền?'
description: 'Theo dõi khoản trả Blocknet qua giao dịch, địa chỉ nhận và đồng bộ ví, rồi thu thập thông tin để làm rõ mục giao dịch chưa xuất hiện.'
tags: ['Thanh toán', 'Ví', 'Khắc phục sự cố']
faq:
  - q: 'Khoản trả đang xếp hàng nghĩa là ví đã nhận chưa?'
    a: 'Xếp hàng là trạng thái xử lý phía pool. Trước hết kiểm tra bản ghi có mã giao dịch chưa và trạng thái hiện tại là gì. Bản ghi thanh toán, giao dịch vào chuỗi và ví nhận ra đầu ra là các bước riêng. Theo dõi giao dịch cụ thể khi có.'
  - q: 'Explorer có hiển thị số dư ví riêng tư không?'
    a: 'Explorer giúp kiểm tra trạng thái công khai của giao dịch trên chuỗi. Blocknet dùng đầu ra riêng tư, nên tìm địa chỉ thông thường không hiển thị số dư ví hay chứng minh một đầu ra thuộc về bạn. Ví quét chuỗi để nhận ra đầu ra của mình.'
  - q: 'Cần cung cấp gì về khoản trả chưa xuất hiện?'
    a: 'Cung cấp địa chỉ nhận đầy đủ dùng tra cứu pool, mã giao dịch, giờ ước tính, trạng thái pool và tiến độ đồng bộ. Nêu giao dịch có trong lịch sử ví chưa. Chỉ chia sẻ dữ liệu tài khoản trong kênh hỗ trợ phù hợp, không cung cấp cụm từ khôi phục, khóa riêng hay mật khẩu.'
sourceHash: '915075cf4e2b'
translation: draft
---

Theo khoản trả Blocknet từ bản ghi pool tới ví nhận. Hướng dẫn bắt đầu khi bạn xác định được một mục thanh toán trên bntpool.com. Nếu chỉ thấy phần thưởng ước tính hoặc đã xác nhận đang chờ trả, hãy xem [hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/) trước; đó là giai đoạn trước với quy tắc kế toán và thanh toán riêng.

## Theo dõi khoản trả chưa có trong ví như thế nào?

<Answer>
Tìm bản ghi cho địa chỉ nhận đầy đủ, ghi mã giao dịch và trạng thái rồi kiểm tra trên chuỗi. Mở ví đúng địa chỉ đó và để đồng bộ cùng quét bắt kịp. So sánh lịch sử trước khi quyết định cần xem xét khoản thanh toán hay màn hình ví cục bộ.
</Answer>

*Bản ghi trả tiền của pool nối tới giao dịch trên chuỗi, rồi tới ví quét các đầu ra nhận được của chính nó.*

## Bắt đầu từ một bản ghi cụ thể

Mở My Stats và tra địa chỉ đầy đủ dùng khi kiếm phần thưởng. Ghi số tiền, giờ, trạng thái và mã giao dịch nếu có. Dùng mục này thay cho tổng đã trả từ trước đến nay, vốn có thể gộp nhiều khoản. [Hướng dẫn theo dõi](/blog/vi/track-your-miner-on-bntpool/) chỉ nơi các trường xuất hiện.

Trang Payouts công khai mô tả lô thanh toán có thể gồm nhiều người nhận và liên kết giao dịch. Giữ bản ghi địa chỉ của mình cạnh giao dịch đang kiểm tra. Tổng lô không phải tiền được phân cho ví bạn. Nếu có nhiều mã giao dịch, mở rộng danh sách và lưu mã liên quan đầy đủ, thay vì chỉ nhãn rút gọn.

## Khoản trả đang xếp hàng, chưa xác nhận hay đã xác nhận?

Đọc trạng thái hiện tại trước khi cho rằng khoản được liệt kê đã hoàn tất mọi bước. Mục xếp hàng vẫn đang xử lý ở pool; giao dịch chưa xác nhận còn cần xác nhận chuỗi. Làm mới bản ghi và mở liên kết khi có. [Trang trạng thái](/status) cho bối cảnh vận hành nếu nhiều dấu hiệu chỉ tới gián đoạn dịch vụ.

Phân biệt độ chín của phần thưởng đào với xác nhận giao dịch chi trả. Khối tạo phần thưởng và giao dịch trả ví sau đó có mã và vai trò khác nhau. [Hướng dẫn xác nhận](/blog/vi/block-confirmations-mining-rewards/) giải thích sự khác biệt. Mã giao dịch là đối tượng cụ thể để kiểm tra, nhưng chưa chứng minh ví cục bộ đã quét và nhận ra khoản đó.

## Kiểm tra giao dịch trên chuỗi

Mở liên kết explorer gắn với khoản trả và đối chiếu mã đầy đủ với ghi chú. Đọc trạng thái chuỗi và thông tin khối nếu có. Nếu explorer chưa tìm thấy, so sánh bản ghi pool đã làm mới và trạng thái dịch vụ trước khi kết luận. Kết quả tra cứu tạm thời tự nó không xác định nguyên nhân.

Tính riêng tư của Blocknet khiến tra cứu địa chỉ thông thường không hiện số dư công khai của người nhận. Trạng thái giao dịch trên explorer và kiểm tra quyền sở hữu trong ví trả lời câu hỏi khác nhau. Ví nhận ra đầu ra khi quét chuỗi. [Hướng dẫn địa chỉ ẩn](https://seineminer.com/vi/blog/stealth-addresses-explained/) giải thích quá trình; danh sách địa chỉ công khai không tái hiện lịch sử riêng tư của ví.

## Bạn đang xem đúng ví nhận không?

So sánh địa chỉ tra cứu pool với địa chỉ nhận của ví đang mở. Dùng giá trị đầy đủ; tiền tố rút gọn giống nhau chưa đủ. Nếu đào bằng Blocknet ID, giữ địa chỉ đầy đủ của công việc ban đầu. Tra tên hiện tại có thể dẫn nơi khác sau khi đổi ánh xạ, còn khoản trả trước vẫn gắn đích ban đầu.

Kiểm tra ví cũ dùng trước khi đổi cấu hình đào. Đổi địa chỉ miner hay ánh xạ tên không chuyển khoản đã được gán. [Hướng dẫn đổi địa chỉ](/blog/vi/change-mining-wallet-address/) giải thích cách theo cả hai lịch sử. Giữ ví gốc và bản sao lưu; phân biệt mở ví có sẵn với tạo ví mới có địa chỉ khác.

## Để ví đồng bộ và quét

Xác nhận node của ví tiến tới chuỗi hiện tại và quét ví đã đến khối chứa giao dịch thanh toán. Đồng bộ node và quét ví là hai tiến độ riêng. Ví mới mở hoặc khôi phục có thể cần thời gian phát hiện đầu ra dù explorer đã hiển thị giao dịch trong khối.

Giữ ví kết nối và theo quy trình đồng bộ được tài liệu hướng dẫn. So sánh lịch sử nhận lẫn số dư chính vì chi tiêu sau đó làm tổng thay đổi. Nếu có mục giao dịch, xem trạng thái xác nhận hoặc khả dụng. Bắt đầu bằng quan sát này trước khi khôi phục hay quét lại toàn bộ, giữ bản sao lưu trước bảo trì. [Hướng dẫn ví](https://seineminer.com/vi/blog/blocknet-wallet-for-mining/) nói về truy cập và sao lưu.

## Thông tin nào giúp làm rõ chênh lệch còn lại?

Ghi địa chỉ pool đầy đủ, mã giao dịch, giờ ước tính và trạng thái pool hiện tại. Thêm phiên bản ví, tiến độ node, tiến độ quét và việc giao dịch nhận có trong lịch sử hay không. Nêu các kiểm tra đã làm. Dữ liệu này phân biệt trạng thái giao dịch, sai địa chỉ và ví còn đang bắt kịp.

Dùng kênh hỗ trợ chính thức và chỉ chia sẻ dữ liệu tài khoản ở nơi phù hợp. Điều tra giao dịch không cần cụm từ khôi phục, khóa riêng hay mật khẩu. Kiểm tra nội dung tệp chẩn đoán trước khi đăng toàn bộ. Cuối cùng khớp cùng bản ghi, giao dịch và ví nhận; hashrate mới hay khởi động lại miner không xác minh việc nhận khoản trả cũ.

<CTA target="pool" title="Theo dõi khoản trả trên bntpool.com" text="Dùng địa chỉ nhận đầy đủ để tìm bản ghi và so sánh giao dịch với lịch sử ví đã đồng bộ." />
