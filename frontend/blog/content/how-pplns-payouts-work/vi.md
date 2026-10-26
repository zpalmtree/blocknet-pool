---
title: Thanh toán PPLNS trên bntpool.com hoạt động thế nào?
description: bntpool.com trả thưởng thợ đào Blocknet bằng PPLNS với cửa sổ share sáu giờ. Tìm hiểu cách chia thưởng, 60 xác nhận và mức tối thiểu 0,1 BNT.
tags: [Thanh toán, PPLNS, bntpool, Hướng dẫn đào coin]
sourceHash: '1ca5549586c5'
translation: draft
faq:
  - q: 'Cửa sổ PPLNS trên bntpool.com dài bao lâu?'
    a: 'Cửa sổ là sáu giờ. Khi pool tìm thấy khối, nó nhìn lại sáu giờ trước khối đó và chia phần thưởng cho mọi người đã gửi share trong thời gian ấy, theo tỷ lệ độ khó share. Giá trị hiện tại được liệt kê trên trang Get Started của pool.'
  - q: 'Mức thanh toán tối thiểu trên bntpool.com là bao nhiêu?'
    a: 'Mức tối thiểu là 0,1 BNT. Khi số dư đã xác nhận đạt mức đó, pool tự động gửi đến địa chỉ Blocknet của bạn, không cần yêu cầu hay nhận thủ công. Số dư nhỏ hơn ở lại tài khoản và tăng theo từng khối đã xác nhận mà bạn có share đóng góp, cho đến khi vượt ngưỡng.'
  - q: 'Vì sao Estimated Rewards khác Confirmed Rewards?'
    a: 'Estimated Rewards là phần của bạn trong các khối pool tìm thấy vẫn đang thu thập 60 xác nhận, nên con số còn có thể đổi. Confirmed Rewards đến từ khối đã sâu 60 khối và đang chờ thanh toán. Khi từng khối trưởng thành, số tiền chuyển từ thẻ đầu sang thẻ thứ hai.'
  - q: 'Phần thưởng có dừng ngay khi tôi ngừng đào không?'
    a: 'Không. Mỗi share ở trong cửa sổ PPLNS sáu giờ sau khi gửi, nên các khối pool tìm được trong thời gian đó vẫn ghi thưởng cho bạn. Phần thưởng đã xác nhận giữ nguyên trong số dư, và pool thanh toán khi số dư đạt mức tối thiểu 0,1 BNT.'
  - q: 'Effort trên 100% ở trang may mắn nghĩa là gì?'
    a: 'Effort so sánh công việc pool đã làm trong vòng với độ khó khối, tức lượng công việc kỳ vọng. Trên 100% nghĩa là vòng đó lâu hơn trung bình, dưới 100% là nhanh hơn. Cả hai đều bình thường; qua nhiều vòng, effort trung bình về khoảng 100%.'
---

Hướng dẫn này giải thích cách bntpool.com biến share trình đào gửi thành khoản thanh toán Blocknet. Đọc xong, bạn sẽ biết cách chia phần thưởng khối, vì sao phải chờ khoảng năm giờ mới có thể thanh toán, khi nào số dư được gửi và cách đọc các thẻ số dư trên [trang thống kê](/stats) cùng [trang may mắn](/luck).

Nếu mới đào bằng pool, [pool đào là gì?](/blog/vi/what-is-a-mining-pool/) trình bày kiến thức cơ bản. Nếu chưa bắt đầu đào, [hướng dẫn đào bằng seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) đưa bạn từ tải xuống đến share đầu tiên trong vài phút.

## PPLNS là gì?

<Answer>
PPLNS nghĩa là “pay per last N shares”, trả theo N share cuối. Khi bntpool.com tìm thấy khối, pool chia phần thưởng cho các share thợ đào gửi trong sáu giờ trước khối đó, tỷ lệ theo độ khó từng share. Bạn đóng góp công việc đều đặn trong cửa sổ càng nhiều thì phần nhận từ mỗi khối càng lớn.
</Answer>

Trên bntpool.com, “N cuối” là khoảng thời gian: sáu giờ. [Trang Get Started](/start) hiển thị phương thức trả thưởng, cửa sổ, mức thanh toán tối thiểu và số xác nhận; các giá trị tương tự được công bố tại `https://bntpool.com/api/info`.

| Cài đặt | Giá trị trên bntpool.com |
|---|---|
| Phương thức trả thưởng | PPLNS |
| Cửa sổ share | 6 giờ |
| Xác nhận trước thanh toán | 60 khối |
| Thanh toán tối thiểu | 0,1 BNT |

Share được tính theo độ khó. Pool đặt độ khó riêng cho mỗi kết nối thợ đào và điều chỉnh để mỗi người gửi share đều đặn. Máy nhanh hơn nhận share khó hơn, nên mỗi share có trọng số lớn hơn. Phần của bạn được quyết định bởi tổng công việc đã làm, một thước đo công bằng cho cả laptop lẫn máy chủ.

## bntpool.com chia phần thưởng khối thế nào?

Khi pool tìm được khối, phần thưởng luôn đi qua cùng các bước:

1. **Khối được ghi nhận.** Nó xuất hiện trên [trang khối](/blocks) với trạng thái chờ.
2. **Cửa sổ được đo.** Pool lấy sáu giờ kết thúc tại khối và cộng độ khó mọi share được chấp nhận từ mỗi địa chỉ trong khoảng đó.
3. **Phần thưởng được chia.** Mỗi địa chỉ nhận tỷ lệ phần thưởng bằng tỷ lệ công việc của mình trong tổng cửa sổ.
4. **Khối trưởng thành.** Phần của bạn hiển thị là ước tính cho đến khi khối sâu 60 khối, rồi chuyển vào số dư đã xác nhận.
5. **Số dư được thanh toán.** Khi số dư đã xác nhận đạt 0,1 BNT, pool gửi đến địa chỉ của bạn.

## Ví dụ tính toán

<Note>
Ví dụ dùng số tròn giả định để minh họa phép tính. Tổng thực tế phụ thuộc hashrate pool, hashrate của bạn và [phần thưởng khối](/blog/vi/blocknet-block-reward-schedule/) lúc đó.
</Note>

Giả sử trong sáu giờ trước một khối, mọi thợ đào trong pool gửi share có tổng độ khó 1.000.000, còn share của bạn cộng lại là 20.000. Tỷ lệ của bạn trong cửa sổ là 20.000 ÷ 1.000.000 = 2%. Nếu pool chia 400 BNT cho khối đó, bạn được ghi 400 × 0,02 = 8 BNT.

Giờ hình dung thợ đào thứ hai có hashrate y hệt bạn nhưng kết nối 30 phút trước khối. Ba mươi phút bằng một phần mười hai của sáu giờ, nên họ có khoảng 1.667 độ khó trong cửa sổ và nhận khoảng 0,67 BNT từ khối này. Tuy vậy công việc của họ tiếp tục được tính: mỗi share vẫn ở cửa sổ sáu giờ tiếp theo và nhận phần từ mọi khối pool tìm được trong thời gian đó.

| Thợ đào (ví dụ) | Công việc trong cửa sổ | Tỷ lệ | Ghi thưởng từ khoản 400 BNT |
|---|---|---|---|
| Bạn, đào trọn sáu giờ | 20.000 | 2% | 8 BNT |
| Cùng hashrate, tham gia 30 phút trước khối | 1.667 | khoảng 0,17% | khoảng 0,67 BNT |
| Thợ đào nhỏ, đào trọn sáu giờ | 100 | 0,01% | 0,04 BNT |

Thợ đào nhỏ ở hàng cuối kiếm 0,04 BNT mỗi khối, nên số dư đã xác nhận vượt mức tối thiểu 0,1 BNT sau ba khối như vậy và được thanh toán.

## Vì sao PPLNS thưởng cho người đào đều đặn?

Mỗi share bạn gửi được tính vào mọi khối pool tìm trong sáu giờ tiếp theo. Người chạy suốt ngày đêm luôn có cửa sổ công việc đầy phía sau, nên mỗi khối mới ghi thưởng theo toàn bộ tốc độ của họ. Thu nhập bám sát hashrate từ khối này sang khối khác.

Điều này cũng khiến PPLNS chịu gián đoạn tốt. Khởi động lại, cập nhật driver hoặc mất mạng ngắn không đặt lại vị trí của bạn, vì share đã gửi vẫn nằm trong cửa sổ sáu giờ. Công việc thêm sau khi tìm thấy khối được tính vào các khối tiếp theo, nên hashrate bổ sung luôn được sử dụng.

Để có kết quả đều nhất, cứ để trình đào chạy và tự tinh chỉnh. seine tiếp tục điều chỉnh số luồng và kernel CPU khi đào mà không kết nối lại pool, nên luồng share liên tục. Bạn có thể [tải seine](https://seineminer.com/vi/#download) cho Windows, macOS hoặc Linux.

## Vì sao phần thưởng khối chờ 60 xác nhận?

Blocknet yêu cầu coin mới đào chờ 60 khối trước khi có thể chi tiêu. Quy tắc daemon gọi đây là độ trưởng thành coinbase, khoảng năm giờ với thời gian khối năm phút của Blocknet (60 × 5 phút = 300 phút). bntpool.com dùng cùng 60 khối trước khi tính phần thưởng là đã xác nhận, nên mọi khoản thanh toán đều từ coin pool thực sự có thể gửi.

Trong lúc khối trưởng thành, pool kiểm tra nó vẫn thuộc chuỗi chính. Khi sâu 60 khối, khối được đánh dấu đã xác nhận và phần thưởng được cộng vào số dư đã xác nhận của mỗi thợ đào. Trong trường hợp hiếm khi mạng chọn khối khác tại độ cao đó, khối bị đánh dấu mồ côi và phần thưởng ước tính bị xóa.

## Khi nào nhận thanh toán bntpool.com?

<Answer>
Sau khi khối có 60 xác nhận, khoảng năm giờ với thời gian khối năm phút của Blocknet, phần của bạn chuyển vào Confirmed Rewards trên trang thống kê. Khi số dư đã xác nhận đạt 0,1 BNT, bntpool.com tự động gửi đến địa chỉ Blocknet. Bạn không cần yêu cầu hay nhận thủ công.
</Answer>

Với người mới, ước tính đầu tiên xuất hiện khi pool tìm khối trong lúc share của bạn nằm trong cửa sổ. Khoản thanh toán đầu tiên đến sau khi khối ấy trưởng thành và số dư đạt tối thiểu. Khoản đã gửi được liệt kê trên [trang thanh toán](/payouts) và lịch sử riêng trên trang thống kê.

## Phần thưởng tạm tính được hiển thị thế nào?

Tra địa chỉ hoặc `@name` trên [trang thống kê](/stats), bạn sẽ thấy ba thẻ số dư:

- **Estimated Rewards:** phần của bạn trong khối chưa trưởng thành. Đây là tạm tính, có thể đổi cho đến khi từng khối xác nhận hoặc thành mồ côi.
- **Confirmed Rewards:** số dư đã trưởng thành, chờ thanh toán.
- **Paid Balance:** tất cả đã gửi đến địa chỉ của bạn.

Pool tách phần thưởng tạm tính để số dư đã xác nhận chỉ chứa các khoản cuối cùng. Dưới thẻ, trang thống kê liệt kê từng khối chưa xác nhận với phần ước tính của bạn, để theo dõi tiến độ xác nhận. [Hướng dẫn theo dõi trình đào](/blog/vi/track-your-miner-on-bntpool/) trình bày phần còn lại của trang.

## Trang may mắn và effort cho biết gì?

Effort đo lượng công việc pool đã làm để tìm khối. Nó bằng công việc được chấp nhận trong vòng chia cho độ khó khối, hiển thị dưới dạng phần trăm. Độ khó Blocknet bằng số hash kỳ vọng mỗi khối, nên effort 100% nghĩa là khối cần đúng lượng công việc trung bình. Dưới 100% là pool tìm nhanh hơn trung bình; trên 100% là lâu hơn.

[Trang may mắn](/luck) liệt kê mỗi vòng với độ cao khối, effort, thời gian vòng, trạng thái và lúc tìm được. [Bảng điều khiển pool](/) hiển thị effort đang tăng của vòng hiện tại và mức may mắn trung bình của pool. Các vòng riêng lẻ dao động trên dưới 100%, điều bình thường ở mọi pool Proof of Work; qua nhiều vòng, effort trung bình về khoảng 100%.

May mắn ảnh hưởng tần suất khối xuất hiện. Phần của bạn trong mỗi khối chỉ phụ thuộc tỷ lệ công việc trong cửa sổ; đào đều đặn giữ tỷ lệ đó đầy đủ dù vòng nhanh hay lâu.

<CTA target="pool" title="Tham gia pool" text="Đào đều đặn trên bntpool.com; phần của bạn trong mỗi khối được tự động thanh toán khi đạt 0,1 BNT." />
