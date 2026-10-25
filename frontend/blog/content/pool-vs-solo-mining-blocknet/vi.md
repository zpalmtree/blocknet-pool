---
title: Đào Blocknet bằng pool hay solo
description: Chọn đào Blocknet bằng pool hoặc solo. Tính số khối kỳ vọng mỗi ngày, xem bntpool.com tạo khoản thanh toán đều đặn và thiết lập đào solo.
tags: [Hướng dẫn đào coin, Đào solo, bntpool, PPLNS]
sourceHash: 171d240d6c4b
translation: draft
faq:
  - q: 'Nếu đào solo, bao lâu tôi tìm được một khối Blocknet?'
    a: 'Trung bình bạn tìm được 288 × (hashrate của bạn ÷ hashrate mạng) khối mỗi ngày, vì Blocknet nhắm một khối mỗi năm phút. Với 50 H/s trên mạng 50.000 H/s, đó là 0,288 khối mỗi ngày, hay khoảng một khối mỗi ba ngày rưỡi. Xem hashrate mạng trực tiếp trên bảng điều khiển bntpool.com.'
  - q: 'Có cần chạy node Blocknet để đào trên bntpool.com không?'
    a: 'Không. Pool chạy node Blocknet riêng, tạo mẫu khối và gửi khối tìm được. Bạn chỉ cần seine cùng địa chỉ Blocknet hoặc tên Blocknet ID. Chạy seine, dán địa chỉ, chấp nhận pool mặc định; pool trả phần của bạn trong mỗi khối về địa chỉ đó.'
  - q: 'seine có đào Blocknet solo được không?'
    a: 'Có. Khởi động seine bằng ./seine --mode daemon để đào trực tiếp với node Blocknet đã đồng bộ của bạn, dùng cookie API của node để kết nối. Mỗi khối tìm được trả toàn bộ phần thưởng vào ví node. Hướng dẫn đào solo trong kho seine trình bày bố trí node tùy chỉnh và mật khẩu ví.'
  - q: 'Sau này có thể chuyển giữa đào pool và solo không?'
    a: 'Có. seine dùng cùng bản tải cho cả hai. Chạy bình thường để đào trên bntpool.com, hoặc thêm --mode daemon để đào với node riêng; khởi động lại khi muốn đổi. Share đã gửi cho pool vẫn nằm trong cửa sổ PPLNS sáu giờ và tiếp tục kiếm thưởng khi còn hiệu lực.'
---

Hướng dẫn này giúp bạn quyết định đào Blocknet trên bntpool.com hay solo với node riêng. Bạn sẽ tính số khối hashrate của mình tự tìm được, xem pool biến nó thành dòng thanh toán đều đặn thế nào và học cách thiết lập cả hai với seine.

## Nên đào Blocknet trong pool hay solo?

<Answer>
Cả hai theo cùng phép tính: tỷ lệ hashrate của bạn trong mạng quyết định phần khối của bạn. Trên bntpool.com, bạn kiếm một phần từ mọi khối pool tìm thấy, nên thanh toán đến đều khi pool tìm khối. Đào solo cho bạn toàn bộ phần thưởng mỗi khối tự tìm, phù hợp hoạt động quy mô lớn chạy node riêng.
</Answer>

Với đa số thợ đào, bntpool.com là nơi dễ bắt đầu. Đây là pool mặc định của seine, nên ngoài địa chỉ không có gì cần thiết lập; bạn không phải chạy hay đồng bộ node. Đào solo cũng được hỗ trợ đầy đủ: seine có chế độ daemon đào thẳng với node Blocknet riêng. Phần còn lại cho thấy cách chọn phương án phù hợp phần cứng.

## Đào solo sẽ tìm được bao nhiêu khối?

<Answer>
Blocknet nhắm một khối mỗi năm phút, tương đương 288 khối mỗi ngày. Đào một mình, bạn có thể kỳ vọng tìm 288 × (hashrate của bạn ÷ hashrate mạng) khối mỗi ngày. Chia một ngày cho số đó để có thời gian trung bình giữa các khối. Xem hashrate mạng trực tiếp trên bảng điều khiển bntpool.com.
</Answer>

Công thức đúng vì Blocknet điều chỉnh độ khó để cả mạng tìm khối trung bình mỗi 300 giây, và độ khó khối gần bằng số hash mạng kỳ vọng thử trước khi thành công. Mỗi hash có cùng cơ hội, nên tỷ lệ hash mạng đến từ máy bạn cũng là tỷ lệ khối kỳ vọng tìm được.

Bạn cần hai số:

- **Hashrate của bạn.** seine hiển thị trên bảng điều khiển khi đào; [trang thống kê](/stats) của pool hiển thị sau khi bạn đào ở đó.
- **Hashrate mạng.** [Bảng điều khiển bntpool.com](/) hiển thị trong thẻ Network Hashrate cạnh Pool Hashrate. Cả hai thay đổi theo thời gian, nên hãy kiểm tra khi tính.

Mỗi khối tìm được trả phần thưởng của tháng đó. [Lịch phần thưởng khối](/blog/vi/blocknet-block-reward-schedule/) liệt kê phần thưởng theo tháng.

## Ví dụ tính toán

<Note>
Các số dưới là giá trị tròn giả định để minh họa phép tính, không phải dữ liệu trực tiếp. Hãy dùng hashrate mạng hiện tại từ bảng điều khiển cho ước tính của bạn.
</Note>

Giả sử mạng chạy ở 50.000 H/s. Một dàn máy 50 H/s, gần mức seine đo trên máy chủ AMD EPYC Zen 5, nắm 0,1% mạng. Đây là số khối dàn máy đó và hai hệ thống lớn hơn sẽ tìm khi đào riêng:

| Hashrate của bạn (ví dụ) | Tỷ lệ mạng | Khối kỳ vọng mỗi ngày | Thời gian trung bình giữa khối |
|---|---|---|---|
| 50 H/s | 0,1% | 0,288 | khoảng 83 giờ (3,5 ngày) |
| 500 H/s | 1% | 2,88 | khoảng 8,3 giờ |
| 5.000 H/s | 10% | 28,8 | khoảng 50 phút |

Giờ đưa dàn máy 50 H/s vào pool có tổng tốc độ 5.000 H/s. Pool tìm khoảng 28,8 khối mỗi ngày, trung bình một khối mỗi 50 phút; phần 1% công việc pool của máy cho nó 1% mỗi khối. Công việc được tính vẫn tương đương 0,288 khối mỗi ngày. Điều thay đổi là cách nhận: một phần nhỏ khoảng mỗi 50 phút, thay vì cả khối vài ngày một lần khi solo.

Bảng cũng cho thấy vì sao đào solo hợp hệ thống lớn. Ở 5.000 H/s, thợ đào solo tìm khối thường xuyên như pool cùng quy mô, nên tự có thu nhập đều đặn.

Để đổi khối mỗi ngày thành BNT cho phần cứng riêng, với ví dụ có ngày từ dữ liệu pool trực tiếp, xem [cách ước tính phần thưởng đào Blocknet](/blog/vi/estimate-blocknet-mining-rewards/).

## Vì sao pool cho thanh toán đều hơn?

bntpool.com kết hợp hashrate của mọi người đào trong pool, nên tìm khối thường xuyên hơn nhiều so với một dàn máy. Phần thưởng mỗi khối chia cho người đóng góp công việc tìm nó, theo PPLNS: pool nhìn lại sáu giờ trước khối và chia theo tỷ lệ độ khó share của từng thợ đào.

Điều này có vài kết quả thực tế cho bạn:

- **Kiếm từ mọi khối.** Khi share còn trong cửa sổ sáu giờ, bạn nhận phần từ mỗi khối pool tìm được. Có thể theo dõi chúng trên [trang khối](/blocks).
- **Thanh toán tự động.** Sau khi khối sâu 60 khối, phần của bạn chuyển vào số dư đã xác nhận; pool gửi về địa chỉ khi đạt 0,1 BNT. Khoản đã gửi được liệt kê trên [trang thanh toán](/payouts).
- **Liên tục qua khởi động lại.** Share đã gửi ở lại cửa sổ sáu giờ, nên khởi động lại hoặc cập nhật driver không đặt lại vị trí.
- **Chỉ cần chạy trình đào.** Pool chạy node, tạo mẫu khối và gửi khối. Bạn có thể đào từ nhiều máy về một địa chỉ hoặc Blocknet ID `@name`.

[Hướng dẫn thanh toán PPLNS](/blog/vi/how-pplns-payouts-work/) trình bày chia thưởng từng bước với ví dụ riêng. [Trang Get Started](/start) của pool liệt kê cài đặt thanh toán hiện tại.

## Vì sao đào Blocknet solo?

Đào solo đặt mọi phần của quy trình dưới quyền kiểm soát của bạn. Node riêng tạo từng mẫu khối và tự kiểm tra mọi khối; mỗi khối bạn tìm trả toàn bộ phần thưởng thẳng vào ví node.

Phù hợp nhất khi hashrate của bạn thường xuyên tìm khối. Trong ví dụ trên, hệ thống nắm 10% mạng tìm khối khoảng mỗi 50 phút, nên thanh toán solo đều như pool. Nếu đã chạy node Blocknet cho ví hoặc dịch vụ khác, đào solo chỉ thêm trình đào lên đó.

seine cũng được thiết kế cho chế độ này. Nó đào trên mẫu khối của node; nếu node đã quên mẫu, chẳng hạn sau khởi động lại, seine gửi lại toàn bộ khối đã giải để khối tìm được vẫn tới chuỗi.

## Thiết lập đào solo bằng seine thế nào?

1. **Cài và đồng bộ node Blocknet.** Với trình bao `blocknet`, chạy:

   ```sh
   blocknet install latest
   blocknet start mainnet
   ```

   Lệnh ghi cookie API của node vào `~/.config/bnt/data/mainnet/api.cookie`. Để rút ngắn đồng bộ đầu, bntpool.com công bố tệp `checkpoints.dat` trên [trang checkpoint](/checkpoints); [đồng bộ node Blocknet bằng checkpoint](/blog/vi/blocknet-checkpoints-fast-sync/) giải thích node dùng nó thế nào.
2. **Chờ đồng bộ xong.** Thông báo `[sync] progress` của node đạt 100%. Node chỉ cung cấp mẫu khối khi đã bắt kịp chuỗi.
3. **Khởi động seine ở chế độ daemon.**

   ```sh
   ./seine --mode daemon
   ```

   Với bố trí node tùy chỉnh, tự trỏ seine tới API và cookie:

   ```sh
   ./seine --mode daemon --api-url http://127.0.0.1:8332 --cookie /path/to/api.cookie
   ```

[Hướng dẫn đào solo](https://github.com/zpalmtree/seine/blob/master/docs/guides/solo-mining.md) trình bày mật khẩu ví và thiết lập tùy chỉnh; [hướng dẫn đào bằng seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) trình bày tải seine và có share đầu tiên. Bạn có thể [tải seine](https://seineminer.com/vi/#download) cho Windows, macOS hoặc Linux.

## Nên chọn cách nào?

Áp dụng công thức với hashrate riêng và hashrate mạng hiện tại từ [bảng điều khiển](/), rồi xem thời gian trung bình giữa các khối.

- **Bắt đầu trên bntpool.com** nếu đào bằng máy bàn, laptop hoặc vài máy chủ, hoặc muốn thanh toán đều mà không chạy node. Chạy seine, dán địa chỉ hoặc `@name`, chấp nhận pool mặc định.
- **Đào solo** nếu hệ thống nắm tỷ lệ lớn trong mạng để tự tìm khối thường xuyên, và bạn đã chạy hoặc muốn chạy node đồng bộ riêng.

seine giúp bạn dễ đổi lựa chọn sau này. Đào pool và chế độ daemon dùng cùng bản tải, nên có thể chuyển bằng một cờ và khởi động lại khi mở rộng phần cứng. Nếu chạy node cục bộ trong khi đào pool, seine cũng hiển thị số dư ví node cạnh số dư pool.

<Note>
Theo [blocknetcrypto.com](https://blocknetcrypto.com/#faraday), Blocknet dự kiến nâng cấp mạng tên Faraday vào khoảng ngày 16 tháng 2 năm 2027. Nâng cấp dự kiến thay đổi Proof of Work. Hãy cập nhật seine: phần mềm kiểm tra bản phát hành mới khi khởi động và mỗi 12 giờ.
</Note>

<CTA target="pool" title="Bắt đầu đào trên bntpool.com" text="Chạy seine, dán địa chỉ và kiếm một phần từ mỗi khối pool tìm được, thanh toán tự động từ 0,1 BNT." />
