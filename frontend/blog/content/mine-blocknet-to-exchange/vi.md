---
title: Đào Blocknet thẳng vào sàn giao dịch
description: 'Đào Blocknet thẳng vào địa chỉ nạp SafeTrade hoặc sàn khác trên bntpool.com. Thêm memo nạp tiền trong seine và pool trả thưởng vào tài khoản của bạn.'
tags: [Hướng dẫn đào coin, bntpool, SafeTrade, Thanh toán]
faq:
  - q: 'Có thể đào Blocknet thẳng vào sàn giao dịch không?'
    a: 'Có. Khởi động seine v0.3.1 trở lên với địa chỉ nạp của sàn và memo nạp tiền của bạn, như ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. bntpool.com trả phần thưởng của bạn vào địa chỉ đó kèm memo, nên sàn ghi có vào tài khoản của bạn.'
  - q: 'Vì sao seine hỏi memo thanh toán?'
    a: 'Các sàn như SafeTrade dùng một địa chỉ nạp chung cho mọi khách hàng và phân biệt các khoản nạp bằng memo. seine nhận biết các địa chỉ chung này, nên khi bạn nhập một địa chỉ như vậy, nó hỏi memo và lưu cùng địa chỉ. Hãy sao chép memo chính xác từ trang nạp tiền của sàn.'
  - q: 'Mức thanh toán tối thiểu vào sàn trên bntpool.com là bao nhiêu?'
    a: 'Thanh toán có memo bắt đầu từ 10 BNT, theo số liệu pool báo ngày 4 tháng 11 năm 2026. Số dư đã xác nhận của bạn tích lũy trên pool đến khi đạt mức đó, rồi pool gửi về địa chỉ sàn kèm memo. Thanh toán về ví cá nhân bắt đầu từ 1 BNT.'
  - q: 'Làm sao kiểm tra số dư khi đào vào sàn?'
    a: 'Trên trang thống kê bntpool.com, nhập địa chỉ nạp của sàn, chọn Thêm memo và nhập memo của bạn. Mỗi cặp địa chỉ và memo có số dư riêng, nên bạn chỉ thấy hashrate, dàn máy và các khoản thanh toán của chính mình, không phải của khách hàng khác dùng cùng địa chỉ sàn.'
sourceHash: 8f73aae25c7a
translation: draft
---

Hướng dẫn này cho bạn biết cách đào Blocknet thẳng vào tài khoản sàn giao dịch trên bntpool.com. Bạn cung cấp cho seine địa chỉ nạp của sàn và memo nạp tiền, pool trả phần thưởng vào đó kèm memo, và sàn ghi có cho bạn. Bài viết trình bày lệnh, mức thanh toán tối thiểu và cách kiểm tra số dư.

## Có thể đào Blocknet thẳng vào sàn không?

<Answer>
Có. bntpool.com trả phần thưởng đào vào địa chỉ nạp của sàn kèm memo nạp tiền của bạn. Khởi động seine v0.3.1 trở lên với địa chỉ sàn và memo: ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO. Thanh toán có memo bắt đầu từ 10 BNT, và sàn ghi có từng khoản vào tài khoản của bạn.
</Answer>

Đào vào sàn giúp bớt một bước khi bạn định giao dịch số coin đào được. Phần thưởng vào thẳng tài khoản sàn, sẵn sàng bán trên [thị trường BNT/USDT](/blog/vi/sell-blocknet-bnt-safetrade/), không cần chuyển qua ví.

Các sàn như SafeTrade cho mọi khách hàng cùng một địa chỉ nạp và phân biệt các khoản nạp bằng memo, một mã ngắn riêng cho tài khoản của bạn. Vì vậy memo rất quan trọng: chính memo gắn khoản thanh toán với bạn. Pool giữ số dư riêng cho từng cặp địa chỉ và memo, nên phần thưởng của bạn không bao giờ lẫn với của thợ đào khác dùng cùng sàn.

## Thiết lập đào vào sàn trong seine thế nào?

<Answer>
Sao chép địa chỉ nạp BNT và memo nạp tiền từ trang nạp tiền của sàn. Khởi động seine với cả hai: --address cho địa chỉ và --payout-memo cho memo. Khi bạn nhập một địa chỉ sàn đã biết ở lời nhắc lần chạy đầu, seine tự hỏi memo và lưu lại. Kiểm tra nhật ký để thấy xác nhận của pool, rồi đào như bình thường.
</Answer>

1. **Lấy thông tin nạp tiền.** Trên sàn, mở trang nạp BNT và sao chép địa chỉ nạp cùng memo. Sao chép memo chính xác: chữ hoa và số 0 ở đầu đều quan trọng.
2. **Khởi động seine với cả hai.** Trên macOS hoặc Linux:

   ```sh
   ./seine --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

   Trên Windows, trong PowerShell:

   ```powershell
   .\seine.exe --address EXCHANGE_ADDRESS --payout-memo YOUR_MEMO
   ```

3. **Để seine lưu lại.** seine lưu memo cùng địa chỉ, nên các lần chạy sau vẫn trả vào tài khoản sàn của bạn. Nếu bạn gõ một địa chỉ sàn đã biết ở lời nhắc lần chạy đầu, seine hỏi memo và lưu cả hai.

[Trang Get Started](/start) của pool có mục “Đào trực tiếp vào sàn giao dịch?” tạo sẵn lệnh này cho bạn: dán địa chỉ và memo, rồi sao chép kết quả. seine cần memo cho các địa chỉ sàn chung đã biết như của SafeTrade. Dịch vụ, script và các trình đào chạy không giám sát khác phải truyền `--payout-memo` trên dòng lệnh, vì không có lời nhắc nào để trả lời.

seine kiểm tra memo với pool khi đăng nhập. bntpool.com xác nhận, rồi việc đào bắt đầu. Để đặt tên dàn máy, dùng `--pool-worker rig-01` như với địa chỉ cá nhân.

## Thanh toán vào sàn diễn ra thế nào?

Phần thưởng đi theo cùng lộ trình như mọi khoản thanh toán bntpool.com. Share của bạn nhận một phần mỗi khối pool tìm được trong cửa sổ PPLNS sáu giờ, và phần thưởng mỗi khối được xác nhận sau 60 khối. Số dư đã xác nhận của bạn sau đó tích lũy trên pool.

Endpoint `/api/info` của pool báo các mức thanh toán tối thiểu sau vào ngày 4 tháng 11 năm 2026:

| Nơi nhận | Thanh toán tối thiểu |
|---|---|
| Địa chỉ sàn kèm memo | 10 BNT |
| Ví cá nhân | 1 BNT |

Khi số dư đã xác nhận đạt 10 BNT, pool gửi về địa chỉ sàn kèm memo của bạn. Sàn sau đó ghi có khoản nạp sau số xác nhận riêng của sàn; trang nạp tiền ghi rõ cần bao nhiêu. [Cách thanh toán PPLNS hoạt động](/blog/vi/how-pplns-payouts-work/) giải thích chi tiết phía pool.

Đào solo cũng dùng được với memo. Thêm `solo:` vào trước địa chỉ sàn và giữ `--payout-memo`, pool sẽ trả toàn bộ phần thưởng của mỗi khối bạn tìm được vào tài khoản sàn. [Đào solo trên bntpool.com](/blog/vi/solo-mine-blocknet-on-bntpool/) giải thích phần thưởng solo.

## Kiểm tra số dư thế nào?

Mở [trang thống kê](/stats), dán địa chỉ nạp của sàn, chọn **Thêm memo** và nhập memo của bạn. Trang sẽ hiển thị hashrate, dàn máy, số dư và các khoản thanh toán của riêng bạn cho cặp địa chỉ và memo đó. Khách hàng khác của cùng sàn có cặp và số dư riêng.

Mỗi khoản thanh toán trong **Recent Payouts** liên kết đến giao dịch trên trình khám phá. So sánh với lịch sử nạp tiền trên sàn để theo dõi coin vào tài khoản. [Theo dõi trình đào trên bntpool.com](/blog/vi/track-your-miner-on-bntpool/) giải thích phần còn lại của trang thống kê.

## Quay lại ví cá nhân thế nào?

Khởi động seine một lần với địa chỉ mới và `--no-payout-memo`:

```sh
./seine --address YOUR_BLOCKNET_ADDRESS --no-payout-memo
```

seine lưu địa chỉ mới không kèm memo, và share mới được tính cho ví cá nhân. Số dư đã ghi có cho cặp sàn vẫn thuộc cặp đó và được trả về đó khi đạt mức tối thiểu; bạn có thể theo dõi trên trang thống kê. [Đổi địa chỉ ví đào](/blog/vi/change-mining-wallet-address/) giải thích cách pool ghi có sau khi đổi địa chỉ.

## Những thiết lập nào hỗ trợ memo?

Memo cần kết nối trực tiếp của seine đến pool, vốn là mặc định. Chế độ daemon của seine đào với node riêng và trả vào ví node, nên không có memo. Farm proxy không chuyển memo, vì vậy hãy kết nối dàn máy đào vào sàn trực tiếp đến pool. seine chỉ đào trên pool đã xác nhận memo của bạn, nên công việc của bạn luôn được trả vào tài khoản bạn đã chọn.

<CTA target="pool" title="Đào vào tài khoản sàn của bạn" text="Khởi động seine với địa chỉ nạp và memo, và bntpool.com trả thưởng thẳng vào tài khoản sàn của bạn từ 10 BNT." />
