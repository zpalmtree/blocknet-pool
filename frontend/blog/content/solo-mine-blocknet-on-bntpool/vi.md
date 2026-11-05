---
title: Đào Blocknet solo trên bntpool.com
description: 'Đào Blocknet solo mà không cần chạy node. Thêm solo: vào trước địa chỉ trong seine và bntpool.com trả cho bạn toàn bộ phần thưởng của mỗi khối bạn tìm được.'
tags: [Hướng dẫn đào coin, Đào solo, bntpool, seine]
faq:
  - q: 'Làm thế nào để đào Blocknet solo trên bntpool.com?'
    a: 'Cập nhật seine lên v0.3.1 trở lên và thêm solo: vào trước địa chỉ nhận thanh toán, ví dụ ./seine --address solo:YOUR_ADDRESS. seine kết nối đến bntpool.com như bình thường, và pool trả cho bạn toàn bộ phần thưởng của mỗi khối dàn máy của bạn tìm được qua các khoản thanh toán thông thường. Bạn không cần chạy node.'
  - q: 'Có cần node Blocknet riêng để đào solo không?'
    a: 'Không. Với tiền tố solo:, bntpool.com chạy node, tạo mẫu khối và gửi các khối bạn tìm được. Dàn máy của bạn chỉ chạy seine. Chế độ daemon của seine là cách khác để đào solo, với node đã đồng bộ của riêng bạn, phù hợp cho thợ đào đã chạy sẵn một node.'
  - q: 'Có thể dùng tên Blocknet ID để đào solo không?'
    a: 'Có. Dùng solo:@name hoặc solo:$name thay cho solo:ADDRESS, và đặt trong dấu nháy trên dòng lệnh để shell truyền nguyên vẹn. Pool phân giải tên thành địa chỉ đầy đủ, rồi ghi có các khối solo của bạn và trả về địa chỉ đó như mọi khoản thanh toán khác.'
  - q: 'Làm sao quay lại phần thưởng pool?'
    a: 'Bỏ tiền tố solo: khỏi địa chỉ và khởi động lại seine. Dàn máy của bạn quay lại chia phần thưởng PPLNS từ mọi khối pool tìm được. Mỗi dàn máy chọn chế độ khi kết nối, nên bạn có thể chạy một số dàn máy solo và số khác nhận phần thưởng chia sẻ với cùng một địa chỉ.'
sourceHash: 623b1e0b356a
translation: draft
---

Hướng dẫn này cho bạn biết cách đào Blocknet solo trên bntpool.com: thêm một tiền tố vào địa chỉ trong seine, và pool trả cho bạn toàn bộ phần thưởng của mỗi khối các dàn máy của bạn tìm được. Bạn không phải cài đặt hay đồng bộ node. Bạn sẽ thiết lập, xem cách khối solo được thanh toán và biết nơi theo dõi chúng trên pool.

## Đào solo trên bntpool.com là gì?

<Answer>
Đào solo trên bntpool.com chỉ trả cho bạn những khối do chính dàn máy của bạn tìm được, và mỗi khối trả toàn bộ phần thưởng về địa chỉ của bạn. Pool vẫn chạy node, tạo mẫu khối và gửi khối của bạn, nên bạn nhận phần thưởng solo với cùng cách thiết lập như đào pool. Bạn bật chế độ này bằng cách thêm solo: vào trước địa chỉ.
</Answer>

Khi đào pool thông thường, bntpool.com chia mỗi khối cho các thợ đào có share nằm trong cửa sổ PPLNS sáu giờ, nên bạn nhận một phần của mỗi khối pool tìm được. Dàn máy solo thì giữ toàn bộ phần thưởng các khối của chính nó và không nhận gì từ khối người khác tìm được. Cả hai dùng cùng máy chủ, cùng địa chỉ và cùng cách thanh toán. Chỉ phần đăng nhập thay đổi.

Đào solo qua pool cho bạn phần thưởng solo mà không phải lo vận hành node:

- **Không cần chạy node.** Node của pool theo dõi chuỗi, cung cấp mẫu khối mới và gửi các khối bạn giải được.
- **Thanh toán tự động.** Khối của bạn được trả về địa chỉ hoặc tên Blocknet ID như mọi khoản thanh toán bntpool.com khác.
- **Thống kê trực tiếp.** Dàn máy, hashrate và khối của bạn hiện trên [trang thống kê](/stats) của pool.
- **Một tiền tố để chuyển đổi.** Bỏ `solo:` và khởi động lại seine để quay lại phần thưởng chia sẻ.

Chế độ daemon của seine là cách khác để đào solo, với node Blocknet đã đồng bộ của riêng bạn. [Đào Blocknet bằng pool hay solo](/blog/vi/pool-vs-solo-mining-blocknet/) so sánh cả ba phương án và cho thấy hashrate của bạn tìm khối thường xuyên đến đâu.

## Bắt đầu đào solo bằng seine thế nào?

<Answer>
Cài seine v0.3.1 trở lên, rồi khởi động với solo: trước địa chỉ Blocknet, ví dụ ./seine --address solo:YOUR_ADDRESS trên macOS hoặc Linux. seine kết nối đến bntpool.com, pool mặc định của nó, và bảng điều khiển gắn nhãn Solo cho tài khoản của bạn. Bạn cũng có thể gõ solo: cùng địa chỉ khi seine hỏi địa chỉ nhận thanh toán ở lần chạy đầu.
</Answer>

1. **Cập nhật seine.** Đăng nhập solo cần seine v0.3.1 trở lên. [Tải seine](https://seineminer.com/vi/#download), hoặc để cập nhật tự động cài giúp bạn.
2. **Khởi động seine với tiền tố.** Trên macOS hoặc Linux:

   ```sh
   ./seine --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   Trên Windows, trong PowerShell:

   ```powershell
   .\seine.exe --address solo:YOUR_BLOCKNET_ADDRESS
   ```

   seine mặc định đào trên bntpool.com. [Trang Get Started](/start) của pool hiển thị cùng lệnh với địa chỉ pool ghi đầy đủ.
3. **Kiểm tra bảng điều khiển.** Bảng số dư gắn nhãn **Solo** cho tài khoản của bạn, và nhật ký hiển thị xác nhận của pool rằng bạn được trả cho các khối địa chỉ này tìm được.

Tiền tố dùng được ở mọi nơi seine nhận địa chỉ: lời nhắc ở lần chạy đầu, cấu hình đã lưu, API điều khiển cục bộ và các dàn máy qua farm proxy. seine lưu tiền tố cùng địa chỉ, nên các lần chạy sau vẫn đào solo.

## Có thể đào solo bằng tên Blocknet ID không?

Có. Tên Blocknet ID hoạt động giống như địa chỉ đầy đủ. Viết `solo:@name` hoặc `solo:$name`, và đặt trong dấu nháy để shell truyền nguyên vẹn:

```sh
./seine --address 'solo:@yourname'
```

Pool phân giải tên tại blocknet.id và trả các khối solo về địa chỉ đứng sau tên đó. Để đặt tên dàn máy, thêm tên vào sau địa chỉ đầy đủ bằng dấu chấm, như `solo:YOUR_ADDRESS.rig1`, hoặc dùng `--pool-worker rig1` với bất kỳ địa chỉ hay tên nào. Tiền tố không phân biệt chữ hoa chữ thường, nên `SOLO:` cũng dùng được.

## Khối solo được thanh toán thế nào?

Khi dàn máy của bạn tìm được khối, pool ghi nhận đó là khối solo. Khi khối đã sâu 60 khối, khoảng năm giờ, pool ghi có toàn bộ phần thưởng khối, trừ phí pool, vào số dư của bạn. Phí là 0% tính đến ngày 4 tháng 11 năm 2026. Số dư sau đó được trả về địa chỉ của bạn qua các khoản thanh toán thông thường của pool.

Thanh toán theo cùng quy tắc như đào pool. Mỗi khối chờ 60 xác nhận để một lần tái tổ chức chuỗi ngắn không thể hủy phần thưởng đã trả, và [xác nhận khối và phần thưởng đào](/blog/vi/block-confirmations-mining-rewards/) giải thích lý do. Endpoint `/api/info` của pool báo mức thanh toán tối thiểu 1 BNT vào ngày 4 tháng 11 năm 2026, và một phần thưởng khối Blocknet cao hơn mức đó rất nhiều, nên khối solo đã xác nhận được trả theo lịch thanh toán thông thường của pool. [Trang thanh toán](/payouts) liệt kê mọi khoản pool gửi đi.

Nếu bạn đào về địa chỉ nạp của sàn kèm memo thanh toán, đào solo cũng hoạt động như vậy. Memo và mức thanh toán tối thiểu riêng của nó áp dụng cho khối solo giống như cho phần thưởng chia sẻ.

Công việc của dàn máy vẫn được tính vào hashrate, effort vòng và luck của pool, và một khối solo kết thúc vòng của pool như mọi khối khác. Share solo không bao giờ vào cửa sổ PPLNS, nên thợ đào khác không nhận gì từ khối của bạn, và bạn cũng không nhận gì từ khối của họ.

## Dàn máy solo tìm được khối thường xuyên đến đâu?

Blocknet nhắm một khối mỗi năm phút, tức 288 khối mỗi ngày. Trung bình, một dàn máy solo tìm được 288 × (hashrate của bạn ÷ hashrate mạng) khối mỗi ngày. Xem hashrate mạng trên [trang chủ bntpool.com](/) và hashrate của bạn trên bảng điều khiển seine, rồi lấy một ngày chia cho kết quả để có thời gian trung bình giữa các khối của bạn.

Phần thưởng solo đến dưới dạng nguyên khối, nên phù hợp với thợ đào có hashrate tìm khối thường xuyên: trang trại lớn, cụm máy chủ và bất kỳ ai nắm tỷ lệ lớn trong mạng. Dàn máy nhỏ hơn kiếm trung bình bằng như vậy với phần thưởng PPLNS chia sẻ, được trả thành từng phần đều đặn từ mọi khối pool tìm được. [Đào Blocknet bằng pool hay solo](/blog/vi/pool-vs-solo-mining-blocknet/) trình bày ví dụ ở ba mức hashrate.

## Xem dàn máy và khối solo ở đâu?

Tra địa chỉ của bạn trên [trang thống kê](/stats). Bạn có thể dán địa chỉ có hoặc không có tiền tố `solo:`. Trang sẽ đánh dấu công việc solo của bạn:

- **Workers** liệt kê từng dàn máy với cột Type ghi **Solo** cho các dàn máy đã gửi share solo trong cửa sổ hashrate.
- **Estimated rewards** đánh dấu các khối solo của bạn trong lúc chờ xác nhận.
- **Blocks** và **Luck** hiển thị cột Type, Pool hoặc Solo, trước trạng thái của mỗi khối. [Trang khối](/blocks) liệt kê mọi khối pool đã tìm, mới nhất trước.

[Theo dõi trình đào trên bntpool.com](/blog/vi/track-your-miner-on-bntpool/) hướng dẫn phần còn lại của trang thống kê.

## Có thể kết hợp dàn máy solo và pool không?

Có. Mỗi dàn máy chọn chế độ khi kết nối, nên một địa chỉ có thể chạy dàn máy solo và dàn máy pool cùng lúc. Một trang trại có thể đào solo trên các máy chủ lớn nhất và giữ máy bàn ở phần thưởng chia sẻ, tất cả trả về một số dư. Pool tách riêng hai loại công việc: share tìm trên kết nối solo chỉ tính cho khối solo của địa chỉ đó.

Để chuyển một dàn máy về lại, bỏ `solo:` khỏi địa chỉ của nó, trong lệnh hoặc cấu hình đã lưu, rồi khởi động lại seine. Share dàn máy đã gửi ở chế độ solo vẫn là solo, còn share mới vào cửa sổ PPLNS ngay lập tức.

<CTA target="pool" title="Đào solo trên bntpool.com" text="Thêm solo: vào địa chỉ trong seine và giữ toàn bộ phần thưởng của mỗi khối dàn máy của bạn tìm được, không cần chạy node." />
