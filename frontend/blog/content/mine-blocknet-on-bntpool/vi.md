---
title: Đào Blocknet trên bntpool.com
description: Tham gia bntpool.com, pool Blocknet mặc định của seine. Kết nối stratum+tcp://bntpool.com:3333 bằng địa chỉ hoặc @name, đặt tên dàn máy và xem thống kê.
tags: [Hướng dẫn đào coin, bntpool, Blocknet ID, seine]
sourceHash: 'df269126bb76'
translation: draft
faq:
  - q: 'Địa chỉ Stratum của bntpool.com là gì?'
    a: 'Địa chỉ là stratum+tcp://bntpool.com:3333, pool seine gợi ý khi chạy lần đầu. Nhấn Enter ở câu hỏi pool để chấp nhận hoặc truyền bằng cờ --pool-url. Địa chỉ Blocknet hoặc tên Blocknet ID là thông tin đăng nhập, và pool trả thưởng đến địa chỉ đó.'
  - q: 'Có thể đào trên bntpool.com bằng tên Blocknet ID không?'
    a: 'Có. Nhập @name hoặc $name ở bất kỳ nơi nào seine hay pool yêu cầu địa chỉ. Pool kiểm tra ánh xạ có chữ ký trên blocknet.id, rồi ghi share và thanh toán vào địa chỉ đầy đủ phía sau tên. Bạn cần seine v0.2.16 trở lên và nên đặt tên trong dấu nháy khi dùng dòng lệnh để shell không sửa nó.'
  - q: 'Có cần tài khoản để đào trên bntpool.com không?'
    a: 'Không. Không đăng ký, mật khẩu hay email. Địa chỉ Blocknet, hoặc tên Blocknet ID trỏ đến nó, vừa là thông tin đăng nhập vừa là nơi nhận thanh toán. Khởi động seine với địa chỉ đó, rồi tra trên My Stats để xem hashrate, share, dàn máy, số dư và thanh toán.'
  - q: 'Có thể đào về một địa chỉ từ nhiều máy tính không?'
    a: 'Có. Chạy seine trên mỗi máy với cùng địa chỉ hoặc tên và đặt tên dàn máy riêng cho từng máy. Pool tính share, số dư và thanh toán theo địa chỉ, nên mọi máy đóng góp vào một số dư và một khoản thanh toán; My Stats liệt kê mỗi dàn máy ở hàng riêng.'
---

Hướng dẫn này trình bày cách tham gia pool bntpool.com bằng seine: địa chỉ pool, đăng nhập bằng địa chỉ Blocknet hoặc Blocknet ID `@name`, đặt tên từng dàn máy và kiểm tra share đến nơi trên trang thống kê. Nếu chưa có seine, hãy làm theo [hướng dẫn cài seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) trước, rồi quay lại đây.

## Địa chỉ pool bntpool.com là gì?

<Answer>
Địa chỉ Stratum của bntpool.com là `stratum+tcp://bntpool.com:3333`. seine đưa ra làm pool mặc định lần đầu chạy, nên chỉ cần nhấn Enter ở câu hỏi pool để kết nối. Bạn đăng nhập bằng địa chỉ Blocknet hoặc Blocknet ID `@name`. Không cần tạo tài khoản và phần thưởng đến địa chỉ đó.
</Answer>

Đây là cài đặt pool do endpoint công khai `/api/info` báo ngày 25 tháng 10 năm 2026:

| Cài đặt | Giá trị |
|---|---|
| Địa chỉ Stratum | `stratum+tcp://bntpool.com:3333` |
| Đăng nhập | Địa chỉ Blocknet, `@name` hoặc `$name` |
| Phương thức trả thưởng | PPLNS với cửa sổ 6 giờ |
| Xác nhận trước thanh toán | 60 khối |
| Thanh toán tối thiểu | 0,1 BNT |

Bạn có thể xem cùng các giá trị bất cứ lúc nào trên trang [Get Started](/start) của pool.

## Vì sao bntpool.com là pool mặc định của seine?

Lần đầu chạy, seine hỏi pool và điền `stratum+tcp://bntpool.com:3333` làm mặc định. Pool và trình đào phối hợp theo một số cách thực tế:

- **Cùng giao thức.** bntpool.com chạy giao thức Stratum mà seine sử dụng (phiên bản 2), nên seine kết nối mà không cần cấu hình ngoài địa chỉ.
- **Tên hoạt động ở cả hai phía.** seine và pool đều nhận tên Blocknet ID và xác minh bằng cùng bản ghi có chữ ký trên blocknet.id.
- **Kiểm tra phiên bản.** Mỗi dàn máy gửi phiên bản seine cùng share; trang thống kê đánh dấu máy chạy seine cũ bằng nhãn “update available” dẫn đến trang phát hành seine.
- **Hướng dẫn thiết lập seine.** Trang Get Started của pool hướng dẫn tải và chạy seine, kèm lệnh có thể sao chép.

## Kết nối seine với bntpool.com thế nào?

Lần đầu khởi động, seine hỏi hai câu:

```text
Enter your Blocknet address or shortname (@name / $name):
Enter pool URL [default: stratum+tcp://bntpool.com:3333]:
```

Dán địa chỉ hoặc nhập tên, rồi nhấn Enter để chấp nhận bntpool.com. seine lưu cả hai câu trả lời vào `seine-data/seine.config.json`, nên mọi lần khởi động sau đi thẳng vào đào. Nó cũng chọn tên dàn máy và lưu cùng tệp (chi tiết bên dưới).

Trên dàn máy không giao diện, dịch vụ hoặc script, không có người trả lời câu hỏi. Hãy truyền cả hai giá trị bằng cờ ở lần khởi động đầu; đây cũng là lệnh trang Get Started cung cấp:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address YOUR_BLOCKNET_ADDRESS
```

Để đổi pool hoặc địa chỉ sau này, sửa hay xóa `seine-data/seine.config.json`, hoặc ghi đè giá trị đã lưu bằng `--address` và `--pool-url` trên dòng lệnh.

## Có thể đào về tên Blocknet ID @name không?

<Answer>
Có. bntpool.com nhận tên ngắn Blocknet ID viết dạng `@name` hoặc `$name` ở mọi nơi nhận địa chỉ. Pool kiểm tra ánh xạ có chữ ký từ blocknet.id, xác thực địa chỉ tên trỏ đến, và ghi share, số dư cùng thanh toán vào địa chỉ đầy đủ đó trong suốt kết nối.
</Answer>

Tên không phân biệt hoa thường, nên `@Alice` và `@alice` là cùng thông tin đăng nhập. Hỗ trợ tên có từ seine v0.2.16; mọi bản hiện hành đều có. Trước khi đào, seine in địa chỉ được phân giải để bạn kiểm tra khớp ví. Trên dòng lệnh, đặt tên trong dấu nháy đơn. Không có nháy, shell sẽ cố mở rộng `$name` thành biến:

```sh
./seine --pool-url stratum+tcp://bntpool.com:3333 --address '@yourname'
```

Nếu sau này trỏ tên sang ví khác, hãy khởi động lại seine. seine phân giải tên đã lưu mỗi lần khởi động và giữ địa chỉ đó suốt lần chạy; pool làm tương tự cho mỗi kết nối. Số dư đã kiếm vẫn ở địa chỉ mà bạn kiếm cho và được trả về đó, nên chuyển tên không bao giờ chuyển coin.

Bạn có thể đăng ký tên tại [blocknet.id](https://blocknet.id). [Hướng dẫn Blocknet ID](https://seineminer.com/vi/blog/blocknet-id-pay-by-name/) giải thích thanh toán bằng tên.

## Đặt tên dàn máy thế nào?

Mỗi kết nối đăng nhập bằng tên dàn máy cùng địa chỉ. Lần đầu chạy, seine chọn tên cho bạn: `seine-` kèm bốn chữ số, rồi lưu trong cấu hình để cùng máy giữ nguyên tên qua các lần khởi động lại. Muốn dùng tên dễ nhận biết, đặt trên dòng lệnh:

```sh
./seine --pool-worker rig-01
```

Pool giữ tối đa 64 ký tự của tên. Tên ổn định cũng hữu ích sau khởi động lại: pool nhớ độ khó share của từng máy trong một giờ, theo cặp địa chỉ và tên máy, nên máy kết nối lại tiếp tục ở độ khó đã ổn định trước đó.

## Có thể đào về một địa chỉ từ nhiều dàn máy không?

Có. Khởi động seine trên mỗi máy với cùng địa chỉ hoặc `@name` và tên dàn máy khác nhau. Pool tính share, số dư và thanh toán theo địa chỉ, nên mọi máy cộng vào một số dư và một khoản thanh toán. Trên trang thống kê, mỗi máy có hàng riêng, giúp dễ thấy tất cả đều đang đào.

## Làm sao kiểm tra trình đào hoạt động?

Bảng điều khiển seine hiển thị hashrate và share được chấp nhận ngay trên máy. Để xem góc nhìn pool, mở [My Stats](/stats), nhập địa chỉ, `@name` hoặc `$name`, rồi nhấn **Lookup**. Nếu nhập tên, trang hiển thị địa chỉ phân giải cạnh ô tìm kiếm. Trang cũng nhớ lần tra cuối trong trình duyệt đó; [hướng dẫn theo dõi trình đào](/blog/vi/track-your-miner-on-bntpool/) trình bày từng thẻ.

Hãy tìm ba điều:

- **Hashrate và Shares Accepted.** Thẻ hashrate hiển thị những gì pool thấy từ bạn; số share chấp nhận tăng khi máy gửi công việc.
- **Một hàng cho mỗi máy.** Mỗi hàng cho biết tên máy, hashrate, share chấp nhận và từ chối, cùng thời gian share cuối. Share vừa gửi chứng tỏ máy đang kết nối và hoạt động.
- **Recent Shares.** Các share gần nhất được liệt kê với máy gửi, độ khó và trạng thái.

Pool đặt độ khó share riêng cho từng kết nối và điều chỉnh khi bạn đào, nên ngay cả CPU nhỏ cũng gửi share đều đặn; các share chấp nhận đầu tiên xuất hiện trong vài phút sau kết nối. Để biết hashrate kỳ vọng từ phần cứng, xem [hướng dẫn phần cứng đào Blocknet](https://seineminer.com/vi/blog/blocknet-mining-hardware-hashrate/).

Phần thưởng đến sau. Estimated Rewards xuất hiện khi pool tìm khối trong lúc share của bạn ở cửa sổ sáu giờ. Sau khi khối có 60 xác nhận, phần của bạn chuyển sang Confirmed Rewards; pool tự động gửi khi có ít nhất 0,1 BNT. [Thanh toán PPLNS hoạt động thế nào](/blog/vi/how-pplns-payouts-work/) giải thích chia thưởng mỗi khối. Lịch sử toàn pool ở các trang [Blocks](/blocks) và [Payouts](/payouts).

## Trang Get Started có gì?

Trang [Get Started](/start) của pool gom mọi thứ trên một màn hình:

- **Thông tin pool.** Địa chỉ Stratum (nhấp để sao chép), phương thức trả thưởng, mức thanh toán tối thiểu và số xác nhận trước thanh toán, đọc trực tiếp từ pool.
- **Bắt đầu nhanh trong ba bước.** Tải seine từ [seineminer.com](https://seineminer.com/vi/#download), chạy một lần và nhập địa chỉ hoặc tên cùng URL pool, rồi bắt đầu đào và theo dõi hashrate, số dư trên My Stats.
- **Lệnh để sao chép.** Lệnh `./seine` đầy đủ đã điền URL pool. Thay bằng địa chỉ của bạn hoặc `'@yourname'` trong dấu nháy.
- **Ghi chú thiết lập.** Nơi seine lưu cấu hình, cách đặt lại và các cờ ghi đè cấu hình.
- **Cách tính phần thưởng.** Giải thích ngắn về PPLNS: share gửi trước khi tìm khối kiếm phần thưởng của khối đó; công việc thêm sau được tính cho khối tiếp theo. Cũng lưu ý phần thưởng từ khối chưa xác nhận được hiển thị riêng trên My Stats cho đến khi khối xác nhận.
- **Ảnh chụp màn hình** thiết lập lần đầu của seine và bảng điều khiển đào, để bạn biết trước giao diện khi bắt đầu.

<CTA target="pool" title="Bắt đầu đào trên bntpool.com" text="Trỏ seine đến bntpool.com bằng địa chỉ hoặc @name và xem share của bạn xuất hiện trên My Stats." />
