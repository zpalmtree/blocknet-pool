---
title: 'Đổi địa chỉ ví đào Blocknet'
description: 'Đổi địa chỉ nhận của các máy đào Blocknet, kiểm tra share mới trên bntpool.com và theo dõi phần thưởng trước đó vẫn gắn với địa chỉ ví cũ.'
tags: ['Hướng dẫn đào', 'Ví', 'Thanh toán']
faq:
  - q: 'Đổi địa chỉ có chuyển số dư cũ không?'
    a: 'Đích mới hướng công việc gửi sau đó đến địa chỉ mới. Share, khoản ghi có và thanh toán hiện có vẫn gắn với địa chỉ gốc. Tra cứu riêng cả hai trong Thống kê của tôi và giữ quyền truy cập ví cũ khi theo dõi phần thưởng còn lại.'
  - q: 'Có phải đổi mọi máy tính không?'
    a: 'Có. Mỗi tiến trình dùng đích đã cấu hình riêng. Cập nhật và kiểm tra một máy trước, rồi các máy khác cùng tệp lưu và tham số khởi chạy. Tên máy riêng giúp nhận biết máy đã gửi công việc đến địa chỉ mới.'
  - q: 'Đổi Blocknet ID có cập nhật máy đang chạy không?'
    a: 'seine phân giải ID đã lưu khi khởi chạy và giữ địa chỉ suốt phiên, kể cả kết nối lại. Sau khi đổi ánh xạ, khởi động lại từng máy đào và kiểm tra địa chỉ đầy đủ. Công việc và số dư cũ không được gán lại.'
sourceHash: 'fca912d46d55'
translation: draft
---

Đổi đích nhận cho công việc tương lai và giữ rõ lịch sử phần thưởng cũ. Hướng dẫn dùng seine đã kết nối bntpool.com. Lần đầu kết nối xem [hướng dẫn pool](/blog/vi/mine-blocknet-on-bntpool/); ví mới xem [hướng dẫn ví](https://seineminer.com/vi/blog/blocknet-wallet-for-mining/).

## Đổi địa chỉ đào như thế nào?

<Answer>
Dừng một máy đào, cập nhật địa chỉ trong tệp hoặc lệnh thường dùng rồi khởi động lại. Kiểm tra địa chỉ đầy đủ đã phân giải và share được chấp nhận, sau đó tra địa chỉ trên bntpool.com. Lặp lại với máy khác. Share và phần thưởng cũ vẫn gắn với địa chỉ lúc được ghi nhận.
</Answer>

*Công việc mới tới ví thứ hai; bản ghi phần thưởng cũ vẫn gắn với ví thứ nhất.*

## Ghi lại cả hai đích trước khi đổi

Sao chép địa chỉ mới trực tiếp từ ví định dùng. Phần mềm cần đích công khai hoặc Blocknet ID đã đăng ký, không cần cụm từ khôi phục hay khóa riêng. Giữ cả địa chỉ đầy đủ cũ: đó vẫn là khóa tra hoạt động trước đây.

Mở Thống kê của tôi cho địa chỉ cũ, ghi phần thưởng chờ, số dư xác nhận, thanh toán gần đây và tên máy đang góp việc. Đây là mốc đối chiếu; trang mới không chứa lịch sử cũ. Giữ quyền truy cập ví ban đầu khi theo dõi phần thưởng và thanh toán còn lại.

## Cập nhật một máy và lệnh khởi chạy

Dừng một máy và xem cách khởi chạy thường dùng. Với cấu hình có sẵn, `--address` ưu tiên mà không thay địa chỉ lưu. Ví dụ PowerShell chọn đích cho lần chạy này. Thay `@yourname` bằng ID mới hoặc địa chỉ đầy đủ, giữ tham số pool, dữ liệu và phần cứng đã kiểm tra.

```powershell
.\seine.exe --address "@yourname"
```

Trên Linux hoặc macOS, dùng cú pháp dưới đây từ thư mục thường dùng. Cả hai ví dụ bắt đầu đào thật; chọn đúng hệ điều hành. Bản cài hiện có cung cấp thiết lập còn lại. Giữ tên máy để nhận ra share của nó dưới địa chỉ mới.

```sh
./seine --address "@yourname"
```

## Dùng địa chỉ mới khi khởi chạy bình thường

Khi phần mềm dừng, sửa `address` trong `seine.config.json`. Giữ các trường khác và JSON hợp lệ. `./seine-data` tương đối với thư mục làm việc; `--data-dir` có thể chọn nơi khác. Sửa đúng tệp mà cách khởi chạy bình thường sử dụng.

Cập nhật hoặc bỏ `--address` cũ trong lối tắt, tập lệnh, tác vụ hoặc dịch vụ. Lệnh thủ công thành công không cập nhật những cách chạy khác. Khởi động lại bình thường và kiểm tra. [Hướng dẫn cấu hình](https://seineminer.com/vi/blog/configure-seine-miner/) giải thích ưu tiên và việc đổi thư mục có thể chọn tệp khác.

## Điều gì khác khi dùng Blocknet ID?

Trong seine v0.2.36, `@name` được phân giải và xác thực lúc khởi chạy. Địa chỉ đầy đủ giữ nguyên trong phiên, kể cả nối lại pool. Nếu đổi ánh xạ tên, khởi động lại từng máy và kiểm tra đích. Chỉ nối lại tiến trình cũ không lặp bước phân giải đầu tiên.

Ghi cả hai địa chỉ đầy đủ dù tên dễ đọc không đổi. Hạch toán theo địa chỉ đã phân giải; đổi ánh xạ không chuyển share hay số dư cũ. Nếu tên không phân giải, kiểm tra ánh xạ hoặc chủ động dùng địa chỉ đầy đủ đã xác nhận trong ví.

## Kiểm tra mọi máy dưới địa chỉ mới

Kiểm tra đích và gửi được chấp nhận, rồi tra địa chỉ đầy đủ mới trong Thống kê của tôi. Đối chiếu tên máy và share gần đây. Ước tính hashrate mới cần công việc và thời gian; kiểm tra việc đến trước. [Hướng dẫn thống kê](/blog/vi/track-your-miner-on-bntpool/) giải thích bộ đếm và máy chưa xuất hiện.

Sau máy đầu tiên, lặp lại trên các máy còn lại. Tên riêng giúp kiểm tra dễ hơn. Thử cả khởi chạy bình thường trên từng máy để lệnh cũ không khôi phục đích trước đó. [Hướng dẫn nhiều máy](/blog/vi/mine-multiple-computers-one-wallet/) giải thích tổ chức dưới một địa chỉ nhận.

## Phần thưởng ở địa chỉ cũ sẽ ra sao?

Ghi có và thanh toán hiện có vẫn ở địa chỉ cũ. Share hợp lệ trước đó có thể tiếp tục góp vào cửa sổ PPLNS tương ứng; phần thưởng chờ tuân theo xác nhận. Hashrate bằng không không có nghĩa hạch toán đã xong. [PPLNS](/blog/vi/how-pplns-payouts-work/) và [xác nhận](/blog/vi/block-confirmations-mining-rewards/) phân biệt công việc, phần thưởng trưởng thành và thanh toán.

Đổi đích không gộp số dư hay bỏ qua ngưỡng thanh toán. Kiểm tra quy tắc hiện tại và cả hai trang đến khi hiểu số tiền còn lại. Đổi tên máy cũng không chuyển phần thưởng. Xác nhận mọi máy dự định đã gửi việc mới tới địa chỉ mới, đồng thời giữ ví cũ và lịch sử.

<CTA target="pool" title="Kiểm tra thiết lập trên bntpool.com" text="Xác nhận đích và công việc được nhận của từng máy, rồi theo dõi riêng phần thưởng cũ và mới." />
