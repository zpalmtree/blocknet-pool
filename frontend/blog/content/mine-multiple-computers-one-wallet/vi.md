---
title: 'Đào Blocknet từ nhiều máy tính về một ví'
description: 'Thêm máy đào Blocknet thứ hai vào cùng ví trên bntpool.com. Đặt tên từng máy, kiểm tra share và hiểu cách gộp phần thưởng cùng cách xử lý tên máy bị trùng.'
tags: ['Hướng dẫn đào', 'Máy tính', 'Thiết lập']
faq:
  - q: 'Mỗi máy tính có cần ví riêng không?'
    a: 'Không. Các máy có thể dùng chung địa chỉ nhận trên bntpool.com và tên riêng để nhận biết từng cài đặt. Pool tính phần thưởng theo địa chỉ. Máy đào chỉ cần địa chỉ nhận hoặc Blocknet ID, không cần cụm từ khôi phục ví.'
  - q: 'Điều gì xảy ra khi hai trình đào cùng tên?'
    a: 'Trong cùng địa chỉ, lịch sử share được gộp dưới tên ấy, khiến việc nhận biết máy tạo ra một số liệu khó hơn. Đặt tên riêng cho mỗi cài đặt và khởi động lại với tên đó; share đã ghi vẫn giữ nhãn ban đầu.'
  - q: 'Thêm máy có chia tách khoản thanh toán không?'
    a: 'Tên máy sắp xếp hoạt động hiển thị và không tạo số dư riêng trong cùng địa chỉ. Công việc hợp lệ từ các máy đóng góp vào phần tham gia PPLNS của địa chỉ đó. Số dư tuân theo quy tắc xác nhận và thanh toán thông thường của pool.'
sourceHash: 'e3e53f4af4c1'
translation: draft
---

Thêm máy tính thứ hai vào bntpool.com mà vẫn dùng một ví nhận. Đặt tên mỗi cài đặt, kiểm tra riêng rồi theo dõi địa chỉ chung. Hướng dẫn này bao gồm thêm máy và xử lý tên trùng. Với máy đầu tiên, hoàn tất [thiết lập pool](/blog/vi/mine-blocknet-on-bntpool/) trước.

## Nhiều máy có thể đào về cùng một ví không?

<Answer>
Có. Chạy seine trên mỗi máy với cùng địa chỉ nhận và tên máy khác nhau. bntpool.com ghi công việc và phần thưởng theo địa chỉ; tên tách hoạt động trong thống kê. Xác nhận share được chấp nhận ở từng máy trước khi dựa vào giao diện tổng hợp.
</Answer>

*Các máy riêng gửi công việc qua pool tới một địa chỉ nhận; mỗi máy giữ một tên khác nhau.*

## Chọn địa chỉ nhận và cách đặt tên

Dùng địa chỉ nhận của ví hoặc Blocknet ID trỏ tới đó. Kiểm tra địa chỉ đã phân giải trên từng miner. Tên giúp nhập thuận tiện, nhưng kế toán theo địa chỉ kết quả. Nếu đổi đích ID, phiên đào đang chạy có thể giữ kết quả cũ đến khi khởi động lại. Phần thưởng đã kiếm vẫn thuộc địa chỉ ban đầu.

[Hướng dẫn ví để đào](https://seineminer.com/vi/blog/blocknet-wallet-for-mining/) giải thích cách lấy địa chỉ. Máy đào chỉ cần đích công khai này; giữ cụm từ khôi phục và khóa riêng cùng ví. Chọn tên ngắn như `desktop-01` và `server-02`. Pool bỏ khoảng trắng ở hai đầu và giữ tối đa 64 ký tự. Đặt phần phân biệt ở đầu tên dài.

## Thêm trình đào thứ hai với tên riêng

Cài bản seine phù hợp trên máy thứ hai rồi chạy từ thư mục giải nén. Ví dụ PowerShell đầu tiên chỉ định rõ tên nhận, pool và máy. Thay `@yourname` bằng ID hoặc địa chỉ đầy đủ của bạn trước khi chạy. Giữ dấu ngoặc kép và thêm tùy chọn backend hoặc tinh chỉnh đã thử cho máy ấy.

```powershell
.\seine.exe --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker desktop-01
```

Trên Linux hoặc macOS, dùng chương trình trong lệnh dưới với tên máy khác. Hệ điều hành có thể khác nhau; địa chỉ nhận phải khớp. Mỗi ví dụ bắt đầu đào thật. Chạy ví dụ phù hợp trên đúng máy rồi chờ khởi động và tinh chỉnh bình thường hoàn tất. Không cần chạy cả hai trên cùng máy.

```sh
./seine --address "@yourname" --pool-url stratum+tcp://bntpool.com:3333 --pool-worker server-02
```

## Những cài đặt nào dùng chung?

Dùng chung địa chỉ nhận và endpoint pool. Mỗi cài đặt giữ thư mục dữ liệu cục bộ và tên riêng. seine lưu kết nối trong `seine.config.json` ở thư mục dữ liệu; mặc định `./seine-data` phụ thuộc thư mục khởi chạy. Chạy từ thư mục khác có thể tìm cấu hình khác. Vị trí cố định giúp bảo trì dễ hơn.

Nếu sao chép cấu hình máy đầu tiên, đặt tên khác trong lệnh khởi chạy máy thứ hai và xác nhận khi bắt đầu. Giữ tên chỉ định ấy trong các lệnh sau: ghi đè dòng lệnh không sửa tên đã lưu. Điều chỉnh lựa chọn phần cứng phù hợp và để máy tự tinh chỉnh cục bộ. Đối số địa chỉ quyết định nơi nhận, đối số tên quyết định nhãn; nhập riêng như ví dụ.

## Làm sao xác nhận cả hai máy đóng góp?

Mở My Stats trên bntpool.com và tra địa chỉ chung. Tìm hai tên cùng share gần đây của từng máy. Kiểm tra cả lượt gửi được chấp nhận trong miner cục bộ. Làm từng máy một để nối tiến trình với dòng thống kê pool; dễ phát hiện lỗi hơn chỉ nhìn tổng hashrate.

Pool ước tính hashrate từ share gửi lên. Máy mới cần thời gian và công việc được chấp nhận để số liệu có ý nghĩa. Tốc độ cục bộ và ước tính pool dùng phép đo, khoảng thời gian khác nhau. Xác nhận hoạt động bằng share gần đây và bộ đếm trước, rồi so sánh thời gian dài hơn. [Hướng dẫn thống kê](/blog/vi/track-your-miner-on-bntpool/) giải thích thẻ và dòng mà không suy số thiết bị từ số kết nối.

## Nếu thiếu máy hoặc các máy trùng tên thì sao?

Cùng tên dưới cùng địa chỉ sẽ gộp share đã ghi. Đổi tên một miner bằng tùy chọn tên rồi khởi động lại. Lượt gửi mới dùng nhãn mới; bản ghi cũ giữ nhãn trước. Dòng cũ có thể vẫn hiện khi lịch sử nằm trong khoảng báo cáo. Đổi tên không chuyển số dư địa chỉ đã kiếm.

Với máy không xuất hiện, so địa chỉ thực đã phân giải, endpoint và tên với trang đang xem. Kiểm tra kết nối và share được chấp nhận. CPU và GPU trong cùng tiến trình seine không cần tên riêng ở quy trình này. Nếu share bị từ chối hoặc lỗi thời, theo [hướng dẫn trạng thái share](/blog/vi/accepted-rejected-stale-shares/) thay vì đổi địa chỉ ví liên tục.

## Phần thưởng và thanh toán được gộp thế nào?

Công việc hợp lệ từ các máy góp vào phần tham gia của cùng địa chỉ trong cửa sổ PPLNS. Tên giúp theo dõi đóng góp, không tạo số dư riêng. Thêm máy bổ sung công việc hữu ích thực gửi, không phải thưởng cố định theo thiết bị hay tên. Phần cứng và thời gian chạy khác nhau có thể góp khác nhau; thêm tên không tự tăng công việc.

[Hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/) giải thích phân bổ khi pool tìm thấy khối. Quy tắc xác nhận và thanh toán vẫn áp dụng cho số dư chung. Không cần gộp ví hay chuyển số dư giữa tên máy. Trước khi để chạy, kiểm tra đích nhận, tên riêng và share được chấp nhận ở từng máy. Dùng lại danh sách ngắn này cho máy tiếp theo.

<CTA target="pool" title="Kết nối thêm máy tính với bntpool.com" text="Dùng một địa chỉ nhận, đặt tên khác nhau cho từng cài đặt và xác nhận share được chấp nhận." />
