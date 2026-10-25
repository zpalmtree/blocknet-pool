---
title: Đồng bộ nhanh node Blocknet bằng checkpoint bntpool.com
description: Node Blocknet đồng bộ nhanh bằng checkpoints.dat của bntpool.com. Tìm hiểu cách checkpoint cố định chuỗi, rồi tải, xác minh và nạp tệp trong vài bước.
tags: [Node Blocknet, Checkpoint, Đồng bộ nhanh, bntpool]
sourceHash: 7a3491743f7d
translation: draft
faq:
  - q: 'Node Blocknet lấy checkpoint ở đâu?'
    a: 'Từ bntpool.com. Khi khởi động lần đầu, nếu thư mục dữ liệu chưa có checkpoints.dat, daemon Blocknet tải tệp từ https://bntpool.com/checkpoints.dat và dùng để đồng bộ nhanh. Bạn cũng có thể tự tải từ trang Checkpoints, kiểm tra SHA-256 và đặt vào thư mục dữ liệu trước khi khởi động node.'
  - q: 'bntpool.com cập nhật checkpoints.dat bao lâu một lần?'
    a: 'Daemon của pool ghi lại tệp khi chuỗi phát triển, với một mục cho mỗi khối thứ 100. Mỗi mục nằm ít nhất 100 khối dưới đầu chuỗi, vượt độ sâu tổ chức lại tối đa của Blocknet, nên checkpoint chỉ bao phủ lịch sử đã chốt hạ. Trang Checkpoints hiển thị độ cao mới nhất và lần thay đổi cuối của tệp.'
  - q: 'Checkpoint có thay đổi số dư hoặc lịch sử ví không?'
    a: 'Không. Node đồng bộ bằng checkpoint vẫn lưu mọi khối và ghi mọi output cùng key image vào chỉ mục, nên quét ví và số dư hoạt động hệt sau đồng bộ đầy đủ. Checkpoint chỉ cho phép bỏ qua kiểm tra lại Proof of Work và chữ ký của lịch sử đã được hash checkpoint bảo chứng.'
  - q: 'Có thể đồng bộ node Blocknet không dùng checkpoint không?'
    a: 'Có. Khởi động daemon với cờ --full-sync hoặc đặt full_sync thành true trong cấu hình blocknet; node bỏ tải checkpoint và xác minh lại Proof of Work cùng chữ ký của mọi khối từ genesis. Việc này lâu hơn đồng bộ checkpoint, vì vậy checkpoint là mặc định cho node mới.'
---

Node Blocknet mới phải bắt kịp mọi khối từ genesis trước khi phục vụ ví hoặc trình đào solo. Checkpoint giúp việc bắt kịp này nhanh hơn. Hướng dẫn này giải thích nội dung tệp checkpoint của bntpool.com, cách daemon Blocknet dùng nó để bỏ qua những kiểm tra chậm nhất trong khi vẫn gắn với chuỗi thật, và cách tự tải, xác minh rồi nạp tệp.

## Checkpoint Blocknet là gì?

<Answer>
Checkpoint Blocknet là danh sách hash khối đã biết, một hash cho mỗi khối thứ 100, được bntpool.com cung cấp dưới dạng checkpoints.dat. Node đang đồng bộ kiểm tra khối nhận được khớp các hash đó và bỏ xác minh lại Proof of Work cùng chữ ký đến checkpoint cuối, nên lần đồng bộ đầu hoàn tất nhanh hơn nhiều.
</Answer>

Tệp là văn bản thuần với một cặp `height:hash` mỗi dòng. Những dòng đầu vào ngày 25 tháng 10 năm 2026 như sau:

```text
100:579099BE8BECF8F60817EA71C882EC061CE60C1A8CF7727F92DBEE2F49272989
200:DDFD65EC01744A61D2A41862141F60C847AC16F43B760E582F8B11A08D18C483
300:B4657F22AE0DDFB9D724138DF4BE44376E0388FFA72162FD65E968CD5AA77625
```

Mỗi hash là ID của khối, tức hash SHA3-256 của tiêu đề khối. Daemon riêng của pool ghi tệp từ chuỗi của nó và chỉ đưa vào những khối sâu ít nhất 100 khối. Một trăm khối là giới hạn chốt hạ cứng của Blocknet, độ sâu tổ chức lại lớn nhất giao thức cho phép, nên mỗi checkpoint nằm trong lịch sử đã chốt hạ.

Ngày 25 tháng 10 năm 2026 lúc 02:04 EDT, tệp chứa 689 checkpoint từ độ cao 100 đến 68.900, trong 48.811 byte. Tệp thêm một dòng khoảng mỗi 100 khối, tức khoảng tám giờ với khối năm phút; hãy xem [trang Checkpoints](/checkpoints) để biết độ cao hiện tại.

## Node Blocknet dùng checkpoint thế nào?

Node đã nạp checkpoint coi độ cao checkpoint cuối là mục tiêu đồng bộ nhanh. Cho đến khi chuỗi đạt độ cao đó, node dựa vào checkpoint đã công bố cho hai kiểm tra tốn kém nhất và chạy mọi kiểm tra khác như bình thường:

- **Proof of Work dựa vào checkpoint.** Proof of Work của mỗi khối Blocknet là một hash Argon2id trên 2 GiB bộ nhớ. Tính lại cho mọi khối lịch sử rất tốn kém, trong khi hash checkpoint đã bảo chứng lịch sử đó.
- **Chữ ký RingCT và range proof cũng dựa vào chúng.** Chú thích của chính daemon gọi xác minh range proof của output là chi phí đơn lẻ lớn nhất khi đồng bộ, vì mọi output đều có một bằng chứng, kể cả coinbase mỗi khối.
- **Mọi kiểm tra khác vẫn chạy.** Liên kết khối, quy tắc độ khó LWMA, quy tắc dấu thời gian, kích thước và cấu trúc khối, kiểm tra chi tiêu hai lần trên key image và kiểm tra liên kết giữa từng đầu vào với chữ ký vẫn chạy bình thường. Output và key image vào chỉ mục node như thường lệ, nên quét ví và số dư giống sau đồng bộ đầy đủ.

Việc bỏ qua chỉ áp dụng cho khối nối tiếp đầu chuỗi hiện tại của node và chỉ dưới checkpoint cuối. Khi vượt độ cao đó, node xác minh đầy đủ mọi khối mới, kể cả Proof of Work.

### Checkpoint cố định chuỗi tin cậy thế nào

Khi node đồng bộ đến checkpoint cuối, mọi khối tại độ cao checkpoint phải có đúng hash trong tệp. Khối có hash khác bị từ chối với lỗi checkpoint không khớp. Vì checkpoint xuất hiện mỗi 100 khối, peer không thể dẫn node đang đồng bộ nhanh sang lịch sử khác: độ cao checkpoint kế tiếp sẽ phát hiện điều đó.

Checkpoint cũng tăng tốc mỗi lần khởi động lại. Khi daemon khởi động với khối đã có trên đĩa, nó tìm checkpoint cao nhất khớp chuỗi đã lưu và chỉ kiểm tra lại các khối phía sau, thay vì duyệt cả chuỗi từ genesis.

## Node mới có tự tải checkpoint không?

<Answer>
Có. Khi daemon Blocknet khởi động và thư mục dữ liệu chưa có checkpoints.dat, nó tải tệp từ https://bntpool.com/checkpoints.dat, nạp và đồng bộ nhanh đến checkpoint cuối. Việc tải được cố gắng thực hiện: nếu không hoàn tất, node vẫn khởi động và xác minh đầy đủ chuỗi.
</Answer>

Địa chỉ này là mặc định được ghi trong chính lõi Blocknet, nên ngay khi cài xong mọi node mới đồng bộ nhanh từ tệp bntpool.com công bố. Daemon chỉ tải khi chưa có tệp, cho yêu cầu 15 giây và giới hạn tải xuống ở 32 MiB.

## Dùng checkpoint bntpool.com từng bước thế nào?

Nếu muốn tự tải và kiểm tra tệp hoặc làm mới bản cũ trước một lần đồng bộ lại dài, hãy thực hiện thủ công các bước sau.

### 1. Tìm thư mục dữ liệu

Tệp checkpoint nằm trong thư mục dữ liệu node, cạnh cơ sở dữ liệu chuỗi:

- **Trình quản lý `blocknet`:** `~/.config/bnt/data/mainnet/` trên macOS và Linux, `C:\Users\<you>\.config\bnt\data\mainnet\` trên Windows, trừ khi bạn đặt `data_dir` trong cấu hình.
- **Chạy lõi trực tiếp:** `./blocknet-data-mainnet` trong thư mục khởi chạy, hoặc đường dẫn bạn đưa qua `--data`.

### 2. Tải checkpoints.dat

Mở [Checkpoints](/checkpoints), nhấn **Download checkpoints.dat**, hoặc lấy từ terminal. Ví dụ này ghi vào thư mục dữ liệu mặc định của lõi:

```sh
curl -L https://bntpool.com/checkpoints.dat -o blocknet-data-mainnet/checkpoints.dat
```

Tải đè lên tệp hiện có sẽ thay thế nó; đó là cách làm mới bản cũ.

### 3. Kiểm tra SHA-256

Trang Checkpoints công bố SHA-256 của tệp hiện tại. Tính hash bản của bạn rồi so sánh:

```sh
sha256sum blocknet-data-mainnet/checkpoints.dat
```

Trên macOS, dùng `shasum -a 256` thay thế. Hash thay đổi mỗi lần pool thêm checkpoint, nên đối chiếu trang ngay sau khi tải.

### 4. Kiểm tra thử một checkpoint trên trình khám phá

Bạn có thể xác nhận bất kỳ dòng nào với [trình khám phá Blocknet](https://explorer.blocknetcrypto.com) công khai. Mở trang khối cho một độ cao trong tệp, chẳng hạn `explorer.blocknetcrypto.com/block/68900`, rồi so hash với dòng 68.900. Chúng tôi đã làm vậy với checkpoint cuối ngày 25 tháng 10 năm 2026 và các hash khớp nhau.

### 5. Khởi động node

Khởi động hoặc khởi động lại daemon. Nó đọc `checkpoints.dat` khi khởi động và đồng bộ nhanh đến checkpoint cuối. Nếu chạy lõi với shell tương tác (`--cli`), lệnh `load-checkpoints` nạp tệp vào node đang chạy và báo độ cao đồng bộ nhanh:

```text
> load-checkpoints
```

Checkpoint hữu ích khi node còn chậm phía sau. Khi chuỗi vượt độ cao checkpoint cuối, node đã xác minh đầy đủ mọi khối và tệp mới hơn không thay đổi gì.

## Kiểm tra tệp checkpoint bằng script thế nào?

Pool công bố chi tiết tệp dưới dạng JSON tại `https://bntpool.com/api/checkpoints`, không cần khóa API:

| Trường | Ý nghĩa |
|---|---|
| `available` | Pool có tệp để cung cấp hay không |
| `url` | Nơi tải xuống |
| `entries` | Số checkpoint trong tệp |
| `latest_height`, `latest_hash` | Checkpoint cao nhất và hash khối |
| `bytes`, `sha256` | Kích thước tệp và SHA-256 |
| `updated_at` | Lần thay đổi cuối, dưới dạng dấu thời gian Unix |

Bản tải xuống mang header `X-Checkpoint-Height`, `X-Checkpoint-Sha256` và `ETag` tạo từ hash, nên script có thể gửi `If-None-Match` và nhận nhanh `304 Not Modified` khi không có gì đổi:

```sh
curl -s https://bntpool.com/api/checkpoints | jq '{latest_height, sha256}'
```

Phản hồi có thể được cache 60 giây, quá đủ cho tệp thay đổi khoảng mỗi tám giờ.

<Note>
blocknetcrypto.com cho biết nâng cấp Faraday dự kiến mang đến Proof of Work nhẹ hơn để node mới bắt kịp chuỗi nhanh hơn. Cho đến lúc đó, checkpoint là cách nhanh để cập nhật node.
</Note>

## Có thể làm gì với node đã đồng bộ?

Node đã đồng bộ hỗ trợ ví Blocknet và API node; seine liên lạc với nó khi bạn đào solo bằng `--mode daemon`. [Đào Blocknet bằng pool so với solo](/blog/vi/pool-vs-solo-mining-blocknet/) cho biết cách đánh giá đào solo có phù hợp phần cứng không. Nếu muốn bắt đầu đào mà không chạy node, seine mặc định đào trên bntpool.com: [hướng dẫn cài seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) đưa bạn đến share đầu tiên, còn [đào trên bntpool.com](/blog/vi/mine-blocknet-on-bntpool/) trình bày phía pool.

<CTA target="pool" title="Đào trên pool cung cấp checkpoint Blocknet" text="Tham gia bntpool.com trong vài phút và theo dõi trực tiếp hashrate, share cùng thanh toán của bạn." />
