---
title: 'Blocknet ra mắt công bằng: không premine, mọi coin đều được đào'
description: Blocknet ra mắt không premine, quỹ phát triển hay ngân quỹ. Xem khối genesis rỗng và quy tắc coinbase một output, tự kiểm tra rồi đào BNT.
tags: [Blocknet, Ra mắt công bằng, Khối genesis, Đào coin, bntpool]
sourceHash: 814839500c3d
translation: draft
faq:
  - q: 'Blocknet có premine không?'
    a: 'Không. Khối genesis của Blocknet, tạo ngày 15 tháng 2 năm 2026, không chứa giao dịch nào, nên không có coin trước khi việc đào bắt đầu. Những coin đầu tiên đến từ phần thưởng khối 1, và mọi BNT kể từ đó được trả cho người đào khối sinh ra nó.'
  - q: 'Blocknet có quỹ phát triển hoặc ngân quỹ không?'
    a: 'Không. Đồng thuận chỉ cho phép đúng một giao dịch coinbase trong mỗi khối, và giao dịch đó phải có đúng một output trị giá chính xác bằng phần thưởng khối. Không có output thứ hai cho nhà phát triển, nhà sáng lập hay ngân quỹ; khối cố thêm output như vậy bị mọi node trong mạng từ chối.'
  - q: 'Khối genesis của Blocknet được tạo khi nào?'
    a: 'Dấu thời gian khối genesis là ngày 15 tháng 2 năm 2026 lúc 20:58:38 EST, tương đương 01:58:38 UTC ngày 16 tháng 2. Khối mang thông điệp "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything", và hash khối trước là hash SHA3-256 của thông điệp đó.'
  - q: 'Làm sao tự xác minh Blocknet ra mắt công bằng?'
    a: 'Mở khối 0 trên trình khám phá Blocknet, bạn sẽ thấy không có giao dịch và gốc Merkle toàn số 0. Sau đó đọc mã daemon nguồn mở: khối genesis được ghi cố định với danh sách giao dịch rỗng, còn kiểm tra coinbase chấp nhận một output bằng phần thưởng khối.'
  - q: 'Làm sao bắt đầu đào Blocknet?'
    a: 'Tải trình đào seine cho Windows, macOS hoặc Linux, chạy và dán địa chỉ Blocknet hoặc tên ngắn Blocknet ID khi được hỏi. Nhấn Enter để chấp nhận pool mặc định bntpool.com; seine bắt đầu đào ngay. Sau đó bạn có thể tra địa chỉ trên trang thống kê bntpool.com.'
---

Blocknet bắt đầu mà không phân bổ gì cho bất kỳ ai. Trang này cho thấy nội dung khối genesis, những quy tắc đồng thuận khiến premine, quỹ phát triển hay ngân quỹ không thể tồn tại, cách kiểm tra trên trình khám phá và trong mã nguồn, cùng cách tự bắt đầu đào BNT trên bntpool.com.

## Blocknet có premine không?

<Answer>
Không. Khối genesis của Blocknet, tạo ngày 15 tháng 2 năm 2026, không chứa giao dịch, nên không có coin trước khi việc đào bắt đầu. Từ khối 1, mọi khối trả phần thưởng bằng một output coinbase duy nhất trị giá chính xác theo lịch, nên mọi BNT lưu thông đều do người chạy Proof of Work đào ra.
</Answer>

Không có phân bổ cho nhà sáng lập, nhà phát triển, nhà đầu tư hoặc quỹ tổ chức, dù trong khối genesis hay bất kỳ khối nào sau đó. Nguồn cung coin chỉ đến từ phần thưởng khối. Các quy tắc thực thi điều này là một phần của đồng thuận, nên mọi node trong mạng kiểm tra chúng ở mọi khối.

## Khối genesis của Blocknet có gì?

Khối genesis được ghi cố định trong daemon, nên mọi node bắt đầu từ cùng khối 0. Các trường của nó là cố định:

| Trường | Giá trị |
|---|---|
| Độ cao | 0 |
| Dấu thời gian | Ngày 15 tháng 2 năm 2026, 20:58:38 EST (01:58:38 UTC ngày 16 tháng 2) |
| Thông điệp | "Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything" |
| Hash trước | SHA3-256 của thông điệp: `a94a7f2b…d909df` |
| Độ khó | 4, mức tối thiểu của mạng |
| Nonce | 0 |
| Giao dịch | không có |
| Gốc Merkle | toàn số 0 |

Thông điệp là tiêu đề một bài Wired ngày 15 tháng 2 năm 2026, và mã nguồn dẫn bài đó làm nguồn. Vì hash trước được suy ra từ thông điệp, tiêu đề được gắn cố định vào danh tính khối 0 và mọi thứ xây dựng trên nó.

Chú thích phía trên hàm genesis trong daemon nói rõ: “Genesis không có giao dịch — không coinbase, không coin bị đốt. Khối thực đầu tiên ở độ cao 1 sẽ có coinbase đầu tiên.” Việc xác thực củng cố chú thích này. Node kiểm tra khối 0 sẽ từ chối nếu khối không có chính xác 0 giao dịch và gốc Merkle toàn số 0, nên ngay cả node bị sửa đổi cũng không thể lén thêm coin vào khối genesis mà không bị mọi node khác từ chối.

## Đồng thuận loại trừ quỹ phát triển hoặc ngân quỹ thế nào?

<Answer>
Mỗi khối Blocknet phải bắt đầu bằng đúng một giao dịch coinbase; coinbase đó không được có đầu vào, phí bằng 0 và phải có đúng một output với số tiền bằng phần thưởng khối tại độ cao đó. Không có chỗ cho khoản trả thứ hai, nên output quỹ phát triển, thưởng nhà sáng lập hoặc ngân quỹ không thể tồn tại trên chuỗi.
</Answer>

Một số kiểm tra trong daemon cùng thực hiện điều này:

- **Một coinbase mỗi khối.** Giao dịch đầu phải là coinbase và không giao dịch nào khác trong khối được là coinbase.
- **Một output, không đầu vào.** Coinbase có 0 hoặc 2 output, hoặc có đầu vào, là không hợp lệ.
- **Đúng phần thưởng.** Blocknet ẩn số tiền bằng Pedersen commitment, nhưng coinbase là trường hợp đặc biệt. Blinding factor của nó được suy ra từ dữ liệu công khai (khóa công khai của giao dịch và độ cao khối), nên mọi node có thể dựng lại commitment cho phần thưởng theo lịch để so sánh. Nếu output cam kết số tiền khác, khối bị từ chối.
- **Memo rỗng.** Memo coinbase phải rỗng, nên phần thưởng khối cũng không thể mang dữ liệu bổ sung.

Giao dịch thông thường không thể tạo coin. Mỗi giao dịch phải cân bằng: đầu vào bằng output cộng phí, kiểm tra trực tiếp trên các commitment. Mỗi output mang một Bulletproofs range proof chứng minh số tiền nằm trong phạm vi hợp lệ và không thể âm. Đó là cách số tiền vẫn riêng tư trong khi phát hành tuân thủ chính xác lịch.

## Làm sao tự kiểm tra Blocknet ra mắt công bằng?

Bạn không cần tin lời ai. Cả chuỗi và mã nguồn đều công khai.

**Trên trình khám phá.** Mở [khối 0 trên trình khám phá Blocknet](https://explorer.blocknetcrypto.com/block/0). Trang liệt kê 0 giao dịch, gốc Merkle gồm 64 số 0, nonce 0 và hash trước bắt đầu bằng `a94a7f2b`. Dòng phần thưởng khối hiển thị số tiền theo lịch cho độ cao đó, nhưng không có giao dịch trong khối thì cũng không có output để nhận nó. Sau đó mở [khối 1](https://explorer.blocknetcrypto.com/block/1): giao dịch duy nhất là coinbase không có đầu vào và có một output, những coin đầu tiên trên chuỗi.

Bạn có thể xác nhận hash trước thực sự là hash của thông điệp genesis. Với OpenSSL 1.1.1 trở lên, chạy:

```sh
printf '%s' 'Wired 2026/02/15 - The Nothing That Has the Potential to Be Anything' | openssl dgst -sha3-256
```

Kết quả kết thúc bằng `a94a7f2b8685ff3c7dc76de5ad0f13568629b1f0e0495440c58a3ed3d0d909df`, đúng giá trị trình khám phá hiển thị cho khối 0.

**Trong mã nguồn.** Daemon có mã nguồn mở tại [github.com/blocknetprivacy/core](https://github.com/blocknetprivacy/core). Trong [`block.go`](https://github.com/blocknetprivacy/core/blob/master/block.go), tìm các hàm sau:

```sh
git clone https://github.com/blocknetprivacy/core.git
cd core
grep -n "func GetGenesisBlock\|func validateGenesisBlock\|func validateCoinbaseConsensus" block.go
```

`GetGenesisBlock` tạo khối 0 với danh sách giao dịch rỗng, `validateGenesisBlock` từ chối mọi khối 0 có giao dịch, và `validateCoinbaseConsensus` thực thi một output duy nhất bằng phần thưởng khối. Trong `transaction.go`, `validateCoinbase` và `verifyCommitmentBalance` xử lý cấu trúc coinbase và kiểm tra cân bằng giao dịch thông thường.

## Vì sao ra mắt công bằng quan trọng với thợ đào?

Với thợ đào, ra mắt công bằng nghĩa là phần thưởng khối giải thích trọn vẹn nguồn gốc coin.

- **Mọi coin mới đến tay thợ đào.** Không premine và không output ngân quỹ, 100% phát hành được trả cho người tìm khối. Khối 1 trả 723,25093035 BNT trong output coinbase duy nhất, và mọi khối sau đó trả toàn bộ phần thưởng theo cùng cách.
- **Mọi người theo cùng quy tắc.** Từ khối 1, cách duy nhất kiếm BNT mới là chạy Proof of Work. [Lịch phần thưởng](/blog/vi/blocknet-block-reward-schedule/) được định sẵn trong mã và áp dụng bình đẳng cho thợ đào đầu tiên cũng như người bắt đầu hôm nay.
- **Proof of Work phù hợp máy tính thông thường.** Blocknet dùng Argon2id với 2 GiB bộ nhớ mỗi hash, được thiết kế chống ASIC. Nhờ đó máy bàn, laptop và máy chủ mà mọi người sẵn có vẫn có thể đào. [Bài giải thích Argon2id trên seineminer.com](https://seineminer.com/vi/blog/argon2id-proof-of-work-explained/) trình bày lý do.
- **Phát hành dễ kiểm toán.** Mỗi coin truy ngược về một output coinbase đã được mọi node kiểm tra, nên tổng phát hành tại bất kỳ độ cao nào đều suy trực tiếp từ lịch phần thưởng.

Nếu bạn mới tìm hiểu đồng coin này, [Blocknet là gì?](https://seineminer.com/vi/blog/what-is-blocknet/) trình bày quyền riêng tư, thanh toán bằng tên và các ví.

## Làm sao tự đào Blocknet?

Tham gia ra mắt công bằng nghĩa là đào coin. Cách nhanh nhất là trình đào seine trên bntpool.com:

1. **Lấy địa chỉ.** Cài [ví dòng lệnh](https://github.com/blocknetprivacy/blocknet/releases/latest) hoặc [ví desktop](https://github.com/blocknetprivacy/blocknet-gui/releases/latest) chính thức của Blocknet rồi sao chép địa chỉ, hoặc dùng tên ngắn Blocknet ID như `@name`.
2. **Tải seine.** Lấy gói cho Windows, macOS hoặc Linux từ [seineminer.com](https://seineminer.com/vi/#download) rồi giải nén. Không cần cài đặt gì.
3. **Chạy và chấp nhận mặc định.** Khởi động seine, dán địa chỉ hoặc `@name`, nhấn Enter để chấp nhận pool mặc định `stratum+tcp://bntpool.com:3333`. seine bắt đầu băm ngay và tinh chỉnh luồng, kernel khi chạy trên CPU, cùng card NVIDIA trên Windows và Linux.
4. **Theo dõi trình đào.** Tra địa chỉ trên [trang thống kê](/stats) để xem hashrate, share và số dư, rồi theo dõi khối pool tìm được trên [trang khối](/blocks).

[Hướng dẫn đào trên bntpool.com](/blog/vi/mine-blocknet-on-bntpool/) trình bày đăng nhập bằng địa chỉ hoặc tên và kiểm tra trình đào; [trang Get Started](/start) liệt kê cài đặt thanh toán của pool; [hướng dẫn đào Blocknet](https://seineminer.com/vi/blog/how-to-mine-blocknet/) hướng dẫn thiết lập seine từng bước trên mọi hệ điều hành.

<CTA target="seine" title="Đào phần của bạn" text="Mọi BNT đều được đào. Tải seine và bắt đầu đào trên bntpool.com trong vài phút." />
