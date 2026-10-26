---
title: 'Stratum là gì? Máy đào giao tiếp với pool ra sao'
description: 'Theo dõi Stratum giữa seine và bntpool.com: đăng nhập, công việc, mục tiêu share, gửi kết quả và phản hồi, để hiểu các giai đoạn kết nối của máy đào.'
tags: ['Kiến thức đào', 'Stratum', 'Đào qua pool']
faq:
  - q: 'Stratum làm gì trong đào qua pool?'
    a: 'Stratum mang cuộc trao đổi giữa máy đào và pool: nhận diện phiên, giao việc, gửi share và phản hồi. Máy đào tính hash cục bộ. Pool nối công việc đó với node của mình và giữ sổ ghi nhận cần cho cơ chế thanh toán.'
  - q: 'Địa chỉ Stratum có giống địa chỉ website không?'
    a: 'Chúng phục vụ kết nối khác nhau. bntpool.com dùng HTTPS cho trang và API; đầu nối đào được ghi rõ là stratum+tcp://bntpool.com:3333. Nhập nó vào cấu hình pool của máy đào. Chỉ mở được website chưa thiết lập kết nối đào.'
  - q: 'Share Stratum được chấp nhận có nghĩa đã thanh toán không?'
    a: 'Không. Phản hồi share báo kết quả gửi trong phiên. Xác minh, khối hợp lệ trên mạng, quyền nhận thưởng và thanh toán hoàn tất là các bước riêng. Theo dõi công việc được chấp nhận ở My Stats và xem hướng dẫn thanh toán để hiểu xác nhận, PPLNS và ngưỡng.'
sourceHash: '1dba87acd7c0'
translation: draft
---

Theo dõi công việc từ bntpool.com đến seine và share quay về pool. Hiểu cuộc trao đổi giúp đọc thông báo kết nối, cập nhật việc và phản hồi. Hướng dẫn mô tả triển khai bntpool.com; để đặt đầu nối và địa chỉ nhận tiền ban đầu, xem [hướng dẫn pool](/blog/vi/mine-blocknet-on-bntpool/).

## Stratum trong đào coin là gì?

<Answer>
Stratum là lớp giao tiếp giúp máy đào nhận công việc từ pool và gửi kết quả. Với seine và bntpool.com, kết nối TCP duy trì mang thông điệp JSON cho đăng nhập, việc và share. Máy tính hash cục bộ; pool lấy mẫu khối từ node và xử lý công việc gửi lên.
</Answer>

*Máy đào gửi đăng nhập và share, nhận công việc cùng phản hồi; pool trao đổi mẫu và ứng viên khối với node.*

## Đầu nối xác định kết nối đào

Đầu nối được công bố là `stratum+tcp://bntpool.com:3333`. Lược đồ chọn cách truyền, tên máy xác định dịch vụ, cổng chọn bộ lắng nghe đào. Kết nối trình duyệt tới `https://bntpool.com` phục vụ mục đích khác. Sao chép đầu nối đầy đủ từ [Get Started](/start) vào thiết lập pool để mở đúng kết nối.

Kết nối duy trì cho phép hai bên gửi thông điệp khi có việc. bntpool dùng JSON phân cách bằng dòng mới, với ID yêu cầu liên kết phản hồi. Đây là định dạng pool này; máy đào phải hỗ trợ nó, khả năng giao thức và thuật toán Blocknet. [Mã giao thức](https://github.com/zpalmtree/blocknet-pool/blob/6521e7c6991629cf1bf364b5b5afea597b6999e0/crates/pool-common/src/protocol.rs) ghi các trường hiện tại.

## Điều gì xảy ra khi máy đào đăng nhập?

Client gửi `login` với địa chỉ nhận tiền, tên rig, phiên bản giao thức và khả năng hỗ trợ. Pool kiểm tra rồi trả thông tin giao thức và khả năng bắt buộc. Điều này xác lập cách trao đổi công việc. TCP thành công là bước đầu; login được chấp nhận cho thấy giao tiếp ứng dụng cũng đã bắt đầu.

Tên rig phân biệt một máy dưới cùng địa chỉ. Dùng tên khác nhau cho nhiều rig, giữ đúng địa chỉ nhận tiền. Login cần địa chỉ nhận, không cần cụm từ khôi phục hay khóa chi tiêu riêng. Khi phiên được chấp nhận và có việc, pool gửi `job` để bắt đầu băm.

## Công việc đào chứa thông tin gì?

Công việc có ID phân công và mẫu, dữ liệu header, chiều cao khối, mục tiêu share, mục tiêu mạng và khoảng nonce. Nonce là giá trị thay đổi khi băm. Khoảng này xác định không gian tìm kiếm của phiên. ID việc giúp liên kết lần gửi sau với công việc và điều kiện ban đầu.

Mục tiêu share xác định kết quả gửi làm bằng chứng công việc. Mục tiêu mạng xác định ứng viên khối. Phần lớn share phục vụ ghi nhận mà không thành khối. [Hướng dẫn độ khó](/blog/vi/mining-difficulty-explained/) giải thích kích thước mục tiêu và công việc kỳ vọng; giao thức cung cấp mục tiêu trong phân công.

## Công việc có thể đổi khi kết nối vẫn mở

Việc mới tự nó không có nghĩa máy đào mất kết nối. Pool có thể cập nhật khi mẫu node đổi hoặc gửi phân công với độ khó share mới. Độ khó biến thiên điều chỉnh ngưỡng gửi theo dòng công việc quan sát được. Máy đào theo phân công mới trong khi kết nối tiếp tục mang thông điệp hai chiều.

Phân biệt ID việc và ID mẫu khi đọc log kỹ thuật. Nhiều phân công có thể dùng cùng mẫu với chi tiết phiên khác. Chỉ chiều cao không mô tả mọi thay đổi. Người vận hành nên kiểm tra việc đến, băm tiến triển và lần gửi có phản hồi; [hướng dẫn theo dõi](/blog/vi/track-your-miner-on-bntpool/) chỉ nơi xem hoạt động.

## Lần gửi share chứa gì?

Khi tìm kết quả phù hợp, seine gửi `submit` chỉ rõ việc, nonce và hash khai báo. Pool xử lý trong ngữ cảnh phân công rồi trả lời theo ID yêu cầu. Giá trị khai báo thuộc thông điệp; xác minh và kiểm tra khối quyết định bước tiếp theo. Ứng viên khối được chuyển lên daemon để kiểm tra.

Phản hồi có thể báo chấp nhận, trạng thái xác minh và độ khó. Đọc riêng từng trường, không coi mọi phản hồi thành công là tìm được khối. Ghi sổ và thanh toán nằm ngoài trao đổi này. [Hướng dẫn kết quả share](/blog/vi/accepted-rejected-stale-shares/) giải thích kết quả; [hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/) nối công việc đủ điều kiện với thưởng và trả tiền.

## Dùng trình tự này để kiểm tra máy đào thế nào?

Đọc lần lượt: tới đầu nối, login được nhận, có việc, băm hoạt động, lần gửi được trả lời. Khi mất kết nối, client nối lại và lập phiên trước khi nhận thêm việc. Dùng đúng thông báo và thời gian để xác định giai đoạn. Website hoạt động không chứng minh máy đó tới được cổng đào.

Mở [My Stats](/stats) cho đúng địa chỉ và [trạng thái pool](/status) để xem dịch vụ. Sau khởi động hoặc nối lại, cho ước tính hashrate dựa trên share thời gian tích lũy. Tách giao thức khỏi lịch trả tiền: login chuẩn bị phiên, job giao việc, share báo kết quả, ghi sổ xác định các giai đoạn thưởng tiếp theo.

<CTA target="pool" title="Kết nối seine với bntpool.com" text="Dùng trang thiết lập pool rồi theo dõi công việc, share và rig trong My Stats." />
