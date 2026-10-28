---
title: Theo dõi trình đào trên bntpool.com
description: Tra địa chỉ hoặc @name trên bntpool.com để xem hashrate, share, số dư và thanh toán; theo dõi khối, may mắn, thời gian hoạt động hoặc dùng API công khai.
tags: [bntpool, Thống kê đào coin, API pool, Blocknet]
sourceHash: '40197b2fa166'
translation: draft
faq:
  - q: 'Vì sao hashrate trên bntpool.com khác số seine hiển thị?'
    a: 'seine đếm mọi hash máy tính, còn pool ước tính hashrate từ độ khó share được chấp nhận, lấy trung bình giờ qua. Share đến ở thời điểm ngẫu nhiên, nên số của pool dao động nhiều hơn rồi ổn định. So biểu đồ 24 giờ trên My Stats với trung bình seine để đối chiếu công bằng.'
  - q: 'Có thể tra thống kê bntpool.com bằng @name không?'
    a: 'Có. Nhập tên ngắn Blocknet ID như @name hoặc $name vào ô tìm kiếm My Stats. Trang phân giải qua blocknet.id, hiển thị thống kê và đặt địa chỉ đầy đủ đã phân giải vào ô tìm kiếm. Dùng địa chỉ đầy đủ đó khi gọi API pool từ script.'
  - q: 'API bntpool.com có cần khóa API không?'
    a: 'Không. Các endpoint thống kê pool, khối, thanh toán, may mắn, trạng thái và từng thợ đào cung cấp JSON công khai qua HTTPS, nên lệnh curl hoặc bảng điều khiển tại nhà có thể đọc mà không đăng ký. Số tiền ở đơn vị nguyên tử; chia 100.000.000 để được BNT, và truy vấn mỗi 30–60 giây.'
  - q: 'Vì sao My Stats hiển thị Estimated Rewards bằng 0?'
    a: 'Estimated Rewards xuất hiện khi pool tìm khối trong lúc share của bạn ở cửa sổ trả thưởng. Với người mới, có thể cần vài vòng đào. Share được chấp nhận và hashrate xuất hiện trước, rồi phần thưởng ước tính, sau đó trưởng thành thành Confirmed Rewards và tự động được thanh toán.'
---

Sau share được chấp nhận đầu tiên, mở My Stats để kiểm tra pool đang nhận công việc của bạn. Tìm máy của bạn, so sánh hashrate trong thời gian dài hơn rồi theo dõi phần thưởng ước tính đến khi xác nhận và thanh toán. Script và API công khai nằm ở cuối hướng dẫn.

Nếu chưa bắt đầu đào, [hướng dẫn cài seine](https://seineminer.com/vi/blog/how-to-mine-blocknet/) đưa bạn đến share đầu trong vài phút.

## Kiểm tra thống kê trình đào trên bntpool.com thế nào?

<Answer>
Mở My Stats tại bntpool.com/stats, dán địa chỉ Blocknet hoặc @name rồi nhấn Lookup. Trang hiển thị hashrate, share chấp nhận và từ chối, số dư ước tính, đã xác nhận và đã trả, biểu đồ hashrate, một hàng mỗi dàn máy và thanh toán gần đây. Trang tự làm mới khi tab còn mở.
</Answer>

Vào [My Stats](/stats), nhập cùng địa chỉ hoặc [tên ngắn Blocknet ID](https://seineminer.com/vi/blog/blocknet-id-pay-by-name/) đã đưa cho seine. Tên như `@name` hoặc `$name` được phân giải qua blocknet.id; ô tìm kiếm sau đó hiển thị địa chỉ đầy đủ mà tên trỏ tới. Trang nhớ lần tra cuối trong trình duyệt đó, nên lần sau số liệu đã có sẵn.

1. Kiểm tra **Shares Accepted** và **Recent Shares**. Share được chấp nhận xác nhận công việc đã đến pool, ngay cả khi mọi số dư phần thưởng vẫn bằng 0.
2. Tìm máy trong **Workers**. Đối chiếu tên với tên trong Seine và kiểm tra thời gian share cuối vẫn cập nhật.
3. **Blocks Found** bằng 0 là bình thường: chỉ số này đếm các block do chính máy của bạn giải được, không cho biết share có nhận phần thưởng của pool hay không.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-first-mobile.webp" width="352" height="379"><img src="/blog/tracking/tracking-first-desktop.webp" width="1180" height="900" alt="Kiểm tra share được chấp nhận trước. Giao diện tiếng Anh thật với dữ liệu ví dụ, không phải thu nhập trực tiếp." loading="lazy" decoding="async"></picture>
<figcaption>Kiểm tra share được chấp nhận trước. Giao diện tiếng Anh thật với dữ liệu ví dụ, không phải thu nhập trực tiếp.</figcaption>
</figure>

## My Stats hiển thị gì?

Trang chia thành nhóm thẻ và bảng, mỗi nhóm trả lời một câu hỏi về trình đào.

### Đào và share

Các thẻ **Mining** hiển thị **Hashrate** hiện tại, số **Blocks Found** mà dàn máy riêng đã giải cho pool và ngày bắt đầu (**Mining Since**). Bên dưới là **Shares Accepted**, **Shares Rejected**, **Reject Rate** và **Avg Difficulty**. Dàn máy khỏe có dòng share chấp nhận đều và tỷ lệ từ chối gần 0.

**Hashrate History** vẽ hashrate trong 1h, 24h, 7d hoặc 30d. Khoảng dài hơn là cách tốt nhất để xem máy có chạy cả đêm không.

### Một hàng mỗi dàn máy

Nếu chạy nhiều máy, đặt tên riêng cho từng máy; pool liệt kê riêng với hashrate, share chấp nhận và từ chối, thời gian share cuối. seine chọn tên lần đầu chạy, `seine-` kèm bốn chữ số; bạn có thể tự đặt trên dòng lệnh:

```sh
./seine --pool-worker rig-01
```

Bảng **Workers** cũng hiển thị phiên bản miner của từng máy. Khi có phiên bản cũ hoặc không xác định, thông báo phía trên thống kê dẫn đến [trang phát hành seine](https://github.com/zpalmtree/seine/releases).

## Vì sao hashrate pool khác seine?

seine đếm mọi hash máy tính. Pool chỉ thấy share gửi lên, nên ước tính hashrate từ tổng độ khó share được chấp nhận trong giờ qua. Mỗi share là sự kiện ngẫu nhiên; bntpool.com đặt độ khó từng kết nối để nhắm khoảng 10 share mỗi 5 phút, nên cửa sổ ngắn có nhiễu.

Hai thói quen giúp đối chiếu số liệu:

- So trung bình: biểu đồ 24h trên My Stats với trung bình seine trong cùng khoảng.
- Cho máy mới thời gian. Với máy vừa bắt đầu, pool lấy trung bình theo số phút đã đào, nên số đầu dựa trên vài share. Sau một giờ, cửa sổ đủ một giờ được áp dụng.

Bảng Recent Shares cho biết ngay share đang đến, kể cả trước khi hashrate ổn định.

## Khi nào bạn nhận được khoản thanh toán đầu tiên?

Không có thời gian đếm ngược cố định từ share đầu tiên đến khoản thanh toán đầu tiên. Pool phải tìm được block khi share của bạn còn trong cửa sổ thanh toán. Sau đó block cần 60 xác nhận: trung bình khoảng năm giờ **sau khi tìm thấy block**, nhưng thời gian thực tế thay đổi. Khi số dư đã xác nhận đạt mức tối thiểu hiện tại 0,1 BNT, số dư đủ điều kiện vào đợt thanh toán tự động. Xem thiết lập hiện tại tại [Get Started](/start).

Theo dõi trạng thái và liên kết giao dịch trong **Recent Payouts**. Khoản thanh toán đang xếp hàng chưa được phát lên mạng. Nếu khoản đã gửi chưa xuất hiện trong ví, xem [hướng dẫn xử lý vấn đề thanh toán](/blog/vi/pool-payout-missing-from-wallet/).

### Số dư

- **Estimated Rewards:** phần của bạn trong các khối pool tìm thấy vẫn đang trưởng thành.
- **Confirmed Rewards:** số dư trưởng thành chờ lần thanh toán tiếp theo.
- **Paid Balance:** tất cả đã gửi đến địa chỉ của bạn.

Khi có phần thưởng đang chờ, bảng **Estimated From Recent Blocks** liệt kê từng khối với độ cao, khoản ghi ước tính, số xác nhận còn thiếu và thời gian tìm thấy. Mỗi độ cao liên kết đến khối trên [trình khám phá Blocknet](https://explorer.blocknetcrypto.com) để tự kiểm tra. [Thanh toán PPLNS hoạt động thế nào](/blog/vi/how-pplns-payouts-work/) giải thích chia thưởng mỗi khối và thời điểm trả số dư.

<figure class="tracking-figure">
<picture><source media="(max-width: 640px)" srcset="/blog/tracking/tracking-rewards-mobile.webp" width="352" height="524"><img src="/blog/tracking/tracking-rewards-desktop.webp" width="1081" height="217" alt="Số dư ước tính, đã xác nhận và đã trả là các giai đoạn riêng. Giao diện tiếng Anh thật với số tiền ví dụ, không phải thu nhập dự kiến." loading="lazy" decoding="async"></picture>
<figcaption>Số dư ước tính, đã xác nhận và đã trả là các giai đoạn riêng. Giao diện tiếng Anh thật với số tiền ví dụ, không phải thu nhập dự kiến.</figcaption>
</figure>

### Thanh toán và share gần đây

**Recent Payouts** liệt kê từng khoản trả đến địa chỉ với số tiền, liên kết giao dịch trên trình khám phá và trạng thái queued, unconfirmed hoặc confirmed. **Recent Shares** hiển thị 20 share mới nhất cùng job, dàn máy, độ khó và trạng thái; đây là cách nhanh nhất xác nhận máy vừa khởi động đang gửi tới pool.

<span id="zero-hashrate"></span>

## Vì sao seine đang đào nhưng My Stats hiển thị hashrate bằng không?

Bắt đầu bằng địa chỉ nhận thanh toán, kết nối pool và các share được chấp nhận. Hashrate cục bộ cho biết máy đang tính hash; ước tính của pool cần các share được chấp nhận cho địa chỉ bạn tra cứu.

1. **Đối chiếu địa chỉ.** Tra cứu đúng địa chỉ nhận hoặc `@name` đã đăng ký mà seine sử dụng. Nếu đã đổi địa chỉ của tên ngắn, hãy khởi động lại seine để phân giải lại tên, rồi kiểm tra địa chỉ đã phân giải trên My Stats.
2. **Đối chiếu pool.** Kiểm tra seine có dùng `stratum+tcp://bntpool.com:3333` hay không. Máy đào ở nơi khác hoặc ở chế độ solo sẽ không thêm share vào đây. [Hướng dẫn thiết lập pool](/blog/vi/mine-blocknet-on-bntpool/) giải thích cách kết nối.
3. **Kiểm tra Recent Shares.** Chờ share được chấp nhận, rồi để trang cập nhật. Share đến theo khoảng thời gian thay đổi; hashrate cục bộ đang chạy không bảo đảm pool có ước tính ngay. Nếu các share được chấp nhận vẫn đến, hãy so sánh giá trị trung bình theo thời gian.
4. **Đọc thông báo kết nối.** Dùng bảng khi máy đào chưa có share được chấp nhận. Xem [Status](/status) để biết sự cố hiện tại.

| Thông báo trong seine | Nội dung cần kiểm tra |
|---|---|
| `failed to connect to pool at` | Kiểm tra tên máy chủ và cổng, kết nối của bạn và việc cho phép TCP đi ra tới cổng 3333. |
| `login rejected:` | Đọc lý do theo sau; kiểm tra địa chỉ nhận hoặc tên ngắn và dùng phiên bản seine hiện tại. |
| `stale job` | Share thuộc một nhiệm vụ cũ. Theo dõi nhiệm vụ mới và share được chấp nhận; nếu tiếp diễn, kiểm tra kết nối và phiên bản máy đào. |
| `submit interrupted during reconnect` | Seine đang kết nối lại. Theo dõi việc chấp nhận đăng nhập và các share mới được chấp nhận sau khi kết nối trở lại. |

**Estimated Rewards** bằng không là vấn đề khác: share có thể được chấp nhận trước khi một block của pool ghi nhận phần thưởng. [Hướng dẫn PPLNS](/blog/vi/how-pplns-payouts-work/) giải thích trình tự này.

## Trang công khai nào hiển thị toàn pool?

Những trang này không cần tra cứu và công khai mọi việc pool làm. [Bảng điều khiển](/) tập hợp số chính: thợ đào kết nối, hashrate pool và mạng, khối tìm được và may mắn trung bình.

### Khối

[Blocks](/blocks) liệt kê mọi khối bntpool.com tìm được, mới nhất trước: độ cao (liên kết trình khám phá), phần thưởng, effort, thời gian vòng, trạng thái và thời gian tìm. Lọc theo Confirmed, Pending hoặc Orphaned để biết tình trạng từng khối.

### Thanh toán

[Payouts](/payouts) liệt kê từng đợt thanh toán với tổng tiền, số thợ đào được trả, liên kết giao dịch và trạng thái. Đợt xuất hiện là unconfirmed ngay khi phát đi, rồi chuyển confirmed không lâu sau xác nhận khối đầu tiên; bạn có thể theo khoản của mình từ phía pool và trình khám phá.

### May mắn

[Luck](/luck) hiển thị effort mỗi vòng. Effort là công việc share pool chấp nhận trong vòng chia độ khó khối: 100% nghĩa là cần đúng lượng công việc kỳ vọng, thấp hơn nghĩa là pool tìm sớm hơn. Vòng riêng dao động mạnh cả hai hướng, nên **Average Luck** trên bảng điều khiển là số nên theo dõi. Bảng **Pool Luck History** liên kết tới đây qua **View All**.

### Trạng thái

[Status](/status) là trang sức khỏe riêng của pool. Nó hiển thị pool, HTTP công khai, API, Stratum, cơ sở dữ liệu và daemon có trực tuyến không; mẫu job hiện tại mới đến đâu; độ cao chuỗi; thời gian hoạt động trong 10 phút, 6 giờ, 24 giờ và 7 ngày qua; cùng lịch sử sự cố. Thời gian hoạt động được đo hai cách: bộ giám sát trên máy chủ pool và đầu dò bên ngoài chạy trên Cloudflare.

## Còn bảng điều khiển riêng của seine?

Pool hiển thị những gì đã ghi cho bạn. Để xem máy trực tiếp, bảng điều khiển terminal của seine hiển thị hashrate và share ngay khi xảy ra. seine còn có thể cung cấp API điều khiển cục bộ với REST, luồng sự kiện do máy chủ gửi và số liệu Prometheus tại `/metrics`, mặc định bind vào `127.0.0.1:9977`. [Hướng dẫn API điều khiển](https://github.com/zpalmtree/seine/blob/master/docs/API.md) trình bày cờ và endpoint. Nhiều thợ đào dùng cả hai: số liệu seine cho từng máy, API pool cho số dư và thanh toán.

## Dùng API bntpool.com thế nào?

<Answer>
API bntpool.com là JSON công khai qua HTTPS, không cần khóa. Gọi https://bntpool.com/api/miner/ kèm địa chỉ để lấy hashrate, dàn máy, share và thanh toán; thêm /balance để lấy số dư; dùng /api/stats cho hashrate pool và mạng. Số tiền là đơn vị nguyên tử: chia 100.000.000 để được BNT.
</Answer>

Mọi trang trên đọc từ các endpoint này, nên bất cứ gì thấy trên trang bạn đều có thể tự lấy:

| Endpoint | Dữ liệu trả về |
|---|---|
| `/api/stats` | Thợ đào kết nối, hashrate pool, khối tìm thấy, hashrate mạng, độ cao hiện tại |
| `/api/stats/history?range=24h` | Hashrate pool theo thời gian (`1h`, `24h`, `7d`, `30d`) |
| `/api/stats/insights` | Vòng hiện tại, effort trung bình, vòng gần đây |
| `/api/info` | Cài đặt pool: cổng Stratum, phương thức và cửa sổ trả thưởng, thanh toán tối thiểu |
| `/api/blocks?status=confirmed` | Khối tìm được, lọc được theo trạng thái |
| `/api/payouts/recent` | Đợt thanh toán với hash giao dịch |
| `/api/luck` | Effort và thời lượng từng vòng |
| `/api/status` | Sức khỏe dịch vụ, cửa sổ thời gian hoạt động, sự cố |
| `/api/miner/{address}` | Hashrate, tổng số, dàn máy, share mới nhất, thanh toán của bạn |
| `/api/miner/{address}/balance` | Số dư ước tính, đã xác nhận và đã trả |
| `/api/miner/{address}/hashrate?range=24h` | Lịch sử hashrate riêng |

Endpoint danh sách nhận `limit` (mặc định 25, tối đa 200) và `offset`, trả `items` cùng `total`. Endpoint thợ đào nhận địa chỉ đầy đủ, nên sau khi tra @name trên My Stats, sao chép địa chỉ đầy đủ từ ô tìm kiếm.

Đọc dữ liệu trình đào từ terminal:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS
```

Với `jq`, bạn có thể lấy giá trị riêng, như hashrate, share chấp nhận hoặc tổng đã trả bằng BNT:

```sh
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS | jq '{hashrate, total_accepted, total_rejected}'
curl -s https://bntpool.com/api/miner/YOUR_ADDRESS/balance | jq '.balance.paid / 100000000'
```

<CTA target="pool" title="Bắt đầu đào và theo dõi trực tiếp" text="Tham gia bntpool.com trong vài phút, rồi theo dõi hashrate, share và số dư trên My Stats." />
