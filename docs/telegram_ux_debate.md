# HỘI ĐỒNG THẢO LUẬN 3 BÊN: THIẾT KẾ GIAO DIỆN & TRẢI NGHIỆM TELEGRAM BOT QUANTITATIVE v2.2
Thành phần tham dự:
- Bên 1: Antigravity / VANTA (Kỹ sư trưởng Thực thi & Điều phối)
- Bên 2: Subagent 1 (Claude Opus 5.5 - Trưởng nhóm Kiến trúc & UI/UX Bot Telegram)
- Bên 3: Subagent 2 (Claude Opus 5.5 - Chuyên gia Nghiên cứu & Phản biện Edge-Cases)

---

## PHẦN 1: BÊN 1 - ANTIGRAVITY (HIỆN TRẠNG & ĐẶT VẤN ĐỀ KỸ THUẬT)
1. Hiện trạng: Bot mới chỉ nhận lệnh dạng chuỗi văn bản (`/start`, `/balance`), người dùng trên điện thoại phải mở bàn phím gõ thủ công rất bất tiện.
2. Thiếu gói lệnh hệ thống (`setMyCommands`): Nút Menu góc dưới bên trái của Telegram trống rỗng, không tự gợi ý lệnh.
3. Rác màn hình chat: Mỗi lệnh gõ ra lại sinh 1 tin nhắn trả lời mới, làm trôi màn hình.
4. Yêu cầu:
   - Đăng ký bộ lệnh native qua Telegram Bot API `setMyCommands`.
   - Chuyển 100% sang Dashboard tương tác bằng Inline Keyboard (nút bấm chạm màn hình).
   - Cơ chế Edit Message In-Place: Cập nhật trực tiếp số dư, trạng thái trên tin nhắn gốc khi bấm nút làm mới.

---

## PHẦN 2: BÊN 2 - SUBAGENT 1 (PLANNER - CLAUDE OPUS 5.5): THIẾT KẾ KIẾN TRÚC UI/UX
1. Đăng ký Native Commands:
   - Tự động gửi POST `/setMyCommands` khi bot khởi động.
   - Định nghĩa 8 lệnh chính thức chuẩn UX Telegram.
2. Kiến trúc Dashboard Trung Tâm (Master Inline Keyboard):
   - Thiết kế lưới nút bấm 2 cột:
     Row 1: [ 💼 Số Dư Chi Tiết ] | [ 💳 Ví & Nạp Tiền ]
     Row 2: [ ⚙️ Đổi Chế Độ ]      | [ 📡 Sức Khỏe Bot ]
     Row 3: [ ⏸️ Tạm Dừng ]        | [ 🔒 Rút Tiền Về ]
     Row 4: [ 🔄 Làm Mới Dữ Liệu (Refresh) ]
3. Trình bày trực quan (Visual Hierarchy):
   - Sử dụng emoji phân loại cấp độ: 🟢 Live, 🟡 Paper, 🔴 Alert, 💳 Wallet, ⛽ Gas.
   - Thêm thanh tiến trình vốn trực quan (ASCII Progress Bar).
   - Rút gọn địa chỉ ví dạng `0xDB3d...814B` có gắn link Polygonscan.

---

## PHẦN 3: BÊN 3 - SUBAGENT 2 (RESEARCHER - CLAUDE OPUS 5.5): PHẢN BIỆN EDGE-CASES
1. Xung đột Callback Query (Rate Limiting & Spam click):
   - Người dùng bấm liên tục nút [Làm Mới] nhiều lần trong 1 giây -> Telegram API sẽ trả lỗi `HTTP 429 Too Many Requests` hoặc `message is not modified`.
   - Khắc phục: Phải bắt exception `message is not modified` và chỉ gọi API cập nhật nếu số liệu thực tế thay đổi hoặc cách nhau tối thiểu 1.5 giây.
2. Xác thực Callback Query:
   - Luôn luôn gọi `answerCallbackQuery` ngay đầu hàm xử lý để tắt vòng xoay loading trên điện thoại người dùng, kèm thông báo toast ngắn gọn (ví dụ: "Đã làm mới dữ liệu!").
3. Bảo mật nút Rút tiền:
   - Khi bấm [Rút Tiền Về], KHÔNG rút ngay. Mở menu xác nhận mức rút [25% | 50% | 100%] -> Sau đó bắt buộc sinh mã OTP xác nhận như kiến trúc đã duyệt.
4. Hard-lock Chat ID:
   - Kiểm tra `query.from.id == 1423475386` trên mọi callback query. Người lạ bấm nút sẽ bị lờ đi hoàn toàn.

---

## PHẦN 4: ĐỒNG THUẬN CHUNG & CHỈ THỊ TRIỂN KHAI CHO ANTIGRAVITY
1. Tích hợp tự động `setMyCommands` trong hàm khởi tạo của `TelegramCommandHandler`.
2. Bổ sung bộ xử lý `callback_query` song song với `message` trong polling loop.
3. Thay thế toàn bộ tin nhắn text phản hồi bằng các mẫu Dashboard có Inline Keyboard tương tác 1 chạm.
