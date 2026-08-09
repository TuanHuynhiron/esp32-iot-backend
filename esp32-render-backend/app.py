import os
import json
import sqlite3
import base64
import time
from datetime import datetime
from zoneinfo import ZoneInfo


from flask import Flask, render_template, request, jsonify
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

app = Flask(__name__)
DATA_DIR = os.environ.get('DATA_DIR', '.')
UPLOAD_FOLDER = os.path.join(DATA_DIR, 'static/uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
DB_FILE = os.path.join(DATA_DIR, 'database.db')

# Bộ đệm lưu tạm các mảnh ảnh trong RAM trước khi ghép
chunks_storage = {}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sensor_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT, temp REAL, hum REAL, vib REAL, img_path TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS thresholds (
                    id INTEGER PRIMARY KEY, temp_max REAL, hum_max REAL, vib_max REAL)''')
    c.execute('INSERT OR IGNORE INTO thresholds (id, temp_max, hum_max, vib_max) VALUES (1, 35.0, 80.0, 5.0)')
    conn.commit()
    conn.close()

init_db()

def clean_expired_chunks():
    """Tự động dọn dẹp các phiên truyền ảnh dở dang nằm trong RAM quá 10 phút"""
    now = time.time()
    expired_ids = []
    # Dùng list() để tránh RuntimeError khi sửa đổi dictionary lúc đang lặp
    for uid, info in list(chunks_storage.items()):
        if now - info.get('created_at', now) > 600: # 10 phút
            expired_ids.append(uid)
    for uid in expired_ids:
        if uid in chunks_storage:
            del chunks_storage[uid]
            print(f"[SERVER RAM] Đã xóa phiên hết hạn/treo: {uid}")

def safe_base64_decode(b64_str):
    """Xử lý và giải mã Base64 an toàn, tự sửa lỗi rụng ký tự/padding"""
    # 1. Làm sạch khoảng trắng, xuống dòng, ký tự rác
    clean_b64 = b64_str.strip().replace(" ", "+").replace("\r", "").replace("\n", "")
    
    # 2. Xử lý trường hợp bị rụng ký tự (độ dài chia 4 dư 1)
    mod = len(clean_b64) % 4
    if mod == 1:
        clean_b64 = clean_b64[:-1]
        mod = len(clean_b64) % 4

    # 3. Bù dấu '=' chuẩn hóa độ dài chia hết cho 4
    if mod == 2:
        clean_b64 += "=="
    elif mod == 3:
        clean_b64 += "="

    return base64.b64decode(clean_b64)

@app.route('/')
def index():
    return render_template('index.html')

# =========================================================================
# 1. API CẤP THỜI GIAN CHO ESP32 (Đồng bộ RTC & Tạo Tên File Ảnh)
# =========================================================================
@app.route('/api/get-time', methods=['GET'])
def get_time():
    now_str = datetime.now(VN_TZ).strftime("%Y%m%d_%H%M%S")
    return now_str, 200, {'Content-Type': 'text/plain'}

# =========================================================================
# 2. API LẤY DỮ LIỆU BẢNG ĐIỀU KHIỂN (Dành cho Giao diện Web Client)
# =========================================================================
@app.route('/api/data', methods=['GET'])
def get_data():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM sensor_data ORDER BY id DESC LIMIT 50")
    rows = c.fetchall()
    c.execute("SELECT temp_max, hum_max, vib_max FROM thresholds WHERE id=1")
    thresh = c.fetchone()
    conn.close()
    
    data = [{
        'id': r[0], 'timestamp': r[1], 'temp': r[2], 
        'hum': r[3], 'vib': r[4], 'img_path': r[5]
    } for r in rows]
    
    t_temp = thresh[0] if thresh else 35.0
    t_hum = thresh[1] if thresh else 80.0
    t_vib = thresh[2] if thresh else 5.0

    response = jsonify({'records': data, 'thresholds': {'temp': t_temp, 'hum': t_hum, 'vib': t_vib}})
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

# =========================================================================
# 3. API NHẬN CHUNK DỮ LIỆU & ẢNH TỪ ESP32 VIA SIM 4G
# =========================================================================
@app.route('/api/data-chunk', methods=['POST'])
def receive_data_chunk():
    clean_expired_chunks() # Kiểm tra dọn RAM rác
    
    req_data = request.get_json()
    if not req_data:
        return jsonify({"status": "error", "message": "No JSON payload"}), 400

    try:
        chunk_idx = int(req_data.get('chunk_index', 0))
        total_chunks = int(req_data.get('total_chunks', 1))
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "Invalid chunk_index or total_chunks"}), 400

    chunk_b64 = req_data.get('data', '')
    upload_id = str(req_data.get('upload_id', 'session_default'))

    # ✅ SỬA LỖI 1: Chỉ tạo mới phiên nếu upload_id CHƯA TỒN TẠI trong RAM.
    # Không tạo lại khi chunk_idx == 0 để tránh đè sạch dữ liệu nếu Chunk 0 tới sau.
    if upload_id not in chunks_storage:
        chunks_storage[upload_id] = {
            'temp': req_data.get('temp', 0),
            'hum': req_data.get('hum', 0),
            'vib': req_data.get('vib', 0),
            'chunks': {},
            'created_at': time.time()
        }
        print(f"\n[SERVER] ===> KHỞI TẠO PHIÊN MỚI: {upload_id} (Tổng số chunk: {total_chunks}) <===")
    else:
        # Cập nhật thông tin cảm biến nếu gói tin mang dữ liệu thực
        if req_data.get('temp'): chunks_storage[upload_id]['temp'] = req_data.get('temp')
        if req_data.get('hum'): chunks_storage[upload_id]['hum'] = req_data.get('hum')
        if req_data.get('vib'): chunks_storage[upload_id]['vib'] = req_data.get('vib')

    # Lưu chunk vào RAM chính xác theo Index key
    chunks_storage[upload_id]['chunks'][chunk_idx] = chunk_b64
    received_count = len(chunks_storage[upload_id]['chunks'])
    print(f"[SERVER CHUNK] Nhận đoạn {chunk_idx + 1}/{total_chunks} (Đã có {received_count}/{total_chunks}) - Phiên: {upload_id}")

    # ✅ SỬA LỖI 2: Kiểm tra ĐỦ TẤT CẢ các index từ 0 -> total_chunks - 1
    all_chunks_present = all(i in chunks_storage[upload_id]['chunks'] for i in range(total_chunks))

    if all_chunks_present:
        print(f"[SERVER CHUNK] Đã nhận ĐỦ toàn bộ {total_chunks} đoạn! Bắt đầu ghép ảnh...")
        
        if upload_id and (upload_id.endswith('.jpg') or upload_id.endswith('.jpeg')):
            img_filename = os.path.basename(upload_id)
        else:
            img_filename = f"IMG_{datetime.now(VN_TZ).strftime('%Y%m%d_%H%M%S')}.jpg"
            
        file_path = os.path.join(UPLOAD_FOLDER, img_filename)

        try:
            # 1. Ghép nối chính xác từ index 0 đến total_chunks - 1
            full_b64 = "".join([chunks_storage[upload_id]['chunks'][i] for i in range(total_chunks)])
            
            # 2. Giải mã Base64
            img_bytes = safe_base64_decode(full_b64)
            
            # 3. Ghi dữ liệu ra file JPEG
            with open(file_path, "wb") as fh:
                fh.write(img_bytes)
                
            print(f"[SERVER OK] Ghép ảnh thành công: {img_filename} ({len(img_bytes)} bytes)")
        except Exception as e:
            print(f"[SERVER ERROR] Lỗi giải mã Base64 ghép ảnh: {e}")
            img_filename = ""

        # Lưu thông tin cảm biến và tên ảnh vào SQLite
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            now = datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
            c.execute("INSERT INTO sensor_data (timestamp, temp, hum, vib, img_path) VALUES (?, ?, ?, ?, ?)",
                      (now, chunks_storage[upload_id]['temp'], chunks_storage[upload_id]['hum'], chunks_storage[upload_id]['vib'], img_filename))
            conn.commit()
            conn.close()
            print("[SERVER DB] Đã lưu dữ liệu & ảnh vào SQLite!")
        except Exception as db_err:
            print(f"[SERVER DB ERROR] Lỗi ghi DB: {db_err}")

        # Xóa phiên khỏi RAM sau khi ghép thành công
        del chunks_storage[upload_id]
        return jsonify({"status": "success", "image": img_filename}), 200

    return jsonify({"status": "received", "chunk_index": chunk_idx, "received": received_count, "total": total_chunks}), 200

# =========================================================================
# 4. API XÓA BẢN GHI & CẬP NHẬT CẤU HÌNH NGƯỠNG
# =========================================================================
@app.route('/api/delete/<int:record_id>', methods=['DELETE'])
def delete_data(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT img_path FROM sensor_data WHERE id=?", (record_id,))
    row = c.fetchone()
    if row and row[0]:
        img_p = os.path.join(UPLOAD_FOLDER, row[0])
        if os.path.exists(img_p):
            os.remove(img_p)
            
    c.execute("DELETE FROM sensor_data WHERE id=?", (record_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "deleted"}), 200

@app.route('/api/thresholds', methods=['POST'])
def set_thresholds():
    req = request.get_json()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE thresholds SET temp_max=?, hum_max=?, vib_max=? WHERE id=1",
              (req.get('temp'), req.get('hum'), req.get('vib')))
    conn.commit()
    conn.close()
    return jsonify({"status": "updated"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
