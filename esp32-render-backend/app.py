import os
import json
import sqlite3
import base64
import time
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory

app = Flask(__name__)

# Cấu hình đường dẫn tuyệt đối cho Render / Linux Host
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'static', 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

DB_FILE = os.path.join(BASE_DIR, 'database.db')

# Bộ nhớ tạm lưu trữ Chunk ảnh trong RAM & Khóa Thread an toàn
chunks_storage = {}
storage_lock = threading.Lock()

def get_db_connection():
    # Khởi tạo kết nối SQLite với timeout và chế độ WAL chống khóa DB
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL;')
    return conn

def init_db():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS sensor_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT, temp REAL, hum REAL, vib REAL, battery REAL, img_path TEXT)''')
        
        # Kiểm tra và thêm cột battery nếu CSDL chưa có
        c.execute("PRAGMA table_info(sensor_data)")
        columns = [col[1] for col in c.fetchall()]
        if 'battery' not in columns:
            c.execute("ALTER TABLE sensor_data ADD COLUMN battery REAL DEFAULT 0.0")

        c.execute('''CREATE TABLE IF NOT EXISTS thresholds (
                        id INTEGER PRIMARY KEY, temp_max REAL, hum_max REAL, vib_max REAL)''')
        c.execute('INSERT OR IGNORE INTO thresholds (id, temp_max, hum_max, vib_max) VALUES (1, 35.0, 80.0, 5.0)')
        conn.commit()
    except Exception as e:
        print(f"[INIT DB ERROR] {e}", flush=True)
    finally:
        if conn:
            conn.close()

init_db()

def clean_expired_chunks():
    # Tự động dọn dẹp các phiên upload treo quá 10 phút trong RAM
    now = time.time()
    with storage_lock:
        expired_ids = [uid for uid, info in chunks_storage.items() if now - info.get('created_at', now) > 600]
        for uid in expired_ids:
            if uid in chunks_storage:
                del chunks_storage[uid]
                print(f"[SERVER RAM] Đã xóa phiên hết hạn: {uid}", flush=True)

def safe_base64_decode(b64_str):
    # Xử lý và giải mã Base64 an toàn tự sửa lỗi padding và xuống dòng
    clean_b64 = b64_str.strip().replace(" ", "+").replace("\r", "").replace("\n", "")
    mod = len(clean_b64) % 4
    if mod == 1:
        clean_b64 = clean_b64[:-1]
    elif mod == 2:
        clean_b64 += "=="
    elif mod == 3:
        clean_b64 += "="
    return base64.b64decode(clean_b64)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/uploads/<path:filename>')
def serve_upload(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# 1. API CẤP THỜI GIAN ĐỒNG BỘ RTC CHO ESP32
@app.route('/api/get-time', methods=['GET'])
def get_time():
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    return now_str, 200, {'Content-Type': 'text/plain'}

# 2. API TRẢ DỮ LIỆU BẢNG ĐIỀU KHIỂN WEB
@app.route('/api/data', methods=['GET'])
def get_data():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT id, timestamp, temp, hum, vib, battery, img_path FROM sensor_data ORDER BY id DESC LIMIT 50")
        rows = c.fetchall()
        c.execute("SELECT temp_max, hum_max, vib_max FROM thresholds WHERE id=1")
        thresh = c.fetchone()

        data = []
        for r in rows:
            raw_img = r[6] if r[6] else ""
            filename_only = os.path.basename(raw_img) if raw_img else ""
            
            data.append({
                'id': r[0], 'timestamp': r[1], 'temp': r[2], 
                'hum': r[3], 'vib': r[4], 'battery': r[5], 'img_path': filename_only
            })

        current_status = data[0] if len(data) > 0 else {'temp': 0, 'hum': 0, 'vib': 0, 'battery': 0}
        
        t_temp = thresh[0] if thresh else 35.0
        t_hum = thresh[1] if thresh else 80.0
        t_vib = thresh[2] if thresh else 5.0

        response = jsonify({
            'current': current_status,
            'records': data, 
            'thresholds': {'temp': t_temp, 'hum': t_hum, 'vib': t_vib}
        })
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response, 200
    except Exception as e:
        print(f"[API DATA ERROR] {e}", flush=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()

# 3. API NHẬN CHUNK DỮ LIỆU & GHÉP ẢNH TỪ ESP32
@app.route('/api/data-chunk', methods=['POST'])
def receive_data_chunk():
    clean_expired_chunks()
    req_data = request.get_json(force=True, silent=True)
    if not req_data:
        return jsonify({"status": "error", "message": "No JSON payload"}), 400

    try:
        upload_id = str(req_data.get('upload_id', 'session_default'))
        chunk_idx = int(req_data.get('chunk_index', 0))
        total_chunks = int(req_data.get('total_chunks', 1))
        chunk_b64 = req_data.get('data', '')

        with storage_lock:
            if upload_id not in chunks_storage:
                chunks_storage[upload_id] = {
                    'temp': req_data.get('temp', 0),
                    'hum': req_data.get('hum', 0),
                    'vib': req_data.get('vib', 0),
                    'battery': req_data.get('battery', 0),
                    'chunks': {},
                    'created_at': time.time()
                }
                print(f"[SERVER] Mở phiên mới: {upload_id} ({total_chunks} chunks)", flush=True)
            else:
                if req_data.get('temp') is not None: chunks_storage[upload_id]['temp'] = req_data.get('temp')
                if req_data.get('hum') is not None: chunks_storage[upload_id]['hum'] = req_data.get('hum')
                if req_data.get('vib') is not None: chunks_storage[upload_id]['vib'] = req_data.get('vib')
                if req_data.get('battery') is not None: chunks_storage[upload_id]['battery'] = req_data.get('battery')

            chunks_storage[upload_id]['chunks'][chunk_idx] = chunk_b64
            received_count = len(chunks_storage[upload_id]['chunks'])
            all_chunks_present = (received_count == total_chunks)

            temp_val = chunks_storage[upload_id]['temp']
            hum_val = chunks_storage[upload_id]['hum']
            vib_val = chunks_storage[upload_id]['vib']
            bat_val = chunks_storage[upload_id]['battery']

            if all_chunks_present:
                sorted_keys = sorted(chunks_storage[upload_id]['chunks'].keys())
                full_b64 = "".join([chunks_storage[upload_id]['chunks'][k] for k in sorted_keys])
                del chunks_storage[upload_id]

        print(f"[SERVER CHUNK] {chunk_idx + 1}/{total_chunks} (Đã nhận {received_count}/{total_chunks})", flush=True)

        if all_chunks_present:
            if upload_id and (upload_id.endswith('.jpg') or upload_id.endswith('.jpeg')):
                img_filename = os.path.basename(upload_id)
            else:
                img_filename = f"IMG_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
                
            file_path = os.path.join(UPLOAD_FOLDER, img_filename)

            try:
                img_bytes = safe_base64_decode(full_b64)
                with open(file_path, "wb") as fh:
                    fh.write(img_bytes)
                print(f"[SERVER OK] Ghép ảnh thành công: {img_filename} ({len(img_bytes)} bytes)", flush=True)
            except Exception as e:
                print(f"[SERVER ERROR] Lỗi giải mã Base64: {e}", flush=True)
                img_filename = ""

            conn = None
            try:
                conn = get_db_connection()
                c = conn.cursor()
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                c.execute("INSERT INTO sensor_data (timestamp, temp, hum, vib, battery, img_path) VALUES (?, ?, ?, ?, ?, ?)",
                          (now_str, temp_val, hum_val, vib_val, bat_val, img_filename))
                conn.commit()
                print("[SERVER DB] ===> ĐÃ LƯU DỮ LIỆU & ẢNH VÀO CSDL THÀNH CÔNG! <===", flush=True)
            except Exception as db_err:
                print(f"[SERVER DB ERROR] {db_err}", flush=True)
            finally:
                if conn:
                    conn.close()

            return jsonify({"status": "success", "image": img_filename}), 200

        return jsonify({"status": "received", "chunk_index": chunk_idx, "received": received_count, "total": total_chunks}), 200

    except Exception as global_err:
        print(f"[SERVER CRITICAL ERROR] {global_err}", flush=True)
        return jsonify({"status": "error", "message": str(global_err)}), 500

# 4. API XÓA BẢN GHI VÀ CẬP NHẬT NGƯỠNG
@app.route('/api/delete/<int:record_id>', methods=['DELETE'])
def delete_data(record_id):
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT img_path FROM sensor_data WHERE id=?", (record_id,))
        row = c.fetchone()
        if row and row[0]:
            fname = os.path.basename(row[0])
            img_p = os.path.join(UPLOAD_FOLDER, fname)
            if os.path.exists(img_p):
                os.remove(img_p)
                
        c.execute("DELETE FROM sensor_data WHERE id=?", (record_id,))
        conn.commit()
        return jsonify({"status": "deleted"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/thresholds', methods=['POST'])
def set_thresholds():
    req = request.get_json(force=True, silent=True)
    if not req:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE thresholds SET temp_max=?, hum_max=?, vib_max=? WHERE id=1",
                  (req.get('temp'), req.get('hum'), req.get('vib')))
        conn.commit()
        return jsonify({"status": "updated"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
