from flask import Flask, render_template, request, send_file, jsonify
import os
import logging
from crawl.thodiamomo import crawl_momo_data

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Cấu hình thư mục lưu file tạm
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# Đảm bảo thư mục tồn tại
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
logger.info("Upload and output directories created successfully")

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/thodiamomo')
def thodiamomo():
    return render_template('thodiamomo.html')

@app.route('/ticketbox')
def ticketbox():
    return render_template('ticketbox.html')

@app.route('/process_thodiamomo', methods=['POST'])
def process_thodiamomo():
    try:
        # Lấy pageSize từ form
        page_size = request.form.get('pageSize', '10')  # Mặc định là 10 nếu không nhập
        if not page_size.isdigit() or int(page_size) <= 0:
            return jsonify({'status': 'error', 'message': 'pageSize phải là số nguyên dương'}), 400

        # Gọi hàm cào dữ liệu
        output_path = crawl_momo_data(int(page_size))
        
        # Trả file JSON về máy người dùng
        logger.info(f"MoMo processed successfully: {output_path}")
        return send_file(
            output_path,
            as_attachment=True,
            download_name='momo_data.json',
            mimetype='application/json'
        )
    except Exception as e:
        logger.error(f"Error in process_thodiamomo: {e}")
        return jsonify({'status': 'error', 'message': f'Error processing MoMo data: {str(e)}'}), 500

@app.route('/process_ticketbox', methods=['POST'])
def process_ticketbox():
    return jsonify({'status': 'error', 'message': 'Chưa triển khai chức năng Ticketbox'}), 501

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)