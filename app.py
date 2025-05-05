from flask import Flask, render_template, request, send_file, jsonify
import os
import logging
from werkzeug.utils import secure_filename
from preprocessor.thodiamomo import process_momo

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
try:
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    logger.info("Upload and output directories created successfully")
except Exception as e:
    logger.error(f"Error creating directories: {e}")

# Hàm xử lý file Ticket (giả định)
def process_ticket(input_path, output_path):
    try:
        import pandas as pd
        # Đọc file JSON
        ticket = pd.read_json(input_path, orient='records')
        
        # Xử lý giả định: ví dụ, chuyển đổi tên cột và thêm cột mới
        ticket = ticket.rename(columns={'ticket_id': 'id', 'price': 'cost'})
        ticket['processed'] = True
        
        # Lưu file JSON
        ticket.to_json(output_path, orient='records', force_ascii=False, indent=4)
        logger.info(f"Ticket processed successfully: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Error processing ticket: {e}")
        raise

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
        if 'file' not in request.files:
            logger.warning("No file uploaded for MoMo")
            return jsonify({'status': 'error', 'message': 'No file uploaded for MoMo'}), 400
        file = request.files['file']
        if file.filename == '':
            logger.warning("No file selected for MoMo")
            return jsonify({'status': 'error', 'message': 'No file selected for MoMo'}), 400
        if file and file.filename.endswith('.json'):
            filename = secure_filename(file.filename)
            input_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            output_filename = f"{os.path.splitext(filename)[0]}-preprocessor.json"  # Đổi tên file đầu ra
            output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_filename)
            file.save(input_path)
            logger.info(f"File uploaded for MoMo: {input_path}")

            # Xử lý file với hàm process_momo
            process_momo(input_path, output_path)

            # Xóa file tạm
            try:
                os.remove(input_path)
                logger.info(f"Temporary file removed: {input_path}")
            except Exception as e:
                logger.warning(f"Error removing temporary file {input_path}: {e}")

            logger.info(f"MoMo processed successfully: {output_path}")
            return send_file(output_path, as_attachment=True, download_name=output_filename, mimetype='application/json')
        logger.warning("Invalid file format for MoMo")
        return jsonify({'status': 'error', 'message': 'Invalid file format for MoMo. Please upload a JSON file'}), 400
    except Exception as e:
        logger.error(f"Error in process_thodiamomo: {e}")
        return jsonify({'status': 'error', 'message': f'Error processing MoMo file: {str(e)}'}), 500

@app.route('/process_ticketbox', methods=['POST'])
def process_ticketbox():
    try:
        if 'file' not in request.files:
            logger.warning("No file uploaded for Ticket")
            return jsonify({'status': 'error', 'message': 'No file uploaded for Ticket'}), 400
        file = request.files['file']
        if file.filename == '':
            logger.warning("No file selected for Ticket")
            return jsonify({'status': 'error', 'message': 'No file selected for Ticket'}), 400
        if file and file.filename.endswith('.json'):
            filename = secure_filename(file.filename)
            input_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            output_filename = f"{os.path.splitext(filename)[0]}-preprocessor.json"  # Đổi tên file đầu ra
            output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_filename)
            file.save(input_path)
            logger.info(f"File uploaded for Ticket: {input_path}")

            # Xử lý file
            process_ticket(input_path, output_path)

            # Xóa file tạm
            try:
                os.remove(input_path)
                logger.info(f"Temporary file removed: {input_path}")
            except Exception as e:
                logger.warning(f"Error removing temporary file {input_path}: {e}")

            logger.info(f"Ticket processed successfully: {output_path}")
            return send_file(output_path, as_attachment=True, download_name=output_filename, mimetype='application/json')
        logger.warning("Invalid file format for Ticket")
        return jsonify({'status': 'error', 'message': 'Invalid file format for Ticket. Please upload a JSON file'}), 400
    except Exception as e:
        logger.error(f"Error in process_ticketbox: {e}")
        return jsonify({'status': 'error', 'message': f'Error processing Ticket file: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)