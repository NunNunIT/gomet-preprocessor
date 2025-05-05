# preprocessor/thodiamomo_json.py
import pandas as pd
import re
import json

def process_momo(input_path, output_path):
    # Đọc dữ liệu từ file JSON
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    momo = pd.DataFrame(data)

    # Đổi tên các cột
    momo = momo.rename(columns={
        'tinh': 'province',
        'qh': 'district',
        'px': 'ward',
        'services': 'exts',
        'open_hour': 'openHour',
        'rating': 'avgRating',
        'count_comments': 'ratingCount',
        'review1': 'post1',
        'review2': 'post2',
        'review3': 'post3',
        'review4': 'post4',
        'review5': 'post5',
        'phone_numbers': 'phones'
    })

    # Tạo cột imgs
    image_columns = [f'Image{i}' for i in range(1, 13)]
    momo['imgs'] = momo[image_columns].values.tolist()
    momo = momo.drop(columns=image_columns)

    # Chuyển đổi openHour
    day_mapping = {
        'thứ 2': 'monday',
        'thứ 3': 'tuesday',
        'thứ 4': 'wednesday',
        'thứ 5': 'thursday',
        'thứ 6': 'friday',
        'thứ 7': 'saturday',
        'chủ nhật': 'sunday'
    }

    def convert_open_hour(open_hour_str):
        if pd.isna(open_hour_str):
            return {v: [] for v in day_mapping.values()}
        open_hour_dict = {v: [] for v in day_mapping.values()}
        entries = str(open_hour_str).split('; ')
        for entry in entries:
            if ',' in entry:
                day_part, time_part = entry.split(', ', 1)
                day_part = day_part.strip().lower()
                time_part = time_part.replace(' đến ', ' - ')
                if day_part in day_mapping:
                    day = day_mapping[day_part]
                    open_hour_dict[day].append(time_part)
        return open_hour_dict

    momo['openHour'] = momo['openHour'].apply(convert_open_hour)

    # Chuyển đổi exts
    momo['exts'] = momo['exts'].apply(lambda x: x.split(';') if pd.notna(x) else [])

    # Xóa cột trùng lặp
    momo = momo.loc[:, ~momo.columns.duplicated()]

    # Xử lý imgs
    momo['imgs'] = momo['imgs'].apply(lambda x: [img for img in x if pd.notna(img)] if isinstance(x, list) else [])

    # Tạo address
    def create_address(row):
        return {
            'province': row['province'] if pd.notna(row['province']) else '',
            'district': row['district'] if pd.notna(row['district']) else '',
            'ward': row['ward'] if pd.notna(row['ward']) else '',
            'street': row['street'] if pd.notna(row['street']) else ''
        }

    momo['address'] = momo.apply(create_address, axis=1)
    momo = momo.drop(columns=['ward', 'district', 'street', 'province'])
    momo['type'] = ''

    # Chuyển đổi categories
    momo['categories'] = momo['categories'].apply(lambda x: x.split('-') if pd.notna(x) else [])

    # Xử lý price
    def extract_price(price_str):
        if pd.isna(price_str):
            return 0
        try:
            return int(price_str.split('đ')[0].replace('.', '').strip())
        except:
            return 0

    momo['price'] = momo['price'].apply(extract_price)

    # Trích xuất tọa độ
    def extract_coordinates(url):
        if pd.isna(url):
            return {'long': '', 'lat': ''}
        match = re.search(r'query=([-+]?\d*\.\d+),([-+]?\d*\.\d+)', url)
        if match:
            lat = float(match.group(1))
            long = float(match.group(2))
            return {'long': long, 'lat': lat}
        return {'long': '', 'lat': ''}

    momo['locate'] = momo['url_address'].apply(extract_coordinates)

    # Bỏ cột không cần thiết
    columns_to_drop = ['url_address', 'logo_avt', 'avt_ImageURL', 'avt_Image_URL_backup', 'post1', 'post2', 'post3', 'post4', 'post5']
    momo = momo.drop(columns=[col for col in columns_to_drop if col in momo.columns])

    # Xử lý phones
    momo['phones'] = momo['phones'].apply(lambda x: [str(x)] if pd.notna(x) else [])

    # Lưu dữ liệu ra file JSON
    momo.to_json(output_path, orient='records', force_ascii=False, indent=2)
    return output_path

process_momo("C:\DATA\gomet-preprocessor\preprocessor\ThoDiaMoMo_Version2.json", "file_output.json")
