import pandas as pd
import re

def process_momo(input_path, output_path):
    # Đọc file JSON
    momo = pd.read_json(input_path, orient='records')

    # Đổi tên các cột
    momo = momo.rename(columns={
        'rating': 'avgRating',
        'count_comments': 'ratingCount',
        'phone_numbers': 'phones',
        'services': 'exts',
        'open_hour': 'openHour'
    })

    # Tạo cột imgs từ Image1 đến Image12
    image_columns = [f'Image{i}' for i in range(1, 13)]
    # Đảm bảo các cột Image tồn tại, nếu không thì tạo cột rỗng
    for col in image_columns:
        if col not in momo.columns:
            momo[col] = ''
    # Tạo cột imgs bằng cách gộp các giá trị từ Image1 đến Image12
    momo['imgs'] = momo[image_columns].apply(
        lambda row: [img for img in row if pd.notna(img) and img != ''], axis=1
    )
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
        if pd.isna(open_hour_str) or not open_hour_str:
            return {v: [] for v in day_mapping.values()}
        open_hour_dict = {v: [] for v in day_mapping.values()}
        entries = open_hour_str.split('; ')
        for entry in entries:
            if entry and ',' in entry:
                day_part, time_part = entry.split(', ', 1)
                day_part = day_part.strip().lower()
                time_part = time_part.replace(' đến ', ' - ')
                if day_part in day_mapping:
                    day = day_mapping[day_part]
                    open_hour_dict[day].append(time_part)
        return open_hour_dict

    momo['openHour'] = momo['openHour'].apply(convert_open_hour)

    # Chuyển đổi exts
    momo['exts'] = momo['exts'].apply(lambda x: x.split(';') if pd.notna(x) and x else [])

    # Chuyển đổi categories
    momo['categories'] = momo['categories'].apply(lambda x: x.split('-') if pd.notna(x) and x else [])

    # Xử lý price
    def extract_price(price_str):
        if pd.isna(price_str) or not price_str:
            return 0
        try:
            return int(price_str.split('đ')[0].replace('.', '').replace(' ', '').split('/')[0])
        except:
            return 0

    momo['price'] = momo['price'].apply(extract_price)

    # Xử lý address
    def parse_address(address_str):
        if pd.isna(address_str) or not address_str:
            return {'province': '', 'district': '', 'ward': '', 'street': ''}
        parts = address_str.split(', ')
        if len(parts) >= 4:
            street = parts[0]
            ward = parts[1]
            district = parts[2]
            province = parts[3]
        else:
            street = address_str
            ward = district = province = ''
        return {
            'province': province,
            'district': district,
            'ward': ward,
            'street': street
        }

    momo['address'] = momo['address'].apply(parse_address)

    # Trích xuất tọa độ từ url_address
    def extract_coordinates(url):
        if pd.isna(url) or not url:
            return {'long': '', 'lat': ''}
        match = re.search(r'query=([-+]?\d*\.\d+),([-+]?\d*\.\d+)', url)
        if match:
            lat = float(match.group(1))
            long = float(match.group(2))
            return {'long': long, 'lat': lat}
        return {'long': '', 'lat': ''}

    momo['locate'] = momo['url_address'].apply(extract_coordinates)

    # Xử lý phones
    momo['phones'] = momo['phones'].apply(lambda x: [str(x)] if pd.notna(x) else [''])

    # Thêm cột type
    momo['type'] = momo['type'] if 'type' in momo.columns else ['family_meal']

    # Bỏ các cột không cần thiết
    columns_to_drop = ['url_address', 'logo_avt', 'avt_ImageURL', 'avt_Image_URL_backup', 'review1', 'review2', 'review3', 'review4', 'review5']
    momo = momo.drop(columns=[col for col in columns_to_drop if col in momo.columns])

    # Xóa cột trùng lặp
    momo = momo.loc[:, ~momo.columns.duplicated()]

    # Lưu file JSON
    momo.to_json(output_path, orient='records', force_ascii=False, indent=4)
    return output_path