import requests
import json
from .momo_map import type_map, server_categories_map, day_mapping

def crawl_momo_data(page_size):
    # API endpoints
    main_url = f"https://business.momo.vn/api/search/v2.1/tdmm/oas/recommend?language=vi&pageSize={page_size}&pageNumber=1&isPromotion=false"

    # Reverse server_categories_map for mapping Vietnamese to English
    reverse_categories_map = {v.lower(): k for k, v in server_categories_map.items()}

    # Initialize output as a list
    content = []

    try:
        # Make GET request to main API
        response = requests.get(main_url)
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Process each item in the main response
            for item in data.get("data", {}).get("content", []):
                # Process openHour
                open_hour = {
                    "monday": [], "tuesday": [], "wednesday": [], "thursday": [],
                    "friday": [], "saturday": [], "sunday": []
                }
                for time_entry in item.get("openingTimes", []):
                    day = day_mapping.get(time_entry.get("dayOfWeek"))
                    if day:
                        for time in time_entry.get("times", []):
                            time_str = f"{time.get('startTime')} - {time.get('endTime')}"
                            open_hour[day].append(time_str)
                
                # Fetch additional data from secondary API
                oa_id = item.get("id")
                secondary_url = f"https://thodia.momo.vn/_next/data/Ngjmk6dQuP_03fqJ-1q8t/oa/{oa_id}.json?oaId={oa_id}"
                try:
                    secondary_response = requests.get(secondary_url)
                    if secondary_response.status_code == 200:
                        secondary_data = secondary_response.json()
                        oa_data = secondary_data.get("pageProps", {}).get("oaData", {})
                        
                        # Extract and process categories
                        raw_categories = oa_data.get("categories", []) or []
                        categories = []
                        for cat in raw_categories:
                            cat_name = cat.get("name", cat) if isinstance(cat, dict) else cat
                            if not isinstance(cat_name, str):
                                continue
                            cat_key = reverse_categories_map.get(cat_name.lower())
                            if cat_key and cat_key not in ["service", "placeholder", "service_desc"]:
                                categories.append(cat_key)
                        
                        # Extract and process utilities
                        raw_utilities = oa_data.get("utilities", []) or []
                        utilities = []
                        for ut in raw_utilities:
                            ut_name = ut.get("name", ut) if isinstance(ut, dict) else ut
                            if not isinstance(ut_name, str):
                                continue
                            ut_key = reverse_categories_map.get(ut_name.lower())
                            if ut_key:
                                utilities.append(ut_key)

                        # Extract and transform address
                        secondary_address = oa_data.get("address")
                        address = None
                        if isinstance(secondary_address, dict):
                            address = {
                                "streetId": secondary_address.get("streetId"),
                                "wardId": secondary_address.get("wardId"),
                                "districtId": secondary_address.get("districtId"),
                                "houseNumber": secondary_address.get("houseNumber"),
                                "province": secondary_address.get("cityName"),
                                "district": secondary_address.get("districtName"),
                                "ward": secondary_address.get("wardName"),
                                "street": f"{secondary_address.get('houseNumber', '')} {secondary_address.get('streetName', '')}".strip()
                            }
                            geojson = {
                                "type": "Point",
                                "coordinates": [secondary_address.get("longitude"), secondary_address.get("latitude")],
                                "location": {
                                    "lat": secondary_address.get("latitude"),
                                    "long": secondary_address.get("longitude"),
                                    "streetId": secondary_address.get("streetId"),
                                    "wardId": secondary_address.get("wardId"),
                                    "districtId": secondary_address.get("districtId"),
                                    "houseNumber": secondary_address.get("houseNumber"),
                                    "province": secondary_address.get("cityName"),
                                    "district": secondary_address.get("districtName"),
                                    "ward": secondary_address.get("wardName"),
                                    "street": f"{secondary_address.get('houseNumber', '')} {secondary_address.get('streetName', '')}".strip(),
                                    "fullAddress": item.get("address")
                                }
                            }
                        
                        contact_number = oa_data.get("contactNumber")
                        description = oa_data.get("description")
                    else:
                        print(f"Secondary API error for ID {oa_id}: {secondary_response.status_code}")
                        categories = []
                        utilities = []
                        secondary_address = None
                        contact_number = None
                        description = None
                except requests.exceptions.RequestException as e:
                    print(f"Secondary API request failed for ID {oa_id}: {e}")
                    categories = []
                    utilities = []
                    secondary_address = None
                    contact_number = None
                    description = None
                
                # Determine type
                determined_type = item.get("type")
                for type_key, type_categories in type_map.items():
                    if any(cat in type_categories for cat in categories):
                        determined_type = type_key
                        break
                
                # Construct processed item
                processed_item = {
                    "name": item.get("name"),
                    "address": address,
                    "locate": {
                        "lat": item.get("location", {}).get("lat"),
                        "long": item.get("location", {}).get("lon")
                    },
                    "geojson": geojson,
                    "imgs": [img.get("originalUrl") for img in item.get("bannerImgUrls", [])],
                    "rating": item.get("rating"),
                    "ratingCount": item.get("ratingCount"),
                    "districtName": item.get("districtName"),
                    "cityName": item.get("cityName"),
                    "type": determined_type,
                    "openHour": open_hour,
                    "price": item.get("avgPrice"),
                    "avgUnit": item.get("avgUnit"),
                    "categories": categories,
                    "phones": [contact_number] if contact_number else [],
                    "exts": utilities,
                    "description": description
                }
                content.append(processed_item)
            
            # Save to JSON file
            output_path = "outputs/momo_data.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(content, f, ensure_ascii=False, indent=2)
            
            print(f"Data successfully saved to {output_path}")
            return output_path
            
        else:
            raise Exception(f"Main API error: {response.status_code} - {response.text}")
            
    except requests.exceptions.RequestException as e:
        raise Exception(f"Main API request failed: {e}")