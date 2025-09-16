import requests
import json
import pandas as pd
import math
import json
import boto3
import os
from datetime import datetime

def transform_helloar_cars(cars: list) -> list:
    transformed = []

    for car in cars:
        data = car  # safely get nested data

        car_id = f"Carly_{data.get("helloArCarId")}"
        maker = data.get("brand").lower().replace("-","").replace(" ","").replace("_","")
        # if maker == 'hyundai' :
        #     model = data.get("model").lower().replace("-","").replace(" ","").replace("_","").replace('grandi10','i10')
        model = data.get("model").lower().replace("-","").replace(" ","").replace("_","").replace('grandi10','i10').replace('corrolla','corolla').replace(maker,'')
        year = int(data.get("year"))
        price = data.get("price")
        mileage = data.get("currentKM")  # direct numeric mileage
        pics = data.get("images", []) or []
        seo_id = data.get("seoVehicleId", "")
        ad_url = f"https://halacarly.com/en/vehicle-details/{seo_id}" if seo_id else None

        transformed.append({
            "id": car_id,
            "maker": maker,
            "model": model,
            "year": year,
            "price": price,
            "mileage": mileage,
            "pics": pics,
            "ad_url": ad_url
        })

    return transformed




def get_carly_data(page_number):
    url = "https://www.halacarly.com/services/api/cars/buy-cars"

    payload = json.dumps({
    "pageIndex": page_number,
    "pageSize": 27,
    "modelIds": [],
    "carCondition": 2 ,
    "brandIds": [],
    "searchKeyword": "",
    "searchTerm": "",
    "sessionRandomSortingNumber": 98130
    })
    headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en',
    'content-type': 'application/json',
    'origin': 'https://halacarly.com',
    'priority': 'u=1, i',
    'referer': 'https://halacarly.com/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-api-key': 'fx9RHNbBylzOzydarcPUw0qgaAOOKKWYiWHbpZfWg0dGiEy8iBEQzFOjBbiKCN7Trh7zNum69zQL4MZPpfSdl7o5yFFdtgaEQh17dXGgeEYCg2PwQXMf6On7sCO3Jiqy'
    }

    response = requests.request("POST", url, headers=headers, data=payload).json()['data']['dataList']
    seoVehicleIds = [car['seoVehicleId'] for car in response]

    return seoVehicleIds






def get_car_details(slug) :

    url = f"https://www.halacarly.com/services/api/cars/vehicle-details/v2/{slug}"
    # url = f"https://www.halacarly.com/services/api/cars/vehicle-details/v2/{slug}"

    payload = {}
    headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en',
    'origin': 'https://halacarly.com',
    'priority': 'u=1, i',
    'referer': 'https://halacarly.com/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'x-api-key': 'fx9RHNbBylzOzydarcPUw0qgaAOOKKWYiWHbpZfWg0dGiEy8iBEQzFOjBbiKCN7Trh7zNum69zQL4MZPpfSdl7o5yFFdtgaEQh17dXGgeEYCg2PwQXMf6On7sCO3Jiqy'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()['data']






def append_daily_data(daily_data: list[dict]):
    MASTER_CSV = "/Users/ebrahim/4sale/scraping/dubbizal_ksa/S3/Monthly_data/transformed.csv"
    """
    Append daily scraped ads (list of dicts) to master CSV.
    Schema: id, maker, model, year, price, mileage, pics, ad_url
    """

    # Convert input to DataFrame
    daily_df = pd.DataFrame(daily_data)


    # Ensure schema consistency
    expected_cols = ["id", "maker", "model", "year", "price", "mileage", "pics", "ad_url"]
    daily_df = daily_df[expected_cols]

    if os.path.exists(MASTER_CSV):
        # Load existing data
        master_df = pd.read_csv(MASTER_CSV)
        old_num_of_ads = len(master_df)

        # Concatenate
        combined_df = pd.concat([master_df, daily_df], ignore_index=True)

        # Deduplicate by id (latest wins)
        combined_df = combined_df.drop_duplicates(subset=["id"], keep="last")

        combined_df.to_csv(MASTER_CSV, index=False)
        new_num_of_ads = len(combined_df)
    else:
        # If master file doesn't exist yet, just save today's batch
        daily_df.to_csv(MASTER_CSV, index=False)

    print(f"✅ Appended {new_num_of_ads-old_num_of_ads} rows.")
    return new_num_of_ads - old_num_of_ads





def Data_extraction():
    slug_list = []
    for page in range(0,17) :
        slugs = get_carly_data(page)
        slug_list.extend(slugs)
        print(f"page number : {page} has {len(slugs)} slug")

    all_cars = []
    for slug in slug_list :
        car = get_car_details(slug)
        all_cars.append(car)
        print(f"slug : {slug} done")
    return all_cars





def append_daily_ads(source: str, ads_number: int, 
                     csv_path="/Users/ebrahim/4sale/scraping/dubbizal_ksa/S3/Monthly_data/ksa_sources_daily_logs.csv"):
    """
    Append daily ads count to CSV file with schema (Source, Daily_ADS, Date).
    
    :param source: Source name (string)
    :param ads_number: Number of daily ads (int)
    :param csv_path: Path to the CSV file
    """
    
    # Ensure today's date
    today = datetime.today().strftime("%Y-%m-%d")
    
    # Create a new row
    new_row = {"Source": source, "Daily_ADS": ads_number, "Date": today}
    
    # If file exists, append; otherwise create new CSV
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df = pd.DataFrame([new_row])
    
    # Save back to CSV
    df.to_csv(csv_path, index=False)
    print(f"✅ Appended row: {new_row}")



def main():
    Daily_data = Data_extraction()
    transformed = transform_helloar_cars(Daily_data)
    return transformed



Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("carly", new_added_ADS)