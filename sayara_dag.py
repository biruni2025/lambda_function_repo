import requests
from bs4 import BeautifulSoup
import math
import pandas as pd
import json
import boto3
import os
import urllib.parse
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


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





def transform_car_data(cars_list):
    # Extract values safely
    all_cars = []
    for car_obj in cars_list :
        car_id = f"Sayara_{car_obj.get("id")}"
        maker = car_obj.get("g4_data_layer", {}).get("post_make").lower().replace("-","").replace(" ","").replace("_","")
        if maker == 'lexus' :
             model = (car_obj.get("g4_data_layer", {}).get("post_model")+car_obj.get("g4_data_layer", {}).get("post_ext")).lower().replace("-","").replace(" ","").replace("_",'').replace("grandi10",'i10')[0:5]
        elif maker == 'chery':
            model = car_obj.get("g4_data_layer", {}).get("post_model").lower().replace("-","").replace(" ","").replace("_",'').replace("pro",'')
        
        else:
            model = car_obj.get("g4_data_layer", {}).get("post_model").lower().replace("-","").replace(" ","").replace("_",'').replace("grandi10",'i10').replace(maker,'')
        year = car_obj.get("year")
        price = car_obj.get("sellingprice", "0").replace(",", "")
        mileage = car_obj.get("g4_data_layer", {}).get("post_mileage", "0").replace(",", "")
        pics = car_obj.get("images_urls", [])
        ad_url = "https://syarah.com" + car_obj.get("product_url", "")

        car_dict = {
            "id": car_id,
            "maker": maker,
            "model": model,
            "year": int(year) if year else None,
            "price": int(price) if price.isdigit() else None,
            "mileage": int(mileage) if mileage.isdigit() else None,
            "pics": pics,
            "ad_url": ad_url,
        }
        all_cars .append(car_dict)
    return all_cars



def get_number_of_pages():

    url = "https://newapi.syarah.com/syarah_v1/en/search/index?ps=1-16&includes=usps,meta_tags&search_data=%7B%22filters%22:%7B%22text%22:%22%22%7D,%22link%22:%22%2Fautos%2Fused-cars%22,%22page%22:187,%22sort%22:%22-first_active_date%22,%22size%22:16,%22new_path%22:true,%22banat%22:%22asdfg%22%7D"

    payload = {}
    headers = {
    'accept': 'application/json',
    'accept-enhancedstatuscodes': '1',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'device': 'web',
    'origin': 'https://syarah.com',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'token': 'JR4iENSB52eTFYnRgmNgtpZXVBf3wHue',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'user-id': 'uid-1754879638298-501'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    number_of_ads=response.json()['data']['products_count']
    number_of_pages =math.ceil(number_of_ads / 16)
    return number_of_pages




def get_page_ads(page):
  url = f"https://newapi.syarah.com/syarah_v1/en/search/index?ps=1-16&includes=usps,meta_tags&search_data=%7B%22filters%22:%7B%22text%22:%22%22%7D,%22link%22:%22%2Fautos%2Fused-cars%22,%22page%22:{page},%22sort%22:%22-first_active_date%22,%22size%22:16,%22new_path%22:true,%22banat%22:%22asdfg%22%7D"

  payload = {}
  headers = {
    'accept': 'application/json',
    'accept-enhancedstatuscodes': '1',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'device': 'web',
    'origin': 'https://syarah.com',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'token': 'JR4iENSB52eTFYnRgmNgtpZXVBf3wHue',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'user-id': 'uid-1754879638298-501'
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  products = response.json()['data']['products']
  return products

def Data_extraction():
    all_cars = []
    number_of_pages = get_number_of_pages()

    for page in range(1, number_of_pages + 1):
        try:
            page_ads = get_page_ads(page)
            all_cars.extend(page_ads)
            print(f"page : {page} done ")
        except Exception as e:
            print(f"❌ Error fetching page {page}: {e}")

    return all_cars





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



def main():
    daily_data = Data_extraction()
    transformed_data = transform_car_data(daily_data)
    return transformed_data
    

Daily_data = main()
len(Daily_data)
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("sayara", new_added_ADS)