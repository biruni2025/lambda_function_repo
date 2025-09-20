import requests
import json
import re
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
from datetime import datetime




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


def get_page_ads(page):
  url = "https://hy5cogiue4fnk2d0p.a1.typesense.net/multi_search"

  payload = (
        f'{{"searches":[{{'
        f'"preset":"default-search",'
        f'"per_page":21,'
        f'"exclude_fields":"searchVector,_vector_distance",'
        f'"collection":"products_production-sa",'
        f'"q":"*",'
        f'"filter_by":"categoryId:=[`655dbd20c60dda003ea4bf2f`] && sellPrice:=[0..1000000]",'
        f'"page":{page}'
        f'}}]}}'
    )
  headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'text/plain',
    'origin': 'https://soum.sa',
    'priority': 'u=1, i',
    'referer': 'https://soum.sa/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-typesense-api-key': '9RPwj65iRtNHpqQqARO94Wx2lWEEOCWS'
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  ads = response.json()['results'][0]['hits']
  return ads





def get_last_page():
    url = "https://hy5cogiue4fnk2d0p.a1.typesense.net/multi_search"

    payload = "{\"searches\":[{\"preset\":\"default-search\",\"per_page\":21,\"exclude_fields\":\"searchVector,_vector_distance\",\"collection\":\"products_production-sa\",\"q\":\"*\",\"filter_by\":\"categoryId:=[`655dbd20c60dda003ea4bf2f`] && sellPrice:=[0..1000000]\",\"page\":4}]}"
    headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'text/plain',
    'origin': 'https://soum.sa',
    'priority': 'u=1, i',
    'referer': 'https://soum.sa/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-typesense-api-key': '9RPwj65iRtNHpqQqARO94Wx2lWEEOCWS'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    num_of_ads = response.json()['results'][0]['found']
    number_of_pages =math.ceil(num_of_ads / 21)
    return number_of_pages




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



def transform_soum_ads(raw_ads):
    transformed = []

    for item in raw_ads:
        doc = item.get("document", {})

        id = doc.get("id") or doc.get("productId")
        Body_Style = doc.get('Body Style')
        maker = doc.get("brandName").lower().replace("-","").replace(" ","")
        model =doc.get("Car Model").lower().replace("-","").replace(" ","").replace(maker,'')
        year =  int(doc.get("Year"))
        price = doc.get("grandTotal")
        try :
            raw_Mileage = doc.get("Mileage")
            mileage_parts = raw_Mileage.replace(',','').replace("KM",'').split('-')
            mileage = (int(mileage_parts[0])+int(mileage_parts[1])) /2 
        except :
            continue 
            

        pics = doc.get("product_images", [])
        ad_url = f'https://soum.sa/en/product/{model}-{model}-{raw_Mileage.replace(" ",'')}-{year}-{Body_Style}-{id}'


        transformed.append({
            "id": f"Soum_{id}",
            "maker": maker,
            "model": model,
            "year":year,
            "price": price,
            "mileage": mileage,
            "pics": pics,
            "ad_url": ad_url
        })

    return transformed



def Data_extraction() :
    all_ads = []
    last_page = get_last_page()
    for page in range(1,last_page+1):
        page_ads = get_page_ads(page)
        all_ads.extend(page_ads)
        print(f"page : {page} is done ")
    return all_ads




def main():
    Daily_data = Data_extraction()
    transformed = transform_soum_ads(Daily_data)
    return transformed





Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("soum", new_added_ADS)