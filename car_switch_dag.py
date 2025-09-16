import requests
from bs4 import BeautifulSoup
import math
import pandas as pd
import requests
import json
import boto3
import os
import urllib.parse
import re
from datetime import datetime



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




def get_number_of_pages () :

    url = "https://ksa.carswitch.com/en/saudi/used-cars/search"

    payload = {}
    headers = {
    'Cookie': 'x-client=other; x-country=ksa; x-language=en'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    html = response.text
    soup = BeautifulSoup(html,'html.parser')
    text = soup.find('h1',class_='md:text-[32px] text-2xl font-extrabold text-label-black').get_text()
    number_of_ads = int(text.replace("Used cars for sale in KSA",""))
    number_of_pages =math.ceil(number_of_ads / 250)
    return number_of_pages





def get_car_switch(page):

    url = f"https://hd7x32pwz5l1k9frp-1.a1.typesense.net/collections/cars_prod/documents/search?q=*&query_by=makeName,modelName&filter_by=countryName:%3D%22ksa%22&per_page=250&page={page}&sort_by=firstPublishedOn:asc,+updatedAt:desc&x-typesense-api-key=Tv1qKAFwcLU5hFb3W2Y2u4Xirp3IG6Ld"

    payload = {}
    headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://ksa.carswitch.com',
    'priority': 'u=1, i',
    'referer': 'https://ksa.carswitch.com/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()['hits']
    return data






def transform_ads(json_list: list) -> list:
    """
    Transform list of JSON objects into schema:
    id, maker, model, year, price, mileage, pics, ad_url
    """
    records = []
    for item in json_list:
        doc = item.get("document", {})
        maker = doc.get("makeName").lower().replace("-","").replace(" ","")
        if maker == 'lexus' :
            model =doc.get("modelName").lower().replace("-","").replace(" ","")[0:5]
        elif maker == 'chery' :
            model =doc.get("modelName").lower().replace("-","").replace(" ","").replace('pro','')
        else:
            model =doc.get("modelName").lower().replace("-","").replace(" ","").replace(maker,'')
        record = {
            "id": f"CarSwitch_{doc.get("id")}" ,
            "maker": maker ,
            "model": model,
            "year": doc.get("year"),
            "price": doc.get("price"),
            "mileage": doc.get("mileage"),
            "pics": doc.get("coverImage"),
            "ad_url": f"https://ksa.carswitch.com/en/{doc.get("cityName")}/used-car/{doc.get("makeName")}/{doc.get("modelName")}/{doc.get("year")}/{doc.get('id')}"
        }
        records.append(record)
    
    return records




def Data_extraction():
    number_of_pages = get_number_of_pages()
    cars_list = []
    for i in range(1,number_of_pages+1) :
        cars = get_car_switch(i)
        cars_list.extend(cars)
        print(f"page {i} fetched with {len(cars)} cars ")
    return cars_list




def main():
    cars_list = Data_extraction()
    transformed_cars = transform_ads(cars_list)
    return transformed_cars



Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("car_switch", new_added_ADS)