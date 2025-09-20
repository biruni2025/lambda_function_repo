import requests
import json
import json
import re
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
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



def Data_extraction():

  url = "https://admin.vwcertified.me/graphql"

  payload = "{\"query\":\"query cars($region: ID, $branch: String, $model: String, $year: Int, $colorGroup: String, $isElectric: Boolean, $hasDiscount: Boolean, $hasOffer: Boolean, $hasDiscountOffer: Boolean, $orderBy: [QueryCarsOrderByOrderByClause!], $page: Int, $first: Int!) {\\n  cars(\\n    region: $region\\n    branch: $branch\\n    model: $model\\n    year: $year\\n    colorGroup: $colorGroup\\n    isElectric: $isElectric\\n    hasDiscount: $hasDiscount\\n    hasOffer: $hasOffer\\n    hasDiscountOffer: $hasDiscountOffer\\n    orderBy: $orderBy\\n    page: $page\\n    first: $first\\n  ) {\\n    data {\\n      id\\n      vin\\n      name\\n      title\\n      year\\n      mileage\\n      exterior_text\\n      price\\n      discount\\n      monthly\\n      discount_monthly\\n      hasPrice\\n      hasMonthly\\n      features {\\n        english\\n        arabic\\n        __typename\\n      }\\n      images\\n      impel_image\\n      branch\\n      status\\n      isElectric\\n      hasDiscount\\n      hasOffer\\n      __typename\\n    }\\n    paginatorInfo {\\n      count\\n      total\\n      currentPage\\n      lastPage\\n      __typename\\n    }\\n    __typename\\n  }\\n  carModels(region: $region) {\\n    name\\n    __typename\\n  }\\n  carYears(region: $region) {\\n    year\\n    __typename\\n  }\\n  carColors(region: $region) {\\n    exterior_text\\n    __typename\\n  }\\n}\",\"variables\":{\"region\":\"5\",\"isElectric\":false,\"hasDiscount\":false,\"hasOffer\":false,\"hasDiscountOffer\":false,\"orderBy\":[{\"column\":\"LISTED_AT\",\"order\":\"DESC\"}],\"page\":1,\"first\":100}}"
  headers = {
    'Accept-Language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Origin': 'https://vwcertified.me',
    'Referer': 'https://vwcertified.me/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'accept': '*/*',
    'content-type': 'application/json',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"'
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  cars = response.json()['data']['cars']['data']
  return cars



def transform_ads(cars):
    transformed = []
    for car in cars:
        id = car.get("id")
        maker = 'volkswagen'
        model = car.get("name").lower().replace("-","").replace(" ","")
        year = car.get("year")
        price = car.get("discount") if car.get("discount") else car.get("price")
        mileage =car.get("mileage")
        pics = car.get("images") if car.get("images") else [car.get("impel_image")] if car.get("impel_image") else []
        ad_url = f"https://vwcertified.me/en/saudiarabia/cars/{model}/{year}/{id}"

        transformed.append({
            "id":f"Volkswagen_{id}",
            "maker": maker,
            "model": model ,
            "year": year,
            "price": price,
            "mileage": mileage,
            "pics": pics,
            "ad_url": ad_url # <-- replace with real ad base url
            
        })
    return transformed



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
    Daily_data = Data_extraction()
    print("Data_extraction done")
    transformed = transform_ads(Daily_data)

    return transformed


Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("volkeswagen", new_added_ADS)