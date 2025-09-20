import requests
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


def get_sudi_daily_listings(page):

    url = f"https://cars.saudisale.com/en/listings-explore?page={page}&is_viewed=0"

    payload = {}
    headers = {
    'accept': '*/*',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://cars.saudisale.com/en',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'Cookie': '_ga=GA1.1.1897698334.1754750498; XSRF-TOKEN=eyJpdiI6IjkwYStuVmxzR2pHSFdGYzNMcGIvcEE9PSIsInZhbHVlIjoicE1zSjJ4SHJWdFBjUjVha0NsNDhOMzdZVUUybGFjOHg2eW5rTUh1YVpPbit3a1FReEJGelFiTW4yMHBLcDg2djFvT1ZONlhlc09aOEpFYlhkdldHamllQzduZ0R6WVVvK0plMFVxQTZTdG9VUGE3Wmo5cTBma082YS9jVSs3bUYiLCJtYWMiOiI0YjRmNzMxYzBkMjkxMGE1YmYxZWFjNzVlYjFkMDg0M2JmNzYyNzVkNmVlZDIzOWE3YzA0ZmFhYWZmOTgwMzRmIiwidGFnIjoiIn0%3D; saudi_sale_session=eyJpdiI6IkJwODVkUms1ZWVDa003TlhjUTJJT2c9PSIsInZhbHVlIjoiSGEvZ0pscDduc3VkWVprMlFnM044KzB1blNzaUdDclR3K2dSRjg2amQ0U1VCNWVhcG5NUHRzU1ZQQnRHNWNYM0hqVUhvVUpZYjlVVXcwc3A5cFAzOWJXellKYnNEdFpYdnpTZmlDeE81SGlaTCtoUFR5bDhQYkNKcWhCODk1TnoiLCJtYWMiOiJiNDZiMjA3MTJiNWJkMzI1MjMxZTE1NjY5ZTM1YmM1OWY5NGI1YzEyYmFjNjFkM2JmMjg1Nzk2M2Y4MDM0Y2ZkIiwidGFnIjoiIn0%3D; locale=eyJpdiI6IlNxcFVCUkc5MnV5QnBzN0NaRG5RUEE9PSIsInZhbHVlIjoidHpiVXZScmNML3RxVFhJeXFKNG1Iakd3VllaVUZJRXBvcVpoSFZDb0JldnlTd2I3bkgzRHBvK1F1ZmNFWE42OSIsIm1hYyI6IjE0N2JjY2I4NmY2ZjU1MzIwNTEzNWI5ZTAwNGE5MmZlMjVkNjhhMTc1Mjk0NDU5YWE5Yzg0MDY0Yjg1NTU4ZjYiLCJ0YWciOiIifQ%3D%3D; _ga_P14RN5RMQR=GS2.1.s1757289132$o11$g1$t1757289798$j45$l0$h0; XSRF-TOKEN=eyJpdiI6IkRzYi9PQ0pZczRGcFYrY2lmL3BUZHc9PSIsInZhbHVlIjoiYVVJakFXVzNzb2dGbXJsSDU4eUZnck5neU9kQkM4UVY4eElrMXYxYnk4cS9scFNWY2dHOHdyelhBbngwc2cwelo1SXJHQ2ZiR3NZM0VHTWtZUnZxZGdoRjVSQTBML29FY2pOMXN6d01PdEtFQ29PYUpkM1BFalo2Z0laMU1LMjYiLCJtYWMiOiI3YjIwMjRiNDZhZjcyY2EyMjA5MThmMjNkM2QxY2ZlMjc4ZDY2YTNjODdhNjY5YjI2NWZhNzE4OTEzMjY3ODQ5IiwidGFnIjoiIn0%3D; locale=eyJpdiI6Im4yYWJ2RVNQV0pQeitlM3ZKbkF0d3c9PSIsInZhbHVlIjoiS2FTTFBYU1V5SWhlWkZLck15WFRLUG9mRmsvbVVOalpzbDA0SlFxSGozVGVRYjB3eEFoUmN4NFh3N09lRmpueCIsIm1hYyI6ImUzOWVhMjMzZjRmOThjZDU5Zjk4NzQ4NTQ5ZGMyYTgyMTNjNTE4M2RlZTAyOWRjM2QxMjIzZmM0MTFlY2JiMGYiLCJ0YWciOiIifQ%3D%3D; saudi_sale_session=eyJpdiI6IjhheEVMZWNkKzJWZjVHQXpTdGk2N0E9PSIsInZhbHVlIjoiM0lESkZKZms1WW5wS1ZUUWluNFloZSswMW82UXN0L3BZYVhyNGFnemdKbGJqNFlxTkJDWDRqOXl4MnBSSGR5bkl4SVZFZ2UzbW1xWnhUT3BtdGFZSDhBQ3hUR1pjQXNyV0lpVTBwYkt5bU5xZlJDTU95NlNlV085Y1BGWXRxbVciLCJtYWMiOiIzZDQ0NjI4OTUwZjA0YzRiZmRlZDJlYmUyYmRhYWMwYTgyMGUwZTRmYzlkNDJmNTQ1YTdmOTZhNTg5ZTJhOWQ3IiwidGFnIjoiIn0%3D'
    }

    response = requests.request("GET", url, headers=headers, data=payload)


    return response.json()['data']


def Data_extraction():
    all_ads = []
    for page in range(1,3) :
        page_ads = get_sudi_daily_listings(page)
        all_ads.extend(page_ads)
        print(f"page : {page} done ")
    return all_ads




def transform_listings(listings):
    transformed = []

    for car in listings:
        # Extract maker
        maker = car.get("maker", {}).get("name", "NA").lower().replace("-","").replace(" ","")

        # Combine class + model for full model name
        class_name = car.get("class", {}).get("name", "")
        model_name = car.get("model", {}).get("name", "")
        if maker in ['lexus' , 'mercedesbenz'] :
            model = f"{class_name}{model_name}".lower().replace("-","").replace(" ","").replace(maker,'')
        else:
            model = class_name.lower().replace("-","").replace(" ","").replace(maker,'')

        # Extract pictures
        pics = [img.get("image_url") for img in car.get("images", []) if img.get("image_url")]

        record = {
            "id": f"SudiSale_{car.get("id")}",
            "maker": maker,
            "model": model,
            "year": car.get("year"),
            "price": car.get("asking_price"),
            "mileage": car.get("mileage"),
            "pics": pics,
            "ad_url": car.get("url")
        }
        transformed.append(record)

    return transformed




def main():
    Daily_data = Data_extraction()
    transformed = transform_listings(Daily_data)
    return transformed



Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("sudi_sales", new_added_ADS)