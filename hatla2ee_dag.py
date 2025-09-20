import requests

from bs4 import BeautifulSoup
import requests
import json
import re
from datetime import datetime
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
def get_number_of_pages():
    url = "https://ksa.hatla2ee.com/en/car/page/1"

    payload = {}
    headers = {
    'Cookie': 'symfony=9336ede5dd9c38968c555e21ca1f376e'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    html = response.text
    soup = BeautifulSoup(html , 'html.parser')
    last_page = int(soup.find('div',class_= 'pagination pagination-right').find_all('li')[-2].get_text())
    return last_page



def get_page_cars(page):
  url = f"https://ksa.hatla2ee.com/en/car/page/{page}"

  payload = {}
  headers = {
    'Cookie': 'symfony=9336ede5dd9c38968c555e21ca1f376e'
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  html = response.text
  soup = BeautifulSoup(html , 'html.parser')
  devs = soup.find_all('div',class_ = 'newCarListUnit_wrap')
  return devs




def Data_extraction():
    last_page = get_number_of_pages()
    print(f" num of pages = {last_page} pages")
    all_cars = []
    for page in range(1,last_page+1):
        page_cars = get_page_cars(page)
        all_cars.extend(page_cars)
        print(f"page : {page} done")
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




def transform_cars_data(divs):
    all_cars = []
    for div in divs :
        imgs = div.find('img')['data-original']
        BASE_URL = "https://ksa.hatla2ee.com/"  # change to real site base
        # Extract ID from the ad link
        link_tag = div.select_one(".newCarListUnit_header a")
        ad_url = BASE_URL + link_tag["href"] if link_tag else None
        ad_id = f"Hatla2ee_{link_tag["href"].split("/")[-1]}" if link_tag else None

        # Maker & model from meta links
        maker_tag = div.select_one(".newCarListUnit_metaTags a[href*='/en/car/']")
        maker = maker_tag.text.strip().lower().replace("-","").replace(" ","") if maker_tag else None
        model = link_tag["href"].split("/")[-2].lower().replace("-","").replace(" ","").replace(maker,'') if link_tag else None

        # Year from header text
        year = None
        if link_tag and link_tag.text:
            year = int(link_tag.text.split(" ")[-1])

        # Mileage
        try :
            mileage_tag = div.find_all('span',class_="newCarListUnit_metaTag")
            mileage = int(mileage_tag[-1].get_text(strip=True).replace("Km",'').replace(",",''))
            # if mileage_tag:
            #     mileage_text = mileage_tag.get_text(strip=True)
            #     if "Km" in mileage_text:
            #         mileage = int(mileage_text.replace("Km", "").replace(",", "").strip())
        except :
            
            continue

        # Price
        try :
            price_tag = div.select_one(".main_price a")
            price = None
            if price_tag:
                price_text = price_tag.get_text(strip=True).split()[0]
                price = int(price_text.replace(",", "").replace("SAR", "").strip())
        except :
            
            continue

        # Pictures (not in snippet, but if available, grab from <img>)
        all_cars.append({
        "id": ad_id,
        "maker": maker,
        "model": model,
        "year": year,
        "price": price,
        "mileage": mileage,
        "pics": imgs,
        "ad_url": ad_url
    })

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
    Daily_data = Data_extraction()
    transformed = transform_cars_data(Daily_data)
    return transformed




Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("hatla2ee", new_added_ADS)