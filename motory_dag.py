from bs4 import BeautifulSoup
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




def transform_motory_items(divs):
    all_cars = []

    for div in divs :
    # ID
        car_id = f"Motory_{div.get("data-key")}"

        # URL + pic
        link_tag = div.select_one(".vehicles-list-item-inner-image a")
        ad_url = link_tag["href"] if link_tag else None
        img_tag = link_tag.find("img") if link_tag else None
        pics = [img_tag["src"]] if img_tag else []

        # Title: e.g. "Suzuki Dzire GL 2025"
        title_tag = div.select_one(".vehicle-name")
        title_text = title_tag.get_text(strip=True) if title_tag else ""
        parts = title_text.split()
        maker = parts[0].lower().replace("-","").replace(" ","") if parts else None
        model = parts[1].lower().replace("-","").replace(" ","").replace('fj','fjcruiser').replace(maker,'')
        if maker == "hyundai" and model == 'grand' :
            model = 'i10'
        # " ".join(parts[1:-1]) if len(parts) > 2 else None
        year = parts[-1] if parts and parts[-1].isdigit() else None
        if year:
            year = int(year)

        # Mileage (e.g. "10 Km")
        mileage_tag = div.select_one(".vehicles-list-item-inner-detail")
        mileage = None
        if mileage_tag:
            mileage_text = mileage_tag.get_text(strip=True)
            if "Km" in mileage_text:
                mileage = int(mileage_text.replace("Km", "").replace(",", "").strip())

        # Price (final discounted one)
        price_tag = div.select_one(".vehicles-list-item-info-price .font-weight-bold.pr-2.font-size-21")
        price = None
        if price_tag:
            price_text = price_tag.get_text(strip=True).replace('SR(VAT Included)','').replace(",",'').replace("SR",'').strip()
            price = int(price_text)
        all_cars.append({
        "id": car_id,
        "maker": maker,
        "model": model,
        "year": year,
        "price": price,
        "mileage": mileage,
        "pics": pics,
        "ad_url": ad_url
    })

    return all_cars



def get_page_cars(page):
  url = f"https://www.motoryshop.com/en/used-cars/?sort=-published_at&page={page}&per-page=16"

  payload = {}
  headers = {
    'accept': '*/*',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://www.motoryshop.com/en/used-cars/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-csrf-token': 'sQGBUh85anT8zOfqLrHAnGQysW3JG2Fu-zKZ4AKLJIPFTNEAL04zN7i8hIJL2rnaPl3bHIxLEzuIXu2ZM74Ttg==',
    'x-pjax': 'true',
    'x-pjax-container': '#id-pjax',
    'x-requested-with': 'XMLHttpRequest',
    'Cookie': 'motory_v3_sv2=79gf9gltcuudh1ec15edcs8g2v; _csrf_shop_motory_v3=aa6dcf66f44da8067b8acc9ddc05a8214c20206dbfe2fc5b90b03b149884eff4a%3A2%3A%7Bi%3A0%3Bs%3A20%3A%22_csrf_shop_motory_v3%22%3Bi%3A1%3Bs%3A32%3A%22tMPR0wYCDpchekyFZojqEPrUslty1575%22%3B%7D; _gcl_au=1.1.1572650269.1754958937; _ga=GA1.1.187111.1754958938; _clck=yt3yc2%7C2%7Cfye%7C0%7C2050; _ga_P3X1PNVYTK=GS2.1.s1754958937$o1$g1$t1754978323$j3$l0$h0; _clsk=9gkxk4%7C1754978328513%7C3%7C1%7Cn.clarity.ms%2Fcollect'
  }

  response = requests.request("GET", url, headers=headers, data=payload)
  html = response.text
  soup = BeautifulSoup(html,'html.parser')
  car_devs = soup.find_all('div',class_='item d-flex col-12 col-md-6 col-lg-4 col-xl-3 mb-4 vehicles-list-item vehicles-list-col') 
  return car_devs




def Data_extraction():
    all_cars = []
    for page in range(1,11) :
        page_cars =get_page_cars(page)
        all_cars.extend(page_cars)
        print(f"page : {page} done")
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
    transformed = transform_motory_items(Daily_data)
    return transformed


Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("motory", new_added_ADS)