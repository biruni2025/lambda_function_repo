import requests
import json
import re
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime



def get_num_of_pages() :

    url = "https://sa.opensooq.com/api/serp/search/v2?Action_screen_name=search&abBucket=8"

    payload = json.dumps({
    "filters": {
        "cat_ids": [
        1729
        ],
        "sub_ids": [
        1731
        ],
        "ConditionUsed": [
        4643
        ]
    },
    "sort_code": "recent",
    "page": 1,
    "vertical_link": "Autos/Cars For Sale"
    })
    headers = {
    'abbucket': '8',
    'accept': '*/*',
    'accept-language': 'en',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdDAiOjE3NTQ5ODg2MjcsImF1ZCI6ImRlc2t0b3AiLCJzdWIiOiIwNzc2MjViZS1iNTI1LTQ0ODItYjk1NC0yYjA2MjgwMGJlMzIiLCJybmQiOiIxMjAxMzMxNDU5MTMxMzEyIiwiZXhwIjoxNzU2ODA2MTIxfQ.okcSAMpSMm5xOWXZUUdOaXTGl-oPjWp4u80HL-eeYyY',
    'cache-control': 'max-age=0',
    'content-type': 'application/json',
    'country': 'sa',
    'currency': 'SAR',
    'origin': 'https://sa.opensooq.com',
    'priority': 'u=1, i',
    'referer': 'https://sa.opensooq.com/en/cars/cars-for-sale?page=1',
    'release-version': '9.4.02',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'session-id': '077625beb525-44826217eca4-c779-4a67-ad1f-65437f26524c',
    'source': 'desktop',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'x-domain-theme': 'opensooq',
    'x-tracking-uuid': '077625be-b525-4482-b954-2b062800be32',
    'Cookie': 'NEXT_LOCALE=en; source=desktop; ecountry=sa; default_currency=SAR; device_uuid=077625be-b525-4482-b954-2b062800be32; userABBucket=8; _ga=GA1.3.1983219268.1754988623; _gcl_au=1.1.1972282456.1754988623; _ga=GA1.1.1983219268.1754988623; auth_v2=%7B%22deviceUUID%22%3A%22077625be-b525-4482-b954-2b062800be32%22%2C%22t%22%3A1754988627%2C%22k%22%3A%2272b5f1a7bfc1828b2a6cfb53d1f9798462f4033f7b775519d6ad257c69762fd3%22%7D; FIREBASE_FCM_TOKEN=FIREBASE_CLICKED_NO; session=%7B%22id%22%3A%22077625beb525-44826217eca4-c779-4a67-ad1f-65437f26524c%22%2C%22startedAt%22%3A1756805742674%7D; _gid=GA1.3.2089377167.1756805744; prevScreen=home%2Csearch; _lastSearch_v5=%7B%22qs%22%3A%7B%22page%22%3A1%2C%22filters%22%3A%7B%22cat_ids%22%3A%5B1729%5D%2C%22sub_ids%22%3A%5B1731%5D%7D%2C%22vertical_link%22%3A%22Autos%2FCars%20For%20Sale%22%2C%22per_page%22%3A30%2C%22sort_code%22%3A%22default%22%7D%2C%22search_key%22%3A%22wYZ76mJrrNLbNqAj2kPJW8mVimXtPPGy4Ka8z81gvR3pEKh%2F6oWgYYp9ABZJVs%2BZkpEsvjdQyboj7HmRbvD7h%2FqCs7wjmGIRS9jgmWv%2FilDSZJbD3a5La%2B22q8ypbS8Rp8f%2F4C66yoQGvgnGXqh85ByY6GYR8c%2BbY3M52Cvk%2B6S4f0hxNcPX2vXNSpJh3zShM%2B%2FwCxkbgHQHVR19h8W03XU1ivk2QObGktOJOca2ypUp9Lgu6hkeIGsKjQS1fXztZPGM0Nn%2BR8oZuJc%2FKAlKdwK0wDQ2N1Oyneg%2F9ahQIXpj3HjG%2BDXVk5GZDnFL3WrY%22%7D; _ga_GXHH539B0K=GS2.1.s1756805744$o5$g1$t1756805782$j22$l0$h0; FCNEC=%5B%5B%22AKsRol_z85Zge_N0QMeSVVM0fT1QF_B4P6014wKqf9U7sx3NPgDzNc5IBZsjfEIj5EoK2b_vo79jifp7evuvsOKejfyMHGhPUCd-yLfeDScO8gGfAVhobdHji2LPp0qHIlvy4ZhFnQbyzCHGzROOocjdjJwY9aX-YA%3D%3D%22%5D%5D; prevWidget=filters; prevAction=%7B%22L1%22%3A%22search%22%2C%22L2%22%3A%22filters%22%2C%22L3%22%3A%22click%22%7D'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    pages = response.json()['listings']['meta']['pages']
    return pages




def get_page_ads(page):

  url = "https://sa.opensooq.com/api/serp/search/v2?Action_screen_name=search&abBucket=8"

  payload = json.dumps({
    "filters": {
      "cat_ids": [
        1729
      ],
      "sub_ids": [
        1731
      ],
      "ConditionUsed": [
        4643
      ]
    },
    "sort_code": "recent",
    "page": page,
    "vertical_link": "Autos/Cars For Sale"
  })
  headers = {
    'abbucket': '8',
    'accept': '*/*',
    'accept-language': 'en',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdDAiOjE3NTQ5ODg2MjcsImF1ZCI6ImRlc2t0b3AiLCJzdWIiOiIwNzc2MjViZS1iNTI1LTQ0ODItYjk1NC0yYjA2MjgwMGJlMzIiLCJybmQiOiIxMjAxMzMxNDU5MTMxMzEyIiwiZXhwIjoxNzU2ODA2MTIxfQ.okcSAMpSMm5xOWXZUUdOaXTGl-oPjWp4u80HL-eeYyY',
    'cache-control': 'max-age=0',
    'content-type': 'application/json',
    'country': 'sa',
    'currency': 'SAR',
    'origin': 'https://sa.opensooq.com',
    'priority': 'u=1, i',
    'referer': 'https://sa.opensooq.com/en/cars/cars-for-sale?page=1',
    'release-version': '9.4.02',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'session-id': '077625beb525-44826217eca4-c779-4a67-ad1f-65437f26524c',
    'source': 'desktop',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'x-domain-theme': 'opensooq',
    'x-tracking-uuid': '077625be-b525-4482-b954-2b062800be32',
    'Cookie': 'NEXT_LOCALE=en; source=desktop; ecountry=sa; default_currency=SAR; device_uuid=077625be-b525-4482-b954-2b062800be32; userABBucket=8; _ga=GA1.3.1983219268.1754988623; _gcl_au=1.1.1972282456.1754988623; _ga=GA1.1.1983219268.1754988623; auth_v2=%7B%22deviceUUID%22%3A%22077625be-b525-4482-b954-2b062800be32%22%2C%22t%22%3A1754988627%2C%22k%22%3A%2272b5f1a7bfc1828b2a6cfb53d1f9798462f4033f7b775519d6ad257c69762fd3%22%7D; FIREBASE_FCM_TOKEN=FIREBASE_CLICKED_NO; session=%7B%22id%22%3A%22077625beb525-44826217eca4-c779-4a67-ad1f-65437f26524c%22%2C%22startedAt%22%3A1756805742674%7D; _gid=GA1.3.2089377167.1756805744; prevScreen=home%2Csearch; _lastSearch_v5=%7B%22qs%22%3A%7B%22page%22%3A1%2C%22filters%22%3A%7B%22cat_ids%22%3A%5B1729%5D%2C%22sub_ids%22%3A%5B1731%5D%7D%2C%22vertical_link%22%3A%22Autos%2FCars%20For%20Sale%22%2C%22per_page%22%3A30%2C%22sort_code%22%3A%22default%22%7D%2C%22search_key%22%3A%22wYZ76mJrrNLbNqAj2kPJW8mVimXtPPGy4Ka8z81gvR3pEKh%2F6oWgYYp9ABZJVs%2BZkpEsvjdQyboj7HmRbvD7h%2FqCs7wjmGIRS9jgmWv%2FilDSZJbD3a5La%2B22q8ypbS8Rp8f%2F4C66yoQGvgnGXqh85ByY6GYR8c%2BbY3M52Cvk%2B6S4f0hxNcPX2vXNSpJh3zShM%2B%2FwCxkbgHQHVR19h8W03XU1ivk2QObGktOJOca2ypUp9Lgu6hkeIGsKjQS1fXztZPGM0Nn%2BR8oZuJc%2FKAlKdwK0wDQ2N1Oyneg%2F9ahQIXpj3HjG%2BDXVk5GZDnFL3WrY%22%7D; _ga_GXHH539B0K=GS2.1.s1756805744$o5$g1$t1756805782$j22$l0$h0; FCNEC=%5B%5B%22AKsRol_z85Zge_N0QMeSVVM0fT1QF_B4P6014wKqf9U7sx3NPgDzNc5IBZsjfEIj5EoK2b_vo79jifp7evuvsOKejfyMHGhPUCd-yLfeDScO8gGfAVhobdHji2LPp0qHIlvy4ZhFnQbyzCHGzROOocjdjJwY9aX-YA%3D%3D%22%5D%5D; prevWidget=filters; prevAction=%7B%22L1%22%3A%22search%22%2C%22L2%22%3A%22filters%22%2C%22L3%22%3A%22click%22%7D'
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  ads = response.json()['listings']['items']
  return ads







def transform_ads(ads):
    BASE_URL = "https://opensooq.com" 
    transformed = []
    
    for ad in ads:



        maker = ad.get("highlightsObject", {}).get("car_make", [{}])[0].get("value").lower().replace("-","").replace(" ","")
        # model = ad.get("highlightsObject", {}).get("car_model", [{}])[0].get("value").lower().replace("-","").replace(" ","")
        year = ad.get("highlightsObject", {}).get("Car_Year", [{}])[0].get("value")
        try :
            if maker == 'lexus' :
                model = ad.get("highlightsObject", {}).get("car_trim", [{}])[0].get("value").lower().replace("-","").replace(" ","")[0:5]
            elif maker == 'chery':
                model = ad.get("highlightsObject", {}).get("car_trim", [{}])[0].get("value").lower().replace("-","").replace(" ","").replace('pro','')
            else :
                model = ad.get("highlightsObject", {}).get("car_model", [{}])[0].get("value").lower().replace("-","").replace(" ","").replace(maker,'')
            
        except :
                model = ad.get("highlightsObject", {}).get("car_model", [{}])[0].get("value").lower().replace("-","").replace(" ","")
            

        # id cleaning
        try :
            id = f"opensooq_{ad.get('id')}"
        except :
            id = None
        
        # price cleaning
        raw_price = ad.get("price_amount", "0").replace(",", "").split()[0]
        try:
            price = int(raw_price)
        except:
            price = None
        
        # mileage extraction
        mileage_obj = ad.get("highlightsObject", {}).get("Kilometers_Cars", [{}])[0]
        mileage_val = mileage_obj.get("value")
        try:
            avg_mileage = int(mileage_val)
        except:
            # fallback: regex from cps or highlights
            mileage_match = re.search(r"([\d,]+)\s*km", " ".join(ad.get("cps", [])))
            avg_mileage = int(mileage_match.group(1).replace(",", "")) if mileage_match else None
        
        # pics (clean full links instead of {size})
        pics = []
        for img in ad.get("images", []):
            pics.append(img.replace("{size}", "1000"))  # pick high resolution
        
        # ad url
        ad_url = BASE_URL + ad.get("post_url", "")
        
        transformed.append({
            "id" : id ,
            "maker": maker,
            "model": model,
            "year": year,
            "price": price,
            "mileage": avg_mileage,
            "pics": pics,
            "ad_url": ad_url
        })
    
    return transformed



def Data_extraction(max_workers: int = 10):
    all_ads = []
    num_of_pages = get_num_of_pages()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # submit all page jobs
        futures = {executor.submit(get_page_ads, page): page for page in range(1, num_of_pages + 1)}

        for future in as_completed(futures):
            page = futures[future]
            try:
                page_ads = future.result()
                all_ads.extend(page_ads)
                print(f"page number : {page} is done")
            except Exception as e:
                print(f"Error fetching page {page}: {e}")

    return all_ads



def main ():
    all_ads = Data_extraction(3)
    transformed = transform_ads(all_ads)
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





Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("opensooq", new_added_ADS)