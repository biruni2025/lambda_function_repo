import requests
import json
import requests
import math
import pandas as pd
import requests
import json
import os
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
    return new_num_of_ads-old_num_of_ads


def get_last_page():
    url = "https://api.gogomotor.com/backend-api/opensearch/search"

    payload = json.dumps({
        "languageID": 1,
        "size": 50,
        "page": 1,
        "wildcard": "",
        "filter": [
        {
            "term": {
            "cityid": "1"
            }
        },
        {
            "term": {
            "isnew": False
            }
        },
        {
            "term": {
            "isoutlet": False
            }
        }
        ],
        
        "selectField": [
        "vehiclelistingid",
        "defaultwebimagethumbnailurl",
        "vehicleprofileid",
        "askingprice",
        "listeddate",
        "manufactureyearid",
        "manufactureyear",
        "vehiclemakeid",
        "vehiclemake",
        "vehiclemodelid",
        "vehiclemodel",
        "specid",
        "spec",
        "fueltypeid",
        "fueltype",
        "bodytypeid",
        "exteriorcolorid",
        "interiorcolorid",
        "transmissionid",
        "transmission",
        "ownershipid",
        "ownership",
        "mileage",
        "cityid",
        "monthlyemi",
        "isselflistedvehicle",
        "importerid",
        "distributorid",
        "fulfilledbyid",
        "mfgwarranty",
        "emicalculationdate",
        "sellerid",
        "dealerid",
        "isnew",
        "isoutlet",
        "issold",
        "isggminspected",
        "isactive",
        "isfinanceapplicable",
        "is360degreeapplicable",
        "ismojazfacilityapplicable",
        "isassistedsellingapplicable",
        "isextendedwarrantyapplicable",
        "vehiclefeaturecsv",
        "etag",
        "vehiclelistingstatuskey",
        "specregionid",
        "defaultwebimageurl",
        "currencysymbol",
        "unitofmeasurevalue",
        "listingsummary",
        "dealerlogourl",
        "vehiclelistingstatuskey",
        "type",
        "vehiclemakekey",
        "vehiclemodelkey",
        "productcatalogueid",
        "thumbnailurl",
        "brandslug",
        "modelslug",
        "ispremium"
        ],
        "searchFields": [
        "vehiclemake",
        "vehiclemodel",
        "vehiclemakekey",
        "vehiclemodelkey"
        ],
        "orFilter": []
    })
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.gogomotor.com',
        'priority': 'u=1, i',
        'referer': 'https://www.gogomotor.com/',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    number_of_ads = response.json()['data']['total']  # Parse JSON response
    number_of_pages =math.ceil(number_of_ads / 50)
    return number_of_pages
    









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
  url = "https://api.gogomotor.com/backend-api/opensearch/search"

  payload = json.dumps({
    "languageID": 1,
    "size": 50,
    "page": page,
    "wildcard": "",
    "filter": [
      {
        "term": {
          "cityid": "1"
        }
      },
      {
        "term": {
          "isnew": False
        }
      },
      {
        "term": {
          "isoutlet": False
        }
      }
    ],
    
    "selectField": [
      "vehiclelistingid",
      "defaultwebimagethumbnailurl",
      "vehicleprofileid",
      "askingprice",
      "listeddate",
      "manufactureyearid",
      "manufactureyear",
      "vehiclemakeid",
      "vehiclemake",
      "vehiclemodelid",
      "vehiclemodel",
      "specid",
      "spec",
      "fueltypeid",
      "fueltype",
      "bodytypeid",
      "exteriorcolorid",
      "interiorcolorid",
      "transmissionid",
      "transmission",
      "ownershipid",
      "ownership",
      "mileage",
      "cityid",
      "monthlyemi",
      "isselflistedvehicle",
      "importerid",
      "distributorid",
      "fulfilledbyid",
      "mfgwarranty",
      "emicalculationdate",
      "sellerid",
      "dealerid",
      "isnew",
      "isoutlet",
      "issold",
      "isggminspected",
      "isactive",
      "isfinanceapplicable",
      "is360degreeapplicable",
      "ismojazfacilityapplicable",
      "isassistedsellingapplicable",
      "isextendedwarrantyapplicable",
      "vehiclefeaturecsv",
      "etag",
      "vehiclelistingstatuskey",
      "specregionid",
      "defaultwebimageurl",
      "currencysymbol",
      "unitofmeasurevalue",
      "listingsummary",
      "dealerlogourl",
      "vehiclelistingstatuskey",
      "type",
      "vehiclemakekey",
      "vehiclemodelkey",
      "productcatalogueid",
      "thumbnailurl",
      "brandslug",
      "modelslug",
      "ispremium"
    ],
    "searchFields": [
      "vehiclemake",
      "vehiclemodel",
      "vehiclemakekey",
      "vehiclemodelkey"
    ],
    "orFilter": []
  })
  headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://www.gogomotor.com',
    'priority': 'u=1, i',
    'referer': 'https://www.gogomotor.com/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  data = response.json()['data']['results']  # Parse JSON response
  return data




def transform_listings(data):
    base_url="https://www.gogomotor.com/en/car-details/"
    
    transformed = []

    for item in data:
        id = item.get("vehiclelistingid")
        spec = item.get("spec").lower().replace("-","").replace(" ","")
        maker = item.get("vehiclemake").lower().replace("-","").replace(" ","")
        model = item.get("vehiclemodel").lower().replace("-","").replace(" ","").replace(maker,'')
        year = item.get("manufactureyear") or item.get("listingsummary").split()[0]
        price = item.get("askingprice")
        mileage = item.get("mileage")
        pics = [item.get("defaultwebimageurl")] if item.get("defaultwebimageurl") else []
        ad_url = f"{base_url}/{maker}-{model}-{spec}-{id}" if base_url else None
        doc = {
            "id": f"Gogomotors_{id}" ,
            "maker": maker,
            "model": model,
            "year": int(year),
            "price": int(price),
            "mileage": int(mileage),
            "pics": pics,
            "ad_url": ad_url
        }
        transformed.append(doc)

    return transformed





def Data_extraction():
    all_ads = []
    last_page =get_last_page()
    for page in range(1,last_page +1):
        page_ads = get_page_ads(page)
        all_ads.extend(page_ads)
        print(f"page : {page} done with : {len(page_ads)} Ad")
    return all_ads



def main():
    Daily_data = Data_extraction()
    Transformed_data = transform_listings(Daily_data)
    return Transformed_data



Daily_data = main()
new_added_ADS = append_daily_data(Daily_data)
append_daily_ads("gogomotor", new_added_ADS)