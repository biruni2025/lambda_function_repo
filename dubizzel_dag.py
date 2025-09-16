import pandas as pd
import requests
import json
import boto3
import os
import urllib.parse
import re
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






def encode_dubizzle_url(raw_url: str) -> str:
    parsed = urllib.parse.urlsplit(raw_url)

    # Fix missing dash before ID
    fixed_path = re.sub(r'(?<!-)(ID\d+)', r'-\1', parsed.path)

    # Encode only the path part (keep safe ASCII characters intact)
    encoded_path = urllib.parse.quote(
        fixed_path,
        safe="/.-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    )

    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, encoded_path, parsed.query, parsed.fragment))




def transform_ads(ads):
    transformed = []
    BASE_URL = "https://www.dubizzle.sa/en/ad/"  # adjust if different
    
    for ad in ads:
        # Maker, model, year
        maker = None
        model = None
        id = f"dubizzle_{ad.get('id')}"
        for f in ad.get("formattedExtraFields", []):
            if f.get("attribute") == "make":
                maker = f.get("formattedValue_l1").lower().replace("-","").replace(" ","") or f.get("formattedValue").lower().replace("-","").replace(" ","")
            elif f.get("attribute") == "model":
                model = f.get("formattedValue_l1").lower().replace("-","").replace(" ","").replace(maker,'') or f.get("formattedValue").lower().replace("-","").replace(" ","").replace(maker,'')

        year = ad.get("extraFields", {}).get("year")
        price = ad.get("extraFields", {}).get("price")
        mileage = ad.get("extraFields", {}).get("mileage")

        # Pics
        pics = []
        for p in ad.get("photos", []):
            photo_id = p['id']
            photo_url = f"https://images.dubizzle.sa/thumbnails/{photo_id}-800x600.webp"
            pics.append(photo_url)
        # Ad URL
        slug = ad.get("slug")
        external_id = ad.get("externalID")
        ad_url = f"{BASE_URL}{slug}ID{external_id}.html"

        transformed.append({
            "id" : id ,
            "maker": maker,
            "model": model,
            "year": year,
            "price": price,
            "mileage": mileage,
            "pics": pics,
            "ad_url": encode_dubizzle_url(ad_url)
        })
    
    return transformed




def extract_dubizzel_data():
    url = "https://search.mena.sector.run/_msearch"

    # Query params
    params = {
        "filter_path": ",".join([
            "took", "*.took", "*.timed_out", "*.suggest.*.options.text",
            "*.suggest.*.options._source.*", "*.hits.total.*", "*.hits.hits._source.*",
            "*.hits.hits._score", "*.hits.hits.highlight.*", "*.error",
            "*.aggregations.*.buckets.key", "*.aggregations.*.buckets.doc_count",
            "*.aggregations.*.buckets.complex_value.hits.hits._source",
            "*.aggregations.*.filtered_agg.facet.buckets.key",
            "*.aggregations.*.filtered_agg.facet.buckets.doc_count",
            "*.aggregations.*.filtered_agg.facet.buckets.complex_value.hits.hits._source"
        ])
    }

    # Headers
    headers = {
        "accept": "*/*",
        "accept-language": "ar-EG,ar;q=0.9,en-US;q=0.8,en;q=0.7",
        "authorization": "Basic b2x4LXNhLXByb2R1Y3Rpb24tc2VhcmNoOkhqOSFyOTM1KUtBQ1dpcT5KKytLV0VFWX1ucTc1SH1B",
        "content-type": "application/x-ndjson",
        "origin": "https://www.dubizzle.sa",
        "referer": "https://www.dubizzle.sa/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    }

    # NDJSON body with modified query: only get used cars
    ndjson_body = (
        '{"index":"olx-sa-production-ads-ar"}\n'
        '{"from":0,"size":10000,"track_total_hits":true,'
        '"query":{"bool":{"must":['
            '{"term":{"category.slug":"cars-for-sale"}},'
            '{"term":{"extraFields.new_used":"used"}},'
            '{"term":{"location.externalID":"0-1"}}]}},'
        '"sort":[{"timestamp":{"order":"desc"}}],'
        '"timeout":"5s"}\n'
    )


    # Send POST request
    response = requests.post(url, headers=headers, params=params, data=ndjson_body)

    # Handle response
    if response.status_code == 200:
        data = response.json()
        # Extract all _source documents
        sources = []
        for res in data.get("responses", []):
            for hit in res.get("hits", {}).get("hits", []):
                sources.append(hit.get("_source", {}))
        # Print in pretty format
        # print(json.dumps(sources, indent=2, ensure_ascii=False))
    else:
        print("Request failed with status:", response.status_code)
        print(response.text)


    return sources


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
    raw_data = extract_dubizzel_data()
    transformed_data = transform_ads(raw_data)
    return transformed_data




daily_data = main()
new_added_ADS = append_daily_data(daily_data)
append_daily_ads("Dubizzel", new_added_ADS)