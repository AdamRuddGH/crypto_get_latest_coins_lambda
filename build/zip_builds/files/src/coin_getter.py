"""
File where we put all the logic for getting the coins

"""
import logging
from pycoingecko import CoinGeckoAPI
from requests.models import HTTPError
import time
import os
import gzip
from . import utils
from . import s3
from . import notification
from dotenv import load_dotenv
load_dotenv()

def orchestrate_get_coin_upload_to_s3():
    """
    orchetrate the getting of coins, then compressing and uploading to s3
    """

    BUCKET = os.getenv('S3_BUCKET')
    ALL_COINS_PATH = os.getenv('ALL_COINS_PATH')

    date_now = utils.datetime_now()
    date_now_epoch = date_now.format("X")
    date_now_iso = date_now.format("YYYY-MM-DDTHH:mm:ssZ")

    folder_partitions = utils.date_to_partition_path(date_now_iso)
    file_path = f"{ALL_COINS_PATH}/{folder_partitions}/{date_now_epoch}_all_coins.json.gz"

    #compress
    latest_coin_data = gzip.compress(utils.list_of_dicts_to_jsonl(get_latest_coins()).encode())
    response = s3.write_to_storage(data=latest_coin_data,bucket=BUCKET,filename_path=file_path)
    notification.notify(f"New data for all coins added for {date_now} ")
    
    return response

    

def get_latest_coins():
    """
    get the latest snapshot of all coins    
    pulls the following data :
    [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "image": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png?1547033579",
            "current_price": 78250,
            "market_cap": 1477137095399,
            "market_cap_rank": 1,
            "fully_diluted_valuation": 1643254472529,
            "total_volume": 56496999675,
            "high_24h": 84421,
            "low_24h": 78280,
            "price_change_24h": -4754.111110602506,
            "price_change_percentage_24h": -5.72755,
            "market_cap_change_24h": -87340092182.10034,
            "market_cap_change_percentage_24h": -5.5827,
            "circulating_supply": 18877100.0,
            "total_supply": 21000000.0,
            "max_supply": 21000000.0,
            "ath": 93482,
            "ath_change_percentage": -16.29412,
            "ath_date": "2021-11-10T14:24:11.849Z",
            "atl": 72.61,
            "atl_change_percentage": 107668.70898,
            "atl_date": "2013-07-05T00:00:00.000Z",
            "roi": null,
            "last_updated": "2021-11-18T22:59:50.489Z"
        }
    ]
    """
    cg = CoinGeckoAPI()

    coins_cleaned = [] #the cleaned set of coined data
    searching = True
    page = 1
    while searching != False:
        time.sleep(1)
        print(f"page {page}")
        try:
            coin_markets_page = cg.get_coins_markets(vs_currency="aud",per_page=250, page=page)
        except HTTPError as e:
            if e.response.status_code == 429:
                retry_number = retry_number + 1
                print(f"too many requests error. Waiting {61} seconds - {e}")
                time.sleep(61) #wait for this to ease up by waiting for a minute
            else:
                break
        if len(coin_markets_page) == 0:
            searching = False
            break
        else:
            #process coins
            for each_entry in coin_markets_page:
                coin_data = transform_coin_market_data(each_entry)
                if coin_data != None:
                    coins_cleaned.append(coin_data)
            page = page +1
    
    return coins_cleaned
    #get market snapshot
    #paginate through all responses

def transform_coin_market_data(input_coin_details):
    """
    will take the market data for a coin from the cg api and output to a standardised form
    """
    try:
        output_coin_details = {
                "id": input_coin_details["id"],
                "date": input_coin_details["last_updated"],
                "prices": input_coin_details["current_price"],
                "market_caps": input_coin_details["market_cap"],
                "total_volumes": input_coin_details["total_volume"],
                # "age": len(coin_details["total_volumes"])-2
            }
        return output_coin_details
    except Exception as e:
        logging.error(e)
        return None



# print(get_latest_coins())