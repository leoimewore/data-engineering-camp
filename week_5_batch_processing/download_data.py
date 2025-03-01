#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
#https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet
import os
from pathlib import Path
import requests

TAXI_TYPE = 'green'
YEAR =2020ls

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"


for MONTH in range(1,13):
    if MONTH < 10:
        FMONTH = f"0{MONTH}"
        print(FMONTH)
    else:
        FMONTH =f"{MONTH}"
       
        

    URL =f"{URL_PREFIX}/{TAXI_TYPE}_tripdata_{YEAR}-{FMONTH}.parquet"

    print(URL)


    LOCAL_PREFIX=f"data/raw/{TAXI_TYPE}/{YEAR}/{FMONTH}"
    LOCAL_FILE= f"{TAXI_TYPE}_tripdata_{YEAR}_{FMONTH}.parquet"
    LOCAL_PATH= f"{LOCAL_PREFIX}/{LOCAL_FILE}"
    
    os.makedirs(LOCAL_PREFIX, exist_ok=True)
    response = requests.get(URL, stream=True)
    with open(LOCAL_PATH, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    print(f'Downloaded {URL} to {LOCAL_PATH}')
        

