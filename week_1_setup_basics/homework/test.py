import gzip
import requests




def main():

    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"

    compressed_file="green_tripdata_2019-10.csv.gz"

    stored_csv_file="green_tripdata_2019-10.csv"

    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(compressed_file, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {compressed_file}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        exit()


    with gzip.open(compressed_file, 'rb') as gz_file:
        with open(stored_csv_file, 'wb') as out_file:
            out_file.write(gz_file.read())
    print(f"Decompressed and stored CSV file at: {stored_csv_file}")


    
    

main()