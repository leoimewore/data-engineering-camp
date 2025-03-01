import os
import wget
from google.cloud import storage
import gunzip

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "$"

client = storage.Client()

buckets = list(client.list_buckets())
print(buckets)


#fhv_tripdata_2019-01.csv.gz

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Uploads a file to GCS."""
    bucket = client.bucket(bucket_name)  # Get bucket reference
    blob = bucket.blob(destination_blob)  # Create blob reference

    blob.upload_from_filename(source_file)  # Upload file
    print(f"File {source_file} uploaded to {destination_blob} in {bucket_name}")

def extract_file():
    for i in range(1,13):
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"

        ch = "0" +str(i)
        if len(ch) <= 2:
            print(f'0{i}-2019 is currently bein downloaded')

        else:
            ch = ch[1:]

        url = url + ch +".csv.gz"
        gz_file = f'fhv_tripdata_2019-{ch}.csv.gz'
        csv_file= f'fhv_tripdata_2019-{ch}.csv'
        if os.path.exists(csv_file):
            print(f'{csv_file} exists')
        else:
            os.system(f'wget -q {url}')
            os.system(f'gunzip {gz_file}')
        blob_path = f'fhv/{csv_file}'
        upload_to_gcs("dataengineerproject-448203-bucket1", csv_file, blob_path )


extract_file()



















   