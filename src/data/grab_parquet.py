from minio import Minio
import urllib.request
import pandas as pd
import sys
import os
import datetime

def main():
    #grab_data()
    grab_last_mounth()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    destination_directory = "../../data/raw/"

    years = [2018, 2019, 2020, 2021, 2022, 2023]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]


    for year in years:
        for month in months:
        
            filename = f"yellow_tripdata_{year}-{month}.parquet"
            
            file_url = base_url + filename
            
            destination_path = destination_directory + filename
            
            try:
                urllib.request.urlretrieve(file_url, destination_path)
                print(f"Le fichier {filename} a été téléchargé avec succès.")
            except Exception as e:
                print(f"Erreur lors du téléchargement de {filename}: {str(e)}")


def grab_last_mounth() -> None: 

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    destination_directory = "../../data/raw/"

    current_date = datetime.datetime.now()
    current_year = current_date.year
    current_month = current_date.month - 1
    
    filename = f"yellow_tripdata_{current_year}-{current_month}.parquet"

    destination_path = destination_directory + filename
    file_url = base_url + filename

    try: 
        urllib.request.urlretrieve(file_url, destination_path)
        print(f"Le fichier {filename} a été téléchargé avec succès.")
    except Exception as e:
        print(f"Erreur lors du téléchargement de {filename}: {str(e)}")



def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "new-york-yellow-taxi-bucket"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Le fichier {bucket} a été créer avec succès")
    else:
        print("Bucket " + bucket + " existe déjà")

    directory = "../../data/raw/"
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'rb') as file_data:
                file_stat = os.stat(file_path)
                client.put_object(
                    bucket_name=bucket,
                    object_name=filename,
                    data=file_data,
                    length=file_stat.st_size,
                    content_type='application/octet-stream'
                )
            print(f"{filename} téléchargé vers MinIO.")

if __name__ == '__main__':
    sys.exit(main())
