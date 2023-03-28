import re
import calendar
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from prefect import flow, task
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import storage
from bigquery_ingestion import bigquery_flow
from prefect.tasks import task_input_hash
import os
import shutil

@task()
def get_url(year:int, month:int) -> list :
    """Takes year and month as input to construct required dataset url. 
    The datasets are rendered in Javascript and therefore needs to be parsed seperately."""
    valid_months=list(range(1,13))
    valid_years= list(range(2016,2023))
    
    if year not in valid_years:
        raise ValueError('Year must be between 2016 and 2022')
    elif month not in valid_months:
        raise ValueError('Month must be between 1 and 12')
    else:   
        home_url='https://cycling.data.tfl.gov.uk/'

        #Parses dataset url.
        js_url='https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/'
        resp=requests.get(js_url)
        soup=BeautifulSoup(resp.text,'html.parser')
        valid_list=[key.get_text() for key in soup.find_all("key")]

        #Filters the list for CSV files with the required month and year inputs.
        eff_month= calendar.month_name[month][0:3]
        searchstr= f'usage-stats/.*{eff_month}.*{year}.*csv$'
        url_list=[home_url+path for path in valid_list if re.match(searchstr,path)]
        
        #handles occurence of empty list
        if url_list:
            return url_list
        else:
            raise IndexError('No dataset for the month and year provided')


@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_dataframe(url_list: list) -> pd.DataFrame:
    """This function creates a dataframe by collecting data from a collection of URLs, 
    combines multiple dataframes into a single one, and produces a final dataframe as the output. 
    To accommodate differences in the datasets' schema for certain years, 
    the function is specifically designed to manage such variations"""
    
    storage_options = {'User-Agent': 'Mozilla/5.0'}
    #Creates dictionary of new column names
    column_names={'Rental Id' : 'rental_id', 'Number': 'rental_id', 'Bike number':'bike_id', 'Bike Id': 'bike_id', 
                   'End Date': 'dropoff_datetime','End date': 'dropoff_datetime','EndStation Id':'dostation_id',
                   'End station number': 'dostation_id','EndStation Name': 'do_station','End station': 'do_station',
                   'Start Date': 'pickup_datetime', 'Start date': 'pickup_datetime','StartStation Id': 'pustation_id',
                   'Start station number':'pustation_id','StartStation Name': 'pu_station','Start station':'pu_station'}
    
    df_list=[]
    for link in url_list:
        try:
            df_list.append(pd.read_csv(str(link), 
                                       storage_options=storage_options)[['Rental Id','Bike Id', 'Start Date', 'StartStation Id', 
                                                                         'StartStation Name','End Date', 'EndStation Id',
                                                                         'EndStation Name']].rename(columns=column_names))
        #Handles Variation in schema of datasets    
        except KeyError:
            try:
                df_list.append(pd.read_csv(str(link), storage_options=storage_options)[['Number', 'Bike number', 'Start date', 
                                                                                    'Start station number', 'Start station',
                                                                                   'End date', 'End station number', 
                                                                                    'End station']].rename(columns=column_names))
            except Exception as e:
                print(e)
                
    #Combines datasets into single dataframe      
    df=pd.concat(df_list, ignore_index=True)

    #Changes id to string
    df['pustation_id']=df['pustation_id'].astype('string')
    df['dostation_id']=df['dostation_id'].astype('string')
    
    return df

@task()
def write_local(df: pd.DataFrame,file_name):
    """Writes dataframe to local storage"""
    #Creates directory
    try:
        os.mkdir('cycling_datasets')
    except:
        pass
    path=f'cycling_datasets/tfl_tripdata_{file_name}.parquet'
    df['pustation_id']=df['pustation_id'].astype('string')
    df['dostation_id']=df['dostation_id'].astype('string')
    df.to_parquet(Path(path), compression="gzip")

    return path

@task()
def write_gcs(path) -> None:
    """"Uploads local file to GCS. Verifies if dataset doesn't exist in lake before upload"""

    client = storage.Client.from_service_account_json('/root/secrets/gcp_credentials.json')
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(path)
    exists=blob.exists()
    if not blob.exists():
        #Createa Prefect Credential Block
        credentials_block = GcpCredentials(
        service_account_file='/root/secrets/gcp_credentials.json')

        #Creates Prefect storage Bucket Block
        gcp_block = GcsBucket(
        gcp_credentials=credentials_block,
        bucket=bucket_name
        )
        gcp_block.upload_from_path(
            from_path=Path(path), to_path=path)
        
        return exists
    else:
        print('Dataset for specified month and year already exists in data lake')
        return exists

@flow()
def etl_flow(year:int,month:int):
    url=get_url(year,month)
    data=get_dataframe(url)
    file_name=f'{year}-{month:02}'
    path=write_local(data,file_name)
    blob_exists=write_gcs(path)

    #Checks before inserting data into bigquery
    if not blob_exists:
        bigquery_flow(year,month,table_name)
    else:
        print('No data inserted into table')

    #deletes preiously created directory and all files
    shutil.rmtree('cycling_datasets')

    return

@flow()
def main_flow(years:list, months: list,bucket_name:str):
    for year in years:
        for month in months:
            etl_flow(year,month)

    return


if __name__=='__main__':
    months=list(range(1,13))
    years=[2022]
    bucket_name='tfl-cycle-trips'
    table_name='tfl'
    main_flow(years,months,bucket_name)
