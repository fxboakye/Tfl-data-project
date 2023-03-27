from prefect import flow
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
import calendar

@flow()
def bigquery_flow(year:int, month: int,table_name):
    """Loads data from cloud storage to bigquery"""
    valid_months=list(range(1,13))
    valid_years= list(range(2016,2023))
    if year not in valid_years:
        raise ValueError('Year must be between 2016 and 2022')
    elif month not in valid_months:
        raise ValueError('Month must be between 1 and 12')
    
    else:

        gcp_credentials = GcpCredentials(service_account_file='/root/secrets/gcp_credentials.json',
                                          project='challlenge-lab-376801')
        client = gcp_credentials.get_bigquery_client()
        client.create_dataset("tfldataset", exists_ok=True)

        query=f"LOAD DATA INTO tfldataset.{table_name} FROM FILES \
                            ( format = 'PARQUET', uris = ['gs://tfl-cycle-trips/cycling_datasets/tfl_tripdata_{year}-{month:02}.parquet']);"

        with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
            try:
                warehouse.execute(query)
                print(f'Finished ingestion for {calendar.month_name[month]}')
            except Exception as e:
                print(e)

                return

if __name__=='__main__':
    bigquery_flow(year,month,table_name)

