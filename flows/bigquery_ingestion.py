from prefect import flow
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse

@flow()
def bigquery_flow(year:int, month: int):
    """Loads data from cloud storage to bigquery"""

    if year not in valid_years:
        raise ValueError('Year must be between 2016 and 2022')
    elif month not in valid_months:
        raise ValueError('Month must be between 1 and 12')
    
    else:
        gcp_credentials = GcpCredentials.load("de-zoomcapmp")
        client = gcp_credentials.get_bigquery_client()
        client.create_dataset("tfldataset", exists_ok=True)

        query=f"LOAD DATA INTO tfldataset.{table_name} FROM FILES \
                            ( format = 'PARQUET', uris = ['gs://tfl-cycle-trips/cycling_datasets/tfl_tripdata_{year}-{month:02}.parquet']);"

        with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
            try:
                warehouse.execute(query)
                print(f'Finished ingestion for {calendar.month[month]}')
            except Exception as e:
                print(e)

                return

@flow()
def main_flow(years: list, months:list,table_name):
    for year in years:
        for month in months:
            bigquery_flow(year,month)

if __name__=='__main__':
    years=[2022]
    months=list(range(1,13))
    table_name='tfl'
    main_flow(years,months, table_name)

