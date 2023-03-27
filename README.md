# tfl-data-project

[Dashboard](https://public.tableau.com/views/dbtnewb/Dashboard2?:language=en-US&:display_count=n&:origin=viz_share_link)&ensp; &ensp;&ensp;&ensp;&ensp;	[Ingestion_Script](https://github.com/fxboakye/tfl-data-project/blob/master/flows/gcs_ingestion.py)

The Dockerized project is a robust solution that automates the extraction of bicycle trip record datasets from the [Transport for London Website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). By simply providing month and year values, the pipeline retrieves the necessary data and processes it into a convenient parquet file format. Due to the non-uniformity of each dataset link, the pipeline utilizes regex to parse through the website's links and extracts the required datasets for each specified month and year.

The ingestion script has been thoughtfully designed to address a common problem encountered in data management - avoiding duplicates. To prevent this, the script has been programmed to ingest data to Google Cloud Storage (GCS) ***only*** if the data is not already present in the data lake. This guarantees that there are no redundant or duplicate entries in the data lake.

After the data has been successfully saved into the GCS bucket and inserted into the corresponding table in the BigQuery database, additional measures were taken to ensure the integrity of the dataset. This was achieved by leveraging the powerful data transformation capabilities of dbt.

With dbt, essential data operations, including data modeling, testing, and documentation, were incorporated into the data management pipeline. This facilitated the process of ensuring that the data is consistent and accurate, and is suitable for downstream analysis.

Following the successful transformation and verification of the data, it was then saved into a designated warehouse for easy storage and retrieval. At this stage, the data warehouse was then used to create a dynamic dashboard that provides a comprehensive overview of key trends and patterns. Through the dashboard, users can quickly identify important insights such as peak months and peak hours of bike demands, which can be useful for making informed decisions related to bike-sharing services or urban transportation planning. 
