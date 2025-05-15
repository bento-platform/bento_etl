# Bento ETL

Notice: WORK IN PROGRESS

The Bento ETL service aims to organize and automate how Bento platforms can ingest external data in a flexible manner.

For a Bento instance that needs ETL processing, the general flow would be:
1. Define an ETL pipeline config file, local or remote (Details/docs coming)
   1. Configure an Extractor to retrieve the raw external data
   2. Configure a Transformer to convert the raw data to Bento compatible data-types
   3. Configure a Loader to load the transformed data into the appropriate data service 
      (Katsu, Gohan, Reference, Takuan)
2. Deploy a bento_etl service in a Bento stack with the config file (docs coming)
3. Configured ETL pipelines run as triggers/crons
   1. Triggers: On demand ingestions
   2. Crons: Regular sync ingestions

## ETL Pipeline configuration

TODO!

## Dev


