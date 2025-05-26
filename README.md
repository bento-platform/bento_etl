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
- Extractors
  - Configure API polling extractor (**WIP**)
  - Configure S3 polling extractor (ROADMAP)
- Transformers
  - Configure a JSON to Phenopackets transformer (**WIP**)
  - Configure a JSON to Experiments transformer (**WIP**)
  - Configure a passthrough transformer (ROADMAP)
  - Configure a CSV to Phenopackets transformer (ROADMAP)
  - Configure a CSV to Experiments transformer (ROADMAP)
  - Configure a VCF to Parquet transformer (ROADMAP)
  - Configure a VCF to VCF transformer (ROADMAP)
- Loaders
  - Configure a Katsu Phenopackets loader (**WIP**)
  - Configure a Katsu Experiments loader (**WIP**)
  - Configure a Gohan VCF loader (ROADMAP)
  - Configure a generic S3 loader (ROADMAP)
  - Configure a generic HTTP loader (ROADMAP)

## Dev

We recommend using docker compose for local dev work:

```
export UID=$(id -u)
docker compose -f docker-compose.dev.yaml up -d
```

You can then open the dev container in VS Code, the repo is mounted at `/etl`.

## OpenAPI docs

FastAPI produces an OpenAPI schema automatically, providing rich API docs.

To access the Swagger UI of a local bento_etl, simply open a browser and visit `localhost:5000/docs`.
