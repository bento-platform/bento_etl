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

## ETL Pipelines

ETL pipelines always consist of 3 ordered steps:
1. `Extract`: retrieve data from a source and pass it to the next step
2. `Transform`: transforms the extracted data into the expected format for the next step
3. `Load`: loads transformed data into a Bento data service (Katsu, Gohan eventually)

A pipeline run for ETL is refered to here as a `Job`.

A bento_etl `Job` object defines all three steps of an ETL pipeline. When a `Job` object is submitted, `bento_etl` 
returns the Job's unique ID and runs it in the background.

Jobs can be configured and invoked in two ways:
1. On-demand ad-hoc jobs submission by sending a `Job` object in the body of a POST request at `/jobs`
2. On-demand pre-defined `Job` configuration file names at `/jobs/pipeline/{pipeline_file_name}`
   1. Where `pipeline_file_name` is the name of a `Job` JSON file (without the extension) at `/etl/pipelines`

In both cases, the actual JSON Job definition is the same.

Pre-defined pipelines can be built into images, or mounted as volumes for convenient configuration.

### Extractors

The `Extractor` configuration defines how bento_etl extracts data at the beginning of an ETL pipeline.

#### REST API fetch
The `api-fetch` extractor can be used to fetch data from a REST API endpoint over HTTP(S).

To connect to external APIs, `bento_etl` will sometimes need to be authorized on private endpoints.

We support a simple bearer token authentication configuration when running bento_etl.
```bash
# Set the token value in an environment variable
export EXTRACTOR_BEARER_TOKEN=<BEARER TOKEN VALUE>

# Start bento_etl
docker compose up
```

When this is set, any extractor request will include an `Authorization` header with the value 
`Bearer <EXTRACTOR_BEARER_TOKEN>`.

This method can be used to authenticate to the PCGL Submission service in order to retrieve clinical data.

> [!NOTE]
> This configuration is a short term hack to support extractions on a specific system.
> Future work will be made so that `bento_etl` can retrieve and renew its own temporary bearer tokens.

Example extractor JSON config:
```JSON
{
  "extract_url": "http//localhost:5000/test/data-sources/pheno-clean",
  "type": "api-fetch",
  "http_verb": "GET",
  "expected_status_code": 200
}
```

#### S3 extractor

The `s3` extractor can be used to extract data from an S3 object store.

To configure a `bento_etl` container for S3 access, you must:
1. Mount an AWS Config file at `/.aws/config` in the container
2. Mount an AWS Shared Credentials file at `/.aws/credentials` in the container
3. Set environement variables:
   1. `AWS_CONFIG_FILE=/.aws/config`
   2. `AWS_SHARED_CREDENTIALS_FILE=/.aws/credentials`
   3. `AWS_PROFILE=<NAME OF YOUR PROFILE>`
   4. `S3_BUCKET=<NAME OF YOUR BUCKET>`

> [!IMPORTANT]
> The docker-compose files in this repo assume that you have configured AWS S3 config files at:
> * `~/.aws/config`
> * `~/.aws/credentials`

Example of an `/.aws/config` file you can use with an S3 compatible API:
```bash
[default]
region = us-east-1
output = json

[profile my-profile]
region = us-east-1
services = s3-private

[services s3-private]
s3 =
    endpoint_url = https://s3.private.endpoint
    addressing_style = path
    signature_version = s3v4
    use_accelerate_endpoint = false
    use_dualstack_endpoint = false

s3api =
    endpoint_url = https://s3.private.endpoint
    addressing_style = path
    signature_version = s3v4
```

Example of an `/.aws/credentials` file you can use with an S3 compatible API:
```bash
[my-profile]
aws_access_key_id = <ACCESS KEY ID>
aws_secret_access_key = <SECRET KEY ID>
```

> [!IMPORTANT]
> Make sure that you correctly set `AWS_PROFILE` to a value that is present in the 
> S3 configuration file.

Start `bento_etl` with S3 enabled:
```bash
# Set the aws config profile
export AWS_PROFILE=my-profile

# Validate that you can list objects in the bucket 'my-bucket' with the S3 CLI
aws s3 ls my-bucket

# Set S3_BUCKET env var
export S3_BUCKET=my-bucket

docker compose up
```

With the configuration above, `bento_etl` will instantiate an `S3Extractor` extractor dependency that 
targets the S3 endpoint `https://s3.private.endpoint` with SSL validation.

The S3Extractor will only be able to connect to the S3 bucket configured in `S3_BUCKET`.

Submitting an ETL Job with an S3 extraction step is as simple as making a `POST` to `/jobs` with a body like this:
```JSON
{
  "extractor": {
    "object_key": "path/to/a/json/file.json"
  },
  "transformer": {
    "type": "None"
  },
  "loader": {
    "dataset_id": "",
    "batch_size": 0,
    "data_type": "print"
  }
}
```

> [!WARNING]
> The S3Extractor currently only handles the `.json` and `.jsonl` (new-line delimited JSON) file extentions.
> New extentions and file types will be added over time (CSVs, VCFs, etc ...).

#### Extractor roadmap
- CSV extractors

### Transformers

Transformers are used to convert the output of an Extractor into the expected input of a loader.

For instance, extracted clinical data needs to be transformed into Katsu's Phenopackets format before it can be loaded 
in Katsu.

The only supported Transformer at the moment if `None`, which is effectively a passthrough transformer that feeds the 
output of the Extractor directly to the Loader, without doing transformations.

This can be used for tests and to ingest data that is already correctly formated.

Example Tranformer JSON:
```JSON
{
  "type": "None"
}
```

#### Transformers roadmap
- Configure a JSON to Phenopackets transformer (**WIP**)
- Configure a JSON to Experiments transformer (**WIP**)
- Configure a CSV to Phenopackets transformer (ROADMAP)
- Configure a CSV to Experiments transformer (ROADMAP)
- Configure a VCF to Parquet transformer (ROADMAP)
- Configure a VCF to VCF transformer (ROADMAP)


### Loaders

Loaders are the final step of an ETL pipeline. A loader's input is the output of a tranformer.

Loaders specificaly target Bento services like Katsu and integrate with Bento's authn/authz system.

Some container environment variables are important for the loader:
- `KATSU_URL` to configure the target Katsu
- To authenticate bento_etl with bento_authz when calling Katsu, set:
  - `ETL_CLIENT_ID`
  - `ETL_CLIENT_SECRET`
  - `BENTO_OPENID_CONFIG_URL`

Two types of loaders are supported at the moment:
1. `phenopackets`: loads Phenopackets V2 into a Katsu service
2. `experiments`: loads Experiment Metadata into a Katsu service
3. `print`: simply prints the items for debugging

Example of a Loader JSON:
```JSON
{
  "dataset_id": "<KATSU DATASET ID TO INGEST INTO>",
  "batch_size": 0,  // number of items to ingest at a time, 0 means all-at-once
  "data_type": "< phenopackets || experiments || print>"
}
```

### Loaders roadmap
- Configure a Gohan VCF loader (ROADMAP)
- Configure a generic S3 loader (ROADMAP)
- Configure a generic HTTP loader (ROADMAP)

### Full Job example

As seen [here](./pipelines/test_experiments.json).

This ETL Job object performs the following:
1. Extracts clean Experiments JSON from the test data endpoint
2. Passthrough Transformer forwards the data to the loader with no modifications
3. Loader ingests the data into one of Katsu's datasets
```JSON
{
    "extractor": {
        "extract_url": "http://localhost:5000/test/data-sources/exps-clean",
        "type": "api-fetch"
    },
    "transformer": {
        "type": "None"
    },
    "loader": {
        "dataset_id": "some_dataset_id",
        "batch_size": 0,
        "data_type": "experiments"
    }
}
```

## Dev

We recommend using docker compose for local dev work:

```bash
# Set BENTO_UID for volume permissions
export BENTO_UID=$(id -u)

# Spin up the ETL service
docker compose -f docker-compose.dev.yaml up -d

# Spin down the ETL service
docker compose -f docker-compose.dev.yaml down
```

You can then open the dev container in VS Code, the repo is mounted at `/etl`:
1. Open the command palette: `CTRL + Shift + P`
2. Select `Dev Containers: Attach to running container`
3. Select the `bento_etl` container (it must be running)
4. If not automatic, open `/etl` folder
5. If not automatic, trust source control
6. (Optional) Add break points and use the provided interactive debugger config `Python Dev Debugger (bento_etl): Remote Attach`

## OpenAPI docs

FastAPI produces an OpenAPI schema automatically, providing rich API docs.

To access the Swagger UI of a local bento_etl, simply open a browser and visit `localhost:5000/docs`.

## Mappings

The ETL uses CSV mapping files located in the `mappings/` directory.  
These files define how PCGL data dictionary fields are transformed into ETL JSON models (Phenopackets V2 and Katsu experiments).

For the full documentation of the CSV mapping pattern, see  
**[docs/mappings.md](docs/mappings.md)**.
