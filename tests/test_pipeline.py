from bento_etl.extractors.base import BaseExtractor
from bento_etl.loaders.base import BaseLoader
from bento_etl.transformers.base import BaseTransformer


def test_csv_experiments_pipeline(logger, config):
    # TODO: use concrete ETL components, this is just to show the general flow.

    # Init ETL pipeline components
    extractor = BaseExtractor(logger)
    transformer = BaseTransformer(logger)
    loader = BaseLoader(logger, config)

    # Run pipeline
    raw_data = extractor.extract()
    transformed_data = transformer.transform(raw_data)
    loader.load(transformed_data)
