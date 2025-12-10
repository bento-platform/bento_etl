import polars as pl
import pytest
from bento_etl.transformers.base import BaseTransformer
from bento_etl.transformers.dependencies import get_transformer
from bento_etl.logger import BoundLogger
from bento_etl.models import Job, ExtractStep, TransformStep, LoadStep


class TestBaseTransformer:
    def test_transform_raises_not_implemented(self, logger: BoundLogger):
        """Test that the base transformer's transform method raises NotImplementedError."""
        transformer = BaseTransformer(logger)
        df = pl.DataFrame({"col1": [1, 2, 3]})

        with pytest.raises(NotImplementedError):
            transformer.transform(df)


class TestTransformerDependencies:
    def test_get_transformer_none_type(self, logger: BoundLogger):
        """Test getting a None transformer from dependencies."""
        job = Job(
            extractor=ExtractStep(extract_url="test_url", type="api-fetch"),
            transformer=TransformStep(type="None"),
            loader=LoadStep(dataset_id="test_id", batch_size=0, data_type="phenopackets")
        )

        transformer = get_transformer(job, logger)
        assert transformer is None
