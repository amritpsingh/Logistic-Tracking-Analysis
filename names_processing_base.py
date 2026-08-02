"""Base transformation for a single-read names-processing pipeline."""

from abc import abstractmethod

from pyspark.sql import DataFrame

from names_processing.name_normaliser import NameNormaliser
from names_processing.name_processing_models import NameInputColumns, NameOutputColumns
from transformations.abstract_transformation import AbstractTransformation


class NamesProcessingBase(AbstractTransformation):
    """Share orchestration while leaving source-field selection to each dataset."""

    source_key: str
    input_columns: NameInputColumns
    output_columns: NameOutputColumns

    def __init__(self, *args, name_normaliser: NameNormaliser | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.name_normaliser = name_normaliser or NameNormaliser()

    @abstractmethod
    def prepare_source_names(self, source: DataFrame) -> DataFrame:
        """Map source-specific fields to the configured generic input columns."""
        pass

    def transform(self, data: dict[str, DataFrame]) -> DataFrame:
        if self.source_key not in data:
            raise KeyError(f"Input dataset not found: {self.source_key}")

        # AbstractTransformation.read() populated this dictionary once. Every operation
        # below contributes to the same lazy DataFrame plan. No table is read again.
        prepared = self.prepare_source_names(data[self.source_key])
        return self.name_normaliser.transform(
            prepared,
            input_columns=self.input_columns,
            output_columns=self.output_columns,
        )
