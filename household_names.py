"""Dwelling-person name-processing transformation."""

from pyspark.sql import DataFrame

from names_processing.name_processor import NameOutputColumns, NameProcessor
from transformations.abstract_transformation import AbstractTransformation


class HouseholdNames(AbstractTransformation):
    SOURCE_KEY = "dwelling_person"

    def transform(self, data: dict[str, DataFrame]) -> DataFrame:
        if self.SOURCE_KEY not in data:
            raise KeyError(f"Input dataset not found: {self.SOURCE_KEY}")

        return NameProcessor(self.spark).transform(
            data[self.SOURCE_KEY],
            first_name_column="first_names",
            family_name_column="family_name",
            output_columns=NameOutputColumns(
                first_name="d_parsed_first_name",
                last_name="d_parsed_last_name",
                first_name_full="d_parsed_first_name_full",
            ),
        )
