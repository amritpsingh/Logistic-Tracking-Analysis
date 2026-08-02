"""Individual name-processing transformation."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, when

from names_processing.name_processor import NameOutputColumns, NameProcessor
from transformations.abstract_transformation import AbstractTransformation


class IndividualNames(AbstractTransformation):
    SOURCE_KEY = "individual"

    def transform(self, data: dict[str, DataFrame]) -> DataFrame:
        if self.SOURCE_KEY not in data:
            raise KeyError(f"Input dataset not found: {self.SOURCE_KEY}")

        source = data[self.SOURCE_KEY]
        selected = (
            source
            .withColumn(
                "_selected_first_names",
                when(
                    lower(trim(col("i_name_confirmed"))) == "yes",
                    col("i_first_name_hsf"),
                ).otherwise(col("i_first_names")),
            )
            .withColumn(
                "_selected_family_name",
                when(
                    lower(trim(col("i_name_confirmed"))) == "yes",
                    col("i_family_name_hsf"),
                ).otherwise(col("i_family_name")),
            )
        )

        result = NameProcessor(self.spark).transform(
            selected,
            first_name_column="_selected_first_names",
            family_name_column="_selected_family_name",
            output_columns=NameOutputColumns(
                first_name="i_parsed_first_name",
                last_name="i_parsed_last_name",
                first_name_full="i_parsed_first_name_full",
            ),
        )
        return result.drop("_selected_first_names", "_selected_family_name")
