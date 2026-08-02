"""Generic name cleaning and parsing for linking.

The processor is shared by individual and dwelling-person transformations.
It deliberately contains no read, write, incremental, SCD2, or logging logic.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    lower,
    pandas_udf,
    trim,
    upper,
    when,
)
from pyspark.sql.types import StringType

from definitions import ROOT_DIR


@dataclass(frozen=True)
class NameOutputColumns:
    """Names of the three derived output columns."""

    first_name: str
    last_name: str
    first_name_full: str


class NameProcessor:
    """Clean concatenated names and derive linking-name fields."""

    TITLES = ("MR", "MISS", "MRS", "MS", "DR", "WIFE", "JNR", "JR")
    SURNAME_PREFIXES = (
        "O", "MC", "MAC", "TE", "DE", "DA", "VAN", "VON", "DEN", "DER",
        "LE", "LA", "AL", "EL", "FA", "COS", "DEL", "DES", "DI", "DU",
    )
    DEFAULT_LOOKUP_FILE = os.path.join(
        ROOT_DIR,
        "classifications",
        "names_lookup",
        "common_first_names_lookup.json",
    )

    def __init__(
        self,
        spark: SparkSession,
        lookup_file: str | None = None,
        common_first_names: Iterable[str] | None = None,
    ) -> None:
        self.spark = spark
        if common_first_names is None:
            common_first_names = self._load_common_first_names(
                lookup_file or self.DEFAULT_LOOKUP_FILE
            )
        self.common_first_names = self._prepare_common_first_names(common_first_names)
        self._broadcast_common_first_names = spark.sparkContext.broadcast(
            self.common_first_names
        )

    @staticmethod
    def _load_common_first_names(path: str) -> list[str]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Common first-name lookup not found: {path}")
        try:
            with open(path, "r", encoding="utf-8") as lookup_file:
                values = json.load(lookup_file)
        except (OSError, json.JSONDecodeError) as exception:
            raise ValueError(f"Unable to read common first-name lookup: {path}") from exception
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise ValueError("Common first-name lookup must be a JSON array of strings")
        return values

    @staticmethod
    def _ascii_letters_and_apostrophes(value: str | None) -> str:
        """Apply the ordered methodology rules to a concatenated name."""
        if value is None:
            return ""

        # Reduce accented characters to ASCII components and uppercase.
        value = unicodedata.normalize("NFKD", value)
        value = value.encode("ascii", "ignore").decode("ascii").upper()

        # Remove bracketed content, including brackets. Repeated application handles
        # multiple non-nested bracketed sections deterministically.
        previous = None
        while previous != value:
            previous = value
            value = re.sub(r"\([^()]*\)", " ", value)
        value = value.replace("(", " ").replace(")", " ")

        # Replace unsupported characters with spaces to avoid joining separate tokens.
        # Apostrophes are retained as required by the methodology.
        value = re.sub(r"[^A-Z' ]+", " ", value)
        value = re.sub(r"\s+", " ", value).strip()

        # Remove repeated leading titles only. Complete-token boundaries prevent
        # accidental removal from legitimate names.
        title_pattern = r"^(?:(?:" + "|".join(NameProcessor.TITLES) + r")\b\s*)+"
        value = re.sub(title_pattern, "", value).strip()

        # HUSBAND is removed only when it is a leading complete token.
        value = re.sub(r"^(?:HUSBAND\b\s*)+", "", value).strip()

        # Collapse spaces again after token removal.
        return re.sub(r"\s+", " ", value).strip()

    @staticmethod
    def _prepare_common_first_names(values: Iterable[str]) -> tuple[str, ...]:
        cleaned = {
            NameProcessor._ascii_letters_and_apostrophes(value).replace(" ", "")
            for value in values
            if value is not None
        }
        cleaned.discard("")
        # Longest first gives deterministic longest-prefix extraction.
        return tuple(sorted(cleaned, key=lambda item: (-len(item), item)))

    @staticmethod
    def _split_joined_given_names(
        cleaned_first_names: str,
        common_first_names: tuple[str, ...],
    ) -> str:
        """Split a joined first/middle-name token using the longest lookup prefix.

        The lookup is used only when the supplied first-name value contains exactly
        one cleaned token. The prefix must be shorter than the complete token, so an
        exact common first name is left unchanged.
        """
        if not cleaned_first_names or " " in cleaned_first_names:
            return cleaned_first_names
        for candidate in common_first_names:
            if (
                len(candidate) < len(cleaned_first_names)
                and cleaned_first_names.startswith(candidate)
            ):
                remainder = cleaned_first_names[len(candidate):]
                if remainder:
                    return f"{candidate} {remainder}"
        return cleaned_first_names

    @classmethod
    def _join_surname_prefixes(cls, value: str) -> str:
        """Join adjacent surname-prefix chains to the token on their right."""
        tokens = value.split()
        if len(tokens) < 2:
            return value

        result: list[str] = []
        index = 0
        prefixes = set(cls.SURNAME_PREFIXES)
        while index < len(tokens):
            if tokens[index] in prefixes and index + 1 < len(tokens):
                combined = tokens[index]
                index += 1
                while index < len(tokens) and tokens[index] in prefixes and index + 1 < len(tokens):
                    combined += tokens[index]
                    index += 1
                combined += tokens[index]
                result.append(combined)
            else:
                result.append(tokens[index])
            index += 1
        return " ".join(result)

    @classmethod
    def _parse_cleaned_name(cls, first_names: str, family_name: str) -> tuple[str | None, str | None, str | None]:
        combined = " ".join(part for part in (first_names, family_name) if part).strip()
        combined = cls._join_surname_prefixes(combined)
        tokens = combined.split()
        if not tokens:
            return None, None, None
        if len(tokens) == 1:
            return None, tokens[0], None
        first_name_full = " ".join(tokens[:-1])
        return tokens[0], tokens[-1], first_name_full

    def transform(
        self,
        data: DataFrame,
        first_name_column: str,
        family_name_column: str,
        output_columns: NameOutputColumns,
    ) -> DataFrame:
        """Clean source name fields and append the three parsed output columns."""
        for required_column in (first_name_column, family_name_column):
            if required_column not in data.columns:
                raise ValueError(f"Required name column not found: {required_column}")

        common_names = self._broadcast_common_first_names
        clean_value = NameProcessor._ascii_letters_and_apostrophes
        split_value = NameProcessor._split_joined_given_names
        parse_value = NameProcessor._parse_cleaned_name

        @pandas_udf(StringType())
        def clean_name(values):
            return values.apply(clean_value)

        @pandas_udf(StringType())
        def split_given_names(values):
            lookup = common_names.value
            return values.apply(lambda value: split_value(value or "", lookup))

        @pandas_udf("struct<first_name:string,last_name:string,first_name_full:string>")
        def parse_name(first_names, family_names):
            import pandas as pd
            parsed = [
                parse_value(first_name or "", family_name or "")
                for first_name, family_name in zip(first_names, family_names)
            ]
            return pd.DataFrame(parsed, columns=["first_name", "last_name", "first_name_full"])

        work = (
            data
            .withColumn("_clean_first_names", clean_name(col(first_name_column)))
            .withColumn("_clean_first_names", split_given_names(col("_clean_first_names")))
            .withColumn("_clean_family_name", clean_name(col(family_name_column)))
            .withColumn("_parsed_name", parse_name(col("_clean_first_names"), col("_clean_family_name")))
        )

        return (
            work
            .withColumn(output_columns.first_name, col("_parsed_name.first_name"))
            .withColumn(output_columns.last_name, col("_parsed_name.last_name"))
            .withColumn(output_columns.first_name_full, col("_parsed_name.first_name_full"))
            .drop("_clean_first_names", "_clean_family_name", "_parsed_name")
        )
