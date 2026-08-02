"""Native PySpark expressions for cleaning and parsing names for linking.

No Python UDF, pandas UDF, RDD conversion, collect, or source-table read is used here.
The methods add expressions to the DataFrame's single lazy Spark execution plan.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array_join,
    col,
    concat_ws,
    element_at,
    lit,
    regexp_extract,
    regexp_replace,
    size,
    slice,
    split,
    trim,
    upper,
    when,
)

from names_processing.name_processing_models import (
    NameInputColumns,
    NameOutputColumns,
    NameProcessingColumns,
    SURNAME_PREFIXES,
    TITLES,
)


# Native Spark translate() needs corresponding source and replacement strings.
# The map covers Māori macrons and common Latin characters expected in Census names.
# Characters not represented here are later removed by the A-Z/apostrophe rule.
_ACCENT_SOURCE = (
    "ĀĂĄÀÁÂÃÄÅāăąàáâãäå"
    "ÇĆČçćč"
    "ĎĐďđ"
    "ĒĔĖĘĚÈÉÊËēĕėęěèéêë"
    "Ğğ"
    "ĪĬĮİÌÍÎÏīĭįıìíîï"
    "ŁĹĽłĺľ"
    "ÑŃŇñńň"
    "ŌŎŐÒÓÔÕÖØōŏőòóôõöø"
    "ŘŔřŕ"
    "ŚŠŞśšş"
    "ŤŢťţ"
    "ŪŬŮŰŲÙÚÛÜūŭůűųùúûü"
    "ÝŸýÿ"
    "ŹŻŽźżž"
)
_ACCENT_TARGET = 'AAAAAAAAAAAAAAAAAACCCCCCDDDDEEEEEEEEEEEEEEEEEEGGIIIIIIIIIIIIIIIILLLLLLNNNNNNOOOOOOOOOOOOOOOOOORRRRSSSSSSTTTTUUUUUUUUUUUUUUUUUUYYYYZZZZZZ'


class NameNormaliser:
    """Build a native Spark plan that cleans and parses name columns."""

    DEFAULT_LOOKUP_FILE = os.path.join(
        ROOT_DIR,
        "classifications",
        "names_lookup",
        "common_first_names_lookup.json",
    )

    def __init__(
        self,
        common_first_names: Iterable[str] | None = None,
        lookup_file: str | None = None,
    ) -> None:
        values = (
            common_first_names
            if common_first_names is not None
            else self._load_lookup(lookup_file or self.DEFAULT_LOOKUP_FILE)
        )
        self._common_first_names = self._prepare_lookup(values)
        self._first_name_pattern = self._build_first_name_pattern(
            self._common_first_names
        )

    @staticmethod
    def _load_lookup(path: str) -> list[str]:
        """Load small static configuration on the driver, not source respondent data."""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Common first-name lookup not found: {path}")
        try:
            with open(path, "r", encoding="utf-8") as file:
                values = json.load(file)
        except (OSError, json.JSONDecodeError) as exception:
            raise ValueError(f"Unable to read common first-name lookup: {path}") from exception
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
            raise ValueError("Common first-name lookup must be a JSON array of strings")
        return values

    @staticmethod
    def _prepare_lookup(values: Iterable[str]) -> tuple[str, ...]:
        cleaned = {
            re.sub(r"[^A-Z']", "", value.upper())
            for value in values
            if value and value.strip()
        }
        cleaned.discard("")
        return tuple(sorted(cleaned, key=lambda value: (-len(value), value)))

    @staticmethod
    def _build_first_name_pattern(values: tuple[str, ...]) -> str | None:
        if not values:
            return None
        alternatives = "|".join(re.escape(value) for value in values)
        # Group 1 is the longest matching common first name. Group 2 is the joined
        # remainder. Requiring group 2 prevents exact lookup names being split.
        return rf"^({alternatives})([A-Z']+)$"

    @staticmethod
    def _normalise_expression(value: Column) -> Column:
        """Return an expression implementing the ordered cleaning rules."""
        # translate is imported locally to keep the expression's intent explicit.
        from pyspark.sql.functions import translate

        cleaned = upper(trim(value))
        cleaned = translate(cleaned, _ACCENT_SOURCE, _ACCENT_TARGET)

        # Remove non-nested parenthesised content. Apply repeatedly so separate and
        # simply nested groups are handled without a UDF.
        for _ in range(3):
            cleaned = regexp_replace(cleaned, r"\([^()]*\)", " ")
        cleaned = regexp_replace(cleaned, r"[()]", " ")

        # Preserve only ASCII letters, apostrophes, and temporary whitespace.
        # Replacing with spaces avoids accidentally joining two tokens.
        cleaned = regexp_replace(cleaned, r"[^A-Z' ]+", " ")
        cleaned = trim(regexp_replace(cleaned, r"\s+", " "))

        titles = "|".join(re.escape(title) for title in TITLES)
        cleaned = regexp_replace(cleaned, rf"^(?:(?:{titles})\b\s*)+", "")
        cleaned = regexp_replace(cleaned, r"^(?:HUSBAND\b\s*)+", "")
        return trim(regexp_replace(cleaned, r"\s+", " "))

    def _split_joined_first_names(self, data: DataFrame, internal: NameProcessingColumns) -> DataFrame:
        """Split one joined first/middle token using a driver-built native regex."""
        if self._first_name_pattern is None:
            return data

        prefix = regexp_extract(col(internal.clean_first_names), self._first_name_pattern, 1)
        remainder = regexp_extract(col(internal.clean_first_names), self._first_name_pattern, 2)
        return (
            data
            .withColumn(internal.lookup_prefix, prefix)
            .withColumn(internal.lookup_remainder, remainder)
            .withColumn(
                internal.clean_first_names,
                when(
                    (col(internal.lookup_prefix) != "")
                    & (col(internal.lookup_remainder) != "")
                    & ~col(internal.clean_first_names).contains(" "),
                    concat_ws(
                        " ",
                        col(internal.lookup_prefix),
                        col(internal.lookup_remainder),
                    ),
                ).otherwise(col(internal.clean_first_names)),
            )
            .drop(internal.lookup_prefix, internal.lookup_remainder)
        )

    @staticmethod
    def _join_surname_prefixes_expression(value: Column) -> Column:
        """Join prefix chains right-to-left using native regexp_replace expressions."""
        result = value
        # Longest tokens first avoids ambiguous alternatives. Repeated passes support
        # chains such as VAN DER VELDEN: DER joins first, then VAN joins.
        prefixes = sorted(SURNAME_PREFIXES, key=lambda item: (-len(item), item))
        for _ in range(3):
            for prefix in prefixes:
                result = regexp_replace(
                    result,
                    rf"\b{re.escape(prefix)}\s+(?=[A-Z'])",
                    prefix,
                )
        return result

    def transform(
        self,
        data: DataFrame,
        input_columns: NameInputColumns,
        output_columns: NameOutputColumns,
    ) -> DataFrame:
        """Append parsed fields without triggering a Spark action or table read."""
        required = (input_columns.first_names, input_columns.family_name)
        missing = [name for name in required if name not in data.columns]
        if missing:
            raise ValueError(f"Required name columns not found: {', '.join(missing)}")

        internal = NameProcessingColumns()
        result = (
            data
            .withColumn(
                internal.clean_first_names,
                self._normalise_expression(col(input_columns.first_names)),
            )
            .withColumn(
                internal.clean_family_name,
                self._normalise_expression(col(input_columns.family_name)),
            )
        )
        result = self._split_joined_first_names(result, internal)
        result = result.withColumn(
            internal.combined_name,
            trim(
                concat_ws(
                    " ",
                    when(col(internal.clean_first_names) != "", col(internal.clean_first_names)),
                    when(col(internal.clean_family_name) != "", col(internal.clean_family_name)),
                )
            ),
        )
        result = result.withColumn(
            internal.combined_name,
            self._join_surname_prefixes_expression(col(internal.combined_name)),
        )
        result = (
            result
            .withColumn(
                internal.tokens,
                when(
                    col(internal.combined_name) != "",
                    split(col(internal.combined_name), r"\s+"),
                ),
            )
            .withColumn(internal.token_count, size(col(internal.tokens)))
            .withColumn(
                output_columns.last_name,
                when(
                    col(internal.token_count) >= 1,
                    element_at(col(internal.tokens), -1),
                ),
            )
            .withColumn(
                output_columns.first_name_full,
                when(
                    col(internal.token_count) >= 2,
                    array_join(
                        slice(
                            col(internal.tokens),
                            1,
                            col(internal.token_count) - 1,
                        ),
                        " ",
                    ),
                ),
            )
            .withColumn(
                output_columns.first_name,
                when(
                    col(internal.token_count) >= 2,
                    element_at(col(internal.tokens), 1),
                ),
            )
            .drop(
                internal.clean_first_names,
                internal.clean_family_name,
                internal.combined_name,
                internal.tokens,
                internal.token_count,
            )
        )
        return result
