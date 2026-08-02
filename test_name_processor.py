import pytest

from transformations.names_processing.name_normaliser import NameNormaliser
from transformations.names_processing.name_processing_models import (
    NAME_INPUT_COLUMNS,
    NameOutputColumns,
)

LOOKUP = ["John", "Mary", "Anne", "Paul", "José", "Hone"]
OUTPUT = NameOutputColumns("first_name", "last_name", "first_name_full")


@pytest.fixture(scope="module")
def normaliser():
    return NameNormaliser(common_first_names=LOOKUP)


def _parse(spark, normaliser, first_names, family_name):
    df = spark.createDataFrame(
        [(first_names, family_name)],
        [NAME_INPUT_COLUMNS.first_names, NAME_INPUT_COLUMNS.family_name],
    )
    row = normaliser.transform(df, NAME_INPUT_COLUMNS, OUTPUT).collect()[0]
    return row["first_name"], row["last_name"], row["first_name_full"]


@pytest.mark.parametrize(
    "first_names,family_name,expected",
    [
        # spec example
        ("Triumph", "Stag GL", ("TRIUMPH", "GL", "TRIUMPH STAG")),
        # titles removed anywhere, not only leading
        ("Mr John", "Smith Jr", ("JOHN", "SMITH", "JOHN")),
        ("Mrs Mary Anne", "Jones", ("MARY", "JONES", "MARY ANNE")),
        # husband dropped at the start of the combined name, kept at the end
        ("Husband John", "Smith", ("JOHN", "SMITH", "JOHN")),
        ("John", "Husband", ("JOHN", "HUSBAND", "JOHN")),
        # accents reduced, macrons included
        ("Hōne", "Māori", ("HONE", "MAORI", "HONE")),
        ("José", "Núñez", ("JOSE", "NUNEZ", "JOSE")),
        # surname prefixes joined, including chains
        ("Jan", "van der Velden", ("JAN", "VANDERVELDEN", "JAN")),
        ("Maria", "de la Cruz", ("MARIA", "DELACRUZ", "MARIA")),
        ("Sean", "O Brien", ("SEAN", "OBRIEN", "SEAN")),
        # hyphens join rather than split
        ("Mary-Anne", "Smith-Jones", ("MARYANNE", "SMITHJONES", "MARYANNE")),
        ("Mary - Anne", "Smith", ("MARYANNE", "SMITH", "MARYANNE")),
        # bracketed content removed with the brackets
        ("John (Jack)", "Smith", ("JOHN", "SMITH", "JOHN")),
        # special characters become separators, apostrophes survive
        ("John$%^", "O'Neill", ("JOHN", "O'NEILL", "JOHN")),
        # joined first and middle split via the lookup
        ("Johnpaul", "Smith", ("JOHN", "SMITH", "JOHN PAUL")),
        # an exact lookup name is never split
        ("John", "Smith", ("JOHN", "SMITH", "JOHN")),
        # whitespace collapsed
        ("  John   Paul  ", " Smith ", ("JOHN", "SMITH", "JOHN PAUL")),
        # sparse and empty inputs
        (None, "Smith", (None, "SMITH", None)),
        ("John", None, (None, "JOHN", None)),
        (None, None, (None, None, None)),
        ("", "", (None, None, None)),
        # everything consumed by title removal
        ("Mr", "Dr", (None, None, None)),
    ],
)
def test_parse_rules(spark, normaliser, first_names, family_name, expected):
    assert _parse(spark, normaliser, first_names, family_name) == expected


def test_lookup_accents_normalised():
    # JOSÉ must fold to JOSE, otherwise JOS would split JOSEPHINE
    prepared = NameNormaliser(common_first_names=["José"])._common_first_names
    assert prepared == ("JOSE",)


def test_missing_input_column_raises(spark, normaliser):
    df = spark.createDataFrame([("A",)], [NAME_INPUT_COLUMNS.first_names])
    with pytest.raises(ValueError, match="Required name columns not found"):
        normaliser.transform(df, NAME_INPUT_COLUMNS, OUTPUT)
