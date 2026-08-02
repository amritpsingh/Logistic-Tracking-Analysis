"""Models and constants used by generic Census name processing."""

from dataclasses import dataclass


@dataclass(frozen=True)
class NameInputColumns:
    first_names: str
    family_name: str


@dataclass(frozen=True)
class NameOutputColumns:
    first_name: str
    last_name: str
    first_name_full: str


@dataclass(frozen=True)
class NameProcessingColumns:
    """Internal columns used temporarily during one Spark transformation plan."""

    clean_first_names: str = "_name_clean_first_names"
    clean_family_name: str = "_name_clean_family_name"
    combined_name: str = "_name_combined"
    tokens: str = "_name_tokens"
    token_count: str = "_name_token_count"
    lookup_prefix: str = "_name_lookup_prefix"
    lookup_remainder: str = "_name_lookup_remainder"


TITLES: tuple[str, ...] = (
    "MR", "MISS", "MRS", "MS", "DR", "WIFE", "JNR", "JR",
)

SURNAME_PREFIXES: tuple[str, ...] = (
    "O", "MC", "MAC", "TE", "DE", "DA", "VAN", "VON", "DEN", "DER",
    "LE", "LA", "AL", "EL", "FA", "COS", "DEL", "DES", "DI", "DU",
)
