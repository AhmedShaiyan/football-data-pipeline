import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def validate_raw_response(raw_data: dict, list_key: str, label: str) -> None:
    """
    Checkpoint after extraction.

    Checks that the API response contains the expected top-level list key
    and that the list is non-empty.

    Raises ValueError on failure so the pipeline aborts the competition run.
    """
    if not isinstance(raw_data, dict):
        raise ValueError(f"[{label}] Expected a dict from API, got {type(raw_data).__name__}")

    if list_key not in raw_data:
        raise ValueError(f"[{label}] API response missing required key '{list_key}'")

    records = raw_data[list_key]
    if not records:
        raise ValueError(f"[{label}] API returned an empty list for '{list_key}'")

    logger.info(f"[{label}] Extract checkpoint passed — {len(records)} records in '{list_key}'")


def validate_dataframe(df: DataFrame, id_column: str, label: str) -> None:
    """
    Checkpoint after transformation.

    Checks that:
    - The DataFrame has at least one row
    - The id_column has no null values (would break deduplication / joins)

    Raises ValueError on failure so the pipeline aborts the competition run.
    """
    count = df.count()
    if count == 0:
        raise ValueError(f"[{label}] Transform produced an empty DataFrame")

    null_count = df.filter(df[id_column].isNull()).count()
    if null_count > 0:
        raise ValueError(
            f"[{label}] Found {null_count} null value(s) in id column '{id_column}' — "
            "this would corrupt deduplication logic"
        )

    logger.info(f"[{label}] Transform checkpoint passed — {count} rows, no nulls in '{id_column}'")
