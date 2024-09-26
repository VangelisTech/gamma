import daft
from daft import col
from typing import List, Tuple

def sorted_bucket_merge_join(
    left_df: daft.DataFrame,
    right_df: daft.DataFrame,
    left_on: List[str],
    right_on: List[str],
    left_sort_keys: List[str],
    right_sort_keys: List[str],
    num_buckets: int
) -> daft.DataFrame:
    """
    Perform a sorted bucket merge join on two DataFrames.

    Args:
        left_df (daft.DataFrame): Left DataFrame
        right_df (daft.DataFrame): Right DataFrame
        left_on (List[str]): Join columns for the left DataFrame
        right_on (List[str]): Join columns for the right DataFrame
        left_sort_keys (List[str]): Columns to sort the left DataFrame
        right_sort_keys (List[str]): Columns to sort the right DataFrame
        num_buckets (int): Number of buckets to use for the join

    Returns:
        daft.DataFrame: Joined DataFrame
    """
    # Create bucket columns for both DataFrames
    left_df = left_df.with_column(
        "bucket",
        (col(left_on[0]).hash() % num_buckets).cast("int32")
    )
    right_df = right_df.with_column(
        "bucket",
        (col(right_on[0]).hash() % num_buckets).cast("int32")
    )

    # Sort DataFrames within each bucket
    left_df = left_df.sort(["bucket"] + left_sort_keys)
    right_df = right_df.sort(["bucket"] + right_sort_keys)

    # Perform the join
    joined_df = left_df.join(
        right_df,
        left_on=["bucket"] + left_on,
        right_on=["bucket"] + right_on,
        how="inner"
    )

    # Remove the bucket column
    joined_df = joined_df.exclude("bucket")

    return joined_df

