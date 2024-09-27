from typing import Optional
from daft import DataFrame
from daft.io import IOConfig
import pyarrow as pa

def validate_schema(schema: pa.Schema, df: DataFrame) -> DataFrame:
    if not df.schema.to_pyarrow_schema().equals(schema):
        raise ValueError(f"DataFrame schema does not match the {schema} schema.")
    return df


def export_to_parquet(df: DataFrame, prefix: str, io_config: Optional[IOConfig] = None) -> DataFrame:
    """
    Exports the DataFrame to Parquet files.

    Parameters:
        df (DataFrame): DataFrame to export.
        prefix (str): Destination path for Parquet files.
        io_config (Optional[IOConfig]): Optional IOConfig for the URI.
    """
    if io_config:
        uri = get_uri(prefix, io_config)
        return df.write_parquet(uri, io_config=io_config)
    else:
        return df.write_parquet(path)

def get_uri(
    custom_prefix: Optional[str] = None,
    io_config: Optional[IOConfig] = None,
    ) -> str:
    """
    Constructs the artifact URI prefix based on the storage backend configurations.

    Parameters:
        custom_prefix (Optional[str]): Optional prefix for the URI.
        io_config (Optional[daft.io.IOConfig]): Optional IOConfig for the URI.

    Returns:
        str: The constructed artifact URI prefix.
    """
    if io_config:
        if io_config.s3:
            s3 = io_config.s3
            if not hasattr(s3, "bucket") or not s3.bucket:
                raise ValueError("S3Config must have a 'bucket' defined.")
            prefix = s3.prefix + custom_prefix if custom_prefix else s3.prefix
            return f"s3://{s3.bucket}/{prefix}/" if prefix else f"s3://{s3.bucket}/"

        elif io_config.gcs:
            gcs = io_config.gcs
            if not hasattr(gcs, "bucket") or not gcs.bucket:
                raise ValueError("GCSConfig must have a 'bucket' defined.")
            prefix = gcs.prefix + custom_prefix if custom_prefix else gcs.prefix
            return (
                f"gs://{gcs.bucket}/{prefix}/" if prefix else f"gs://{gcs.bucket}/"
            )

        elif io_config.azure:
            azure = io_config.azure
            if not hasattr(azure, "container") or not azure.container:
                raise ValueError("AzureConfig must have a 'container' defined.")
            prefix = azure.prefix + custom_prefix if custom_prefix else azure.prefix
            return (
                f"az://{azure.container}/{prefix}/"
                if prefix
                else f"az://{azure.container}/"
            )

    else:
        return f"{custom_prefix}/" if custom_prefix else "/"
    

def get_by_id(df: DataFrame, id: Union[str, List[str]]) -> DataFrame:
    """
    Retrieves an artifact by its ID.

    Parameters:
        id: Union[str, List[str]]: The ULID of the artifact.

    Returns:
        DataFrame: data or None if not found.
    """
    filtered_df = df.where(col("id") == id)
    if filtered_df.count() == 0:
        return None
    return filtered_df.collect()