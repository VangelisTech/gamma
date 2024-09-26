import daft
from daft import col
from daft.io import IOConfig

def from_storage(df: daft.DataFrame, io_config: IOConfig) -> daft.DataFrame:
    df = df.with_column("node_binary", df["path"].url.download(io_config=io_config))
    return df

