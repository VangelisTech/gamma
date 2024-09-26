




def read_lancedb(uri: str, io_config: IOConfig) -> DataFrame:
    df = daft.read_lance(uri, io_config)
    return df

def write_lancedb(uri: str, df: DataFrame, io_config: IOConfig):
    df.write_lance(uri, io_config)


def query(sql: str, connn)