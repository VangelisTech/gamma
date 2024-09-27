from typing import Optional
from daft import DataFrame
from daft.io import IOConfig
from gamma.components.data.structs.utils.base_utils import validate_schema, export_to_parquet, get_uri, get_by_id

class BaseAccessor:
    """
    Base accessor class for managing data.
    """
    schema: ClassVar[daft.Schema] = daft.Schema(base_schema)
    namespace: ClassVar[str] = "data"
    table_name: ClassVar[str] = "base"
    partitioned_on: ClassVar[List[str]] = ["created_at"]

    def __init__(self, df: DataFrame, io_config: Optional[IOConfig] = None, uri: Optional[str] = ""):
        self.df = df
        self.io_config = io_config
        self.uri = uri

        self.schema_wrapper = SchemaWrapper(self.schema)

    def export_to_parquet(self, prefix: str):
        return export_to_parquet(self.df, prefix, self.io_config)

    def get_uri(self, custom_prefix: Optional[str] = None):
        return get_uri(custom_prefix, self.io_config)

    def get_by_id(self, id):
        return get_by_id(self.df, id)

    def upsert(self, id, updates):
        return upsert(self.df, id, updates)

    def sql(self, query: str):
        return self.df.sql(query)

    def repartition(self, partition_by: List[str]):
        return self.df.repartition(partition_by)

    def delete(self, id:):
        self.df = self.df.filter(self.df['id'] != id)
        return self.df

   
