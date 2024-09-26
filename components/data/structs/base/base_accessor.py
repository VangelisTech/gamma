from typing import Optional
from daft import DataFrame
from daft.io import IOConfig
from gamma.components.data.structs.utils.base_utils import validate_schema, export_to_parquet, get_uri_prefix, get_by_id

class BaseAccessor:
    def __init__(self, df: DataFrame, io_config: Optional[IOConfig] = None, uri_prefix: Optional[str] = ""):
        self.df = df
        self.io_config = io_config
        self.uri_prefix = uri_prefix

    def validate_schema(self, schema):
        self.df = validate_schema(schema, self.df)

    def export_to_parquet(self, prefix: str):
        return export_to_parquet(self.df, prefix, self.io_config)

    def get_uri_prefix(self, custom_prefix: Optional[str] = None):
        return get_uri_prefix(custom_prefix, self.io_config)

    def get_by_id(self, id):
        return get_by_id(self.df, id)

    def upsert(self, id, updates):
        return upsert(self.df, id, updates)

    def update(self, id, updates):
        for key, value in updates.items():
            self.df = self.df.with_column(key, value).where(self.df['id'] == id)
        return self.df

    def delete(self, id):
        self.df = self.df.filter(self.df['id'] != id)
        return self.df

    def create(self, new_data):
        new_df = DataFrame.from_pydict(new_data)
        self.df = self.df.concat(new_df)
        return self.df
