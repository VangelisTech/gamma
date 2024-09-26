from typing import Optional

import daft
from daft import DataFrame, Schema
from daft.io import IOConfig
import pyarrow as pa
from pydantic import BaseModel

from .base_schema import base_schema



class BaseDF:
    schema: ClassVar[pa.Schema] = base_schema
    namespace: ClassVar[str] = "data"
    table_name: ClassVar[str] = "base"
    partitioned_on: ClassVar[str] = "created_at"

    def __init__(self,
        df: Optional[DataFrame] = daft.from_arrow(base_schema.empty_table()),
        io_config: Optional[IOConfig] = IOConfig(),
        uri_prefix: Optional[str] = ""
        storage
    ):
        self.df = df
        self.io_config = io_config
        self.uri_prefix = uri_prefix

        self.validate_schema = validate_schema
        self.repartition_by_date = repartition_by_date
        self.export_to_parquet = export_to_parquet

        self.df = self.validate_schema(self.df)
        self.df = self.repartition_by_date(self.df)
    
    def read



    @classmethod
    def __repr__(cls) -> str:
        return f"<{cls.__name__} with {cls.df.count()} records>"

    @classmethod
    def __str__(cls) -> str:
        return cls.__repr__()
