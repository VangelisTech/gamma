import daft
from typing import Dict, Any, Optional

class StorageInterface:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self) -> daft.Engine:
        engine_type = self.config.get("engine_type", "local")
        if engine_type == "local":
            return daft.Engine.local()
        elif engine_type == "remote":
            return daft.Engine.remote(self.config.get("uri", "memory://"))
        else:
            raise ValueError(f"Unsupported engine type: {engine_type}")

    def read_table(self, table_name: str) -> daft.DataFrame:
        return self.engine.read_table(table_name)

    def write_table(self, table_name: str, df: daft.DataFrame, mode: str = "overwrite"):
        self.engine.write_table(table_name, df, mode=mode)

    def delete_table(self, table_name: str):
        self.engine.delete_table(table_name)

    def table_exists(self, table_name: str) -> bool:
        return self.engine.table_exists(table_name)

    def list_tables(self) -> list[str]:
        return self.engine.list_tables()

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        if self.table_exists(table_name):
            return self.read_table(table_name).schema().to_dict()
        return None

    def execute_query(self, query: str) -> daft.DataFrame:
        return self.engine.sql(query)

    def get_config(self) -> Dict[str, Any]:
        return self.config

    def update_config(self, new_config: Dict[str, Any]):
        self.config.update(new_config)
        self.engine = self._create_engine()


