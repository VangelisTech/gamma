import pyarrow as pa

base_schema = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            nullable=False,
            metadata={"description": "Unique identifier for the record"},
        ),
        pa.field(
            "created_at",
            pa.timestamp("ns", tz="UTC"),
            nullable=False,
            metadata={"description": "Creation timestamp"},
        ),
        pa.field(
            "updated_at",
            pa.timestamp("ns", tz="UTC"),
            nullable=False,
            metadata={"description": "Last update timestamp"},
        ),
        pa.field(
            "inserted_at",
            pa.timestamp("ns", tz="UTC"),
            nullable=False,
            metadata={"description": "Insertion timestamp into the database"},
        ),
    ]
)

metadata_schema = [
    pa.field("metadata", pa.map_(pa.string(), pa.string(), nullable=False),
]


        self,
        files: Optional[List[str]] = None,
        df: Optional[daft.DataFrame] = None,
        io_config: Optional[IOConfig] = IOConfig(),
        uri: Optional[str] = "",

class SchemaWrapper:
    def __init__(self, schema: daft.Schema):
        self.schema = schema

    def validate_schema(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Validate that the DataFrame matches the schema.
        
        Args:
            df (daft.DataFrame): The DataFrame to validate.
        
        Returns:
            daft.DataFrame: The validated DataFrame.
        
        Raises:
            ValueError: If the DataFrame does not match the schema.
        """
        if df.schema != self.schema:
            raise ValueError(f"DataFrame schema does not match expected schema.\nExpected: {self.schema}\nGot: {df.schema}")
        return df

    def coerce_schema(self, df: daft.DataFrame) -> daft.DataFrame:
        """
        Attempt to coerce the DataFrame to match the schema.
        
        Args:
            df (daft.DataFrame): The DataFrame to coerce.
        
        Returns:
            daft.DataFrame: The coerced DataFrame.
        """
        return df.cast(self.schema)

    def get_schema(self) -> daft.Schema:
        """
        Get the schema.
        
        Returns:
            daft.Schema: The schema.
        """
        return self.schema

    def append_fields(self, fields: List[daft.Field]) -> 'SchemaWrapper':
        """
        Append fields to the schema.
        
        Args:
            fields (List[daft.Field]): The fields to append.
        
        Returns:
            SchemaWrapper: A new SchemaWrapper with the appended fields.
        """
        new_schema = self.schema.append(fields)
        return SchemaWrapper(new_schema)

    def remove_fields(self, field_names: List[str]) -> 'SchemaWrapper':
        """
        Remove fields from the schema.
        
        Args:
            field_names (List[str]): The names of the fields to remove.
        
        Returns:
            SchemaWrapper: A new SchemaWrapper with the fields removed.
        """
        new_schema = self.schema.remove(field_names)
        return SchemaWrapper(new_schema)
