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
        uri_prefix: Optional[str] = "",