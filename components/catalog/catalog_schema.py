import pyarrow as pa

# Data Catalog Schema
catalog_fields = [
    pa.field("catalog_id", pa.string()),
    pa.field("name", pa.string()),
    pa.field("description", pa.string()),
    pa.field("created_at", pa.timestamp('us')),
    pa.field("updated_at", pa.timestamp('us')),
    pa.field("owner", pa.string()),
    pa.field("tags", pa.list_(pa.string())),
    pa.field("metadata", pa.map_(pa.string(), pa.string())),
    pa.field("data_sources", pa.list_(pa.struct([
        ('source_id', pa.string()),
        ('source_type', pa.string()),
        ('connection_details', pa.map_(pa.string(), pa.string()))
    ]))),
    pa.field("schemas", pa.list_(pa.struct([
        ('schema_id', pa.string()),
        ('schema_name', pa.string()),
        ('fields', pa.list_(pa.struct([
            ('field_name', pa.string()),
            ('field_type', pa.string()),
            ('is_nullable', pa.bool_())
        ])))
    ]))),
    pa.field("access_control", pa.list_(pa.struct([
        ('user_id', pa.string()),
        ('permission', pa.string())
    ])))
]

catalog_schema = pa.schema(catalog_fields)
