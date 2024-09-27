# Edge fields
edge_fields = [
    pa.field("edge_id", pa.string()),
    pa.field("from_node", pa.string()),
    pa.field("to_node", pa.string()),
    pa.field("metadata", pa.map_(pa.string(), pa.string()))  # Simplified metadata as string key-value pairs
]