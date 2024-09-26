document_corpus_schema = pa.schema([
    pa.field("id", pa.string()),
    pa.field("created_at", pa.timestamp('us')),
    pa.field("updated_at", pa.timestamp('us')),

    pa.field("name", pa.string()),
    pa.field("type", pa.string()),
    pa.field("size", pa.int64()),
    pa.field("location", pa.string()),
    pa.field("metadata", pa.map_(pa.string(), pa.string())),
    pa.field("content", pa.string()),
    pa.field("embedding", pa.list_(pa.float64())),
])


# Define the Corpus Management System
class DocumentCorpusManager:
    def __init__(self, lancedb_uri: str = "lancedb://localhost"):
        self.lancedb_client = lancedb.connect(lancedb_uri)
        logging.info(f"LanceDB client connected: {self.lancedb_client}")

    def store_corpus(self, corpus_df: DataFrame, table_name: str = "corpus_metadata"):
        pa_table = corpus_df.to_arrow()
        if table_name not in self.lancedb_client.table_names():
            self.lancedb_client.create_table(table_name, pa_table)
            logging.info(f"Created new table '{table_name}' in LanceDB.")
        else:
            table = self.lancedb_client.open_table(table_name)
            table.add(pa_table)
            logging.info(f"Appended to existing table '{table_name}' in LanceDB.")

    def query_corpus_by_entity(self, entity_label: str, table_name: str = "corpus_metadata") -> DataFrame:
        table = self.lancedb_client.open_table(table_name)
        query = f"entities contains '{entity_label}'"
        results = table.search(query, limit=100)
        pandas_df = results.to_pandas()
        daft_df = DataFrame.from_pandas(pandas_df)
        return daft_df

# Interface for Document Corpus
class DocumentCorpus: 
    schema: pa.Schema = document_corpus_schema
    storage_manager: StorageManager = StorageManager()
    

    def __init__(self, id: str, name: str, type: str, size: int, location: str, metadata: Dict[str, str], content: str, embedding: List[float]):
        self.id = id
        self.name = name
        self.type = type
        self.size = size
        self.location = location
        self.metadata = metadata
        self.content = content
        self.embedding = embedding
        
        