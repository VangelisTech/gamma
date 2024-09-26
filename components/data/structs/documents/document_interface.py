# Define Enums and Pydantic Models
import pyarrow as pa


content_type_enum = pa.map_(pa.string(), pa.string())
pa.table([], schema=content_type_enum)

# Base content node fields
base_content_node_fields = [
    pa.field("node_id", pa.string()),
    pa.field("content_type", content_type_enum),
    pa.field("metadata", pa.map_(pa.string(), pa.string()))  # Simplified metadata as string key-value pairs
]

# Text node fields
text_node_fields = base_content_node_fields + [
    pa.field("text", pa.string())
]

# Image node fields
image_node_fields = base_content_node_fields + [
    pa.field("image_data", pa.binary()),
    pa.field("width", pa.int32()),
    pa.field("height", pa.int32())
]

# Audio node fields
audio_node_fields = base_content_node_fields + [
    pa.field("duration", pa.float32()),
    pa.field("sample_rate", pa.int32())
]

# Video node fields
video_node_metadata_fields = [
    pa.field("duration", pa.float32()),
    pa.field("width", pa.int32()),
    pa.field("height", pa.int32()),
    pa.field("frame_rate", pa.float32())
]

# Edge fields
edge_fields = [
    pa.field("edge_id", pa.string()),
    pa.field("from_node", pa.string()),
    pa.field("to_node", pa.string()),
    pa.field("metadata", pa.map_(pa.string(), pa.string()))  # Simplified metadata as string key-value pairs
]

content_node_schema = pa.schema(content_node_fields)
edge_schema = pa.schema(edge_fields)




class Document(BaseModel):
    """
    A Multimodal Document made up of content nodes. 
    
    Supported Types Include:
        - Text
        - Image
        - Audio
        - Video
        - PDF
        - Markdown
        - Code (ie python, sql, HTML, typescript, etc)
    """
    document_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    nodes: List[ContentNode]
    edges: List[Edge]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    def get_nodes_by_type(self, content_type: ContentType) -> List[ContentNode]:
        return [node for node in self.nodes if node.content_type == content_type]
    
    def get_edges_between(self, from_node: str, to_node: str) -> List[Edge]:
        return [
            edge for edge in self.edges
            if edge.from_node == from_node and edge.to_node == to_node
        ]
    
    def add_node(self, node: ContentNode):
        self.nodes.append(node)
    
    def add_edge(self, edge: Edge):
        self.edges.append(edge)