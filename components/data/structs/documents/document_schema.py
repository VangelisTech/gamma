

token_fields = [
    pa.field("text", pa.string()),
    pa.field("lemma", pa.string()),
    pa.field("pos", pa.string()),
    pa.field("tag", pa.string()),
    pa.field("dep", pa.string()),
    pa.field("shape", pa.string())
    pa.field("is_alpha", pa.bool_()),
    pa.field("is_stop", pa.bool_()),
    pa.field("is_punct", pa.bool_()),
]

span_fields = [
    pa.field("start", pa.int32()),
    pa.field("end", pa.int32()),
    pa.field("label", pa.string()),
]

image_fields = [
    pa.field("uri", pa.string()),
    pa.field("width", pa.int32()),
    pa.field("height", pa.int32()),
]

document_fields = [
    pa.field("id", pa.string()),
    pa.field("text", pa.string()),
    pa.field("tokens", pa.list_(pa.struct(token_fields))),
    pa.field("spans", pa.list_(pa.struct(span_fields))),
    pa.field("images", pa.list_(pa.struct(image_fields))),
]

segment_fields = [
    pa.field("mask", pa.binary()),  # Binary representation of the segmentation mask
    pa.field("score", pa.float32()),  # Confidence score for the segment
    pa.field("area", pa.int32()),  # Area of the segment in pixels
    pa.field("bbox", pa.list_(pa.int32(), 4)),  # Bounding box coordinates [x, y, width, height]
    pa.field("predicted_iou", pa.float32()),  # Predicted IoU with the ground truth
    pa.field("point_coords", pa.list_(pa.float32(), 2)),  # Coordinates of the point prompt
    pa.field("stability_score", pa.float32()),  # Stability score of the segment
    pa.field("crop_box", pa.list_(pa.int32(), 4)),  # Crop box coordinates if applicable
]

image_fields.extend([
    pa.field("segments", pa.list_(pa.struct(segment_fields))),
])


content_nodedaft.Schema()

content_node_fields = [
    pa.field("node_id", pa.string(), nullable=False),
    pa.field("node_type", pa.string(), nullable=False),  
    pa.field("node_binary", pa.binary(), nullable=False), 
    pa.field("node_metadata", pa.map_(pa.string(), pa.string()), nullable=True), 
]

document_fields = [
    pa.field("doc_id", pa)
    pa.field("nodes", pa.list_(pa.struct(content_node_fields))),
    
]

class ContentNode:
    schema: pa.Schema = pa.schema(content_node_fields)

    def __init__(self, df: daft.DataFrame = ):



content_node_schema = pa.schema(content_node_fields)

df.read_lance(uri, )