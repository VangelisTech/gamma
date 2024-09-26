
import pyarrow as pa

from ..base.base_schema import base_schema

artifact_fields = [
    pa.field(
        "name",
        pa.string(),
        nullable=False,
        metadata={"description": "Name of the artifact"},
    ),
    pa.field(
        "artifact_uri",
        pa.string(),
        nullable=False,
        metadata={"description": "URI where the artifact is stored"},
    ),
    pa.field(
        "payload",
        pa.binary(),
        nullable=False,
        metadata={"description": "Gzipped content of the artifact"},
    ),
    pa.field(
        "extension",
        pa.string(),
        nullable=False,
        metadata={"description": "File extension of the artifact"},
    ),
    pa.field(
        "mime_type",
        pa.string(),
        nullable=False,
        metadata={"description": "MIME type of the artifact"},
    ),
    pa.field(
        "version",
        pa.string(),
        nullable=False,
        metadata={"description": "Version of the artifact"},
    ),
    pa.field(
        "size_bytes",
        pa.int64(),
        nullable=False,
        metadata={"description": "Size of the artifact in bytes"},
    ),
    pa.field(
        "checksum",
        pa.string(),
        nullable=False,
        metadata={"description": "MD5 checksum of the artifact"},
    ),
]

artifact_schema = base_schema.append(artifact_fields)

print(artifact_schema)