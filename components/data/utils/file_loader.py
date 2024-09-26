import os
import gzip
import hashlib
import mimetypes
from typing import List, Optional, Any
from ulid import ULID
import daft
from daft import col
from datetime import datetime
from beta.data.obj.artifacts.monad.result import Result
from beta.data.obj.artifacts.components.uri_manager import URImanager

class FileLoader:
    """
    Responsible for loading files and converting them into a Daft DataFrame.
    Uses monadic Result to handle success and failure states.
    """

    def __init__(self, uri_manager: URImanager):
        self.uri_manager = uri_manager

    def load_files(
        self,
        files: List[str],
        uri_prefix: Optional[str] = "",
        io_config: Optional[Any] = None,
        obj_type: str = "Artifact"
    ) -> Result[daft.DataFrame]:
        rows = []
        for file in files:
            try:
                with open(file, "rb") as f:
                    content = f.read()

                ulid_id = ULID()
                file_id = str(ulid_id)
                now = ulid_id.datetime
                file_name = os.path.basename(file).replace(" ", "_")
                artifact_uri = self.uri_manager.get_artifact_uri(file_id, file_name, uri_prefix, io_config)

                new_row = {
                    "id": file_id,
                    "type": obj_type,
                    "created_at": now,
                    "updated_at": now,
                    "inserted_at": now,
                    "name": file_name,
                    "artifact_uri": artifact_uri,
                    "payload": content,
                    "extension": os.path.splitext(file)[1][1:],
                    "mime_type": mimetypes.guess_type(file)[0] or "application/octet-stream",
                    "version": "1.0",
                    "size_bytes": len(content),
                    "checksum": hashlib.md5(content).hexdigest(),
                }
                rows.append(new_row)
            except Exception as e:
                return Result(e, is_success=False)

        try:
            df = daft.from_pylist(rows, schema=self.uri_manager.schema_handler.schema)
            df.collect()
            return Result.pure(df)
        except Exception as e:
            return Result(e, is_success=False)