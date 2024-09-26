from __future__ import annotations

import hashlib
import gzip
import mimetypes
import os
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional

import daft
import pyarrow as pa
import pandas as pd
from daft import col
from daft.io import IOConfig, S3Config
from daft.context import set_runner_ray
from ulid import ULID
import lancedb 

from .artifact_schema import artifact_schema

class Artifact:
    """
    Dataframe model for managing artifacts.
    Utilizes Daft's DataFrame for efficient data manipulation and scalability.
    Integrates with Ray for distributed processing.
    """

    schema: ClassVar[daft.Schema] = daft.Schema(artifact_schema)
    obj_type: ClassVar[str] = "Artifact"

    def __init__(
        self,
        files: Optional[List[str]] = None,
        df: Optional[daft.DataFrame] = None,
        io_config: Optional[IOConfig] = IOConfig(),
        uri_prefix: Optional[str] = "",
    ):
        self.io_config = io_config
        self.uri_prefix = uri_prefix
        self.df = df

        self.accessor = ArtifactAccessor(df, io_config, uri_prefix)
        self.factory = ArtifactFactory(schema=self.schema, )
        
        

    

    

    def add_files(
        self,
        files: List[str],
        uri_prefix: Optional[str] = "",
        io_config: Optional[IOConfig] = None,
    ) -> None:
        """
        Adds new files to the Artifact DataFrame.

        Parameters:
            files (List[str]): List of file paths to add.
            uri_prefix (Optional[str]): Prefix for the artifact URI.
        """
        new_df = self._load_files(files, uri_prefix, io_config)
        self.df = self.df.concat(new_df).collect()



    def verify_checksum(self, artifact_id: str) -> bool:
        """
        Verifies the checksum of an artifact.

        Parameters:
            artifact_id (str): The ULID of the artifact.

        Returns:
            bool: True if checksum matches, False otherwise.
        """
        artifact = self.get_artifact_by_id(artifact_id)
        if not artifact:
            return False
        actual_payload = gzip.decompress(artifact["payload"])
        actual_checksum = hashlib.md5(actual_payload).hexdigest()
        return actual_checksum == artifact["checksum"]


    
    @classmethod
    def __repr__(cls) -> str:
        return f"<{cls.__name__} with {cls.df.count()} records>"

    def __str__(self) -> str:
        return self.__repr__()
