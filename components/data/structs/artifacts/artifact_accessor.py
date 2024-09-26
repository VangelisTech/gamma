from __future__ import annotations

import hashlib
import gzip
import mimetypes
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import daft
from daft import col
from daft.io import IOConfig
from ulid import ULID

from ..utils.base_utils import get_artifact_uri_prefix, get_by_id

class ArtifactAccessor:
    """
    Accessor class for managing artifacts.
    Provides methods for CRUD operations on artifacts using upsert with sorted bucket merge join.
    """

    def __init__(
        self,
        df: daft.DataFrame,
        io_config: Optional[IOConfig] = None,
        uri_prefix: Optional[str] = "",
    ):
        self.df = df
        self.io_config = io_config
        self.uri_prefix = uri_prefix
    def get(self, query_df: daft.DataFrame) -> daft.DataFrame:
        """
        Get the delta from deltacat. 

        Args:
            df (daft.DataFrame): The DataFrame to compare against.

        Returns:
            daft.DataFrame: The delta between the two DataFrames.
        """
        return self.df.join(df, on="id", how="left_anti")

    def upsert(self, files: List[str]) -> daft.DataFrame:
        """
        Upsert artifacts from files using sorted bucket merge join.

        Args:
            files (List[str]): List of file paths to upsert as artifacts.

        Returns:
            daft.DataFrame: Updated DataFrame with upserted artifacts.
        """
        rows = []
        for file in files:
            with open(file, "rb") as f:
                content = f.read()

            ulid_id = ULID()
            file_id = str(ulid_id)
            now = ulid_id.datetime
            file_name = os.path.basename(file).replace(" ", "_")
            artifact_uri_prefix = get_artifact_uri_prefix(self.uri_prefix, self.io_config)
            artifact_uri = f"{artifact_uri_prefix}{file_id}__{file_name}"

            new_row = {
                "id": file_id,
                "type": "Artifact",
                "created_at": now,
                "updated_at": now,
                "inserted_at": now,
                "name": file_name,
                "artifact_uri": artifact_uri,
                "payload": content,
                "extension": os.path.splitext(file)[1][1:],
                "mime_type": mimetypes.guess_type(file)[0] or "application/x-gzip",
                "version": "1.0",
                "size_bytes": len(content),
                "checksum": hashlib.md5(content).hexdigest(),
            }
            rows.append(new_row)

                new_df = daft.from_pylist(rows, schema=self.df.schema)
        
        # Perform the upsert using a left anti join followed by a union
        existing_ids = self.df.join(
            new_df,
            on="id",
            how="left_anti"
        )
        
        self.df = existing_ids.union(new_df)


        return self.df

    def get(self, id: Union[str, List[str]]) -> Optional[daft.DataFrame]:
        """
        Retrieve artifact(s) by ID.

        Args:
            id (Union[str, List[str]]): ID or list of IDs of artifacts to retrieve.

        Returns:
            Optional[daft.DataFrame]: DataFrame containing the requested artifact(s) or None if not found.
        """
        return get_by_id(self.df, id)

    def update(self, id: str, updates: Dict[str, Any]) -> daft.DataFrame:
        """
        Update an artifact by ID using upsert with sorted bucket merge join.

        Args:
            id (str): ID of the artifact to update.
            updates (Dict[str, Any]): Dictionary of fields to update and their new values.

        Returns:
            daft.DataFrame: Updated DataFrame.
        """
        updates["updated_at"] = datetime.now()
        update_df = daft.from_pydict({**{"id": id}, **updates}, schema=self.df.schema)
        
        # Sort both DataFrames by 'id' for the merge join
        sorted_df = self.df.sort("id")
        sorted_update_df = update_df.sort("id")

        # Perform the upsert using a sorted bucket merge join
        self.df = sorted_df.join(
            sorted_update_df,
            on="id",
            how="left",
            algorithm="sorted_bucket_merge"
        ).select(
            col("id"),
            *[col(field).coalesce(sorted_update_df[field], sorted_df[field]) for field in self.df.schema.names if field != "id"]
        )

        return self.df

    def delete(self, id: Union[str, List[str]]) -> daft.DataFrame:
        """
        Delete artifact(s) by ID.

        Args:
            id (Union[str, List[str]]): ID or list of IDs of artifacts to delete.

        Returns:
            daft.DataFrame: Updated DataFrame with artifacts removed.
        """
        if isinstance(id, str):
            id = [id]
        self.df = self.df.filter(~col("id").is_in(id))
        return self.df


