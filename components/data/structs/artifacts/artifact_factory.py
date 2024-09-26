import daft
from typing import List, Dict, Any, Optional, Callable
import os
import mimetypes
import hashlib
from ulid import ULID
from gamma.components.data.structs.artifacts.artifact import Artifact

class ArtifactFactory:
    def __init__(self, schema: Dict[str, Any], df: Optional[daft.DataFrame] = None):
        if df is None:
            empty_data = {field.name: field.type for field in self.schema.to_pyarrow_schema().fields}
            self.df = daft.from_pydict(empty_data, schema=self.schema)
            if files:
                self.df = self.from_files(files, uri_prefix, self.io_config)
        else:
            self.df = self.from_files(files, uri_prefix, self.io_config)

        self.df = self.validate_schema(self.df)

    def from_dataframe(df: daft.DataFrame) -> 'Artifact':
        """
        Create an Artifact instance from a Daft DataFrame.
        
        Args:
            df (daft.DataFrame): The DataFrame containing artifact data.
        
        Returns:
            Artifact: An instance of Artifact.
        """
        return Artifact(df=df)

    def from_files(
        self,
        files: List[str],
        custom_prefix: Optional[str] = "",
        io_config: Optional[IOConfig] = None,
    ) -> daft.DataFrame:
        """
        Create an Artifact instance from a list of files.
        
        Args:
            files (List[str]): List of file paths.
            custom_prefix (Optional[str]): Custom prefix for the artifact URI.
            io_config (Optional[IOConfig]): I/O configuration.
        
        Returns:
            daft.DataFrame: A DataFrame containing the artifact data.
        """
        rows = []
        for file in files:
            with open(file, "rb") as f:
                content = f.read()

            ulid_id = ULID()
            file_id = str(ulid_id)
            now = ulid_id.datetime
            file_name = os.path.basename(file)
            file_name = file_name.replace(" ", "_")
            artifact_uri_prefix = self.get_artifact_uri_prefix(custom_prefix, io_config)
            artifact_uri = f"{artifact_uri_prefix}{file_id}__{file_name}"

            new_row = {
                "id": file_id,
                "type": self.obj_type,
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

        df = daft.from_pylist(rows, schema=self.schema)
        df.collect()
        df = self.validate_schema(df)
        return df

    @staticmethod
    def compose(artifacts: List['Artifact']) -> 'Artifact':
        """
        Compose multiple Artifact instances into a single Artifact.
        
        Args:
            artifacts (List[Artifact]): List of Artifact instances to compose.
        
        Returns:
            Artifact: A new Artifact instance composed from the input artifacts.
        """
        
        combined_df = daft.([artifact.df for artifact in artifacts])
        return Artifact(df=combined_df)

    @staticmethod
    def map(artifact: 'Artifact', func: Callable[[daft.DataFrame], daft.DataFrame]) -> 'Artifact':
        """
        Apply a mapping function to the Artifact's DataFrame.
        
        Args:
            artifact (Artifact): The input Artifact.
            func (Callable[[daft.DataFrame], daft.DataFrame]): The mapping function.
        
        Returns:
            Artifact: A new Artifact with the mapping function applied.
        """
        mapped_df = func(artifact.df)
        return Artifact(df=mapped_df)

    @staticmethod
    def flatmap(artifact: 'Artifact', func: Callable[[daft.DataFrame], List[daft.DataFrame]]) -> List['Artifact']:
        """
        Apply a flatmap function to the Artifact's DataFrame.
        
        Args:
            artifact (Artifact): The input Artifact.
            func (Callable[[daft.DataFrame], List[daft.DataFrame]]): The flatmap function.
        
        Returns:
            List[Artifact]: A list of new Artifacts resulting from the flatmap operation.
        """
        dataframes = func(artifact.df)
        return [Artifact(df=df) for df in dataframes]