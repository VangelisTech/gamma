"""Defines the Agent class and related types and functions."""

from __future__ import annotations
import asyncio
from typing import Protocol, List, Any, Optional, Union, Dict
from enum import Enum
from datetime import datetime
import logging
from altair import Vector2string
from daft import DataFrame, Schema, DataType, lit, col
import daft
import lancedb
from lancedb.embeddings import get_registry
import numpy as np
from pydantic import BaseModel, Field
import pyarrow as pa
from ray.serve.handle import DeploymentResponse
import uuid
import os
from ulid import ULID

from beta.data.obj.artifacts.artifact_obj import Artifact
from beta.models.obj.base_language_model import LLM
from beta.models.serve.engines.base_engine import BaseEngine
from beta.models.serve.engines.openai_engine import OpenAIEngine, OpenAIEngineConfig
from beta.data.obj.result import Result
from beta.data.obj.memory import Memory, MemoryStore
from beta.tools.git_api import GitAPI

from beta.memory.lancedb_store import LanceDBMemoryStore
from beta.git.git_api import GitAPI


class AgentStatus(str, Enum):
    """
    Enum representing the status of an agent.
    """

    INIT = "initializing"
    IDLE = "idle"
    PENDING = "pending"
    WAITING = "waiting"
    PROCESSING = "processing"


class ToolInterface(Protocol):
    """
    Protocol representing a tool that can be executed by an agent.
    """

    def execute(self, data: Any) -> Result[Any, Exception]: ...


class Prompt(BaseModel):
    """
    Pydantic model representing a prompt.
    """

    content: str


class Message(BaseModel):
    """
    Pydantic model representing a message.
    """

    role: str
    content: str


class SchemaWrapper(BaseModel):
    """
    Custom type for the schema field.
    """

    schema: Any

    class Config:
        """
        Pydantic configuration for the SchemaWrapper class.
        """

        arbitrary_types_allowed = True

    @classmethod
    def __get_validators__(cls):
        """
        Get validators for the SchemaWrapper class.
        """
        yield cls.validate

    @classmethod
    def validate(cls, value):
        """
        Validate the schema.
        """
        if not isinstance(value, Schema):
            raise ValueError("Must be a Schema instance")
        return cls(schema=value)


class Metadata(BaseModel):
    """
    Pydantic model representing metadata for an agent.
    """

    schema: SchemaWrapper
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    provenance: Optional[str] = None

    def serialize(self, serializer):
        """
        Serialize the metadata.
        """
        # Implement serialization logic if needed


class Agent:
    """
    Orchestrates the execution of models and tools.

    Attributes:
        name (str): The name of the agent.
        model (OpenAIEngine): Instance of the model engine.
        embedding (OpenAIEngine): Instance of the embedding engine.
        stt (OpenAIEngine): Instance of the speech-to-text engine.
        tools (List[ToolInterface]): List of tools the agent can utilize.
        metadata (Metadata): Metadata information for the agent.
        engine (BaseEngine): Inference engine.
        artifacts (List[DataFrame]): List of artifacts.
        memory (List[DataFrame]): List of memory.
        is_async (bool): Use asynchrony (i.e. for streaming).
    """

    def __init__(
        self,
        name: str,
        model: LLM = LLM(
            "openai/gpt-4o", engine=OpenAIEngine(config=OpenAIEngineConfig())
        ),
        embedding: LLM = LLM(
            "openai/text-embedding-3-large",
            engine=OpenAIEngine(config=OpenAIEngineConfig()),
        ),
        stt: LLM = LLM(
            "openai/whisper-1", engine=OpenAIEngine(config=OpenAIEngineConfig())
        ),
        tools: Optional[List[ToolInterface]] = None,
        metadata: Optional[Metadata] = None,
        engine: Optional[BaseEngine] = None,
        artifacts: Optional[List[DataFrame]] = None,
        memory: Optional[List[DataFrame]] = None,
        memory_db_uri: Optional[str] = "lancedb",
        git_repo_path: Optional[str] = None,
        is_async: bool = False,
        memory_store=None,
        git_api=None,
    ):
        self.name = name
        self.model = model
        self.embedding = embedding
        self.stt = stt
        self.tools = tools or []
        self.metadata = metadata
        self.engine = engine
        self.is_async = is_async
        self.status = AgentStatus.INIT
        self.memory = memory
        self.artifacts = artifacts

        self.lancedb_client = lancedb.connect(memory_db_uri)
        logging.info(f"Lancedb client connected: {self.lancedb_client}")

        # Initialize the embedding function
        self.embedding_func = (
            get_registry()
            .get("sentence-transformers")
            .create(name="BAAI/bge-small-en-v1.5")
        )
        self.memory_store = memory_store or LanceDBMemoryStore(memory_db_uri)
        self.git_api = GitAPI(memory_db_uri)

    async def process(
        self,
        prompt: Optional[Prompt | str] = None,
        messages: Optional[List[Message]] = None,
        speech: Optional[
            Union[np.ndarray, List[float], List[np.ndarray], List[List[float]]]
        ] = None,
    ) -> str:
        logging.info(f"Agent {self.name} called with prompt: {prompt}")
        self.status = AgentStatus.PROCESSING
        logging.info(f"Agent status set to {self.status}")

        if self.memory:
            for df in self.memory:
                logging.info(f"Memory: {df}")
                messages.append(Message(role="user", content=f"Memory: {df}"))

        if speech is not None:
            logging.info("Processing speech input")
            stt_response: DeploymentResponse = await self.stt.generate.remote(speech)
            transcription = await stt_response
            logging.info(f"Speech transcribed: {transcription}")
            if prompt:
                prompt.content += f"\nTranscription: {transcription}"
            elif messages:
                messages.append(
                    Message(role="system", content=f"Transcription: {transcription}")
                )

        logging.info("Sending request to model")
        if isinstance(prompt, Prompt):
            prompt = prompt.content

        logging.info(f"Prompt: {prompt}")

        if messages is None:
            messages = [
                {"role": "system", "content": "You are a helpful assistant"},
                {"role": "user", "content": prompt},
            ]

        if self.artifacts:
            for artifact in self.artifacts:
                # store in lancedb
                await self.store_in_lancedb(artifact)

        llm_response: DeploymentResponse = await self.model.generate(
            input=prompt, messages=messages
        )
        logging.info("Awaiting model response")
        logging.info(f"Model response received: {llm_response}")

        self.status = AgentStatus.IDLE
        logging.info(f"Agent status set to {self.status}")
        return llm_response

    async def store_in_lancedb(self, artifact: Artifact):
        vector_table_name = "artifacts_vectors"
        try:
            logging.info(f"Storing artifact in LanceDB: {artifact}")
            # Get the DataFrame from the Artifact
            artifact_df = artifact.df
            # gzip unzip the payload column
            import gzip

            def unzip_payload(zipped_payload: DataType.string):
                try:
                    return gzip.decompress(zipped_payload)

                except Exception as e:
                    logging.warning(f"Failed to unzip payload: {e}")
                    return zipped_payload  # Return original payload if unzipping fails

            unzip_payloads = unzip_payload(artifact_df.select("payload"))

            @daft.udf(
                return_dtype=DataType.embedding(
                    DataType.float32(), size=self.embedding_func.dimension
                )
            )
            def get_embeddings(payload: DataType.string):
                return self.embedding_func.compute_source_embeddings([payload])

            # Vectorize all payloads using the embedding function
            vectors_df = unzip_payloads.with_column(
                "vector", get_embeddings(unzip_payloads.select("payload"))
            )
            vectors_df.collect()
            # Check the dimensions of the vectors
            vector_dim = len(vectors_df.select("vector").to_pylist()[0])
            logging.info(
                f"Vector dimension: {vector_dim}, expected: {self.embedding_func.dimension}"
            )

            # Create a PyArrow table with the vectors
            pa_table = vectors_df.to_arrow()

            # Check if the vector table exists
            if vector_table_name not in self.lancedb_client.table_names():
                # Create a new vector table
                lancedb_table = self.lancedb_client.create_table(
                    vector_table_name, pa_table
                )
                logging.info(
                    f"Created new vector table '{vector_table_name}' in LanceDB."
                )
            else:
                # Append to the existing vector table
                lancedb_table = self.lancedb_client.open_table(vector_table_name)
                lancedb_table.add(pa_table)
                logging.info(
                    f"Appended to existing table '{vector_table_name}' in LanceDB."
                )

            return lancedb_table
        except Exception as e:
            logging.error(f"Failed to store artifact vectors in LanceDB: {e}")
            raise

    async def add_message(
        self, conversation_id: str, content: str, sender: str
    ) -> Dict:
        self.initialize_lancedb()  # Ensure the dataset is initialized
        message = {
            "id": str(uuid.uuid4()),
            "content": content,
            "sender": sender,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self.memory_store.add_memory(conversation_id, message)

        # Commit the new message to the current branch
        commit_id = self.git_api.commit_memory(message["content"])

        # Retrieve the updated conversation history
        history = await self.get_conversation_history(conversation_id)

        # Include commit_id and history in the response
        message["commit_id"] = commit_id
        message["history"] = history

        return message

    async def search_memories(self, query: str, limit: int = 5) -> List[Memory]:
        query_vector = await self.model.engine.get_embedding(query)
        return self.memory_store.search_memories(query_vector, limit)

    def create_memory_branch(self, branch_name: str):
        self.git_api.create_branch(branch_name)

    def switch_memory_branch(self, branch_name: str):
        self.git_api.switch_branch(branch_name)
        self._reload_memories()

    def time_travel(self, commit_hash: str):
        self.git_api.checkout_commit(commit_hash)
        self._reload_memories()

    def _reload_memories(self):
        self.memory_store.reload_memories()

    async def execute_pipeline(self, input_data: Any) -> Result[Any, Exception]:
        """
        Executes the agent's processing pipeline.

        Args:
            input_data (Any): The initial input data for the pipeline.

        Returns:
            Result[Any, Exception]: The final output wrapped in a Result monad.
        """
        print(f"Agent {self.name} is executing pipeline.")
        result = Result.Ok(input_data)

        for tool in self.tools:
            result = result.bind(tool.execute)
            if result.is_error:
                print(f"Pipeline halted due to error: {result.value}")
                break

        if self.engine and not result.is_error:
            try:
                inference_result = self.engine.generate(result.value)
                result = Result.Ok(inference_result)
            except Exception as e:
                result = Result.Error(e)

        # Add memory of the execution
        await self.add_message(f"Executed pipeline with input: {input_data}")

        return result

    def get_status(self) -> AgentStatus:
        return self.status

    async def infer_action(
        self, prompt: Prompt, actions: List[str], tools: List[ToolInterface]
    ) -> str:
        """
        Infer the action to perform based on the prompt and tools.
        """

        action_prompt = f"{prompt.content}\n\nAvailable actions: {', '.join(actions)}\n\nBased on the prompt, which action should be taken?"

        action_response: DeploymentResponse = await self.model.generate.remote(
            prompt=Prompt(content=action_prompt)
        )
        inferred_action = await action_response

        # Clean up the response to match one of the available actions
        inferred_action = inferred_action.strip().lower()
        if inferred_action not in [action.lower() for action in actions]:
            # If the inferred action doesn't match any available action, default to the first action
            print(
                f"Inferred action '{inferred_action}' not found in available actions. Defaulting to '{actions[0]}'."
            )
            inferred_action = actions[0]
        else:
            # Find the original action with correct capitalization
            inferred_action = next(
                action for action in actions if action.lower() == inferred_action
            )

        print(f"Inferred action: {inferred_action}")

        # Add memory of the inferred action
        await self.add_message(
            f"Inferred action: {inferred_action} for prompt: {prompt.content}"
        )

        return inferred_action

    def choose_tool(self, task: str, tools: List[ToolInterface]) -> ToolInterface:
        """
        Choose the tool to use based on the tools.
        """
        # TODO: Implement tool selection logic
        return next((tool for tool in tools if tool.can_handle(task)), None)

    async def get_conversation_history(self, conversation_id: str) -> List[Dict]:
        try:
            memories = self.memory_store.get_conversation_memories(conversation_id)
            return [
                {
                    "id": m["id"],
                    "content": m["content"],
                    "sender": m["sender"],
                    "timestamp": m["timestamp"],
                }
                for m in memories
            ]
        except Exception as e:
            print(f"Error fetching conversation history: {e}")
            return []

    async def get_conversation_branches(
        self, conversation_id: str
    ) -> List[Dict[str, Any]]:
        try:
            heads = self.git_api.get_branches()
            for head in heads:
                print(f"Branch: {head.name}")
            return [
                {
                    "name": branch.name,
                    "commit_id": branch.commit.hexsha,
                    "last_modified": branch.commit.committed_datetime.isoformat(),
                }
                for branch in heads
            ]
        except Exception as e:
            print(f"Error retrieving conversation branches: {e}")
            raise

    async def create_conversation_branch(
        self, conversation_id: str, new_branch_id: str, commit_id: str
    ):
        try:
            branch_name, new_commit_id = await self.git_api.create_branch_async(
                new_branch_id, commit_id
            )
            if branch_name is None or new_commit_id is None:
                raise Exception(
                    "Failed to create branch. The repository might be empty or the commit ID might be invalid."
                )

            # Create an initial commit if the branch is new
            if not self.memory_store.get_conversation_memories(branch_name):
                self.memory_store.add_memory(
                    branch_name,
                    {
                        "id": str(uuid.uuid4()),
                        "content": f"Branch created: {branch_name}",
                        "sender": "system",
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )

            return {
                "branch_name": branch_name,
                "commit_id": new_commit_id,
                "history": await self.get_conversation_history(branch_name),
            }
        except Exception as e:
            print(f"Error creating branch: {e}")
            raise

    async def get_latest_commit_id(self, conversation_id: str) -> str:
        # Implement logic to retrieve the latest commit ID for the given conversation
        # For simplicity, we'll return the latest commit in the main branch
        try:
            commit = self.git_api.repo.head.commit.hexsha
            return commit
        except Exception as e:
            print(f"Error retrieving latest commit ID: {e}")
            raise

    async def switch_conversation_branch(self, branch_name: str):
        self.switch_memory_branch(branch_name)
        return await self.get_conversation_history(branch_name)

    async def merge_conversation_branches(self, source_branch: str, target_branch: str):
        self.git_api.merge_branches(source_branch, target_branch)
        self._reload_memories()
        return await self.get_conversation_history(target_branch)

    async def get_conversation_page(
        self, conversation_id: str, skip: int, limit: int
    ) -> Dict:
        memories = self.memory_store.get_conversation_memories(conversation_id)
        if not memories:
            # If no memories exist, create an initial commit
            self.git_api.create_branch(conversation_id)
            self.memory_store.add_memory(
                conversation_id,
                {
                    "id": str(uuid.uuid4()),
                    "content": "Welcome to the conversation!",
                    "sender": "system",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
        memories = self.memory_store.get_conversation_memories(conversation_id)
        paginated_memories = memories[skip : skip + limit]
        return {
            "id": conversation_id,
            "messages": [
                {
                    "id": m["id"],
                    "content": m["content"],
                    "sender": m["sender"],
                    "timestamp": m["timestamp"],
                }
                for m in paginated_memories
            ],
            "total_messages": len(memories),
        }

    async def get_conversation_history(self, conversation_id: str) -> List[Dict]:
        try:
            memories = self.memory_store.get_conversation_memories(conversation_id)
            return [
                {
                    "id": m["id"],
                    "content": m["content"],
                    "sender": m["sender"],
                    "timestamp": m["timestamp"],
                }
                for m in memories
            ]
        except Exception as e:
            print(f"Error fetching conversation history: {e}")
            return []

    async def edit_message(
        self, conversation_id: str, message_id: str, new_content: str
    ) -> Dict:
        try:
            # Get the conversation
            conversation = self.memory_store.get_conversation_memories(conversation_id)

            # Find the message to edit
            message_to_edit = next(
                (m for m in conversation if m["id"] == message_id), None
            )
            if not message_to_edit:
                raise ValueError(
                    f"Message {message_id} not found in conversation {conversation_id}"
                )

            # Update the message
            message_to_edit["content"] = new_content
            message_to_edit["timestamp"] = datetime.utcnow().isoformat()

            # Update the memory store
            self.memory_store.update_conversation_memories(
                conversation_id, conversation
            )

            # Commit the change
            commit_id = self.git_api.commit_memory(f"Edit message {message_id}")

            # Return the edited message
            return {
                "message_id": message_id,
                "content": new_content,
                "role": message_to_edit["sender"],
                "timestamp": message_to_edit["timestamp"],
                "commit_id": commit_id,
            }
        except Exception as e:
            print(f"Error editing message: {e}")
            raise

    async def process_message(
        self, conversation_id: str, content: str, sender: str
    ) -> Dict:
        try:
            # Add the user's message to the conversation
            user_message = await self.add_message(conversation_id, content, sender)

            # Generate a response from the agent
            response_content = await self.generate_response(content)
            agent_message = await self.add_message(
                conversation_id, response_content, "ai"
            )

            return agent_message
        except Exception as e:
            print(f"Error processing message: {e}")
            raise

    async def generate_response(self, content: str) -> str:
        try:
            response = await self.engine.generate(prompt=content)
            return response
        except Exception as e:
            print(f"Error generating response: {e}")
            raise

    def initialize_lancedb(self):
        try:
            # Check if the dataset exists
            if "memories" not in self.lancedb_client.table_names():
                # Create the dataset if it doesn't exist
                schema = pa.schema([
                    pa.field("conversation_id", pa.string()),
                    pa.field("timestamp", pa.float64()),
                    pa.field("user", pa.string()),
                    pa.field("message", pa.string()),
                    pa.field("vector", pa.list_(pa.float32()))
                ])
                self.lancedb_client.create_table("memories", schema)
                print("Created LanceDB dataset 'memories'.")
            else:
                print("LanceDB dataset 'memories' already exists.")
        except Exception as e:
            print(f"Error initializing LanceDB: {e}")
            raise

    async def create_post(self, title: str, content: str, author: str, time_point: str, tags: str, parent_id: str = None, version: float = 1.0):
        try:
            new_post = {
                "id": str(uuid.uuid4()),
                "title": title,
                "content": content,
                "author": author,
                "timePoint": time_point,
                "tags": tags,
                "timestamp": datetime.now().isoformat(),  # Store as ISO format string
                "upvotes": 0,
                "downvotes": 0,
                "comments": [],
                "branches": [],
                "parent_id": parent_id,
                "version": np.float32(version)
            }
            self.memory_store.add_post(new_post)
            print(f"Created new post: {new_post}")
            
            # Generate agent response
            agent_response = await self.generate_response_post(new_post)
            
            return new_post, agent_response
        except Exception as e:
            print(f"Error creating post: {e}")
            raise

    async def generate_response_post(self, original_post: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Fetch recent posts as context
            recent_posts = self.memory_store.get_recent_posts(limit=5)
            
            # Prepare context for the AI
            context = f"Recent posts:\n"
            for post in recent_posts:
                context += f"- Title: {post.get('title', 'No title')}\n  Content: {post.get('content', 'No content')}\n\n"
            
            context += f"Respond to this post:\nTitle: {original_post.get('title', 'No title')}\nContent: {original_post.get('content', 'No content')}\n"
            
            # Generate response using the AI engine
            response_content = await self.engine.generate(context)
            
            # Create a new post as the agent's response
            response_post = {
                "id": str(uuid.uuid4()),
                "title": f"Re: {original_post.get('title', 'No title')}",
                "content": response_content,
                "author": "AI Agent",
                "timePoint": original_post.get('timePoint', ''),
                "tags": original_post.get('tags', ''),
                "timestamp": datetime.now().isoformat(),
                "upvotes": 0,
                "downvotes": 0,
                "comments": [],
                "branches": [],
                "parent_id": original_post['id'],
                "version": np.float32(1.0)
            }
            
            self.memory_store.add_post(response_post)
            print(f"Created agent response post: {response_post}")
            
            return response_post
        except Exception as e:
            print(f"Error generating response post: {e}")
            raise

    async def get_all_posts(self) -> List[Dict[str, Any]]:
        try:
            posts = self.memory_store.get_all_posts()
            print(f"Retrieved {len(posts)} posts")
            # Convert version to numpy.float32 for each post
            for post in posts:
                post['version'] = np.float32(post.get('version', 1.0))
            return posts
        except Exception as e:
            print(f"Error fetching posts: {e}")
            return []


__all__ = ["Agent", "AgentStatus", "ToolInterface", "Prompt", "Message", "Metadata"]
