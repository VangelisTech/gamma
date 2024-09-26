import pyarrow as pa
from datetime import datetime

message_create_fields = [
    pa.field("content", pa.string(), metadata={"description": "Content of the message"}),
    pa.field("sender", pa.string(), metadata={"description": "Sender of the message"})
]

message_edit_fields = [
    pa.field("content", pa.string(), metadata={"description": "Updated content of the message"})
]

branch_response_fields = [
    pa.field("branch_name", pa.string(), metadata={"description": "Name of the branch"}),
    pa.field("commit_id", pa.string(), metadata={"description": "ID of the commit"}),
    pa.field("history", pa.list_(pa.struct([
        pa.field("id", pa.string()),
        pa.field("content", pa.string()),
        pa.field("sender", pa.string()),
        pa.field("timestamp", pa.string())
    ])), metadata={"description": "List of history items"})
]

branch_create_fields = [
    pa.field("new_branch_id", pa.string(), metadata={"description": "ID of the new branch"}),
    pa.field("commit_id", pa.string(), metadata={"description": "ID of the commit"})
]

history_item_fields = [
    pa.field("id", pa.string(), metadata={"description": "ID of the history item"}),
    pa.field("content", pa.string(), metadata={"description": "Content of the history item"}),
    pa.field("sender", pa.string(), metadata={"description": "Sender of the history item"}),
    pa.field("timestamp", pa.string(), metadata={"description": "Timestamp of the history item"})
]

message_response_fields = [
    pa.field("id", pa.string(), metadata={"description": "ID of the message"}),
    pa.field("content", pa.string(), metadata={"description": "Content of the message"}),
    pa.field("sender", pa.string(), metadata={"description": "Sender of the message"}),
    pa.field("timestamp", pa.string(), metadata={"description": "Timestamp of the message"}),
    pa.field("commit_id", pa.string(), metadata={"description": "ID of the commit"}),
    pa.field("history", pa.list_(pa.struct(history_item_fields)), metadata={"description": "List of history items"})
]

post_create_fields = [
    pa.field("title", pa.string(), metadata={"description": "Title of the post"}),
    pa.field("content", pa.string(), metadata={"description": "Content of the post"}),
    pa.field("author", pa.string(), metadata={"description": "Author of the post"}),
    pa.field("timePoint", pa.string(), metadata={"description": "Time point of the post"}),
    pa.field("tags", pa.string(), metadata={"description": "Tags associated with the post"}),
    pa.field("parent_id", pa.string(), nullable=True, metadata={"description": "ID of the parent post, if any"}),
    pa.field("version", pa.float32(), metadata={"description": "Version of the post as a float32"})
]

comment_create_fields = [
    pa.field("content", pa.string(), metadata={"description": "Content of the comment"}),
    pa.field("author", pa.string(), metadata={"description": "Author of the comment"})
]

vote_fields = [
    pa.field("type", pa.string(), metadata={"description": "Type of vote ('up' or 'down')"})
]

post_fields = [
    pa.field("id", pa.string(), metadata={"description": "ID of the post"}),
    pa.field("title", pa.string(), nullable=True, metadata={"description": "Title of the post"}),
    pa.field("content", pa.string(), nullable=True, metadata={"description": "Content of the post"}),
    pa.field("author", pa.string(), metadata={"description": "Author of the post"}),
    pa.field("timestamp", pa.string(), metadata={"description": "Timestamp of the post"}),
    pa.field("upvotes", pa.int32(), metadata={"description": "Number of upvotes"}),
    pa.field("downvotes", pa.int32(), metadata={"description": "Number of downvotes"}),
    pa.field("comments", pa.int32(), metadata={"description": "Number of comments"}),
    pa.field("branches", pa.int32(), metadata={"description": "Number of branches"}),
    pa.field("timePoint", pa.string(), nullable=True, metadata={"description": "Time point of the post"}),
    pa.field("tags", pa.string(), nullable=True, metadata={"description": "Tags associated with the post"}),
    pa.field("parent_id", pa.string(), nullable=True, metadata={"description": "ID of the parent post, if any"}),
    pa.field("version", pa.float32(), metadata={"description": "Version of the post as a float32"})
]


conversation_fields = [
    pa.field("id", pa.string(), metadata={"description": "ID of the conversation"}),
    pa.field("messages", pa.list_(pa.struct(message_response_fields)), metadata={"description": "List of messages in the conversation"}),
    pa.field("branches", pa.list_(pa.string()), metadata={"description": "List of branch IDs"}),
    pa.field("current_branch", pa.string(), metadata={"description": "Current active branch ID"}),
    pa.field("created_at", pa.timestamp('ms'), metadata={"description": "Timestamp of conversation creation"}),
    pa.field("updated_at", pa.timestamp('ms'), metadata={"description": "Timestamp of last update"}),
]

conversation_schema = pa.schema(conversation_fields)



