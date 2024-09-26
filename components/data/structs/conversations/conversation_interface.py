class Conversation:
    """
    Dataframe model for managing conversations.
    Utilizes Daft's DataFrame for efficient data manipulation and scalability.
    """

    schema: ClassVar[daft.Schema] = daft.Schema(conversation_schema)
    obj_type: ClassVar[str] = "Conversation"

    def __init__(
        self,
        df: Optional[daft.DataFrame] = None,
        io_config: Optional[IOConfig] = IOConfig(),
        uri_prefix: Optional[str] = "",
    ):
        self.io_config = io_config
        self.uri_prefix = uri_prefix
        self.df = df

        self.accessor = ConversationAccessor(self.df, self.io_config, self.uri_prefix)

        if df is None:
            empty_data = {field.name: field.type for field in self.schema.to_pyarrow_schema().fields}
            self.df = daft.from_pydict(empty_data, schema=self.schema)

        self.df = self.validate_schema(self.df)

    def validate_schema(self, df: daft.DataFrame) -> daft.DataFrame:
        return validate_schema(self.schema.to_pyarrow_schema(), df)

    async def create_conversation_branch(self, conversation_id: str, new_branch_id: str, latest_commit: str) -> Dict[str, Any]:
        return await self.accessor.create_conversation_branch(conversation_id, new_branch_id, latest_commit)

    async def get_conversation_history(self, conversation_id: str) -> List[Dict[str, Any]]:
        return await self.accessor.get_conversation_history(conversation_id)

    async def get_conversation_branches(self, conversation_id: str) -> List[str]:
        return await self.accessor.get_conversation_branches(conversation_id)

    async def switch_conversation_branch(self, conversation_id: str, branch_name: str) -> Dict[str, Any]:
        return await self.accessor.switch_conversation_branch(conversation_id, branch_name)

    async def merge_conversation_branches(self, conversation_id: str, source_branch: str, target_branch: str) -> Dict[str, Any]:
        return await self.accessor.merge_conversation_branches(conversation_id, source_branch, target_branch)

    async def get_conversation_page(self, conversation_id: str, skip: int, limit: int) -> List[Dict[str, Any]]:
        return await self.accessor.get_conversation_page(conversation_id, skip, limit)

    async def add_message(self, conversation_id: str, content: str, sender: str) -> Dict[str, Any]:
        return await self.accessor.add_message(conversation_id, content, sender)

    async def edit_message(self, conversation_id: str, message_id: str, content: str) -> Dict[str, Any]:
        return await self.accessor.edit_message(conversation_id, message_id, content)

    async def process_message(self, conversation_id: str, content: str, sender: str) -> Dict[str, Any]:
        return await self.accessor.process_message(conversation_id, content, sender)
