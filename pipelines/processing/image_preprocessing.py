# Define the Pipeline
def extract_text_from_images(

def annotate_texts(extracted_texts: List[Dict[str, Any]], spacy_agent: SpacyAgent.remote()) -> List[Dict[str, Any]]:
    spacy_futures = [spacy_agent.annotate_text.remote(item) for item in extracted_texts if 'text' in item]
    annotated_texts = ray.get(spacy_futures)
    return annotated_texts


def generate_summary(annotated_texts: List[Dict[str, Any]], summarization_agent: SummarizationAgent.remote()) -> List[Dict[str, Any]]:
    summarization_futures = [summarization_agent.generate_summary.remote(item) for item in annotated_texts if 'text' in item]
    summarized_texts = ray.get(summarization_futures)
    return summarized_texts

def create_document_from_texts(summarized_texts: List[Dict[str, Any]]) -> MultimodalDocument:
    nodes = [
        ContentNode(content_type="text", content=item['text']) for item in summarized_texts
    ]
    return MultimodalDocument(nodes=nodes)

def create_document_from_images(image_paths: List[str], image_agent: ImageAgent.remote()) -> MultimodalDocument:
    image_futures = [image_agent.process_image.remote(image_path) for image_path in image_paths]
    processed_images = ray.get(image_futures)
    nodes = [
        ContentNode(content_type="image", content=item['image_data'], metadata=item['metadata']) for item in processed_images
    ]
    
