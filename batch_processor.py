"""
Batch Processor - Flexible interface for processing Excel files with OpenAI Assistant
This module provides a simple function interface without modifying the existing main.py flow
"""
import logging
from typing import List, Dict, Optional
from main import AssistantIntegration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_files(
    user_message: str,
    assistant_json_file: str,
    input_folder: str,
    output_summary_file: Optional[str] = None,
    extra_attachments: Optional[List[str]] = None,
    output_dir: Optional[str] = None,
    use_conversation: bool = False,
) -> List[Dict]:
    """
    Process multiple Excel files from a folder using OpenAI Assistant.
    
    Args:
        user_message: The message/instruction to send to the assistant for each file
        assistant_json_file: Path to the assistant JSON configuration file (e.g., "assistant_1.json")
        input_folder: Path to the folder containing Excel files to process
        output_summary_file: Optional path to save batch results summary JSON (default: "batch_results.json")
        
    Returns:
        List of dictionaries containing results for each processed file
        
    Example:
        ```python
        from batch_processor import process_files
        
        user_msg = "Analyze this Excel file and map keywords to lines and items."
        results = process_files(
            user_message=user_msg,
            assistant_json_file="assistant_1.json",
            input_folder="C:/path/to/excel/files",
            output_summary_file="my_results.json"
        )
        
        print(f"Processed {len(results)} files")
        for result in results:
            if result['status'] == 'success':
                print(f"✓ {result['input_file']}")
            else:
                print(f"✗ {result['input_file']}: {result.get('error')}")
        ```
    """
    logger.info("=" * 80)
    logger.info("BATCH PROCESSOR - Starting")
    logger.info("=" * 80)
    logger.info(f"Assistant: {assistant_json_file}")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"User message: {user_message[:100]}..." if len(user_message) > 100 else f"User message: {user_message}")
    logger.info("=" * 80)
    
    # Initialize the integration (uses existing flow)
    integration = AssistantIntegration()
    
    # Use the existing process_batch method from main.py
    results = integration.process_batch(
        assistant_json_path=assistant_json_file,
        user_message=user_message,
        input_folder=input_folder,
        extra_attachments=extra_attachments,
        output_dir=output_dir,
        use_conversation=use_conversation,
    )
    
    # If custom output summary file is specified, save it
    if output_summary_file and results:
        import json
        with open(output_summary_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Custom batch summary saved to {output_summary_file}")
    
    logger.info("=" * 80)
    logger.info("BATCH PROCESSOR - Completed")
    logger.info("=" * 80)
    
    return results


def process_single_file(
    user_message: str,
    assistant_json_file: str,
    file_path: str,
    use_conversation: bool = True
) -> Dict:
    """
    Process a single Excel file using OpenAI Assistant.
    
    Args:
        user_message: The message/instruction to send to the assistant
        assistant_json_file: Path to the assistant JSON configuration file
        file_path: Path to the Excel file to process
        use_conversation: Whether to use stateful conversation (default: True)
        
    Returns:
        Dictionary containing the response and metadata
        
    Example:
        ```python
        from batch_processor import process_single_file
        
        result = process_single_file(
            user_message="Analyze this file",
            assistant_json_file="assistant_1.json",
            file_path="C:/path/to/file.xlsx"
        )
        
        print(result['text'])
        for file in result.get('files', []):
            print(f"Downloaded: {file['filename']}")
        ```
    """
    logger.info("=" * 80)
    logger.info("SINGLE FILE PROCESSOR - Starting")
    logger.info("=" * 80)
    logger.info(f"Assistant: {assistant_json_file}")
    logger.info(f"File: {file_path}")
    logger.info("=" * 80)
    
    # Initialize the integration (uses existing flow)
    integration = AssistantIntegration()
    
    # Use the existing process_request method from main.py
    result = integration.process_request(
        assistant_json_path=assistant_json_file,
        user_message=user_message,
        file_paths=[file_path],
        use_conversation=use_conversation
    )
    
    logger.info("=" * 80)
    logger.info("SINGLE FILE PROCESSOR - Completed")
    logger.info("=" * 80)
    
    return result


if __name__ == "__main__":
    """
    Example usage when running this file directly
    """
    # Example 1: Process multiple files from a folder
    example_user_message = """
You will be provided with an Excel attachment containing web search report Key Words and a structured key-value dataset, where each key represents a Line and each value represents an Item belonging to that Line. For each Key Word, first compare the Key Word against all available Item values across all Lines using strict fuzzy matching, with strong emphasis on handling spelling mistakes, including missing letters, extra letters, swapped characters, and spacing differences. If a relevant Item is confidently identified, assign that exact Item to the Key Word and derive the corresponding Line from the Items parent key. Never assign a Line directly if a better match exists at the Item level; Item-level matches always take precedence over Line-level matches. If none of the "Item" is relevant to key word, keep "Line" and "Item" columns blank. Finally, generate an Excel file containing all input Key Words with the following columns only: Key Word, Line, and Item, ensuring no input data is dropped or omitted and that every Key Word has been evaluated against the full set of Lines.
    """
    
    # Batch processing
    results = process_files(
        user_message=example_user_message,
        assistant_json_file="assistant_1.json",
        input_folder="../split_files_20260109_154054",
        output_summary_file="my_custom_results.json"
    )
    
    print(f"\n✓ Processed {len(results)} files")
    
    # Example 2: Process a single file
    # single_result = process_single_file(
    #     user_message=example_user_message,
    #     assistant_json_file="assistant_1.json",
    #     file_path="../split_files_20260109_154054/keywords_chunk_001_rows_1-100.xlsx"
    # )
    # print(f"\n✓ Single file processed: {single_result['response_id']}")

