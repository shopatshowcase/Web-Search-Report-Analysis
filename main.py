"""
Main script to use OpenAI Responses API with assistant configurations from JSON files
"""
import json
import logging
import os
import glob
from pathlib import Path
from openai_service import OpenAIService
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import BATCH_WORKERS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AssistantIntegration:
    """Main integration class for OpenAI Responses API"""
    
    def __init__(self):
        """Initialize services"""
        self.openai_service = OpenAIService()
        logger.info("Assistant Integration initialized (Responses API)")
    
    def load_assistant_from_file(self, json_file_path: str) -> Dict:
        """
        Load assistant configuration from JSON file
        Returns only the fields needed for OpenAI (matching MongoDB version)
        
        Args:
            json_file_path: Path to the JSON file
            
        Returns:
            Normalized assistant configuration dictionary
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                doc = json.load(f)
            
            # Extract and normalize - matching MongoDB's fetchAssistantFromAssistantsCollection
            normalized = {
                'id': doc.get('assistant_id') or doc.get('id'),
                'name': doc.get('name'),
                'model': doc.get('model'),
                'instructions': doc.get('instructions') if isinstance(doc.get('instructions'), str) else '',
                'functions': doc.get('functions', []) if isinstance(doc.get('functions'), list) else [],
                'builtin_tools': doc.get('builtin_tools', []) if isinstance(doc.get('builtin_tools'), list) else [],
                'sampling': doc.get('sampling'),  # Will be None or {temperature, top_p}
            }
            
            logger.info(f"Loaded assistant: {normalized.get('name', 'Unknown')}")
            return normalized
            
        except Exception as e:
            logger.error(f"Error loading assistant from {json_file_path}: {e}")
            raise
    
    def convert_tools_format(self, tools: List) -> List[dict]:
        """
        Convert assistant tools to Responses API format
        Handles both string format ["code_interpreter"] and object format [{"type": "code_interpreter"}]
        
        Args:
            tools: List of tool configurations (strings or dicts)
            
        Returns:
            Formatted tools list
        """
        formatted_tools = []
        for tool in tools:
            # Handle string format: "code_interpreter"
            if isinstance(tool, str):
                if tool in ['file_search', 'code_interpreter', 'web_search', 'computer_use', 'image_generation']:
                    formatted_tools.append({"type": tool})
            # Handle object format: {"type": "code_interpreter"}
            elif isinstance(tool, dict):
                tool_type = tool.get('type')
                if tool_type in ['file_search', 'code_interpreter', 'web_search', 'computer_use', 'image_generation']:
                    formatted_tools.append({"type": tool_type})
                elif tool_type == 'function':
                    formatted_tools.append(tool)
        return formatted_tools
    
    def process_request(self, 
                       assistant_json_path: str,
                       user_message: str, 
                       file_paths: Optional[List[str]] = None,
                       use_conversation: bool = False,
                       conversation_id: Optional[str] = None,
                       output_dir: Optional[str] = None) -> dict:
        """
        Process a user request using assistant from JSON file
        
        Args:
            assistant_json_path: Path to assistant JSON file
            user_message: The user's message/question
            file_paths: Optional list of file paths to attach
            use_conversation: Whether to use stateful conversation (default: True)
            conversation_id: Optional existing conversation ID
            
        Returns:
            Dictionary with response and metadata
        """
        try:
            # Load assistant configuration
            logger.info(f"Loading assistant from {assistant_json_path}")
            assistant_data = self.load_assistant_from_file(assistant_json_path)
            
            # Extract fields (matching MongoDB flow)
            assistant_id = assistant_data.get('id', 'unknown')
            assistant_name = assistant_data.get('name', 'Unknown Assistant')
            model = assistant_data.get('model', 'gpt-4o')
            instructions = assistant_data.get('instructions', 'You are a helpful assistant.')
            
            # Get tools from builtin_tools field
            tools = assistant_data.get('builtin_tools', [])
            
            # Get sampling (temperature, top_p) if present
            sampling = assistant_data.get('sampling')  # Can be None or {temperature, top_p}
            
            logger.info(f"Using assistant: {assistant_name}")
            logger.info(f"Model: {model}")
            # Handle both string and dict tool formats for logging
            tool_names = []
            for t in tools:
                if isinstance(t, str):
                    tool_names.append(t)
                elif isinstance(t, dict):
                    tool_names.append(t.get('type', 'unknown'))
            logger.info(f"Tools: {tool_names}")
            
            # Convert tools to Responses API format
            formatted_tools = self.convert_tools_format(tools) if tools else None
            
            # Call OpenAI Responses API with sampling
            logger.info("Calling OpenAI Responses API...")
            response = self.openai_service.get_assistant_response(
                model=model,
                instructions=instructions,
                user_message=user_message,
                tools=formatted_tools,
                file_paths=file_paths,
                use_conversation=use_conversation,
                conversation_id=conversation_id,
                sampling=sampling,  # Pass sampling to match MongoDB version
                metadata={
                    "assistant_id": assistant_id,
                    "assistant_name": assistant_name
                },
                output_dir=output_dir,
            )
            
            # Format response
            result = {
                "assistant": {
                    "id": assistant_id,
                    "name": assistant_name,
                    "model": model
                },
                "response_id": response['response_id'],
                "conversation_id": response.get('conversation_id'),
                "status": response['status'],
                "text": response['text']
            }
            
            logger.info("Request processed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            raise
    
    def save_response_to_file(self, response: dict, output_file: str = "response.json"):
        """
        Save response to a JSON file
        
        Args:
            response: Response dictionary
            output_file: Output file path
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(response, f, indent=2, ensure_ascii=False)
            logger.info(f"Response saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving response: {e}")
            raise
    
    def process_batch(self,
                     assistant_json_path: str,
                     input_folder: str,
                     user_message: str,
                     use_conversation: bool = True,
                     extra_attachments: Optional[List[str]] = None,
                     output_dir: Optional[str] = None) -> List[dict]:
        """
        Process multiple Excel files from a folder
        
        Args:
            assistant_json_path: Path to assistant JSON file
            input_folder: Path to folder containing Excel files
            user_message: Message to send with each file
            use_conversation: Whether to use conversation (creates new conversation per file)
            
        Returns:
            List of results for each processed file
        """
        results = []
        
        # Find all Excel files in the folder
        excel_patterns = ['*.xlsx', '*.xls']
        excel_files = []
        
        for pattern in excel_patterns:
            excel_files.extend(glob.glob(os.path.join(input_folder, pattern)))
        
        if not excel_files:
            logger.warning(f"No Excel files found in {input_folder}")
            return results
        
        logger.info(f"Found {len(excel_files)} Excel file(s) to process")
        logger.info("=" * 80)
        
        def _process_one(index_and_path: Tuple[int, str]) -> Dict:
            idx, file_path = index_and_path
            filename = os.path.basename(file_path)
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing file {idx}/{len(excel_files)}: {filename}")
            logger.info("=" * 80)

            # Attach extra files (e.g., mapping txt) to every request, if provided
            all_attachments = [file_path]
            if extra_attachments:
                for p in extra_attachments:
                    if p and p not in all_attachments:
                        all_attachments.append(p)

            response = self.process_request(
                assistant_json_path=assistant_json_path,
                user_message=user_message,
                file_paths=all_attachments,
                use_conversation=use_conversation,
                conversation_id=None,  # New conversation for each file
                output_dir=output_dir,
            )

            return {
                "input_file": filename,
                "status": "success",
                "response": response
            }

        workers = max(1, int(BATCH_WORKERS or 1))
        if workers == 1:
            # Sequential processing (original behavior)
            for idx, file_path in enumerate(excel_files, 1):
                filename = os.path.basename(file_path)
                try:
                    result = _process_one((idx, file_path))
                    results.append(result)
                    logger.info(f"✓ Successfully processed {filename}")
                except Exception as e:
                    logger.error(f"✗ Error processing {filename}: {e}")
                    results.append({"input_file": filename, "status": "error", "error": str(e)})
        else:
            logger.info(f"Running batch with parallel workers: {workers}")
            # Keep output order stable (same as excel_files order)
            ordered_results: List[Optional[Dict]] = [None] * len(excel_files)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_index = {
                    executor.submit(_process_one, (i, fp)): (i, fp)
                    for i, fp in enumerate(excel_files, 1)
                }
                for future in as_completed(future_to_index):
                    i, fp = future_to_index[future]
                    filename = os.path.basename(fp)
                    try:
                        res = future.result()
                        logger.info(f"✓ Successfully processed {filename}")
                    except Exception as e:
                        logger.error(f"✗ Error processing {filename}: {e}")
                        res = {"input_file": filename, "status": "error", "error": str(e)}
                    ordered_results[i - 1] = res

            results = [r for r in ordered_results if r is not None]
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("BATCH PROCESSING SUMMARY")
        logger.info("=" * 80)
        
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Total files: {len(excel_files)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        
        return results


def main():
    """
    Main function - supports both single file and batch processing
    """
    # Initialize integration
    integration = AssistantIntegration()
    
    # Configuration
    BATCH_MODE = True  # Set to True for batch processing, False for single file
    assistant_json_path = "assistant_1.json"  # or "assistant_2.json"
    
    user_message = """
Check the attachment of Key words of web searches report. For each term, assign the most relevant Line item using fuzzy matching, ensuring that every Key Word receives a Line assignment. If you are not able to map a specific "Line" to an item, keep it blank. Once "Line" is mapped, you have to extract the exact item name from the Key words values. If key word value has only line name, keep the "Item" column blank. Else you have to extract the exact item name by removing the "Line" value from it. Then create an excel file which should have a "Key Word", "Line" and "Item" as columns and their respective data.
    """
    
    try:
        if BATCH_MODE:
            # ============= BATCH PROCESSING MODE =============
            input_folder = "../split_files_20260109_154054"  # Adjust path as needed
            
            logger.info("=" * 80)
            logger.info("BATCH PROCESSING MODE")
            logger.info("=" * 80)
            logger.info(f"Input folder: {input_folder}")
            logger.info(f"Assistant: {assistant_json_path}")
            logger.info("=" * 80)
            
            # Process all files in the folder
            results = integration.process_batch(
                assistant_json_path=assistant_json_path,
                input_folder=input_folder,
                user_message=user_message,
                use_conversation=True
            )
            
            # Save batch results
            batch_summary = {
                "total_files": len(results),
                "successful": sum(1 for r in results if r['status'] == 'success'),
                "failed": sum(1 for r in results if r['status'] == 'error'),
                "results": results
            }
            
            integration.save_response_to_file(batch_summary, "batch_results.json")
            logger.info("\nBatch results saved to batch_results.json")
            
        else:
            # ============= SINGLE FILE MODE =============
            file_paths = [
                'C:/Users/Suyesh Bhagwat/Desktop/Web Searches Report Server Analysis API/split_files_20260108_152537/keywords_chunk_001_rows_1-100.xlsx',
            ]
            
            logger.info("=" * 80)
            logger.info("SINGLE FILE MODE")
            logger.info("=" * 80)
            
            # Process single file request
            response = integration.process_request(
                assistant_json_path=assistant_json_path,
                user_message=user_message,
                file_paths=file_paths if file_paths else None,
                use_conversation=True
            )
            
            # Display response
            logger.info("=" * 80)
            logger.info("RESPONSE")
            logger.info("=" * 80)
            
            print(json.dumps(response, indent=2))
            
            # Display assistant's reply
            if response.get('text'):
                logger.info("\n" + "=" * 80)
                logger.info("ASSISTANT'S RESPONSE:")
                logger.info("=" * 80)
                print(response['text'])
            
            # Display downloaded files
            if response.get('files'):
                logger.info("\n" + "=" * 80)
                logger.info("DOWNLOADED FILES:")
                logger.info("=" * 80)
                for file_info in response['files']:
                    print(f"✓ {file_info['filename']} (saved to: {file_info['local_path']})")
            
            # Save to file
            integration.save_response_to_file(response, "assistant_response.json")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
