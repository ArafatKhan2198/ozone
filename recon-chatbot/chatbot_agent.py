"""
Chatbot Agent for Ozone Recon

This is the central orchestrator that manages the multi-step conversation flow:
1. Load API schema into context
2. Get tool call from Gemini based on user query
3. Execute API call
4. Get summarization from Gemini
5. Return formatted response
"""

import os
import yaml
import concurrent.futures
import time
from typing import Dict, Any, Optional, List
from gemini_client import GeminiClient
from api_client import ReconApiClient


class ReconChatbotAgent:
    def __init__(self, gemini_api_key: str, recon_base_url: str = "http://localhost:9888"):
        """
        Initialize the Recon Chatbot Agent.
        
        Args:
            gemini_api_key: Google AI API key for Gemini
            recon_base_url: Base URL of the Recon service
        """
        self.gemini_client = GeminiClient(gemini_api_key)
        self.api_client = ReconApiClient(recon_base_url)
        self.api_schema = None
        self.api_guide = None
        
        # Load the API schema and guide
        self._load_api_schema()
    
    def _load_api_schema(self):
        """Load the recon-api.yaml file and detailed API guide into memory."""
        schema_path = os.path.join(os.path.dirname(__file__), 'api_schema', 'recon-api.yaml')
        guide_path = os.path.join(os.path.dirname(__file__), 'api_schema', 'recon-api-guide.md')
        
        # Load the OpenAPI schema
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                self.api_schema = f.read()
            print("✓ API schema loaded successfully")
        except FileNotFoundError:
            raise Exception(f"API schema file not found at {schema_path}")
        except Exception as e:
            raise Exception(f"Error loading API schema: {e}")
        
        # Load the detailed API guide
        try:
            with open(guide_path, 'r', encoding='utf-8') as f:
                self.api_guide = f.read()
            print("✓ API guide loaded successfully")
        except FileNotFoundError:
            print("⚠️  API guide not found, proceeding with schema only")
            self.api_guide = ""
        except Exception as e:
            print(f"⚠️  Error loading API guide: {e}, proceeding with schema only")
            self.api_guide = ""
    
    def test_connections(self) -> Dict[str, Any]:
        """
        Test connections to both Gemini and Recon services.
        
        Returns:
            Dictionary with connection status information
        """
        results = {
            "gemini": {"status": "unknown"},
            "recon": {"status": "unknown"}
        }
        
        # Test Gemini connection
        try:
            test_response = self.gemini_client.handle_fallback("test connection")
            if test_response:
                results["gemini"] = {"status": "connected"}
            else:
                results["gemini"] = {"status": "error", "message": "No response from Gemini"}
        except Exception as e:
            results["gemini"] = {"status": "error", "message": str(e)}
        
        # Test Recon connection
        try:
            if self.api_client.test_connection():
                recon_info = self.api_client.get_service_info()
                results["recon"] = recon_info
            else:
                results["recon"] = {"status": "error", "message": "Cannot connect to Recon service"}
        except Exception as e:
            results["recon"] = {"status": "error", "message": str(e)}
        
        return results
    
    def process_query(self, user_query: str) -> str:
        """
        Process a user query through the complete chatbot pipeline.
        Supports both single and multi-endpoint queries.
        
        Args:
            user_query: The user's natural language question
            
        Returns:
            Natural language response to the user's question
        """
        if not self.api_schema:
            return "Error: API schema not loaded. Please check the configuration."
        
        try:
            # Step 1: Get tool call(s) from Gemini
            print(f"🤔 Analyzing query: {user_query}")
            tool_call = self.gemini_client.get_tool_call(user_query, self.api_schema, self.api_guide)
            
            if not tool_call:
                # No suitable endpoint found, use fallback
                print("❌ No suitable API endpoint found")
                return self.gemini_client.handle_fallback(user_query)
            
            # Check if this is a documentation query
            if tool_call.get('type') == 'documentation':
                print("📚 Documentation query detected")
                if tool_call.get('reasoning'):
                    print(f"💭 Reasoning: {tool_call['reasoning']}")
                print("✅ Response generated from documentation")
                return tool_call.get('answer', 'Documentation information not available.')
            
            # Check if multiple API calls are needed
            if tool_call.get('requires_multiple_calls') and 'tool_calls' in tool_call:
                print(f"🔧 Multiple endpoints detected: {len(tool_call['tool_calls'])} API calls needed")
                return self._process_multi_endpoint_query(user_query, tool_call['tool_calls'])
            
            # Single endpoint query (existing logic)
            return self._process_single_endpoint_query(user_query, tool_call)
            
        except Exception as e:
            error_msg = f"I encountered an error while processing your request: {str(e)}"
            print(f"❌ Error: {error_msg}")
            return error_msg
    
    def _process_single_endpoint_query(self, user_query: str, tool_call: Dict[str, Any]) -> str:
        """
        Process a query that requires only a single API call.
        
        Args:
            user_query: The user's question
            tool_call: Single endpoint tool call dictionary
            
        Returns:
            Natural language summary of the response
        """
        print(f"🔧 Selected endpoint: {tool_call.get('endpoint', 'unknown')}")
        if tool_call.get('reasoning'):
            print(f"💭 Reasoning: {tool_call['reasoning']}")
        
        # Execute the API call
        print("📡 Calling Recon API...")
        api_response = self.api_client.execute_tool_call(tool_call)
        
        # Get summarization from Gemini
        print("✨ Generating summary...")
        summary = self.gemini_client.summarize_response(
            user_query, 
            api_response, 
            tool_call.get('endpoint', 'unknown')
        )
        
        print("✅ Response generated successfully")
        return summary
    
    def _process_multi_endpoint_query(self, user_query: str, tool_calls: List[Dict[str, Any]]) -> str:
        """
        Process a query that requires multiple API calls.
        Executes calls in parallel for better performance.
        
        Args:
            user_query: The user's question
            tool_calls: List of tool call dictionaries
            
        Returns:
            Unified natural language summary combining all responses
        """
        combined_responses = {}
        total_calls = len(tool_calls)
        
        print(f"📊 Executing {total_calls} API calls in parallel...")
        
        # Log which endpoints are being called together (analytics)
        endpoints_list = [tc.get('endpoint', 'unknown') for tc in tool_calls]
        print(f"📋 Endpoints: {', '.join(endpoints_list)}")
        
        start_time = time.time()
        
        # Execute API calls in parallel using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(5, total_calls)) as executor:
            # Submit all API calls
            future_to_tool_call = {
                executor.submit(self._safe_api_call, tc): tc 
                for tc in tool_calls
            }
            
            # Collect results as they complete
            completed = 0
            for future in concurrent.futures.as_completed(future_to_tool_call):
                tool_call = future_to_tool_call[future]
                endpoint = tool_call.get('endpoint', 'unknown')
                completed += 1
                
                try:
                    api_response = future.result()
                    combined_responses[endpoint] = {
                        'response': api_response,
                        'reasoning': tool_call.get('reasoning', ''),
                        'error': None
                    }
                    print(f"  ✅ [{completed}/{total_calls}] {endpoint} - Success")
                    
                except Exception as e:
                    combined_responses[endpoint] = {
                        'response': None,
                        'reasoning': tool_call.get('reasoning', ''),
                        'error': str(e)
                    }
                    print(f"  ❌ [{completed}/{total_calls}] {endpoint} - Failed: {str(e)[:50]}")
        
        elapsed_time = time.time() - start_time
        successful = sum(1 for r in combined_responses.values() if r.get('error') is None)
        failed = total_calls - successful
        
        print(f"⚡ Completed {total_calls} API calls in {elapsed_time:.2f}s ({successful} successful, {failed} failed)")
        
        # If all calls failed, return error
        if successful == 0:
            return "I encountered errors calling all required API endpoints. Please check if the Recon service is accessible."
        
        # Generate unified summary from Gemini
        print("✨ Generating unified summary...")
        summary = self.gemini_client.summarize_multi_response(user_query, combined_responses)
        
        print("✅ Multi-endpoint response generated successfully")
        return summary
    
    def _safe_api_call(self, tool_call: Dict[str, Any]) -> Dict[str, Any]:
        """
        Safely execute an API call with error handling.
        
        Args:
            tool_call: Tool call dictionary with endpoint info
            
        Returns:
            API response dictionary
            
        Raises:
            Exception: If the API call fails
        """
        try:
            return self.api_client.execute_tool_call(tool_call)
        except Exception as e:
            # Re-raise with context
            raise Exception(f"API call failed: {str(e)}")
    
    def get_capabilities(self) -> str:
        """
        Return a description of the chatbot's capabilities based on the API schema.
        
        Returns:
            String describing what the chatbot can help with
        """
        capabilities = """I can help you get information about your Ozone Recon cluster. Here's what I can assist with:

**Container Information:**
- Unhealthy containers (missing, under-replicated, over-replicated, mis-replicated)
- Missing containers
- Deleted containers
- Container replica history

**Keys and Storage:**
- Open keys and their summaries
- Keys pending deletion
- Blocks pending deletion
- Keys within specific containers

**Cluster Overview:**
- Overall cluster state and health
- Datanode information and status
- Pipeline information
- Task status and background processes

**Storage Utilization:**
- File count utilization by size
- Container count utilization by size

**Namespace Information:**
- Path summaries and metadata
- Disk usage for specific paths
- Quota information for volumes and buckets
- File size distribution

**Volumes and Buckets:**
- List all volumes
- List all buckets (optionally filtered by volume)

**Metrics:**
- Prometheus metrics queries

Just ask me questions in natural language like:
- "How many unhealthy containers are there?"
- "Show me the open keys summary"
- "What's the cluster state?"
- "List all datanodes"
- "What's the disk usage for /vol1/bucket1?"

I'll analyze your question and fetch the relevant data from your Recon service."""

        return capabilities
    
    def interactive_mode(self):
        """
        Start an interactive chat session with the user.
        """
        print("🚀 Ozone Recon Chatbot Starting...")
        print("=" * 50)
        
        # Test connections
        print("🔍 Testing connections...")
        connections = self.test_connections()
        
        for service, status in connections.items():
            if status["status"] == "connected":
                print(f"✅ {service.capitalize()}: Connected")
                if service == "recon" and "cluster_info" in status:
                    cluster = status["cluster_info"]
                    print(f"   📊 Cluster: {cluster.get('total_datanodes', '?')} datanodes, "
                          f"{cluster.get('containers', '?')} containers, "
                          f"{cluster.get('pipelines', '?')} pipelines")
            else:
                print(f"❌ {service.capitalize()}: {status.get('message', 'Connection failed')}")
                if service == "recon":
                    print("   ⚠️  Make sure Recon is running and accessible")
                    return
        
        print("\n" + "=" * 50)
        print("💬 Chat started! Type 'help' for capabilities, 'quit' to exit.")
        print("=" * 50)
        
        while True:
            try:
                user_input = input("\n🙋 You: ").strip()
                
                if user_input.lower() in ['quit', 'exit', 'bye']:
                    print("👋 Goodbye!")
                    break
                
                if user_input.lower() in ['help', 'capabilities']:
                    print(f"\n🤖 Assistant:\n{self.get_capabilities()}")
                    continue
                
                if not user_input:
                    continue
                
                # Process the query
                response = self.process_query(user_input)
                print(f"\n🤖 Assistant: {response}")
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")
                continue