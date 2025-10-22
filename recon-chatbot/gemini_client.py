"""
Gemini Client for Ozone Recon Chatbot

This module handles all communication with the Gemini API, including:
- Authentication and API key management
- Prompt construction for both tool selection and summarization
- Response parsing for tool calls and natural language responses
"""

import json
import re
from typing import Dict, Any, Optional, Tuple
import google.generativeai as genai


class GeminiClient:
    def __init__(self, api_key: str, model_name: str = "models/gemini-2.5-flash"):
        """
        Initialize the Gemini client.
        
        Args:
            api_key: Google AI API key for Gemini
            model_name: The Gemini model to use (default: models/gemini-1.5-flash)
        """
        self.api_key = api_key
        self.model_name = model_name
        
        # Configure the Gemini API
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        
    def get_tool_call(self, user_query: str, api_schema: str, api_guide: str = "") -> Optional[Dict[str, Any]]:
        """
        First Gemini call: Analyze user query against API schema to determine which endpoint to call.
        
        Args:
            user_query: The user's natural language question
            api_schema: The complete recon-api.yaml content as a string
            api_guide: Optional detailed API documentation/guide
            
        Returns:
            Dictionary containing the tool call information, or None if no suitable endpoint found
        """
        system_prompt = """You are an expert on Apache Ozone Recon, a service that provides insights into Ozone cluster data.

Your task is to analyze user queries and determine the appropriate response:

1. **For DATA queries** (asking for current cluster information): Identify the most appropriate API endpoint to call
2. **For DOCUMENTATION queries** (asking about API use cases, purposes, or capabilities): Respond with "DOCUMENTATION_QUERY" and provide the information directly

You have access to the complete Recon API specification and a detailed guide explaining the purpose and use cases of each API.

For DATA queries, return this JSON format:
{
    "endpoint": "/api/v1/endpoint/path",
    "method": "GET",
    "parameters": {
        "param_name": "param_value"
    },
    "reasoning": "Brief explanation of why this endpoint was chosen"
}

For DOCUMENTATION queries, return this JSON format:
{
    "type": "DOCUMENTATION_QUERY",
    "answer": "Direct answer based on the API guide",
    "reasoning": "Explanation of what documentation was referenced"
}

If the query cannot be answered by any available API endpoint OR documentation, respond with "NO_SUITABLE_ENDPOINT".

API Specification:
""" + api_schema

        # Add the detailed API guide if available
        if api_guide:
            system_prompt += "\n\nDetailed API Guide:\n" + api_guide
            system_prompt += "\n\nRemember: Use the API Guide to answer documentation questions directly. Use the API Specification to select endpoints for data queries."

        user_prompt = f"User Query: {user_query}"
        
        try:
            response = self.model.generate_content(system_prompt + "\n\n" + user_prompt)
            response_text = response.text.strip()
            
            if "NO_SUITABLE_ENDPOINT" in response_text:
                return None
                
            # Try to extract JSON from the response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                tool_call = json.loads(json_match.group())
                # Check if this is a documentation query
                if tool_call.get('type') == 'DOCUMENTATION_QUERY':
                    # Return a special format that the agent can recognize
                    return {
                        'type': 'documentation',
                        'answer': tool_call.get('answer', ''),
                        'reasoning': tool_call.get('reasoning', '')
                    }
                return tool_call
            else:
                return None
                
        except Exception as e:
            print(f"Error in get_tool_call: {e}")
            return None
    
    def summarize_response(self, user_query: str, api_response: Dict[str, Any], endpoint: str) -> str:
        """
        Second Gemini call: Generate a natural language summary of the API response.
        
        Args:
            user_query: The original user question
            api_response: The JSON response from the Recon API
            endpoint: The API endpoint that was called
            
        Returns:
            Natural language summary of the data
        """
        system_prompt = """You are an expert on Apache Ozone Recon data analysis.

Your task is to analyze API response data and provide clear, concise summaries that directly answer the user's question.

Guidelines:
- Focus on the key information that answers the user's specific question
- Use clear, non-technical language when possible
- Include relevant numbers, counts, and statistics
- If the data shows problems (unhealthy containers, missing data, etc.), highlight them
- Keep responses concise but informative
- If the API response is empty or doesn't contain relevant data, say so clearly

Format your response as a direct answer to the user's question."""

        user_prompt = f"""User asked: "{user_query}"

API endpoint called: {endpoint}

API response data:
{json.dumps(api_response, indent=2)}

Please provide a clear summary that answers the user's question."""

        try:
            response = self.model.generate_content(system_prompt + "\n\n" + user_prompt)
            return response.text.strip()
        except Exception as e:
            return f"I encountered an error while analyzing the data: {e}"
    
    def handle_fallback(self, user_query: str) -> str:
        """
        Generate a helpful fallback response when no suitable API endpoint is found.
        
        Args:
            user_query: The user's original query
            
        Returns:
            A helpful fallback message
        """
        fallback_prompt = f"""The user asked: "{user_query}"

This question cannot be answered using the available Ozone Recon API endpoints. 

Provide a helpful response that:
1. Politely explains that you can only answer questions about Ozone Recon cluster data
2. Briefly mentions the types of information you can provide (containers, keys, datanodes, pipelines, etc.)
3. Suggests how they might rephrase their question if it's related to Ozone

Keep the response friendly and concise."""

        try:
            response = self.model.generate_content(fallback_prompt)
            return response.text.strip()
        except Exception as e:
            return "I'm sorry, I can only answer questions about the Ozone Recon cluster data. Please ask about containers, keys, datanodes, pipelines, or cluster state."

