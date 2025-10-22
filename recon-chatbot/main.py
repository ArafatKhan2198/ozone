#!/usr/bin/env python3
"""
Ozone Recon Chatbot - Main Entry Point

This is the main application that starts the Gemini-powered chatbot for Apache Ozone Recon.
The chatbot can answer questions about cluster state, containers, keys, datanodes, and more
by intelligently calling the appropriate Recon REST API endpoints.
"""

import os
import sys
import argparse
from chatbot_agent import ReconChatbotAgent


def get_api_key():
    """
    Get the Gemini API key from environment variable or user input.
    
    Returns:
        The API key string
    """
    # First try to get from environment variable
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("🔑 Gemini API key not found in environment variable GEMINI_API_KEY")
        api_key = input("Please enter your Gemini API key: ").strip()
        
        if not api_key:
            print("❌ API key is required to use the chatbot.")
            sys.exit(1)
    
    return api_key


def main():
    """Main function to start the chatbot."""
    parser = argparse.ArgumentParser(
        description="Ozone Recon Chatbot - Ask questions about your Ozone cluster in natural language",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                                    # Use default Recon URL (localhost:9888)
  python main.py --recon-url http://recon:9888      # Use custom Recon URL
  python main.py --query "How many unhealthy containers?"  # Single query mode
  
Environment Variables:
  GEMINI_API_KEY    Your Google AI API key for Gemini
  RECON_URL         Default Recon service URL (optional)
        """
    )
    
    parser.add_argument(
        '--recon-url',
        default=os.getenv('RECON_URL', 'http://localhost:9888'),
        help='Base URL of the Recon service (default: http://localhost:9888)'
    )
    
    parser.add_argument(
        '--query',
        help='Single query mode - ask one question and exit'
    )
    
    parser.add_argument(
        '--test-connections',
        action='store_true',
        help='Test connections to Gemini and Recon services and exit'
    )
    
    parser.add_argument(
        '--capabilities',
        action='store_true',
        help='Show chatbot capabilities and exit'
    )
    
    args = parser.parse_args()
    
    # Get API key
    try:
        api_key = get_api_key()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)
    
    # Initialize the chatbot agent
    try:
        print("🤖 Initializing Ozone Recon Chatbot...")
        agent = ReconChatbotAgent(api_key, args.recon_url)
    except Exception as e:
        print(f"❌ Failed to initialize chatbot: {e}")
        sys.exit(1)
    
    # Handle different modes
    if args.test_connections:
        print("🔍 Testing connections...")
        connections = agent.test_connections()
        
        for service, status in connections.items():
            print(f"\n{service.upper()}:")
            if status["status"] == "connected":
                print(f"  ✅ Status: Connected")
                if service == "recon" and "cluster_info" in status:
                    cluster = status["cluster_info"]
                    print(f"  📊 Datanodes: {cluster.get('total_datanodes', '?')} total, {cluster.get('healthy_datanodes', '?')} healthy")
                    print(f"  📦 Containers: {cluster.get('containers', '?')}")
                    print(f"  🔗 Pipelines: {cluster.get('pipelines', '?')}")
            else:
                print(f"  ❌ Status: {status.get('message', 'Connection failed')}")
        
        sys.exit(0)
    
    if args.capabilities:
        print(agent.get_capabilities())
        sys.exit(0)
    
    if args.query:
        # Single query mode
        print(f"🙋 Query: {args.query}")
        response = agent.process_query(args.query)
        print(f"🤖 Response: {response}")
        sys.exit(0)
    
    # Interactive mode
    try:
        agent.interactive_mode()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)


if __name__ == "__main__":
    main()


