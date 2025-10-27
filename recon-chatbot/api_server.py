"""
Ozone Recon Chatbot - Flask API Server

This script creates a simple Flask web server to expose the ReconChatbotAgent
as a REST API. The UI can call this API to get responses for user queries.
"""

from flask import Flask, request, jsonify
from chatbot_agent import ReconChatbotAgent
from flask_cors import CORS
import os
import sys

# --- Configuration & Initialization ---

def get_api_key():
    """Get the Gemini API key from environment variable."""
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("FATAL: GEMINI_API_KEY environment variable not set.")
        sys.exit(1)
    return api_key

# Initialize Flask App
app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing

# Initialize Chatbot Agent
try:
    print("🤖 Initializing Ozone Recon Chatbot Agent...")
    GEMINI_API_KEY = get_api_key()
    RECON_URL = os.getenv('RECON_URL', 'http://localhost:9888')
    agent = ReconChatbotAgent(GEMINI_API_KEY, RECON_URL)
    print("✅ Chatbot Agent initialized successfully.")
except Exception as e:
    print(f"❌ Failed to initialize chatbot agent: {e}")
    agent = None

# --- API Endpoints ---

@app.route('/chat', methods=['POST'])
def chat():
    """
    Handle chat requests from the UI.
    Expects a JSON payload with a 'query' field.
    """
    if not agent:
        return jsonify({"error": "Chatbot agent is not initialized."}), 500

    data = request.get_json()
    user_query = data.get('query')

    if not user_query:
        return jsonify({"error": "Query field is missing."}), 400

    try:
        print(f"💬 Received query: {user_query}")
        response = agent.process_query(user_query)
        print(f"🤖 Sending response: {response}")
        return jsonify({"response": response})
    except Exception as e:
        print(f"❌ Error processing query: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """A simple health check endpoint."""
    if agent:
        return jsonify({"status": "ok", "message": "Chatbot agent is initialized."})
    else:
        return jsonify({"status": "error", "message": "Chatbot agent is not initialized."}), 503

# --- Main Execution ---

if __name__ == '__main__':
    print("🚀 Starting Flask server for Recon Chatbot...")
    app.run(host='0.0.0.0', port=5001, debug=False)





