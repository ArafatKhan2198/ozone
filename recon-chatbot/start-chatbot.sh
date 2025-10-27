#!/bin/bash

echo "🤖 Ozone Recon Chatbot Setup"
echo "=============================="

# Check if GEMINI_API_KEY is set
if [ -z "$GEMINI_API_KEY" ]; then
    echo "❌ GEMINI_API_KEY environment variable is not set."
    echo ""
    echo "Please get your API key from: https://makersuite.google.com/app/apikey"
    echo "Then run: export GEMINI_API_KEY='your-api-key-here'"
    echo ""
    read -p "Or enter your API key now: " api_key
    if [ -n "$api_key" ]; then
        export GEMINI_API_KEY="$api_key"
    else
        echo "❌ API key is required. Exiting."
        exit 1
    fi
fi

echo "✅ API key is set"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

echo "✅ Docker is available"

# Check if Recon is accessible
echo "🔍 Checking Recon connection..."
if curl -s http://localhost:9888/api/v1/clusterState > /dev/null; then
    echo "✅ Recon is accessible at http://localhost:9888"
else
    echo "❌ Cannot connect to Recon at http://localhost:9888"
    echo "   Make sure your Ozone/Recon containers are running"
    exit 1
fi

echo ""
echo "🚀 Starting Recon Chatbot..."
echo "=============================="

# Build and run the chatbot
docker-compose up --build










