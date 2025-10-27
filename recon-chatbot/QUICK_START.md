# Quick Start Guide - Ozone Recon Chatbot

## Prerequisites ✅

You already have:
- ✅ Recon running in Docker on port 9888
- ✅ Docker installed

## Step 1: Get Gemini API Key 🔑

1. Go to [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Create API Key"
4. Copy the generated key

## Step 2: Set Environment Variable 🌍

```bash
export GEMINI_API_KEY="your-api-key-here"
```

## Step 3: Start the Chatbot 🚀

### Option A: Easy Start (Recommended)
```bash
cd recon-chatbot
./start-chatbot.sh
```

### Option B: Manual Docker
```bash
cd recon-chatbot
docker-compose up --build
```

### Option C: Single Query
```bash
cd recon-chatbot
docker-compose run recon-chatbot python main.py --query "What's the cluster state?"
```

## Example Usage 💬

Once running, you can ask questions like:

```
🙋 You: How many containers are there?
🤖 Assistant: Your Ozone cluster currently has 0 containers. This suggests the cluster might be newly initialized or no data has been written yet.

🙋 You: What's the cluster state?
🤖 Assistant: Your cluster shows 0 datanodes, 0 containers, 0 volumes, and 0 keys. The cluster appears to be in an initial state with no active components.

🙋 You: help
🤖 Assistant: [Shows full capabilities list]
```

## Troubleshooting 🔧

**"Could not connect to Recon service"**
- Check: `curl http://localhost:9888/api/v1/clusterState`
- Ensure your Ozone containers are running

**"API key is required"**
- Make sure you set `GEMINI_API_KEY` environment variable

**Docker issues**
- Try: `docker-compose down && docker-compose up --build`

## Commands Reference 📋

```bash
# Interactive mode
./start-chatbot.sh

# Single query
docker-compose run recon-chatbot python main.py --query "your question"

# Test connections
docker-compose run recon-chatbot python main.py --test-connections

# Show capabilities
docker-compose run recon-chatbot python main.py --capabilities

# Custom Recon URL
RECON_URL=http://your-host:9888 docker-compose up
```

## Next Steps 🎯

1. Try asking about your cluster state
2. Add some data to Ozone and ask about containers/keys
3. Explore different types of queries
4. Consider integrating with your existing Ozone workflow










