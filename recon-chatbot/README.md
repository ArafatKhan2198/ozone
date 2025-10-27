# Ozone Recon Chatbot

An intelligent chatbot for Apache Ozone Recon that allows you to query cluster information using natural language. Powered by Google's Gemini AI with a 1 million token context window.

## Features

- **Natural Language Queries**: Ask questions in plain English about your Ozone cluster
- **Intelligent API Mapping**: Automatically determines which Recon API endpoint to call based on your question
- **Comprehensive Coverage**: Supports all major Recon API endpoints including containers, keys, datanodes, pipelines, and more
- **Smart Summarization**: Provides clear, human-readable summaries of complex API responses
- **Interactive Mode**: Chat-like interface for ongoing conversations
- **Single Query Mode**: Perfect for scripting and automation

## Prerequisites

1. **Apache Ozone Recon**: A running Recon service (default: http://localhost:9888)
2. **Google AI API Key**: Get one from [Google AI Studio](https://makersuite.google.com/app/apikey)
3. **Python 3.7+**: Required for the application

## Installation

1. Clone or download this project
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Set your Gemini API key as an environment variable:

```bash
export GEMINI_API_KEY="your-gemini-api-key-here"
```

Alternatively, the application will prompt you for the API key when you run it.

## Usage

### Interactive Mode (Default)

Start a chat session:

```bash
python main.py
```

Example conversation:
```
🙋 You: How many unhealthy containers are there?
🤖 Assistant: There are 3 unhealthy containers in your cluster. 2 are missing and 1 is under-replicated.

🙋 You: Show me the cluster state
🤖 Assistant: Your Ozone cluster has 4 datanodes (all healthy), 26 containers, 6 volumes, and 5 pipelines. There are currently 25 keys stored.
```

### Single Query Mode

Ask one question and exit:

```bash
python main.py --query "What's the status of my datanodes?"
```

### Custom Recon URL

If your Recon service is running on a different host/port:

```bash
python main.py --recon-url http://your-recon-host:9888
```

### Test Connections

Verify connectivity to both Gemini and Recon:

```bash
python main.py --test-connections
```

### Show Capabilities

See what the chatbot can help with:

```bash
python main.py --capabilities
```

## Example Queries

The chatbot understands natural language questions like:

- "How many unhealthy containers are there?"
- "Show me the open keys summary"
- "What's the cluster state?"
- "List all datanodes"
- "What's the disk usage for /vol1/bucket1?"
- "Are there any missing containers?"
- "Show me pipeline information"
- "What tasks are running in the background?"

## Architecture

The chatbot uses a two-step process:

1. **Intent Analysis**: Gemini analyzes your question against the complete Recon API specification to determine the best endpoint to call
2. **Response Summarization**: After getting data from the API, Gemini generates a natural language summary that directly answers your question

## Project Structure

```
recon-chatbot/
├── main.py                 # Main application entry point
├── chatbot_agent.py        # Core agent orchestrating the conversation flow
├── gemini_client.py        # Handles all Gemini API communication
├── api_client.py           # Handles Recon REST API calls
├── requirements.txt        # Python dependencies
├── README.md              # This file
└── api_schema/
    └── recon-api.yaml     # Complete Recon API specification
```

## Environment Variables

- `GEMINI_API_KEY`: Your Google AI API key (required)
- `RECON_URL`: Default Recon service URL (optional, defaults to http://localhost:9888)

## Error Handling

The chatbot includes comprehensive error handling:

- **Connection Issues**: Clear messages if Recon service is unavailable
- **API Errors**: Helpful explanations for API failures
- **Invalid Queries**: Intelligent fallbacks for questions outside the scope
- **Rate Limiting**: Graceful handling of API rate limits

## Limitations

- Requires internet connection for Gemini API calls
- Currently read-only (no cluster modification capabilities)
- Depends on Recon service availability
- Subject to Gemini API rate limits and quotas

## Contributing

This chatbot is designed to be easily extensible. To add new capabilities:

1. The API schema is automatically loaded from `api_schema/recon-api.yaml`
2. Gemini handles intent mapping automatically
3. New endpoints are supported without code changes

## Troubleshooting

**"Could not connect to Recon service"**
- Verify Recon is running and accessible
- Check the URL with `--recon-url` parameter
- Test with `--test-connections`

**"API key is required"**
- Set the `GEMINI_API_KEY` environment variable
- Or provide it when prompted

**"No suitable endpoint found"**
- Try rephrasing your question
- Use `--capabilities` to see what's supported
- The chatbot will suggest alternatives

## License

This project follows the same license as Apache Ozone.











