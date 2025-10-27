FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Make main.py executable
RUN chmod +x main.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "main.py"]










