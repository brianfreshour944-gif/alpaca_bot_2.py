# Use a lightweight, stable Python runtime
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy and install dependencies first (utilizes Docker caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual trading script into the container
COPY alpaca_hybrid_bot.py .

# Run the script when the container launches
CMD ["python", "alpaca_hybrid_bot.py"]

