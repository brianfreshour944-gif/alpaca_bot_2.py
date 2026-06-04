# Use a lightweight, stable Python runtime
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy and install dependencies first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# COPY THE EXACT FILENAME FROM YOUR REPO:
COPY alpaca_hybrid_bot_2.py .

# RUN THE EXACT FILENAME:
CMD ["python", "alpaca_hybrid_bot_2.py"]

