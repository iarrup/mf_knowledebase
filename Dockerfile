# Use a modern, slim Python base
FROM python:3.11-slim

# Install uv, the Python package manager
RUN pip install uv

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN uv pip install -r requirements.txt

# Copy the source code and data
COPY src/ /app/src
COPY data/ /app/data

# Expose the port the API will run on
EXPOSE 8000

# Set the command to run the Uvicorn server
CMD ["uvicorn", "src.main_api:app", "--host", "0.0.0.0", "--port", "8000"]