FROM python:3.10-slim

# Install dependencies
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Copy the application code
COPY async-pull.py /app/

# Set the working directory
WORKDIR /app

# Run the application
CMD ["python", "async-pull.py"]
