# Use an official Python runtime as the base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . .

# Run dbt dependencies
RUN dbt deps

# Expose port 8080 for HTTP requests
EXPOSE 8080

# Define the command to run the application
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]