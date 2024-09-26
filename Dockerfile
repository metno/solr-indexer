# Use an official Ubuntu 20.04 image as the base
FROM ubuntu:latest

# Set the working directory to /app
WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && \
    apt-get install -y wget python3-pip nano python3-wheel \
      gdal-bin gdal-data gdal-plugins libgdal-dev proj-bin proj-data \
      python3-gdal netcdf-bin python3-venv git

# Create a virtual environment
RUN python3.12 -m venv /opt/venv

# Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN /opt/venv/bin/pip install -r requirements.txt

# Copy the project code
COPY . .

# Set the command to run when the container starts
CMD ["pip", "install", "solrindexer"]
