FROM python:3.11.11-bookworm

# Set working directory in the container
WORKDIR /chicago_traffic_crashes

# Install system dependencies required by dbt
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dbt (you can specify a version if needed)
RUN pip install --upgrade pip && \
    pip install dbt

# Optionally install any dbt adapters you need
# Example: installing the dbt adapter for BigQuery
RUN pip install dbt-bigquery

RUN mkdir -p /root/.dbt

COPY profiles.yml /root/.dbt/profiles.yml


# Copy your DBT project files into the container
COPY . /chicago_traffic_crashes

# COPY /keys/my-cred.json  /root/.dbt/my-cred.json


# RUN echo "$GOOGLE_APPLICATION_CREDENTIALS" > /root/.dbt/my-cred.json



# Optional: Install dbt dependencies listed in the packages.yml file
COPY packages.yml /chicago_traffic_crashes/packages.yml
RUN dbt deps

# Expose port for any web service (if needed)
EXPOSE 8080

# Command to run your DBT project (can be adjusted to your needs)
CMD ["dbt" , "build"]
