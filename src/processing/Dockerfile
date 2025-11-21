FROM python:3.10-slim

# Switch to stable Debian Bookworm repositories to ensure Java availability
RUN echo "deb http://deb.debian.org/debian bookworm main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian bookworm-updates main" >> /etc/apt/sources.list

# Install Java 17 (Verified working on new Debian images) and procps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Install PySpark + Database tools + Retry tools
RUN pip install --no-cache-dir pyspark==3.4.1 pandas minio sqlalchemy psycopg2-binary tenacity

COPY spark_job.py .
CMD ["python", "spark_job.py"]