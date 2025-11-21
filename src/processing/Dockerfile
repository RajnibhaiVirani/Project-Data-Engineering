FROM python:3.10-slim

# I need to install Java (OpenJDK) because Spark runs on the JVM
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Installing PySpark and Database connectors
RUN pip install --no-cache-dir pyspark==3.4.1 pandas minio sqlalchemy psycopg2-binary tenacity

COPY spark_job.py .
CMD ["python", "spark_job.py"]