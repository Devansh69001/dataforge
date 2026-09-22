# Application image: pipeline CLI, analytics API, dashboard, mock reference API, dbt.
# Java 17 is included so PySpark runs in local mode inside the container (Hadoop natives
# are available on Linux, so the native parquet writer is used - no Windows shim).
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    DATAFORGE_LOG_FORMAT=json

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dataforge

COPY requirements.txt pyproject.toml ./
RUN pip install -r requirements.txt

COPY dataforge ./dataforge
COPY api ./api
COPY dashboard ./dashboard
COPY dbt ./dbt
COPY scripts ./scripts
COPY dags ./dags
COPY .streamlit ./.streamlit
RUN pip install --no-deps -e .

RUN useradd -m -u 1000 dataforge && mkdir -p /opt/dataforge/data /tmp/dataforge && chown -R dataforge:dataforge /opt/dataforge /tmp/dataforge
USER dataforge

ENV DATAFORGE_DATA_DIR=/opt/dataforge/data \
    SPARK_TMP_DIR=/tmp/dataforge

CMD ["python", "-m", "dataforge.pipeline.runner", "--list"]
