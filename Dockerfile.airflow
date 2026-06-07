FROM apache/airflow:3.2.2

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY scripts/entrypoint-airflow.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

USER airflow

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/custom-entrypoint.sh"]
