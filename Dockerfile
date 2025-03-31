FROM apache/airflow:2.9.2-python3.9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        procps \
        curl \
        wget \
        gcc \
        python3-dev \
        liblz4-dev \
        build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz && \
    chown -R airflow:root /opt/spark*

RUN mkdir -p /opt/airflow/dags/jars && \
    wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar -O /opt/airflow/dags/jars/clickhouse-jdbc.jar && \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/airflow/dags/jars/postgresql-jdbc.jar && \
    chown -R airflow:root /opt/airflow/dags/jars

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV SPARK_CLASSPATH=/opt/airflow/dags/jars/clickhouse-jdbc.jar:/opt/airflow/dags/jars/postgresql-jdbc.jar

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9 \
    clickhouse-driver==0.2.6 \
    clickhouse-sqlalchemy==0.2.4 \
    sqlalchemy==1.4.46 \
    faker==24.11.0 \
    pandas==1.5.3 \
    tenacity==8.2.3

RUN pip freeze > /opt/airflow/requirements_installed.txt

WORKDIR /opt/airflow




