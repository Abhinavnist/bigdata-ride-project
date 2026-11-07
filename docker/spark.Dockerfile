ARG SPARK_IMAGE=apache/spark:3.5.3-python3
FROM ${SPARK_IMAGE}
USER root
RUN python3 -m pip install --no-cache-dir numpy pandas pyarrow requests
