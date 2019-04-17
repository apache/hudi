ARG HADOOP_VERSION=2.8.4 
ARG HIVE_VERSION=2.3.3
ARG SPARK_VERSION=2.4.0
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-hive_${HIVE_VERSION}-sparkbase_${SPARK_VERSION}

COPY worker.sh /opt/spark

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

EXPOSE 8081

CMD ["/bin/bash", "/opt/spark/worker.sh"]
