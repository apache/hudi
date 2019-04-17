ARG HADOOP_VERSION=2.8.4 
ARG HADOOP_HISTORY_PORT=8188
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-base:latest

ENV HADOOP_HISTORY_PORT ${HADOOP_HISTORY_PORT}

ENV YARN_CONF_yarn_timeline___service_leveldb___timeline___store_path=/hadoop/yarn/timeline
RUN mkdir -p /hadoop/yarn/timeline
VOLUME /hadoop/yarn/timeline

ADD run_history.sh /run_history.sh
RUN chmod a+x /run_history.sh

CMD ["/run_history.sh"]
