ARG HADOOP_VERSION=2.8.4 
ARG HADOOP_DN_PORT=50075
FROM apachehudi/hudi-hadoop_${HADOOP_VERSION}-base:latest

ENV HADOOP_DN_PORT ${HADOOP_DN_PORT}

ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
RUN mkdir -p /hadoop/dfs/data
VOLUME /hadoop/dfs/data

ADD run_dn.sh /run_dn.sh
RUN chmod a+x /run_dn.sh

CMD ["/run_dn.sh"]
