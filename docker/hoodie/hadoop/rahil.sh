docker build base -t yihua/hudi-hadoop_3.1.0-base
docker build namenode -t yihua/hudi-hadoop_3.1.0-namenode
docker build datanode -t yihua/hudi-hadoop_3.1.0-datanode
docker build historyserver -t yihua/hudi-hadoop_3.1.0-history

docker build hive_base -t yihua/hudi-hadoop_3.1.0-hive_3.1.2

docker build spark_base -t yihua/hudi-hadoop_3.1.0-hive_3.1.2-sparkbase_3.2.1
docker build sparkmaster -t yihua/hudi-hadoop_3.1.0-hive_3.1.2-sparkmaster_3.2.1
docker build sparkadhoc -t yihua/hudi-hadoop_3.1.0-hive_3.1.2-sparkadhoc_3.2.1
docker build sparkworker -t yihua/hudi-hadoop_3.1.0-hive_3.1.2-sparkworker_3.2.1


docker build prestobase -t yihua/hudi-hadoop_3.1.0-prestobase_0.271

docker build base_java11 -t yihua/hudi-hadoop_3.1.0-base-java11
docker build trinobase -t yihua/hudi-hadoop_3.1.0-trinobase_368
docker build trinocoordinator -t yihua/hudi-hadoop_3.1.0-trinocoordinator_368
docker build trinoworker -t yihua/hudi-hadoop_3.1.0-trinoworker_368
