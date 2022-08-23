docker build base -t apachehudi/hudi-hadoop_2.8.4-base
docker build namenode -t apachehudi/hudi-hadoop_2.8.4-namenode
docker build datanode -t apachehudi/hudi-hadoop_2.8.4-datanode
docker build historyserver -t apachehudi/hudi-hadoop_2.8.4-history

docker build hive_base -t apachehudi/hudi-hadoop_2.8.4-hive_2.3.3

docker build spark_base -t apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkbase_3.2.1
docker build sparkmaster -t apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkmaster_3.2.1
docker build sparkadhoc -t apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkadhoc_3.2.1
docker build sparkworker -t apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkworker_3.2.1


docker build prestobase -t apachehudi/hudi-hadoop_2.8.4-prestobase_0.271

docker build base_java11 -t apachehudi/hudi-hadoop_2.8.4-base-java11
docker build trinobase -t apachehudi/hudi-hadoop_2.8.4-trinobase_368
docker build trinocoordinator -t apachehudi/hudi-hadoop_2.8.4-trinocoordinator_368
docker build trinoworker -t apachehudi/hudi-hadoop_2.8.4-trinoworker_368