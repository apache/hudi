docker build base -t apachehudi/hudi-hadoop_3.1.0-base
docker build namenode -t apachehudi/hudi-hadoop_3.1.0-namenode
docker build datanode -t apachehudi/hudi-hadoop_3.1.0-datanode
docker build historyserver -t apachehudi/hudi-hadoop_3.1.0-history

docker build hive_base -t apachehudi/hudi-hadoop_3.1.0-hive_3.1.2

docker build spark_base -t apachehudi/hudi-hadoop_3.1.0-hive_3.1.2-sparkbase_3.2.1
docker build sparkmaster -t apachehudi/hudi-hadoop_3.1.0-hive_3.1.2-sparkmaster_3.2.1
docker build sparkadhoc -t apachehudi/hudi-hadoop_3.1.0-hive_3.1.2-sparkadhoc_3.2.1
docker build sparkworker -t apachehudi/hudi-hadoop_3.1.0-hive_3.1.2-sparkworker_3.2.1

docker build base_java11 -t apachehudi/hudi-hadoop_3.1.0-base-java11

docker build prestobase -t apachehudi/hudi-hadoop_3.1.0-prestobase_0.271

docker build trinobase -t apachehudi/hudi-hadoop_3.1.0-trinobase_368
docker build trinocoordinator -t apachehudi/hudi-hadoop_3.1.0-trinocoordinator_368
docker build trinoworker -t apachehudi/hudi-hadoop_3.1.0-trinoworker_368
