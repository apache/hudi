mvn clean
mvn install

java -cp target/hudi-utils-1.0-SNAPSHOT-jar-with-dependencies.jar:$HOME/.m2/repository/org/apache/hudi/hudi-utilities-bundle_2.11/0.9.0-SNAPSHOT/hudi-utilities-bundle_2.11-0.9.0-SNAPSHOT.jar:$HOME/.m2/repository/org/apache/hudi/hudi-spark-bundle_2.11/0.9.0-SNAPSHOT/hudi-spark-bundle_2.11-0.9.0-SNAPSHOT.jar:$HOME/.m2/repository/org/apache/hudi/hudi-flink-bundle_2.11/0.9.0-SNAPSHOT/hudi-flink-bundle_2.11-0.9.0-SNAPSHOT.jar org.apache.hudi.utils.HoodieConfigDocGenerator

