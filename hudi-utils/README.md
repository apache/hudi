mvn clean
mvn install

java -cp target/hudi-utils-1.0-SNAPSHOT-jar-with-dependencies.jar:org.apache.hudi:hudi-utilities-bundle_2.11:jar:0.9.0-SNAPSHOT:org.apache.hudi:hudi-spark-bundle_2.11:jar:0.9.0-SNAPSHOT:org:org.apache.hudi:hudi-flink-bundle_2.11:jar:0.9.0-SNAPSHOT org.apache.hudi.utils.HoodieConfigDocGenerator
