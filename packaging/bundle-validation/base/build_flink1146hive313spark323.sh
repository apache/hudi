docker build \
 --build-arg HIVE_VERSION=3.1.3 \
 --build-arg FLINK_VERSION=1.14.6 \
 --build-arg SPARK_VERSION=3.2.3 \
 --build-arg SPARK_HADOOP_VERSION=2.7 \
 -t hudi-ci-bundle-validation-base:flink1146hive313spark323 .
docker image tag hudi-ci-bundle-validation-base:flink1146hive313spark323 apachehudi/hudi-ci-bundle-validation-base:flink1146hive313spark323
