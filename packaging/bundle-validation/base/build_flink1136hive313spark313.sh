docker build \
 --build-arg HIVE_VERSION=3.1.3 \
 --build-arg FLINK_VERSION=1.13.6 \
 --build-arg SPARK_VERSION=3.1.3 \
 --build-arg SPARK_HADOOP_VERSION=2.7 \
 -t hudi-ci-bundle-validation-base:flink1136hive313spark313 .
docker image tag hudi-ci-bundle-validation-base:flink1136hive313spark313 apachehudi/hudi-ci-bundle-validation-base:flink1136hive313spark313
