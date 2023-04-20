docker build \
 --build-arg HIVE_VERSION=3.1.3 \
 --build-arg FLINK_VERSION=1.15.3 \
 --build-arg SPARK_VERSION=3.3.1 \
 --build-arg SPARK_HADOOP_VERSION=2 \
 -t hudi-ci-bundle-validation-base:flink1153hive313spark331 .
docker image tag hudi-ci-bundle-validation-base:flink1153hive313spark331 apachehudi/hudi-ci-bundle-validation-base:flink1153hive313spark331
