---
title: Integrations
keywords: [hudi, writing, reading]
---
# Integrations FAQ

### Does AWS GLUE support Hudi ?

AWS Glue jobs can write, read and update Glue Data Catalog for hudi tables. In order to successfully integrate with Glue Data Catalog, you need to subscribe to one of the AWS provided Glue connectors named "AWS Glue Connector for Apache Hudi". Glue job needs to have "Use Glue data catalog as the Hive metastore" option ticked. Detailed steps with a sample scripts is available on this article provided by AWS - [https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/](https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/).

In case if your using either notebooks or Zeppelin through Glue dev-endpoints, your script might not be able to integrate with Glue DataCatalog when writing to hudi tables.

### How to override Hudi jars in EMR?

If you are looking to override Hudi jars in your EMR clusters one way to achieve this is by providing the Hudi jars through a bootstrap script.

Here are the example steps for overriding Hudi version 0.7.0 in EMR 0.6.2.

**Build Hudi Jars:**

```bash
# Git clone
git clone https://github.com/apache/hudi.git && cd hudi   

# Get version 0.7.0
git checkout --track origin/release-0.7.0

# Build jars with spark 3.0.0 and scala 2.12 (since emr 6.2.0 uses spark 3 which requires scala 2.12):
mvn clean package -DskipTests -Dspark3  -Dscala-2.12 -T 30 
```

**Copy jars to s3:**

These are the jars we are interested in after build completes. Copy them to a temp location first.

```bash
mkdir -p ~/Downloads/hudi-jars
cp packaging/hudi-hadoop-mr-bundle/target/hudi-hadoop-mr-bundle-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-hive-sync-bundle/target/hudi-hive-sync-bundle-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.12-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-timeline-server-bundle/target/hudi-timeline-server-bundle-0.7.0.jar ~/Downloads/hudi-jars/
cp packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.7.0.jar ~/Downloads/hudi-jars/
```

Upload all jars from ~/Downloads/hudi-jars/ to the s3 location s3://xxx/yyy/hudi-jars

**Include Hudi jars as part of the emr bootstrap script:**

Below script downloads Hudi jars from above s3 location. Use this script as part `bootstrap-actions` when launching the EMR cluster to install the jars in each node.

```bash
#!/bin/bash
sudo mkdir -p /mnt1/hudi-jars

sudo aws s3 cp s3://xxx/yyy/hudi-jars /mnt1/hudi-jars --recursive

# create symlinks
cd /mnt1/hudi-jars
sudo ln -sf hudi-hadoop-mr-bundle-0.7.0.jar hudi-hadoop-mr-bundle.jar
sudo ln -sf hudi-hive-sync-bundle-0.7.0.jar hudi-hive-sync-bundle.jar
sudo ln -sf hudi-spark-bundle_2.12-0.7.0.jar hudi-spark-bundle.jar
sudo ln -sf hudi-timeline-server-bundle-0.7.0.jar hudi-timeline-server-bundle.jar
sudo ln -sf hudi-utilities-bundle_2.12-0.7.0.jar hudi-utilities-bundle.jar
```

**Using the overriden jar in Deltastreamer:**

When invoking DeltaStreamer specify the above jar location as part of spark-submit command.
