---
title: "Detailed Contribution Guide To Hudi"
excerpt: " learn Detailed Guide to Contribute to Apache Hudi"
author: Mani
category: blog
---

we are always welcome new contributors to community , in this blog we will see step by step guide to contribute

Steps :- .

**Step 1**: Setup your intellij for development, learn  code style that we follow [Contribution Guide](https://hudi.apache.org/contributing). <br/>
**Step 2**: By then hope you are ready with you Intellij setup , explored code style and picked some ticket to work next step is to learn some important classes to start with [watch](https://www.youtube.com/watch?v=N2eDfU_rQ_U). <br/>
**Step 3**: Hope you understand design , some concepts and Important classes time to pick [JIRA ticket](https://issues.apache.org/jira/issues/?jql=project+%3D+HUDI+AND+component+%3D+newbie) and start Development. <br/>
**Step 4**: Completed development want to test ? setup your docker Env to test your code [docker setup](https://hudi.apache.org/docs/docker_demo.html). <br/>
**Step 5**: Oh failed on Docker Setup ? no worries you can debug this by remote debugging please follow below steps. <br/>   

Remote Debugging : -

Run your Delta Streamer Job with --conf as defined this will ensure to wait till you attach your intellij with Remote Debugging on port 4044

```scala
spark-submit \
  --conf spark.driver.extraJavaOptions="-Dconfig.resource=myapp.conf  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4044" \
  --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --base-file-format parquet \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
```

Attaching Intellij : -

Come to Intellij --> Edit Configurations -> Remote -> Add Remote - > Put Below Configs -> Apply & Save -> Put Debug Point -> Start. <br/>
Name : Hudi Remote <br/>
Port : 4044 <br/>
Command Line Args for Remote JVM : -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4044 <br/>
Use Module ClassPath : select hudi <br/>



