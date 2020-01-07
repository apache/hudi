---
title: "Talks & Powered By"
keywords: hudi, talks, presentation
permalink: /docs/powered_by.html
last_modified_at: 2019-12-31T15:59:57-04:00
---

## Adoption

### Uber

Apache Hudi was originally developed at [Uber](https://uber.com), to achieve [low latency database ingestion, with high efficiency](http://www.slideshare.net/vinothchandar/hadoop-strata-talk-uber-your-hadoop-has-arrived/32).
It has been in production since Aug 2016, powering the massive [100PB data lake](https://eng.uber.com/uber-big-data-platform/), including highly business critical tables like core trips,riders,partners. It also 
powers several incremental Hive ETL pipelines and being currently integrated into Uber's data dispersal system.

### Amazon Web Services
Amazon Web Services is the World's leading cloud services provider. Apache Hudi is [pre-installed](https://aws.amazon.com/emr/features/hudi/) with the AWS Elastic Map Reduce 
offering, providing means for AWS users to perform record-level updates/deletes and manage storage efficiently.

### EMIS Health

[EMIS Health](https://www.emishealth.com/) is the largest provider of Primary Care IT software in the UK with datasets including more than 500Bn healthcare records. HUDI is used to manage their analytics dataset in production and keeping them up-to-date with their upstream source. Presto is being used to query the data written in HUDI format.

### Yields.io

Yields.io is the first FinTech platform that uses AI for automated model validation and real-time monitoring on an enterprise-wide scale. Their data lake is managed by Hudi. They are also actively building their infrastructure for incremental, cross language/platform machine learning using Hudi.

### Yotpo

Using Hudi at Yotpo for several usages. Firstly, integrated Hudi as a writer in their open source ETL framework https://github.com/YotpoLtd/metorikku and using as an output writer for a CDC pipeline, with events that are being generated from a database binlog streams to Kafka and then are written to S3. 
 
### Tathastu.ai

[Tathastu.ai](https://www.tathastu.ai) offers the largest AI/ML playground of consumer data for data scientists, AI experts and technologists to build upon. They have built a CDC pipeline using Apache Hudi and Debezium. Data from Hudi datasets is being queried using Hive, Presto and Spark.

## Talks & Presentations

1. ["Hoodie: Incremental processing on Hadoop at Uber"](https://conferences.oreilly.com/strata/strata-ca/public/schedule/detail/56511) -  By Vinoth Chandar & Prasanna Rajaperumal
   Mar 2017, Strata + Hadoop World, San Jose, CA

2. ["Hoodie: An Open Source Incremental Processing Framework From Uber"](http://www.dataengconf.com/hoodie-an-open-source-incremental-processing-framework-from-uber) - By Vinoth Chandar.
   Apr 2017, DataEngConf, San Francisco, CA [Slides](https://www.slideshare.net/vinothchandar/hoodie-dataengconf-2017) [Video](https://www.youtube.com/watch?v=7Wudjc-v7CA)

3. ["Incremental Processing on Large Analytical Datasets"](https://spark-summit.org/2017/events/incremental-processing-on-large-analytical-datasets/) - By Prasanna Rajaperumal
   June 2017, Spark Summit 2017, San Francisco, CA. [Slides](https://www.slideshare.net/databricks/incremental-processing-on-large-analytical-datasets-with-prasanna-rajaperumal-and-vinoth-chandar) [Video](https://www.youtube.com/watch?v=3HS0lQX-cgo&feature=youtu.be)

4. ["Hudi: Unifying storage and serving for batch and near-real-time analytics"](https://conferences.oreilly.com/strata/strata-ny/public/schedule/detail/70937) - By Nishith Agarwal & Balaji Vardarajan
   September 2018, Strata Data Conference, New York, NY

5. ["Hudi: Large-Scale, Near Real-Time Pipelines at Uber"](https://databricks.com/session/hudi-near-real-time-spark-pipelines-at-petabyte-scale) - By Vinoth Chandar & Nishith Agarwal
   October 2018, Spark+AI Summit Europe, London, UK

6. ["Powering Uber's global network analytics pipelines in real-time with Apache Hudi"](https://www.youtube.com/watch?v=1w3IpavhSWA) - By Ethan Guo & Nishith Agarwal, April 2019, Data Council SF19, San Francisco, CA.

7. ["Building highly efficient data lakes using Apache Hudi (Incubating)"](https://www.slideshare.net/ChesterChen/sf-big-analytics-20190612-building-highly-efficient-data-lakes-using-apache-hudi) - By Vinoth Chandar 
   June 2019, SF Big Analytics Meetup, San Mateo, CA

8. ["Apache Hudi (Incubating) - The Past, Present and Future Of Efficient Data Lake Architectures"](https://docs.google.com/presentation/d/1FHhsvh70ZP6xXlHdVsAI0g__B_6Mpto5KQFlZ0b8-mM) - By Vinoth Chandar & Balaji Varadarajan
   September 2019, ApacheCon NA 19, Las Vegas, NV, USA
  
9. ["Insert, upsert, and delete data in Amazon S3 using Amazon EMR"](https://www.portal.reinvent.awsevents.com/connect/sessionDetail.ww?SESSION_ID=98662&csrftkn=YS67-AG7B-QIAV-ZZBK-E6TT-MD4Q-1HEP-747P) - By Paul Codding & Vinoth Chandar
   December 2019, AWS re:Invent 2019, Las Vegas, NV, USA  
       
10. ["Building Robust CDC Pipeline With Apache Hudi And Debezium"](https://www.slideshare.net/SyedKather/building-robust-cdc-pipeline-with-apache-hudi-and-debezium) - By Pratyaksh, Purushotham, Syed and Shaik December 2019, Hadoop Summit Bangalore, India

## Articles

1. ["The Case for incremental processing on Hadoop"](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop) - O'reilly Ideas article by Vinoth Chandar
2. ["Hoodie: Uber Engineering's Incremental Processing Framework on Hadoop"](https://eng.uber.com/hoodie/) - Engineering Blog By Prasanna Rajaperumal
