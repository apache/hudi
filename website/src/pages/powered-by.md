---
title: "Powered By"
keywords: [hudi, powered-by]
last_modified_at: 2019-12-31T15:59:57-04:00
---
export const companiesList = [
    { img_path: '/assets/images/powers/uber.png', },
    { img_path: '/assets/images/powers/aws.jpg', },
	{ img_path: '/assets/images/powers/alibaba.png', },
	{ img_path: '/assets/images/powers/clinbrain.png', },
	{ img_path: '/assets/images/powers/emis.jpg', },
	{ img_path: '/assets/images/powers/yield.png', },
	{ img_path: '/assets/images/powers/qq.png', },
	{ img_path: '/assets/images/powers/tongcheng.png', },
	{ img_path: '/assets/images/powers/yotpo.png', },
	{ img_path: '/assets/images/powers/kyligence.png', },
	{ img_path: '/assets/images/powers/tathastu.png', },
	{ img_path: '/assets/images/powers/shunfeng.png', },
	{ img_path: '/assets/images/powers/lingyue.png', },
	{ img_path: '/assets/images/powers/37.PNG', },
	{ img_path: '/assets/images/powers/H3C.JPG', },
	{ img_path: '/assets/images/powers/moveworks.png', },
];

# Who's Using

## Adoption

### Alibaba Cloud
Alibaba Cloud provides cloud computing services to online businesses and Alibaba's own e-commerce ecosystem, Apache Hudi is integrated into Alibaba Cloud [Data Lake Analytics](https://www.alibabacloud.com/help/product/70174.htm)
offering real-time analysis on hudi dataset.

### Amazon Web Services
Amazon Web Services is the World's leading cloud services provider. Apache Hudi is [pre-installed](https://aws.amazon.com/emr/features/hudi/) with the AWS Elastic Map Reduce 
offering, providing means for AWS users to perform record-level updates/deletes and manage storage efficiently.

### Clinbrain
[Clinbrain](https://www.clinbrain.com/)  is the leader of big data platform and usage in medical industry. We have built 200 medical big data centers by integrating Hudi Data Lake solution in numerous hospitals. Hudi provides the ability to upsert and delete on hdfs, at the same time, it can make the fresh data-stream up-to-date efficiently in hadoop system with the hudi incremental view.

### EMIS Health
[EMIS Health](https://www.emishealth.com/) is the largest provider of Primary Care IT software in the UK with datasets including more than 500Bn healthcare records. HUDI is used to manage their analytics dataset in production and keeping them up-to-date with their upstream source. Presto is being used to query the data written in HUDI format.

### Grofers
[Grofers](https://grofers.com) is a grocery delivery provider operating across APAC region. Grofers has [integrated hudi](https://lambda.grofers.com/origins-of-data-lake-at-grofers-6c011f94b86c) in its central pipelines for replicating backend database CDC into the warehouse.

### H3C Digital Platform

[H3C digital platform](http://www.h3c.com/) provides the whole process capability of data collection, storage, calculation and governance, and enables the construction of data center and data governance ability for medical, smart park, smart city and other industries;
Apache Hudi is integrated in the digital platform to meet the real-time update needs of massive data

### Kyligence

[Kyligence](https://kyligence.io/zh/) is the leading Big Data analytics platform company. We’ve built end to end solutions for various Global Fortune 500 companies in US and China. We adopted Apache Hudi in our Cloud solution on AWS in 2019. With the help of Hudi, we are able to process upserts and deletes easily and we use incremental views to build efficient data pipelines in AWS. The Hudi datasets can also be integrated to Kyligence Cloud directly for high concurrent OLAP access.

### Lingyue-digital Corporation

[Lingyue-digital Corporation](https://www.lingyue-digital.com/) belongs to BMW Group. Apache Hudi is used to perform ingest MySQL and PostgreSQL change data capture. We build up upsert scenarios on Hadoop and spark.

### Logical Clocks

[Hopsworks 1.x series](https://www.logicalclocks.com/blog/introducing-the-hopsworks-1-x-series) supports Apache Hudi feature groups, to enable upserts and time travel.

### SF-Express

[SF-Express](https://www.sf-express.com/cn/sc/) is the leading logistics service provider in China. HUDI is used to build a real-time data warehouse, providing real-time computing solutions with higher efficiency and lower cost for our business.

### Tathastu.ai

[Tathastu.ai](https://www.tathastu.ai) offers the largest AI/ML playground of consumer data for data scientists, AI experts and technologists to build upon. They have built a CDC pipeline using Apache Hudi and Debezium. Data from Hudi datasets is being queried using Hive, Presto and Spark.

### Tencent 

[EMR from Tencent](https://intl.cloud.tencent.com/product/emr) Cloud has integrated Hudi as one of its BigData components [since V2.2.0](https://intl.cloud.tencent.com/document/product/1026/35587). Using Hudi, the end-users can handle either read-heavy or write-heavy use cases, and Hudi will manage the underlying data stored on HDFS/COS/CHDFS using Apache Parquet and Apache Avro.

### Uber

Apache Hudi was originally developed at [Uber](https://uber.com), to achieve [low latency database ingestion, with high efficiency](http://www.slideshare.net/vinothchandar/hadoop-strata-talk-uber-your-hadoop-has-arrived/32).
It has been in production since Aug 2016, powering the massive [100PB data lake](https://eng.uber.com/uber-big-data-platform/), including highly business critical tables like core trips,riders,partners. It also 
powers several incremental Hive ETL pipelines and being currently integrated into Uber's data dispersal system.

### Udemy 

At [Udemy](https://www.udemy.com/), Apache Hudi on AWS EMR is used to perform ingest MySQL change data capture.

### Yields.io

Yields.io is the first FinTech platform that uses AI for automated model validation and real-time monitoring on an enterprise-wide scale. Their [data lake](https://www.yields.io/Blog/Apache-Hudi-at-Yields) is managed by Hudi. They are also actively building their infrastructure for incremental, cross language/platform machine learning using Hudi.

### Yotpo

Using Hudi at Yotpo for several usages. Firstly, integrated Hudi as a writer in their open source ETL framework, [Metorikku](https://github.com/YotpoLtd/metorikku) and using as an output writer for a CDC pipeline, with events that are being generated from a database binlog streams to Kafka and then are written to S3. 

### 37 Interactive Entertainment

[37 Interactive Entertainment](https://www.37wan.net/) is a global Top20 listed game company, and a leading company on A-shares market of China.
Apache Hudi is integrated into our Data Middle Platform offering real-time data warehouse and solving the problem of frequent changes of data.
Meanwhile, we build a set of data access standards based on Hudi, which provides a guarantee for massive data queries in game operation scenarios.

## Powered by

<div className="page__hero--overlay">
    <div className="home-power-items">
      {
         companiesList.map(
            company => (
               (() => {
                  return <div className="who-uses"><img src={ company.img_path }/></div>
               })()
            )  
         )
      }
    </div>
</div>
