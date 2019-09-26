---
title: Talks & Powered By
keywords: hudi, talks, presentation
sidebar: mydoc_sidebar
permalink: powered_by.html
toc: false
---

## 已使用

#### Uber

Hudi最初由[Uber](https://uber.com)开发，用于实现[低延迟、高效率的数据库摄取](http://www.slideshare.net/vinothchandar/hadoop-strata-talk-uber-your-hadoop-has-arrived/32)。
Hudi自2016年8月开始在生产环境上线，在Hadoop上驱动约100个非常关键的业务表，支撑约几百TB的数据规模(前10名包括行程、乘客、司机)。
Hudi还支持几个增量的Hive ETL管道，并且目前已集成到Uber的数据分发系统中。

#### EMIS Health

[EMIS Health](https://www.emishealth.com/)是英国最大的初级保健IT软件提供商，其数据集包括超过5000亿的医疗保健记录。HUDI用于管理生产中的分析数据集，并使其与上游源保持同步。Presto用于查询以HUDI格式写入的数据。

#### Yields.io

Yields.io是第一个使用AI在企业范围内进行自动模型验证和实时监控的金融科技平台。他们的数据湖由Hudi管理，他们还积极使用Hudi为增量式、跨语言/平台机器学习构建基础架构。

#### Yotpo

Hudi在Yotpo有不少用途。首先，在他们的[开源ETL框架](https://github.com/YotpoLtd/metorikku)中集成了Hudi作为CDC管道的输出写入程序，即从数据库binlog生成的事件流到Kafka然后再写入S3。

## 演讲 & 报告

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

## 文章

1. ["The Case for incremental processing on Hadoop"](https://www.oreilly.com/ideas/ubers-case-for-incremental-processing-on-hadoop) - O'reilly Ideas article by Vinoth Chandar
2. ["Hoodie: Uber Engineering's Incremental Processing Framework on Hadoop"](https://eng.uber.com/hoodie/) - Engineering Blog By Prasanna Rajaperumal
