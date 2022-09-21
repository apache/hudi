---
title: "Apache Hudi - 2021 a Year in Review"
excerpt: "A reflection on the growth and momentum of Apache Hudi in 2021"
author: vinoth
category: blog
image: /assets/images/Hudi_community.png
tags:
- blog
- community
- apache hudi
---

As the year came to end, I took some time to reflect on where we are and what we accomplished in 2021. I am humbled by how strong our community is and how regardless of it being another tough pandemic year, that people from around the globe leaned in together and made this the best year yet for Apache Hudi. In this blog I want to recap some of the 2021 highlights.

<!--truncate-->
<img src="/assets/images/Hudi_community.png" alt="drawing" width="600"/>

**_Community_**

I want to call out how amazing it is to see such a diverse group of people step up and contribute to this project. There were over 30,000 interactions with the [project on github](https://github.com/apache/hudi/), up 2x from last year. Over the last year 300 people have contributed to the project, with over 3,000 PRs over 5 releases. We moved Apache Hudi from release 0.5.X all the way to our feature packed 0.10.0 release. Come and join us on our [active slack channel](https://join.slack.com/t/apache-hudi/shared_invite/zt-1e94d3xro-JvlNO1kSeIHJBTVfLPlI5w)! Over 850 community members engaged on our slack, up about 100% from the year before. I want to add a special shout out to our top slack participants who have helped answer so many questions and drive rich discussions on our channel. Sivabalan Narayanan, Nishith Agarwal, Bhavani Sudha Saktheeswaran, Vinay Patil, Rubens Soto, Dave Hagman, Raghav Tandon, Sagar Sumit, Joyan Sil, Jake D, Felix Jose, Nick Vintila, KimL, Andrew Sukhan, Danny Chan, Biswajit Mohapatra, and Pratyaksh Sharma! I know I am missing plenty of other important callouts, every PR that landed this year has helped shape Hudi into what it is today. Thank you!

<img src="/assets/images/powers/logo-wall.png" alt="drawing" width="600"/>

**_Impact_**

In 2021, I personally developed a deeper gratitude and understanding of the magnitude of the impact we are making in the industry. Throughout the year I met more and more people that told me about how Hudi transformed their business and I was impressed by the large variety of use cases and applications that Hudi was able to serve. Some from the community who publicly shared their story include: [Amazon](https://aws.amazon.com/blogs/big-data/how-amazon-transportation-service-enabled-near-real-time-event-analytics-at-petabyte-scale-using-aws-glue-with-apache-hudi/), [GE](https://aws.amazon.com/blogs/big-data/how-ge-aviation-built-cloud-native-data-pipelines-at-enterprise-scale-using-the-aws-platform/), [Robinhood](https://s.apache.org/hudi-robinhood-talk), [ByteDance](http://hudi.apache.org/blog/2021/09/01/building-eb-level-data-lake-using-hudi-at-bytedance), [Halodoc](https://blogs.halodoc.io/data-platform-2-0-part-1/), [Baixin Bank](https://developpaper.com/baixin-banks-real-time-data-lake-evolution-scheme-based-on-apache-hudi/), [BiliBili](https://developpaper.com/practice-of-apache-hudi-in-building-real-time-data-lake-at-station-b/), and so many more that haven’t even shared yet. One particular highlight from 2021 was attending [AWS Re:Invent](https://youtu.be/lGm8qe4tBrg?t=2115) and meeting an overwhelmingly large number of users who expressed joy with using Apache Hudi. This raises my sense of responsibility even more to be aware of just how many people depend on Apache Hudi.

**_New Features_**

Apache Hudi has come a long way in 2021 from v0.5.X to 0.10.0. Throughout this year we have developed innovative and leading edge features that make it easier and easier to build streaming data lakes. Some of these features include [Spark SQL DML Support](https://hudi.apache.org/docs/table_management), [Clustering](https://hudi.apache.org/docs/clustering), [Z-Order/Hilbert curves](https://hudi.apache.org/blog/2021/12/29/hudi-zorder-and-hilbert-space-filling-curves), [Metadata Table file listing elimination](https://hudi.apache.org/docs/metadata), [Timeline Server Markers](https://hudi.apache.org/docs/markers), [Precommit Validators](https://hudi.apache.org/docs/precommit_validator), [Flink MOR write/read](https://hudi.apache.org/docs/writing_data#flink-sql-writer), [Parallel Write support with OCC](https://hudi.apache.org/docs/concurrency_control), [Clustering](https://hudi.apache.org/docs/clustering), [Incremental Queries for MOR](https://hudi.apache.org/docs/querying_data#spark-incr-query), [Kafka Connect Sink](https://github.com/apache/hudi/tree/master/hudi-kafka-connect), Delta Streamer sources for [S3](https://hudi.apache.org/docs/hoodie_deltastreamer/#s3-events) and [Debezium](https://hudi.apache.org/releases/release-0.10.0/#debezium-deltastreamer-sources), [DBT Support](https://hudi.apache.org/releases/release-0.10.0/#dbt-support) all of which are were added in 2021. To top it all, we put together [a manifesto](https://hudi.apache.org/blog/2021/07/21/streaming-data-lake-platform) to realize our vision for streaming data lakes.

**_The Road Ahead_**

2021 may have been our best year so far, but it still feels like we are just getting started when we look at our new year's resolutions for 2022. In the year ahead we have bold plans to realize the first cut of our entire vision and take Hudi 1.0, that includes full-featured multi-modal indexing for faster writes/queries, pathbreaking lock free concurrency, new server components for caching/metadata and finally Flink based incremental materialized views!  _You can find our_ [_detailed roadmap here_](https://hudi.apache.org/roadmap)_._

I look forward to continued collaboration with the growing Hudi community! Come join our [_community events_](https://hudi.apache.org/community/syncs) _and discussions in our_ [_slack channel_](https://join.slack.com/t/apache-hudi/shared_invite/zt-1e94d3xro-JvlNO1kSeIHJBTVfLPlI5w)_! Happy new year 2022!_