---
title: "What's Apache Hudi, anyway?"
excerpt: "It's been called many things. But, we have always been building a streaming data lake platform"
author: vinoth
category: blog
---

So, why we are writing a post defining the project after 4+ years of existence as an open source project? 
With the exciting developments in the past year or so, that have propelled transactional data 
lakes mainstream, we thought some perspective can help users see the project with the right lens. 

**TODO: insert stats on community etc**
At this time, we also wanted to shine some light on all the great work done by 150+ contributors on the 
project, working with more than 1000 unique users over slack/github/jira, contributing all the different capabilities
Hudi has gained over the past years, from its humble beginnings at Uber.

We also wanted author this as both a backward-looking statement - that gives due credit to the community for all that's 
been built in this time, and a forward-looking statement - that puts forward a vision for what we are building towards.

# Project Evolution

Sometimes it's easy to forget, but Apache Hudi was the original pioneer in bringing upserts/deletes to data lakes, 
way back in 2016, powering all of Uber's data lake, across all three major query engines Hive/Spark/Presto at that time. 
The only other system back was Apache Hive ACID tied to just the one engine, making just one choice for
storage layout, and we believed in a more general purpose, cross-engine design, which has now withstood the test of time.
We also experience a lot of joy to see that similar systems (Delta Lake for e.g) have also adopted the same 
"serverless" transaction layer model, that we originally shared back in Spark Summit '17. We consciously introduced two 
table types `Copy On Write` (simpler to operate) and `Merge On Read` (the swiss army knife) and now these terms
are being used in [projects](https://github.com/apache/iceberg/pull/1862) outside Hudi, to refer to similar ideas being borrowed.

Our grand vision for the project, has always been around making "incremental data processing" at large scale/complexity, 
a reality for data engineers at large. We drew inspiration from database design (indexes, change data capture) and 
stream processing (tying together columnar data and streams), from the get-go. Our goal was that Hudi behave more 
like an OLTP store for writing and an OLAP system for reading, absorbing large amount of mutations like state stores in 
stream processing, while still remaining optimized for large analytical scans.

![whats-hudi](/assets/images/blog/streaming-datalake/hudi-comic.png)

Given our goals, it was only pertinent that we designed our table format and transaction management around events and logs,
that let us efficiently deal with updates to even table metadata or reduce contention points for doing large scale concurrent
actions to the table (more on this below). We have also built a lot of tools/services on top of this foundation, 
to ultimately help users do meaningful things out-of-box without integrating different open source systems themselves.

# Streaming Data Lake Platform 

The best way to describe Apache Hudi is as a _Streaming Data Lake Platform_ - the words carry significant meaning.

**Streaming**: At its core, by optimizing for record level upserts & change streams, Hudi provides primitives to data 
lake workloads that are comparable to what Apache Kafka does for event-streaming (namely, incremental produce/consume of 
events and a state-store for interactive querying)

**Data Lake**: A great debate is going on around data lakes, warehouses and lakehouse. Given Hudi provides the capabilities
that define a lakehouse, and been cited on the paper and blogs as well. But, we want to build lot more around unstructured data, 
including ML/AI vision use-cases. That, combined with the streaming capabilities, led us to keep the broader data lake moniker, 
to match the scope of our vision.

**Platform**: Often times in Open Source, there is great tech, but there is just too many of them - all differing ever
so slightly in their opinionated ways, ultimately making the integration task onerous on the end user. Would n't
we all enjoy great usability from a single platform, in addition to the freedom and transparency of true open source
community? That's exactly the approach we took and building out our data and table services, while tightly integrated
with the Hudi "kernel" (if you will), gives us the ability to deliver much more reliability and usability.

![streaming-data-lake-platform](/assets/images/blog/streaming-datalake/hudi-streaming-data-lake-platform.png)

Rest of the blog will delve in each layer, in our stack - explaining what it does, how its designed and how it will evolve 
in the future. We hope this serves as a good reference for where the project is headed.

# Table Format


# 


# Lessons Learnt (work it in parts and remove this)

This is a transparent message we want to write to the community at large (and not really a marketing prop :)). So, its also
worth sharing some lessons and few things we could have done differently. When we started, we set out to solve just two
core problems : upserts and change streams, while letting the rest of the ecosystem do things "their" way.

In general, we were focussing lot more on the "data plane" and matching the snapshot query performance of the existing engines.
For e.g, while we knew of limitations like storage listings being slow and we can easily construct this metadata from our
timeline logs, we relied on listings until 0.7.0 release, may be wishfully hoping, it gets fixed at the lower layer.
After all, cloud object stores are general purpose storage systems, used (if not more) outside the data lake use-cases.
The fact that we solved this differently for HDFS scaling at Uber, also meant things did not get prioritized high enough.
It did not take us a lot of effort to ultimately do this, but we want to apologize to folks, who had to suffer in silence in the meantime.

Rest assured, we are moving ahead with large overhauls/improvements in the comings releases, to leverage our foundations better
towards snapshot query performance as well.