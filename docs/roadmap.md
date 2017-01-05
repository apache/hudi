---
title: Roadmap
keywords: usecases
sidebar: mydoc_sidebar
permalink: roadmap.html
---

## Planned Features

* Support for Self Joins - As of now, you cannot incrementally consume the same table more than once, since the InputFormat does not understand the QueryPlan.
* Hoodie Spark Datasource -  Allows for reading and writing data back using Apache Spark natively (without falling back to InputFormat), which can be more performant
* Hoodie Presto Connector - Allows for querying data managed by Hoodie using Presto natively, which can again boost [performance](https://prestodb.io/docs/current/release/release-0.138.html)


