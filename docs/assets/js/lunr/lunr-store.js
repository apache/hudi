var store = [{
        "title": "S3 Filesystem",
        "excerpt":"In this page, we explain how to get your Hudi spark job to store into AWS S3. AWS configs There are two configurations required for Hudi-S3 compatibility: Adding AWS Credentials for Hudi Adding required Jars to classpath AWS Credentials Simplest way to use Hudi with S3, is to configure your...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/s3_hoodie.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "GCS Filesystem",
        "excerpt":"For Hudi storage on GCS, regional buckets provide an DFS API with strong consistency. GCS Configs There are two configurations required for Hudi GCS compatibility: Adding GCS Credentials for Hudi Adding required jars to classpath GCS Credentials Add the required configs in your core-site.xml from where Hudi can fetch them....","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/gcs_hoodie.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Migration Guide",
        "excerpt":"Hudi maintains metadata such as commit timeline and indexes to manage a dataset. The commit timelines helps to understand the actions happening on a dataset as well as the current state of a dataset. Indexes are used by Hudi to maintain a record key to file id mapping to efficiently...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/migration_guide.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Docker Demo",
        "excerpt":"A Demo using docker containers Lets use a real world example to see how hudi works end to end. For this purpose, a self contained data infrastructure is brought up in a local docker cluster within your computer. The steps have been tested on a Mac laptop Prerequisites Docker Setup...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/docker_demo.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Quick-Start Guide",
        "excerpt":"This guide provides a quick peek at Hudi’s capabilities using spark-shell. Using Spark datasources, we will walk through code snippets that allows you to insert and update a Hudi dataset of default storage type: Copy on Write. After each write operation we will also show how to read the data...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/quick-start-guide.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Structure",
        "excerpt":"Hudi (pronounced “Hoodie”) ingests &amp; manages storage of large analytical datasets over DFS (HDFS or cloud stores) and provides three logical views for query access. Read Optimized View - Provides excellent query performance on pure columnar storage, much like plain Parquet tables. Incremental View - Provides a change stream out...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/structure.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Use Cases",
        "excerpt":"Near Real-Time Ingestion Ingesting data from external sources like (event logs, databases, external sources) into a Hadoop Data Lake is a well known problem. In most (if not all) Hadoop deployments, it is unfortunately solved in a piecemeal fashion, using a medley of ingestion tools, even though this data is...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/use_cases.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Talks & Powered By",
        "excerpt":"Adoption Uber Apache Hudi was originally developed at Uber, to achieve low latency database ingestion, with high efficiency. It has been in production since Aug 2016, powering the massive 100PB data lake, including highly business critical tables like core trips,riders,partners. It also powers several incremental Hive ETL pipelines and being...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/powered_by.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Comparison",
        "excerpt":"Apache Hudi fills a big void for processing data on top of DFS, and thus mostly co-exists nicely with these technologies. However, it would be useful to understand how Hudi fits into the current big data ecosystem, contrasting it with a few related systems and bring out the different tradeoffs...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/comparison.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Concepts",
        "excerpt":"Apache Hudi (pronounced “Hudi”) provides the following streaming primitives over datasets on DFS Upsert (how do I change the dataset?) Incremental pull (how do I fetch data that changed?) In this section, we will discuss key concepts &amp; terminologies that are important to understand, to be able to effectively use...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/concepts.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Writing Hudi Datasets",
        "excerpt":"In this section, we will cover ways to ingest new changes from external sources or even other Hudi datasets using the DeltaStreamer tool, as well as speeding up large Spark jobs via upserts using the Hudi datasource. Such datasets can then be queried using various query engines. Write Operations Before...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/writing_data.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Querying Hudi Datasets",
        "excerpt":"Conceptually, Hudi stores data physically once on DFS, while providing 3 logical views on top, as explained before. Once the dataset is synced to the Hive metastore, it provides external Hive tables backed by Hudi’s custom inputformats. Once the proper hudi bundle has been provided, the dataset can be queried...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/querying_data.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Configurations",
        "excerpt":"This page covers the different ways of configuring your job to write/read Hudi datasets. At a high level, you can control behaviour at few levels. Spark Datasource Configs : These configs control the Hudi Spark Datasource, providing ability to define keys/partitioning, pick out the write operation, specify how to merge...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/configurations.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Performance",
        "excerpt":"In this section, we go over some real world performance numbers for Hudi upserts, incremental pull and compare them against the conventional alternatives for achieving these tasks. Upserts Following shows the speed up obtained for NoSQL database ingestion, from incrementally upserting on a Hudi dataset on the copy-on-write storage, on...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/performance.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Administering Hudi Pipelines",
        "excerpt":"Admins/ops can gain visibility into Hudi datasets/pipelines in the following ways Administering via the Admin CLI Graphite metrics Spark UI of the Hudi Application This section provides a glimpse into each of these, with some general guidance on troubleshooting Admin CLI Once hudi has been built, the shell can be...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/admin_guide.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Privacy Policy",
        "excerpt":"Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following: The IP address from which you access the website; The type of browser and operating system you use to access our site; The date and time...","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/privacy.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Docs Versions",
        "excerpt":"        latest     ","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/docs/docs-versions.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Connect with us at Strata San Jose March 2017",
        "excerpt":"We will be presenting Hudi &amp; general concepts around how incremental processing works at Uber. Catch our talk “Incremental Processing on Hadoop At Uber”   ","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/blog/strata.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},{
        "title": "Hudi entered Apache Incubator",
        "excerpt":"In the coming weeks, we will be moving in our new home on the Apache Incubator.   ","categories": [],
        "tags": [],
        "url": "http://0.0.0.0:4000/blog/asf.html",
        "teaser":"http://0.0.0.0:4000/assets/images/500x300.png"},]
