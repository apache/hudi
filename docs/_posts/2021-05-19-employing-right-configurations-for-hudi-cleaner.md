---
title: "Employing correct configurations for Hudi's cleaner table service"
excerpt: "Achieving isolation between Hudi writer and readers using `HoodieCleaner.java`"
author: pratyakshsharma
category: blog
---

Apache Hudi provides snapshot isolation between writers and readers. This is made possible by Hudi’s MVCC concurrency model. In this blog, we will explain how to employ the right configurations to manage multiple file versions. Furthermore, we will discuss mechanisms available to users generating Hudi tables on how to maintain just the required number of old file versions so that long running readers do not fail. 

### Reclaiming space and bounding your data lake growth

Hudi provides different table management services to be able to manage your tables on the data lake. One of these services is called the **Cleaner**. As you write more data to your table, for every batch of updates received, Hudi can either generate a new version of the data file with updates applied to records (COPY_ON_WRITE) or write these delta updates to a log file, avoiding rewriting newer version of an existing file (MERGE_ON_READ). In such situations, depending on the frequency of your updates, the number of file versions of log files can grow indefinitely. If your use-cases do not require keeping an infinite history of these versions, it is imperative to have a process that reclaims older versions of the data. This is Hudi’s cleaner service.

### Problem Statement

In a data lake architecture, it is a very common scenario to have readers and writers concurrently accessing the same table. As the Hudi cleaner service periodically reclaims older file versions, scenarios arise where a long running query might be accessing a file version that is deemed to be reclaimed by the cleaner. Here, we need to employ the correct configs to ensure readers (aka queries) don’t fail.

### Deeper dive into Hudi Cleaner

To deal with the mentioned scenario, lets understand the  different cleaning policies that Hudi offers and the corresponding properties that need to be configured. Options are available to schedule cleaning asynchronously or synchronously. Before going into more details, we would like to explain a few underlying concepts:

 - **Hudi base file**: Columnar file which consists of final data after compaction. A base file’s name follows the following naming convention: `<fileId>_<writeToken>_<instantTime>.parquet`. In subsequent writes of this file, file id remains the same and commit time gets updated to show the latest version. This also implies any particular version of a record, given its partition path, can be uniquely located using the file id and instant time. 
 - **File slice**: A file slice consists of the base file and any log files consisting of the delta, in case of MERGE_ON_READ table type.
 - **Hudi File Group**: Any file group in Hudi is uniquely identified by the partition path and the  file id that the files in this group have as part of their name. A file group consists of all the file slices in a particular partition path. Also any partition path can have multiple file groups.

### Cleaning Policies

Hudi cleaner currently supports below cleaning policies:

 - **KEEP_LATEST_COMMITS**: This is the default policy. This is a temporal cleaning policy that ensures the effect of having lookback into all the changes that happened in the last X commits. Suppose a writer ingesting data  into a Hudi dataset every 30 minutes and the longest running query can take 5 hours to finish, then the user should retain atleast the last 10 commits. With such a configuration, we ensure that the oldest version of a file is kept on disk for at least 5 hours, thereby preventing the longest running query from failing at any point in time. Incremental cleaning is also possible using this policy.
 - **KEEP_LATEST_FILE_VERSIONS**: This is a static numeric policy that has the effect of keeping N number of file versions irrespective of time. This policy is use-ful when it is known how many MAX versions of the file does one want to keep at any given time. To achieve the same behaviour as before of preventing long running queries from failing, one should do their calculations based on data patterns. Alternatively, this policy is also useful if a user just wants to maintain 1 latest version of the file.

### Examples

Suppose a user uses the below configs for cleaning:

```java
hoodie.cleaner.policy=KEEP_LATEST_COMMITS
hoodie.cleaner.commits.retained=10
```

Cleaner selects the versions of files to be cleaned by taking care of the following:

 - Latest version of a file should not be cleaned.
 - The commit times of the last 10 (configured) + 1 commits are determined. One extra commit is included because the time window for retaining commits is essentially equal to the longest query run time. So if the longest query takes 5 hours to finish, and ingestion happens every 30 minutes, you need to retain last 10 commits since 10*30 = 300 (5 hours). At this point of time, the longest query can still be using files written in 11th commit in reverse order.  Now for any file group, only those file slices are scheduled for cleaning which are not savepointed (another Hudi table service) and whose commit time is less than the 11th commit in reverse order.

Suppose a user uses the below configs for cleaning:

```java
hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
hoodie.cleaner.fileversions.retained=2
```

Cleaner does the following:

 - For any file group, last 2 versions (including any for pending compaction) of file slices are kept and the rest are scheduled for cleaning.

### Configurations

You can find the details about all the possible configurations along with the default values [here](https://hudi.apache.org/docs/configurations.html#compaction-configs).

### Run command

Hudi's cleaner table service can be run as a separate process or along with your data ingestion. As mentioned earlier, it basically cleans up any stale/old files lying around. In case you want to run it along with ingesting data, configs are available which enable you to run it in [parallel or in sync](https://hudi.apache.org/docs/configurations.html#withAsyncClean). You can use the below command for running the cleaner independently:

```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.HoodieCleaner \
  --props s3:///temp/hudi-ingestion-config/kafka-source.properties \
  --target-base-path s3:///temp/hudi \
  --spark-master yarn-cluster
```

### Future Scope

Work is currently going on for introducing a new cleaning policy based on time elapsed. This will help in achieving a consistent retention throughout regardless of how frequently ingestion happens. You may track the progress [here](https://issues.apache.org/jira/browse/HUDI-349).

We hope this blog gives you an idea about how to configure the Hudi cleaner and the supported cleaning policies. Please visit the [blog section](https://hudi.apache.org/blog.html) for a deeper understanding of various Hudi concepts. Cheers!