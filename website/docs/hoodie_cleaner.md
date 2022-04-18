---
title: Cleaning
toc: true
---

Hoodie Cleaner is a utility that helps you reclaim space and keep your storage costs in check. Apache Hudi provides 
snapshot isolation between writers and readers by managing multiple files with MVCC concurrency. These file versions 
provide history and enable time travel and rollbacks, but it is important to manage how much history you keep to balance your costs.

[Automatic Hudi cleaning](/docs/configurations/#hoodiecleanautomatic) is enabled by default. Cleaning is invoked immediately after
each commit, to delete older file slices. It's recommended to leave this enabled to ensure metadata and data storage growth is bounded. 

### Cleaning Retention Policies 
When cleaning old files, you should be careful not to remove files that are being actively used by long running queries.
Hudi cleaner currently supports the below cleaning policies to keep a certain number of commits or file versions:

- **KEEP_LATEST_COMMITS**: This is the default policy. This is a temporal cleaning policy that ensures the effect of 
having lookback into all the changes that happened in the last X commits. Suppose a writer is ingesting data 
into a Hudi dataset every 30 minutes and the longest running query can take 5 hours to finish, then the user should 
retain atleast the last 10 commits. With such a configuration, we ensure that the oldest version of a file is kept on 
disk for at least 5 hours, thereby preventing the longest running query from failing at any point in time. Incremental cleaning is also possible using this policy.
- **KEEP_LATEST_FILE_VERSIONS**: This policy has the effect of keeping N number of file versions irrespective of time. 
This policy is useful when it is known how many MAX versions of the file does one want to keep at any given time. 
To achieve the same behaviour as before of preventing long running queries from failing, one should do their calculations 
based on data patterns. Alternatively, this policy is also useful if a user just wants to maintain 1 latest version of the file.

### Configurations
For details about all possible configurations and their default values see the [configuration docs](https://hudi.apache.org/docs/configurations#Compaction-Configs).

### Run Independently
Hoodie Cleaner can be run as a separate process or along with your data ingestion. In case you want to run it along with 
ingesting data, configs are available which enable you to run it [synchronously or asynchronously](https://hudi.apache.org/docs/configurations#hoodiecleanasync).

You can use this command for running the cleaner independently:
```java
[hoodie]$ spark-submit --class org.apache.hudi.utilities.HoodieCleaner \
  --props s3:///temp/hudi-ingestion-config/kafka-source.properties \
  --target-base-path s3:///temp/hudi \
  --spark-master yarn-cluster
```

### Run Asynchronously
In case you wish to run the cleaner service asynchronously with writing, please configure the below:
```java
hoodie.clean.automatic=true
hoodie.clean.async=true
```

### CLI
You can also use [Hudi CLI](/docs/cli) to run Hoodie Cleaner.

CLI provides the below commands for cleaner service:
- `cleans show`
- `clean showpartitions`
- `cleans run`

You can find more details and the relevant code for these commands in [`org.apache.hudi.cli.commands.CleansCommand`](https://github.com/apache/hudi/blob/master/hudi-cli/src/main/java/org/apache/hudi/cli/commands/CleansCommand.java) class. 
