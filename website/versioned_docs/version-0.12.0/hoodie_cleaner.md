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
  Number of commits to retain can be configured by `hoodie.cleaner.commits.retained`.

- **KEEP_LATEST_FILE_VERSIONS**: This policy has the effect of keeping N number of file versions irrespective of time. 
This policy is useful when it is known how many MAX versions of the file does one want to keep at any given time. 
To achieve the same behaviour as before of preventing long running queries from failing, one should do their calculations 
based on data patterns. Alternatively, this policy is also useful if a user just wants to maintain 1 latest version of the file.
Number of file versions to retain can be configured by `hoodie.cleaner.fileversions.retained`.

- **KEEP_LATEST_BY_HOURS**: This policy clean up based on hours.It is simple and useful when knowing that you want to keep files at any given time.
  Corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.
  Currently you can configure by parameter `hoodie.cleaner.hours.retained`.

### Configurations
For details about all possible configurations and their default values see the [configuration docs](https://hudi.apache.org/docs/configurations#Compaction-Configs).

### Run Independently
Hoodie Cleaner can be run as a separate process or along with your data ingestion. In case you want to run it along with 
ingesting data, configs are available which enable you to run it [synchronously or asynchronously](https://hudi.apache.org/docs/configurations#hoodiecleanasync).

You can use this command for running the cleaner independently:
```
spark-submit --master local --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar` --help
        Usage: <main class> [options]
        Options:
        --help, -h

        --hoodie-conf
        Any configuration that can be set in the properties file (using the CLI
        parameter "--props") can also be passed command line using this
        parameter. This can be repeated
        Default: []
        --props
        path to properties file on localfs or dfs, with configurations for
        hoodie client for cleaning
        --spark-master
        spark master to use.
        Default: local[2]
        * --target-base-path
        base path for the hoodie table to be cleaner.
```
Some examples to run the cleaner.    
Keep the latest 10 commits
```
spark-submit --master local --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar`\
  --target-base-path /path/to/hoodie_table \
  --hoodie-conf hoodie.cleaner.policy=KEEP_LATEST_COMMITS \
  --hoodie-conf hoodie.cleaner.commits.retained=10 \
  --hoodie-conf hoodie.cleaner.parallelism=200
```
Keep the latest 3 file versions
```
spark-submit --master local --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar`\
  --target-base-path /path/to/hoodie_table \
  --hoodie-conf hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS \
  --hoodie-conf hoodie.cleaner.fileversions.retained=3 \
  --hoodie-conf hoodie.cleaner.parallelism=200
```
Clean commits older than 24 hours
```
spark-submit --master local --class org.apache.hudi.utilities.HoodieCleaner `ls packaging/hudi-utilities-bundle/target/hudi-utilities-bundle-*.jar`\
  --target-base-path /path/to/hoodie_table \
  --hoodie-conf hoodie.cleaner.policy=KEEP_LATEST_BY_HOURS \
  --hoodie-conf hoodie.cleaner.hours.retained=24 \
  --hoodie-conf hoodie.cleaner.parallelism=200
```
Note: The parallelism takes the min value of number of partitions to clean and `hoodie.cleaner.parallelism`.

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

Example of cleaner keeping the latest 10 commits
```
cleans run --sparkMaster local --hoodieConfigs hoodie.cleaner.policy=KEEP_LATEST_COMMITS,hoodie.cleaner.commits.retained=3,hoodie.cleaner.parallelism=200
```

You can find more details and the relevant code for these commands in [`org.apache.hudi.cli.commands.CleansCommand`](https://github.com/apache/hudi/blob/master/hudi-cli/src/main/java/org/apache/hudi/cli/commands/CleansCommand.java) class. 
