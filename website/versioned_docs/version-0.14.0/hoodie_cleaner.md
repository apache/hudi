---
title: Cleaning
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---
## Background
Cleaning is a table service employed by Hudi to reclaim space occupied by older versions of data and keep storage costs 
in check. Apache Hudi provides snapshot isolation between writers and readers by managing multiple versioned files with **MVCC** 
concurrency. These file versions provide history and enable time travel and rollbacks, but it is important to manage 
how much history you keep to balance your costs. Cleaning service plays a crucial role in manging the tradeoff between 
retaining long history of data and the associated storage costs.  

Hudi enables [Automatic Hudi cleaning](configurations/#hoodiecleanautomatic) by default. Cleaning is invoked 
immediately after each commit, to delete older file slices. It's recommended to leave this enabled to ensure metadata 
and data storage growth is bounded. Cleaner can also be scheduled after every few commits instead of after every commit by 
configuring [hoodie.clean.max.commits](https://hudi.apache.org/docs/configurations#hoodiecleanmaxcommits).

### Cleaning Retention Policies 
When cleaning old files, you should be careful not to remove files that are being actively used by long running queries.

For spark based:

| Config Name                                        | Default                        | Description                                                                                                                 |
|----------------------------------------------------|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| hoodie.cleaner.policy                              | KEEP_LATEST_COMMITS (Optional) | org.apache.hudi.common.model.HoodieCleaningPolicy: Cleaning policy to be used. <br /><br />`Config Param: CLEANER_POLICY`   |

The corresponding config for Flink based engine is [`clean.policy`](https://hudi.apache.org/docs/configurations/#cleanpolicy).

Hudi cleaner currently supports the below cleaning policies to keep a certain number of commits or file versions:

- **KEEP_LATEST_COMMITS**: This is the default policy. This is a temporal cleaning policy that ensures the effect of
  having lookback into all the changes that happened in the last X commits. Suppose a writer is ingesting data
  into a Hudi dataset every 30 minutes and the longest running query can take 5 hours to finish, then the user should
  retain atleast the last 10 commits. With such a configuration, we ensure that the oldest version of a file is kept on
  disk for at least 5 hours, thereby preventing the longest running query from failing at any point in time. Incremental
  cleaning is also possible using this policy.
  Number of commits to retain can be configured by [`hoodie.cleaner.commits.retained`](https://analytics.google.com/analytics/web/#/p300324801/reports/intelligenthome). 
  The corresponding Flink related config is [`clean.retain_commits`](https://hudi.apache.org/docs/configurations/#cleanretain_commits). 

- **KEEP_LATEST_FILE_VERSIONS**: This policy has the effect of keeping N number of file versions irrespective of time.
  This policy is useful when it is known how many MAX versions of the file does one want to keep at any given time.
  To achieve the same behaviour as before of preventing long running queries from failing, one should do their calculations
  based on data patterns. Alternatively, this policy is also useful if a user just wants to maintain 1 latest version of the file.
  Number of file versions to retain can be configured by [`hoodie.cleaner.fileversions.retained`](https://hudi.apache.org/docs/configurations/#hoodiecleanerfileversionsretained).
  The corresponding Flink related config is [`clean.retain_file_versions`](https://hudi.apache.org/docs/configurations/#cleanretain_file_versions).

- **KEEP_LATEST_BY_HOURS**: This policy clean up based on hours.It is simple and useful when knowing that you want to 
  keep files at any given time. Corresponding to commits with commit times older than the configured number of hours to 
  be retained are cleaned. Currently you can configure by parameter [`hoodie.cleaner.hours.retained`](https://hudi.apache.org/docs/configurations/#hoodiecleanerhoursretained).
  The corresponding Flink related config is [`clean.retain_hours`](https://hudi.apache.org/docs/configurations/#cleanretain_hours).

### Configs
For details about all possible configurations and their default values see the [configuration docs](https://hudi.apache.org/docs/configurations/#Clean-Configs).
For Flink related configs refer [here](https://hudi.apache.org/docs/configurations/#FLINK_SQL).

### Ways to trigger Cleaning

#### Inline

By default, in Spark based writing, cleaning is run inline after every commit using the default policy of `KEEP_LATEST_COMMITS`. It's recommended 
to keep this enabled, to ensure metadata and data storage growth is bounded. To enable this, users do not have to set any configs. Following are the relevant basic configs.

| Config Name                      | Default          | Description                                                                                                                                                                                                                                                                            |
|----------------------------------| -----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.clean.automatic           | true (Optional)  | When enabled, the cleaner table service is invoked immediately after each commit, to delete older file slices. It's recommended to enable this, to ensure metadata and data storage growth is bounded.<br /><br />`Config Param: AUTO_CLEAN`                                           |
| hoodie.cleaner.commits.retained  | 10 (Optional)    | Number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.<br /><br />`Config Param: CLEANER_COMMITS_RETAINED` |


#### Async
In case you wish to run the cleaner service asynchronously along with writing, please enable the [`hoodie.clean.async`](https://hudi.apache.org/docs/configurations#hoodiecleanasync) as shown below:
```java
hoodie.clean.automatic=true
hoodie.clean.async=true
```

For Flink based writing, this is the default mode of cleaning. Please refer to [`clean.async.enabled`](https://hudi.apache.org/docs/configurations/#cleanasyncenabled) for details.

#### Run independently
Hoodie Cleaner can also be run as a separate process. Following is the command for running the cleaner independently:
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

#### CLI
You can also use [Hudi CLI](cli) to run Hoodie Cleaner.

CLI provides the below commands for cleaner service:
- `cleans show`
- `clean showpartitions`
- `cleans run`

Example of cleaner keeping the latest 10 commits
```
cleans run --sparkMaster local --hoodieConfigs hoodie.cleaner.policy=KEEP_LATEST_COMMITS hoodie.cleaner.commits.retained=3 hoodie.cleaner.parallelism=200
```

You can find more details and the relevant code for these commands in [`org.apache.hudi.cli.commands.CleansCommand`](https://github.com/apache/hudi/blob/master/hudi-cli/src/main/java/org/apache/hudi/cli/commands/CleansCommand.java) class. 
