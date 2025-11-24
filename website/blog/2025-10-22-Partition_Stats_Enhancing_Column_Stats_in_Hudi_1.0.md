---
title: "Partition Stats: Enhancing Column Stats in Hudi 1.0"
excerpt: ""
author: Aditya Goenka and Shiyan Xu
category: lakehouse
image: /assets/images/blog/2025-10-22-Partition_Stats_Enhancing_Column_Stats_in_Hudi_1.0/fig1.jpg
tags:
- hudi
- indexing
- data lakehouse
- data skipping
---

For those tracking Apache Hudi's performance enhancements, the introduction of the column stats index was a significant development, as [detailed in this blog](https://www.onehouse.ai/blog/hudis-column-stats-index-and-data-skipping-feature-help-speed-up-queries-by-an-orders-of-magnitude). It represented a major advancement for query optimization by implementing a straightforward yet highly effective concept: storing lightweight, file-level statistics (such as min/max values and null counts) for specific columns. This provided Hudi's query engine a substantial performance improvement.

![cover](/assets/images/blog/2025-10-22-Partition_Stats_Enhancing_Column_Stats_in_Hudi_1.0/fig1.jpg)

Instead of blindly scanning every single file for a query, the engine could first peek at the index entries—which is far more efficient than reading all the Parquet footers—to determine which files *couldn't* possibly contain the relevant data. This data-skipping capability meant engines could bypass large amounts of irrelevant data, slashing query latency. But that skipping process is conducted at the file level—what if we could apply a similar skipping logic at the partition level? Since a single physical partition can contain thousands of data files, applying this logic at the partition level can further amplify the performance gains by only considering files in the relevant partitions. This is precisely the capability that Hudi 1.0’s partition stats index introduces.

## Multimodal Indexing

Hudi’s [multimodal indexing subsystem](https://hudi.apache.org/docs/indexes#multi-modal-indexing) enhances both read and write performance in data lakehouses by supporting versatile index types optimized for different workloads. This subsystem is built on a scalable, internal metadata table that ensures ACID-compliant updates and efficient lookups, which in turn reduces full data scans. It houses various indexes—such as the files, column stats, and partition stats—which work together to improve efficiency in reads, writes, and upserts, providing scalable, low-latency query performance for large datasets in the lakehouse.

The partition stats index is built on top of the column stats index by aggregating its file-level statistics up to the partition level. As we've covered, the column stats index tracks statistics (min, max, null counts) for *individual files*, enabling fine-grained file pruning. The partition stats index, in contrast, summarizes these same statistics across *all files* within a single partition.

This partition-level aggregation allows Hudi to efficiently prune entire physical partitions before even examining file-level indexes, leading to faster query planning and execution by skipping large chunks of irrelevant data early in the process. In other words, the partition stats index provides a coarse-grained, high-level pruning layer on top of the fine-grained, file-level pruning enabled by the column stats index.

Because partition-level pruning happens first, it narrows down the scope of files that the column stats index needs to inspect, improving overall query performance and reducing overhead on large datasets. The diagram below illustrates the file pruning process:

![file pruning process](/assets/images/blog/2025-10-22-Partition_Stats_Enhancing_Column_Stats_in_Hudi_1.0/fig2.png)

During query planning, the Hudi integration for the query engine takes the predicates parsed from user queries and queries the indexes within the metadata table.

* The files index is queried first to return an initial list of all partitions in the table.  
* The partition stats index then filters this partition list by checking if each partition’s min/max values for the indexed columns fall within the predicate's range. For example, with a predicate of `A = 100`, the index skips any partition whose `min(A)` is greater than 100 or whose `max(A)` is less than 100\.  
* The files index is queried again to retrieve a list of all files *within* these pruned partitions.  
* This file list is then passed to the column stats index, which performs the final, fine-grained pruning by applying the query predicates to the file-level statistics.  
* Finally, this pruned list of files is returned to the query engine to complete query planning.

This dual-layer pruning strategy is especially impactful in production systems managing large amounts of data. By complementing the fine-grained column stats index with this coarse-grained partition skipping, Hudi’s metadata table significantly reduces I/O, computation, and cost. For end-users, this translates directly into a better experience, turning queries that once took minutes into operations that complete in seconds.

## Example: US Shipping Addresses

To understand the impact, let's use the example table below, which stores US shipping addresses for online orders and is partitioned by `state`. This table could contain billions of records, and we want to run a query filtering on the `zip_code` column.

By default, the files, column stats, and partition stats indexes are all enabled in Hudi 1.0. You can create the Hudi table using Spark SQL, for example, without needing additional configs to enable column stats and partition stats:

```sql
CREATE TABLE shipping_address (
    order_id STRING,
    state STRING,
    zip_code STRING,
    ...
) USING HUDI
TBLPROPERTIES (
    primaryKey ='order_id',
    hoodie.metadata.index.column.stats.column.list = 'zip_code'
)
PARTITIONED BY (state);
```

Note that, in practice, you would most likely want to use `hoodie.metadata.index.column.stats.column.list` to indicate which column(s) to index according to your business use case, otherwise, the first 32 columns in the table schema will be indexed by default, which probably won’t be optimal. The specified columns apply to both the column stats and partition stats indexes.

Without the column and partition stats indexes, a query for a specific ZIP code (e.g., `zip_code = '90001'`) would force the query engine to perform a full table scan. This is highly inefficient, leading to high query latency and excessive resource consumption.

With the indexes enabled, the process is drastically different.

1. During write operations, the Hudi writer tracks statistics for the `zip_code` column. The column stats index stores min/max values for each data file, and the partition stats index aggregates and stores the min/max `zip_code` for each `state`.  
2. At query time, suppose the partition stats index shows that the "California" partition contains ZIP codes from "90000" to "96199", while the "New York" partition contains ZIP codes from "10000" to "14999". When the query for `zip_code = '90001'` is executed, the query planner first consults the partition stats index. It sees that "90001" falls within the "California" partition's range but outside the "New York" partition's range.  
3. The engine can therefore skip the entire "New York" partition (and any other partition like "Texas" or "Florida" whose ZIP code range doesn't include "90001"). The query proceeds by only reading data from the "California" partition—the only one that could possibly contain the data.

This ability to prune entire partitions before reading any files is what provides such a significant performance gain.

## Results: the Data Skipping Effect

We conducted a focused benchmarking exercise using a synthetic dataset generated by the open-source tool [lake\_loader](https://github.com/onehouseinc/lake-loader). Specifically, we created a 1 TB table for the US shipping addresses example and built both the column stats and partition stats indexes on this dataset.

The benchmarking objective was to evaluate the performance impact from the two indexes for data skipping. To do this, we executed the following query in two scenarios:

```sql
select count(1) from shipping_address where zip_code = '10001'
```

One with the column and partition stats indexes enabled (default), and one with both indexes disabled for reads, which forced a full table scan.

The Spark job was configured with:

* Executor cores \= 4  
* Executor memory \= 10g  
* Number of executors \= 60

The Spark DAGs for the two scenarios show the file pruning effect:

![Spark DAGs comparison](/assets/images/blog/2025-10-22-Partition_Stats_Enhancing_Column_Stats_in_Hudi_1.0/fig3.png)

With both column stats and partition stats indexes enabled (the left-side DAG), the number of files read was 19,304. In contrast, the disabled setup (the right-side DAG) resulted in reading 393,360 files—about 20 times more.

The runtime comparison chart below shows the query time difference (shorter is better):

![perf run time chart](/assets/images/blog/2025-10-22-Partition_Stats_Enhancing_Column_Stats_in_Hudi_1.0/fig4.jpg)

Enabling data skipping with both the column stats and partition stats indexes for the Hudi table delivers approximately a 93% reduction in query runtime compared to the full scan (no data skipping).

## Conclusion

The new partition stats index is a powerful addition to Hudi's multimodal indexing subsystem, directly addressing the challenge of query performance on large-scale partitioned tables. By working in concert with the existing column stats index, it provides a crucial layer of coarse-grained pruning, allowing the query engine to eliminate entire partitions from consideration *before* inspecting individual files. As our benchmark showed, this two-level pruning strategy—first by partition, then by file—is not just a minor tweak. It results in a dramatic reduction in I/O, slashing query runtimes by over 93% and enabling near-interactive query speeds. This feature solidifies Hudi's data-skipping capabilities, making it even more efficient to run demanding analytical queries directly on the data lakehouse, saving both time and computation costs.
