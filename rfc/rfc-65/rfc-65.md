## Proposers
- @stream2000
- @hujincalrin
- @huberylee
- @YuweiXiao
## Approvers
## Status
JIRA: [HUDI-5823](https://issues.apache.org/jira/browse/HUDI-5823)
## Abstract
In some classic hudi use cases, users partition hudi data by time and are only interested in data from a recent period of time. The outdated data is useless and costly,  we need a TTL(Time-To-Live) management mechanism to prevent the dataset from growing infinitely.
This proposal introduces Partition TTL Management policies to hudi, people can config the policies by table config directly or by call commands. With proper configs set, Hudi can find out which partitions are outdated and delete them.
## Background
TTL management mechanism is an important feature for databases. Hudi already provides a delete_partition interface to delete outdated partitions. However, users still need to detect which partitions are outdated and call `delete_partition` manually, which means that users need to define and implement some kind of TTL policies and maintain proper statistics to find expired partitions by themself. As the scale of installations grew,  it's more important to implement a user-friendly TTL management mechanism for hudi.
## Implementation
There are 3 components to implement Partition TTL Management

- TTL policy definition & storage
- Partition statistics for TTL management
- Appling policies
### TTL Policy Definition
We have three main considerations when designing TTL policy:

1. User hopes to manage partition TTL not only by  expired time but also by sub-partitions count and sub-partitions size. So we need to support the following three different TTL policy types.
    1. **KEEP_BY_TIME**. Partitions will expire N days after their last modified time.
    2. **KEEP_BY_COUNT**. Keep N sub-partitions for a  high-level partition. When sub partition count exceeds, delete the partitions with smaller partition values until the sub-partition count meets the policy configuration.
    3. **KEEP_BY_SIZE**. Similar to KEEP_BY_COUNT, but to ensure that the sum of the data size of all sub-partitions does not exceed the policy configuration.
2. User need to set different policies for different partitions. For example, the hudi table is partitioned by two fields (user_id, ts). For partition(user_id='1'), we set the policy to keep 100G data for all sub-partitions, and for partition(user_id='2') we set the policy that all sub-partitions will expire 10 days after their last modified time.
3. It's possible that there are a lot of high-level partitions in the user's table,  and they don't want to set TTL policies for all the high-level partitions. So we need to provide a default policy mechanism so that users can set a default policy for all high-level partitions and add some explicit policies for some of them if needed. Explicit policies will override the default policy.

So here we have the TTL policy definition:
```java
public class HoodiePartitionTTLPolicy {
  public enum TTLPolicy {
    KEEP_BY_TIME, KEEP_BY_SIZE, KEEP_BY_COUNT
  }

  // Partition spec for which the policy takes effect
  private String partitionSpec;

  private TTLPolicy policy;

  private long policyValue;
}
```

### User Interface for TTL policy
Users can config partition TTL management policies through SparkSQL Call Command and through table config directly.  Assume that the user has a hudi table partitioned by two fields(user_id, ts), he can config partition TTL policies as follows.

```sql
-- Set default policy for all user_id, which keeps the data for 30 days.
call add_ttl_policy(table => 'test', partitionSpec => 'user_id=*/', policy => 'KEEP_BY_TIME', policyValue => '30');
 
--For partition user_id=1/, keep 10 sub partitions.
call add_ttl_policy(table => 'test', partitionSpec => 'user_id=1/', policy => 'KEEP_BY_COUNT', policyValue => '10');

--For partition user_id=2/, keep 100GB data in total
call add_ttl_policy(table => 'test', partitionSpec => 'user_id=2/', policy => 'KEEP_BY_SIZE', policyValue => '107374182400');

--For partition user_id=3/, keep the data for 7 day.
call add_ttl_policy(table => 'test', partitionSpec => 'user_id=3/', policy => 'KEEP_BY_TIME', policyValue => '7');

-- Show all the TTL policies including default and explicit policies
call show_ttl_policies(table => 'test');
user_id=*/	KEEP_BY_TIME	30
user_id=1/	KEEP_BY_COUNT	10
user_id=2/	KEEP_BY_SIZE	107374182400
user_id=3/	KEEP_BY_TIME	7
```

### Storage for TTL policy
The partition TTL policies will be stored in `hoodie.properties`since it is part of table metadata. The policy configs in `hoodie.properties`are defined as follows. Explicit policies are defined using a JSON array while default policy is defined using separate configs.

```properties
# Default TTL policy definition
hoodie.partition.ttl.management.default.policy=KEEP_BY_TIME
hoodie.partition.ttl.management.default.fields=user_id
hoodie.partition.ttl.management.default.policy.value=30

# Explicit TTL policy definition
hoodie.partition.ttl.policies=[{"partitionSpec"\:"user_id\=2/","policy"\:"KEEP_BY_SIZE","policyValue"\:107374182400},{"partitionSpec"\:"user_id\=1/","policy"\:"KEEP_BY_COUNT","policyValue"\:10},{"partitionSpec"\:"user_id\=3/","policy"\:"KEEP_BY_TIME","policyValue"\:7}]
```

### Partition Statistics
#### Partition Statistics Entity
We need need to maintain a partition stat for every partition in the hudi table. The stat will contain three fields:

- RelativePath. Relative path to the base path, or we can call it PartitionPath.
- LastModifiedTime. Last instant time that modified the partition. We need this information to support  the `KEEP_BY_TIME` policy.
- PartitionTotalFileSize. The sum of all valid data file sizes in the partition, to support the `KEEP_BY_SIZE`policy. We calculate only the latest file slices in the partition currently.
#### Gathering Partition Statistics
The simplest way to get partition statistics is that reading information from the metadata table. However, the write and compaction of the metadata table will severely affect the throughput and latency of data ingestion.  So we design an asynchronous partition statistics as follows.

- At the first time we gather the partitions statistics, we list all partitions in the hudi table and calculate `PartitionTotalFileSize`and `LastModifiedTime`for each partition. Then we store all the partition stats in persistent storage along with the instant time we do the stats.  For example, we can directly store all partition stats in a JSON file whose file name contains the instant time.
- After initializing the partition statistics, we can list only affected partitions by reading timeline metadata and store the new partition statistics back to the storage with new instant time.
- Note that deleted partition will be deleted from partition statistics too. If a partition was deleted before and have no new data, we will remove it from partition statistics so it won't calculated as expired partition again.
### Appling Policies
Once we have defined the TTL policies and have proper partition statistics,  it's easy to apply the policies and delete expired partitions.

1. Gather partitions statistics.
2. Apply each TTL policy. For explicit policies, find sub-partitions matched `PartitionSpec`defined in the policy and check if there are expired partitions according to the policy type and  size. For default policy, find partitions that do not match any explicit policy and check if they are expired.
3. For all expired partitions, call delete_partition to delete them, which will generate a replace commit and mark all files in those partitions as replaced. For pending clustering and compaction that affect the target partition to delete,  [HUDI-5553](https://issues.apache.org/jira/browse/HUDI-5553) introduces a pre-check that will throw an exception, and further improvement could be discussed in [HUDI-5663](https://issues.apache.org/jira/browse/HUDI-5663).
4. Clean then will delete all replaced file groups.
## Rollout/Adoption Plan
This will be updated once we decide on one of the approaches proposed above.
## Test Plan
This will be updated once we decide on one of the approaches proposed above.




