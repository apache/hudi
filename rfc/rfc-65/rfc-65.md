## Proposers

- @stream2000
- @hujincalrin
- @huberylee
- @YuweiXiao

## Approvers

## Status

JIRA: [HUDI-5823](https://issues.apache.org/jira/browse/HUDI-5823)

## Abstract

In some classic hudi use cases, users partition hudi data by time and are only interested in data from a recent period
of time. The outdated data is useless and costly, we need a TTL(Time-To-Live) management mechanism to prevent the
dataset from growing infinitely.
This proposal introduces Partition TTL Management policies to hudi, people can config the policies by table config
directly or by call commands. With proper configs set, Hudi can find out which partitions are outdated and delete them.

## Background

TTL management mechanism is an important feature for databases. Hudi already provides a delete_partition interface to
delete outdated partitions. However, users still need to detect which partitions are outdated and
call `delete_partition` manually, which means that users need to define and implement some kind of TTL policies and
maintain proper statistics to find expired partitions by themselves. As the scale of installations grew, it's more
important to implement a user-friendly TTL management mechanism for hudi.

## Implementation

There are 3 components to implement Partition TTL Management

- TTL policy definition & storage
- Gathering proper partition statistics for TTL management
- Executing TTL management

### TTL Policy Definition

We have four main considerations when designing TTL policy:

1. Usually user hopes to manage partition TTL by expired time, so we support the policy `KEEP_BY_TIME` that partitions
   will expire N days after their last modified time. Note that the last modified time here means any inserts/updates to
   the partition. Also, we will make it easy to extend new policies so that users can implement their own polices to
   decide which partitions are expired.
2. User need to set different policies for different partitions. For example, the hudi table is partitioned
   by `user_id`. For partition(user_id='1'), we set the policy that this partition will expire 7 days after its last
   modified time, while for
   partition(user_id='2') we can set the policy that it will expire 30 days after its last modified time.
3. It's possible that users have multi-fields partitioning, and they don't want set policy for all partition. So we need
   to support partition regex when defining
   partition TTL policies. For example, for a hudi table partitioned by (user_id, ts), we can first set a default
   policy(which policy value contains `*`)
   that for all partitions matched (user_id=*/) will expire in 7 days, and explicitly set a policy that for partition
   matched user_id=3 will expire in 30 days. Explicit policy will override default polices, which means default policies
   only takes effects on partitions that do not match any explicit ttl policies.
4. We may need to support record level ttl policy in the future. So partition TTL policy should be an implementation of
   HoodieTTLPolicy.

So we have the TTL policy definition like this (may change when implementing):

```java
public class HoodiePartitionTTLPolicy implements HoodieTTLPolicy {
  public enum TTLPolicy {
    // We supports keep by last modified time at the first version
    KEEP_BY_TIME
  }

  // Partition spec for which the policy takes effect, could be a regex or a static partition value
  private String partitionSpec;

  private TTLPolicy policy;

  private long policyValue;
}
```

### User Interface for TTL policy Definition

Users can config partition TTL management policies through SparkSQL Call Command and through table config directly.
Assume that the user has a hudi table partitioned by two fields(user_id, ts), he can config partition TTL policies as
follows.

```sparksql
-- Set policy for all user_id using partition regex, which keeps the data for 30 days.
call add_ttl_policy(table => 'test', partitionSpec => 'user_id=*/', policy => 'KEEP_BY_TIME', policyValue => '30');

--For partition user_id=3/, keep the data for 7 day.
call add_ttl_policy(table => 'test', partitionSpec => 'user_id=3/', policy => 'KEEP_BY_TIME', policyValue => '7');

-- Show all the TTL policies including default and explicit policies
call show_ttl_policies(table => 'test');
user_id=*/	KEEP_BY_TIME	30
user_id=3/	KEEP_BY_TIME	7
```

### Storage for TTL policy

We need persistent partition ttt policies in hudi table config and users should interact with hudi only by
setting/getting ttl policies. Hudi then will takes care of doing the ttl management in an async table service.

The partition TTL policies will be stored in `hoodie.properties` and they are defined as follows. Explicit policies are
defined using a JSON array, note we support regex here.

```properties
# TTL policy definition
hoodie.partition.ttl.policies=[{"partitionSpec"\:"user_id\=*/","policy"\:"KEEP_BY_TIME","policyValue"\:7},{"partitionSpec"\:"user_id\=3/","policy"\:"KEEP_BY_TIME","policyValue"\:30}]
```

### Partition Statistics

We need to gather proper partition statistics for different kinds of partition ttl policies. Specifically, for
KEEP_BY_TIME policy that we want to support first, we need to know the last modified time for every partition in hudi
table.

To simplify the design, we decide to use the largest commit time of committed file groups in the partition as partition
last modified time. But for file groups generated by replace commit, it may not reveal the real insert/update time for
the file group. We can also introduce other mechanism to get last modified time of the partition, like add a new kind of
partition metadata or add a new field in metadata table. Open to ideas for this design choice.

### Executing TTL management

Once we have defined the TTL policies and have proper partition statistics, it's easy to apply the policies and delete
expired partitions. The process of TTL management is very similar to other table services in hudi, we will provide a new
method called `managePartitionTTL` in `BaseHoodieTableServiceClient` and we can invoke this method in async table
service, SparkSQL Call Command, cli, JAVA code etc.

We will provide an Async Table Service as the default interface to the users that want to do partition TTL management
and we can add more interfaces in the future.

User will config the async ttl management service as follows. TTL management will be trigger N commits after last
TTL replace commit.

```properties
hoodie.partition.ttl.management.async=true
hoodie.partition.ttl.management.min.commits=10
```

The process of manage partition TTL is as follows:

1. Gather partitions statistics, list all partitions and the largest commit time of committed
   file groups of each partition as their last modified time.
2. Apply each TTL policy. For explicit policies, find sub-partitions matched `PartitionSpec` defined in the policy and
   check if there are expired partitions according to the policy type and size. For default policy, find partitions that
   do not match any explicit policy and check if they are expired.
3. For all expired partitions, call delete_partition to delete them, which will generate a replace commit and mark all
   files in those partitions as replaced. For pending clustering and compaction that affect the target partition to
   delete,  [HUDI-5553](https://issues.apache.org/jira/browse/HUDI-5553) introduces a pre-check that will throw an
   exception, and further improvement could be discussed
   in [HUDI-5663](https://issues.apache.org/jira/browse/HUDI-5663).
4. Clean then will delete all replaced file groups.

## Rollout/Adoption Plan

This will be updated once we decide on one of the approaches proposed above.

## Test Plan

This will be updated once we decide on one of the approaches proposed above.




