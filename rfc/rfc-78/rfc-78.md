<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-76: [Bridge release for 1.x]

## Proposers

- @nsivabalan
- @vbalaji

## Approvers
 - @yihua
 - @codope

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-7882

> Please keep the status updated in `rfc/README.md`.

## Abstract

[Hudi 1.x](https://github.com/apache/hudi/blob/ae1ee05ab8c2bd732e57bee11c8748926b05ec4b/rfc/rfc-69/rfc-69.md) is a powerful 
re-imagination of the transactional database layer in Hudi to power continued innovation across the community in the coming 
years. It introduces lot of differentiating features for Apache Hudi. Feel free to checkout the 
[release page](https://hudi.apache.org/releases/release-1.0.0-beta1) for more info. We had beta1 and beta2 releases which was meant for 
interested developers/users to give a spin on some of the  advanced features. But as we are working towards 1.0 GA, we are proposing 
a bridge release (0.16.0) for smoother migration for existing hudi users. 

## Objectives 
Goal is to have a smooth migration experience for the users from 0.x to 1.0. We plan to have a 0.16.0 bridge release asking everyone to first migrate to 0.16.0 before they can upgrade to 1.x. 

A typical organization might have a medallion architecture deployed to run 1000s of Hudi pipelines i.e. bronze, silver and gold layer. 
For this layout of pipelines, here is how a typical migration might look like(w/o a bridge release)

a. Existing pipelines are in 0.15.x. (bronze, silver, gold) 
b. Migrate gold pipelines to 1.x. 
- We need to strictly migrate only gold to 1x. Bcoz, a 0.15.0 reader may not be able to read 1.x hudi tables. So, if we migrate any of silver pipelines to 1.x before migrating entire gold layer, we might end up in a situation 
- where a 0.15.0 reader (gold) might end up reading 1.x table (silver). This might lead to failures. So, we have to follow certain order in which we migrate pipelines. 
c. Once all of gold is migrated to 1.x, we can move all of silver to 1.x. 
d. Once all of gold and silver is migrated to 1.x, finally we can move all of bronze to 1.x.

In the end, we would have migrated all of existing hudi pipelines from 0.15.0 to 1.x. 
But as you could see, we need some coordination with which we need to migrate. And in a very large organization, sometimes we have not have good control over downstream consumers. 
Hence, coordinating entire migration workflow and orchestrating the same might be challenging.

Hence to ease the migration workflow for 1.x, we are introducing 0.16.0 as a bridge release.  

Here are the objectives with this bridge release:

- 1.x reader should be able to read 0.14.x to 0.16.x tables w/o any loss in functionality and no data inconsistencies.
- 0.16.x should have read capability for 1.x tables w/ some limitations. For features ported over from 0.x, no loss in functionality should be guaranteed. 
But for new features that was introduced in 1.x, we may not be able to support all of them. Will be calling out which new features may not work with 0.16.x reader. 
- In this case, we explicitly request users to not turn on these features untill all readers are completely migrated to 1.x.

Connecting back to our example above, lets see how the migration might look like for an existing user. 

a. Existing pipelines are in 0.15.x. (bronze, silver, gold)
b. Migrate pipelines to 0.16.0 (in any order. we do not have any constraints around which pipeline should be migrated first). 
c. Ensure all pipelines are in 0.16.0(both readers and writers)
d. Start migrating pipelines in a rolling fashion to 1.x. At this juncture, we could have few pipelines in 1.x and few pipelines in 0.16.0. but since 0.16.x 
can read 1.x tables, we should be ok here. Just that do not enable new features like Non blocking concurrency control yet. 
e. Migrate all of 0.16.0 to 1.x version. 
f. Once all readers and writers are in 1.x, we are good to enable any new features (like NBCC). 

As you could see, company/org wide coordination to migrate gold before migrating silver or bronze is relaxed with the bridge release. Only requirement to keep a tab on, 
is to ensure to migrate all pipelines completely to 0.16.x before starting to migrate to 1.x

So, here are the objectives of this RFC with the bridge release. 
- 1.x reader should be able to read 0.14.x to 0.16.x tables w/o any loss in functionality and no data inconsistencies.
- 0.16.x should have read capability for 1.x tables w/ some limitations. For features ported over from 0.x, no loss in functionality should be guaranteed.
  But for new features that was introduced in 1.x, we may not be able to support all of them. Will be calling out which new features may not work with 0.16.x reader.
- Document upgrade steps from 0.16.x to 1.x with limited user perceived latency. This will be auto upgrade, but document clearly what needs to be done.
- Downgrade from 1.x to 0.16.x documented with call outs on any functionality.

### Considerations when choosing Migration strategy
- While migration is happening, we want to allow readers to continue reading data. This means, we cannot employ a stop-the-world strategy when we are migrating. 
All the actions that we are performing as part of table upgrade should not have any side-effects of breaking snapshot isolation for readers.
- Also, users should have migrated to 0.16.x before upgrading to 1.x. We do not want to add read support for very old versions of hudi in 1.x(for eg 0.7.0). 
- So, in an effort to bring everyone to latest hudi versions, 1.x reader will have full read capabilities for 0.16.x, but for older hudi versions, 1.x reader may not have full reader support. 
The reocmmended guideline is to upgrade all readers and writers to 0.16.x. and then slowly start upgrading to 1.x(readers followed by writers). 

Before we dive in further, lets understand the format changes:

## Format changes
### Table properties
- Payload class ➝ payload type.
- hoodie.record.merge.mode
- New metadata partitions could be added (optionally enabled)

### MDT changes
- New MDT partitions are available in 1.x. MDT schema upgraded.
- RLI schema is upgraded to hold row positions.

### Timeline:
- [storage changes] Completed write commits have completed times in the file name.
- [storage changes] Completed and inflight write commits are in avro format which were json in 0.x.
- We are switching the action type for pending clustering from “replace commit” to “cluster”.
- [storage changes] Timeline ➝ LST timeline. There is no archived timeline in 1.x.
- [In-memory changes] HoodieInstant changes due to presence of completion time for completed HoodieInstants.

### Filegroup/FileSlice changes:
- Log file names contain delta commit time instead of base instant time.
- Log appends are disabled in 1.x. In other words, each log block is already appended to a new log file.
- File Slice determination logic for log files changed. In 0.x, we have base instant time in log files and its straight forward. 
In 1.x, we find completion time for a log file and find the base instant time (parsed from base files) for a given HoodieFileGroup, 
- which has the highest value lesser than the completion time of the log file) of interest. 
- Log file ordering within a file slice. (in 0.x, we use base instant time ➝ log file versions ➝ write token) to order diff log files. in 1.x, we will be using completion time to order).
- Rollbacks in 0.x appends a new rollback block (new log file). While in 1.x, rollback will remove the partially failed log files. 

### Log format changes:
- We have added new header types in 1.x. (IS_PARTIAL)

## Changes to be ported over to 0.16.x to support reading 1.x tables

### What will be supported
- For features introduced in 0.x, and tables written in 1.x, 0.16.0 reader should be able to provide consistent reads w/o any breakage.
- 
### What will not be supported
- A 0.16 writer cannot write to a table that has been upgraded-to/created using 1.x without downgrading to 0.16.
- For new features introduced in 1.x, we might call out that 0.16.x reader may not support reads.
- If deletion vector is enabled, 0.16.0 reader should fallback to key based merges, giving up on the performance optimization. 
- If partial updates are enabled, 0.16.0 reader should fallback to key based merges, giving up on the performance optimization.
- 0.16.x reader may not support functional indexes built in 1.x. Read may not break, just that the optimization may not kick in.
- 0.16.x reader may not support secondary indexes built in 1.x. Read may not break, just that the optimization may not kick in.
- 0.16.x reader may not support Completion time based log file ordering while reading log files/blocks. Disclaimer: - If we were to make NBCC default with MDT in 1.0, we have to support completion time based ordering in 0.16. For now, we are not covering those details since we are yet to finalize it with 1.x
  So, if you are looking to enable any of unsupported features like NBCC for your 1.x tables, would recommend to upgrade the readers to 1.x before tuning the feature on.  

### Timeline
- Timeline read of 1.x need to be supported.
- Commit instants w/ completion time should be readable. HoodieInstant parsing logic to parse completion time should be ported over.
- Commit metadata in avro instead of json should be readable.
   - More details on this under Implementation section.
- Pending Clustering commits using “cluster” action should be readable in 0.16.0 reader.
- HoodieDefaultTimeline should be able to support both 0.x timeline and 1.x timeline.
   - More details on this under Implementation section.

#### Completion time based read in FileGroup/FileSlice grouping and log block reads
- As of this write up, we are not supporting completion time based log file ordering in in 0.16. Punting it as called out earlier.
- What's the impact if not for this support:
    - For users having single writer mode or OCC in 1.x, 0.16.0 reader not supporting completion time based read(log file ordering) should still be ok. Both 1.x reader and 0.16.0 reader should have same behavior.
    - But only if someone has NBCC writes and have log files in different ordering written compared to the log file versions, 0.16.0 reader might result in data consistency issues. but since we are calling out that 0.16.0 is a bridge release and recommend users to migrate all the readers to 1.x fully before starting to enable any new features for 1.x tables.
    - Example scenarios. say, we have lf1_10_25, lf2_15_20(format "logfile[index]_[starttime]_[completiontime]") for a file slice. In 1.x reader, we will order and read it as lf2 followed by lf1. w/o this support in 0.16.0, we might read lf1 followed by lf2. Just to re-iterate this might only impact users who have enabled NBCC and having multi writers writing log files in different ordering. Even if they were using OCC, one of the writers is expected to have failed (on the writer side) since data is overlapping for two writers in 1.x writer.

### FileSystemView:
- Support ignoring partially failed log files from FSV. In 0.16.0, from FSV standpoint, all log files(including partially failed) are valid. We let the log record reader ignore the partially failed log files. But
  in 1.x, log files could be rolledback (deleted) by a concurrent rollback. So, the FSV should ensure it ignores the uncommitted log files. 
- Completion time might be used only for file slice determination and not for log file ordering.
    - More details on this under Implementation section.
- FSV building/reading should account for commit instants in both 0.x and 1.x formats. Since completion time log file ordering is not supported in 0.16.0 (that’s our current assumption), we may not need to support exactly how a FSV in 1.x reader supports. But below items should be supported.
    - File slicing should be intact. For a single writer and OCC based writer in 1.x, the mapping of log files to base instant time or file slice should be same across 1.x reader and 0.16.0 reader.
    - Log file ordering will follow 0.16.0 logic. I.e. log version followed by write token based comparator. Even if there are log files which completed in different order using 1.x writer, since we don’t plan to support that feature in 0.16.0 reader, we can try to maintain parity with 0.16.0 reader or log file ordering.
    - We might have to revisit this if we plan to make NBCC default with MDT in 1.x
- If not for completion time based read support, what will break, check [here](#Completion time based read in FileGroup/FileSlice initialization and log block reads) 

### Table properties
- payload type inference from payload class need to be ported.

### MDT changes:
- MDT schema changes need to be ported.

### Log reader/format changes:
- New log header type need to be ported.
- For unsupported features, meaningful errors should be thrown. For eg, partial update has been enabled and if those log files are read using 0.16.0 reader, we should fail and throw a meaningful error.
- For deletion vector, we should add the fallback option to 0.16.0 so that we give up on perf optimization, but still support reads w/o any failures. 

### Read optimized query:
- No explicit changes required (apart from timeline, and FSV) to support read optimized query.  

### Incremental reads and time travel query: 
- Incremental reads in 1.x is expected to have some changes and design is not fully out. So, until then we have to wait to design 0.16.x read support for 1.x tables for incremental queries. 
- Time travel query: I don't see any changes in 1.x wrt time travel query. So, as per master, we are still using commit time(i.e not completed time) to serve time travel query. Until we make any changes to that, we do not need to add any additional support to 0.16.x reader. But if we plan to make changes in 1.x, we might have to revisit this. 

### CDC reads:
- There has not been a lot of changes in 1.x wrt CDC. Only minor change was done in HoodieAppendHandle. but that affects only the writer side logic. So, we are good wrt CDC. i.e we do not need to port 
any additional logic to 0.16.x reader just for the purpose of CDC in addition to changes covered in this RFC. 

## 0.16.0 ➝ 1.0 upgrade steps
This will be an automatic upgrade for users when they start using 1.x hudi library. Listing the changes we might need to do with the upgrade.
- Rewrite archival timeline to LSM based timeline.
- Do not touch active timeline, since there could be concurrent readers reading the table. So, 1.x reader should be capable of handling timeline w/ a mix of 0.x commit files and 1.x commit files.
- But we need to trigger rollback of any failed writes using 0.16.x rollback logic. Since with 1.x, rollback will delete the log files based on delta commit times in log file naming, this rollback logic may not work for 
log files written in 0.16.0, since we have log appends. So, we need to trigger rollbacks of failed writes using 0.16.x rollback flow and not 1.x flow. 
- No changes to log reader.
- Check custom payload class and hoodie.record.merge.mode in table properties and switch to respective 0.16.x properties.
- Debatable: Trigger compaction for latest file slices. We do not want a single file slice having a mix of log files from 0.x and log files from 1.x. So, we will trigger a full compaction 
of the table to ensure all latest file slices has just the base files. I need to dug deeper on this(need to have detailed discussion w/ initial authors who designed this). But if we ensure all partially failed writes are rollbacked completely, 
we should be good to upgrade w/o needing to trigger full compaction. Lets walk through an example:
Say we have a file group and latest file slice is fs3. and it has 2 log files in 0.x. 
  fs3(t3):
  lf1_(3_10) lf2_(3_10)
here format is lf[log version]_([committime]_[completiontime])
base file instant time is 3 and so all log files (in memory) has same begin and completion time.(Remember completion time is only applicable for commits and not for data files. and so, from 1.x reader standpoint,
lf1 and lf2 has delta commit time as t3 (which matches the base file's instant time) and its completion time is t10. 

lets trigger an upgrade and add 2 new files. 

fs3(t3):
lf1_(3_10) lf2_(3_10) [upgrade] lf3_15_20, lf4_40_100.

With this layout, 1.x reader should be able to read this file slice correctly w/o any issues. So, we should not require a full compaction during upgrade. But need to consult w/ authors to ensure we are not missing anything.

## 1.0 ➝ 0.16.0 downgrade steps
Users will have to use hudi-cli to do the downgrade. Here are the steps we might need to do during downgrade.
- Similar to our upgrade, lets trigger full compaction to latest file slice so that all latest file slices only has base files. We do not want to have any file slices w/ a mix of 1.x log files and 0.x log files. 
We could afford to have some long compute during downgrade, but upgrade path should be kept to as minimum as possible. So, during upgrade, we are not enforcing this.   
- Rewrite LSM based timeline to archived timeline. we have to deduce writer properties and introduce boundaries based on that (i.e. until which commit to archive).
- We have two options wrt handling active timeline instants(for eg, pending clustering instants):
   A: No changes to active instants. In order to support 1.x tables w/ 0.16.0 reader, already these changes might have been ported to 0.16.0 reader. But we might have to ensure downgrade from 0.16.0 to any older version of hudi does take care of rewriting the active timeline instants.
   B: While downgrading from 1.x to 0.16.0, let’s rewrite the active timeline instants too. So that after downgrade, we can't differentiate whether table was natively created in 0.16.0 or was it upgraded to 1.x and then later downgraded. The con in Option A is not an issue here since we take care of active timeline instants during 1.x to 0.16.0 downgrade only. So, downgrade from 0.16.0 to any lower hudi version does not need to do any special handling if table was native to 0.16.0 or was it downgraded from 1.x to 0.16.0
   Our proposal is to go with Option B to ensure 0.16.0 table will be intact and not have any residues from 1.x writes. While the rewrite of active instants are happening, we could see some read failures. But 
   we do not want to have a 0.16.0 (post upgrade) table to deal w/ 1.x timeline intricacies. 
- If there are new MDT partitions (functional index, sec index), nuke them. Update table properties.
- Check latest snapshot files for log files w/ new features enabled. For eg, deletion vector, partial update. If any log files are found in latest snapshot, we have to trigger compaction for those file groups. This calls for a custom compaction strategy as well which compacts only for certain file groups.
   - In order to reduce downtime to downgrade, we are inclining to do this only for snapshot reads. If there are older file slices and users are issuing time travel or incremental reads, and if they have any of the new features enabled (deletion vector, partial update), readers could break after downgrade.

## Implementation details

Atleast for adding read capability of 1.x tables in 0.16.0, documenting the changes required at class level. Other two sections (0.16.0 upgrade to 1.x or 1.x downgrade to 0.16.0) should be fairly straightforward.

### Timeline changes:
Let’s reiterate what we need to support w/ 0.16.0 reader.

### Timeline read of 1.x need to be supported
- Commit instants w/ completion time should be readable. HoodieInstant parsing logic to parse completion time should be ported over.
- We could ignore the completion time log file ordering semantics since we don’t plan to support it yet in 0.16.0. But reading should not break.
- Pending Clustering commits using “cluster” action should be readable in 0.16.0 reader.
- HoodieDefaultTimeline should be able to support both 0.x timeline and 1.x timeline. 
- Commit metadata in both avro and json should be supported. The challenging part here is, for every commit metadata, we might have to deserialize to avro and on exception try json. We could deduce the format using completion file name, but as per current code layering, deserialization methods does not know the file name( method takes byte[]). Similarly for clustering commits, unless we have some kind of watermark, we have to keep considering replace commits as well in the FSV building logic to ensure we do not miss any pending clustering commits.

Actual code changes: 
- So for above mentioned reasons, we plan to introduce a new table config named “table version upgrade commit time” which will be referring to the latest commit which was written in 0.16.0 just before upgrading to 1.x. Once we have this commit time as water mark, which marks the upgrade in the timeline, even a 1.x reader should only account for older compatible reads up until the water mark. For eg, to deser pending replace commits for the interest of clustering can be dropped after the water mark. Once we get past that, only “cluster”ing pending commits can represent pending clustering writes. We could use the same watermark for commit metadata parsing as well. So, that at some point(after the water mark) we could safely assume entire timeline is only in avro (speaking from a 1.x FSV/reader standpoint) and not in json.
- We need to port any changes we have done in HoodieDefaultTimeline in 1.x to 0.16.0 as well. 
- a. Commit metadata parsing logic will have to support both avro and json. Using above watermark, we might need to add additional argument to deser methods. If not, we might try to deser using avro or inspect magic bytes and then fallback to json.
- b. Some of the methods were renamed in 1.x compared to 0.16.0.
findInstantsInRangeByStateTransitionTime -> findInstantsInRangeByCompletionTime
findInstantsModifiedAfterByStateTransitionTime -> findInstantsModifiedAfterByCompletionTime
getInstantsOrderedByStateTransitionTime -> getInstantsOrderedByCompletionTime
We need to add back these older methods to HoodieDefaultTimeline, so that we do not need to fix all the callers in 0.16.0. We could certainly introduce private methods and avoid code duplication.
- c. HoodieInstant changes from 1.x need to be ported over so that completion time is accounted for. If completion time is present, we should account for that, if not, we can fallback to last mod time of the completed commit meta file.
- d. We need to port code changes which accounts for uncommitted log files. In 0.16.0, from FSV standpoint, all log files(including partially failed) are valid. We let the log record reader ignore the partially failed log files. But
  in 1.x, log files could be rolledback (deleted) by a concurrent rollback. So, the FSV should ensure it ignores the uncommitted log files.
- e. full LSM based read capability needs to be ported back to 0.16.x timeline reader. 
- f. Given the requirement to port over above changes, we have two options to go about HoodieDefaultTimeline. But one option to consider: we could introduce Hoodie016xDefaultTimeline and Hoodie1xDefaultTimeline and use delegate pattern to delegate to either of the timelines. Using hoodie table version we could instantiate (internally to HoodieDefaultTimeline) to either of Hoodie016xDefaultTimeline or Hoodie1xDefaultTimeline. 
Or we need to make pointed fixes to entire HoodieDefaultTimeline to account for some of these backwards compatible logic. But given that 1.x reader should anyway support most of these backwards compatible ones, our proposal is to just have one HoodieDefaultTimeline(ported over from 1.x) and make pointed fixes to ensure its backwards compatible.  

### FileSystemView changes
Once all timeline changes are incorporated, we need to account for FSV changes. Major change as called out earlier is the Completion time based log files from 1.x writer and the log file naming referring to delta commit time instead of base commit time. So, w/o any changes to FSV/HoodieFileGroup/HoodieFileSlice code snippets, our file slice deduction logic might be wrong. 
Each log file could be tagged as its own file slice since each has a different base commit time (thats how 0.16.x HoodieLogFile would deduce it). So, we might have to port over CompletionTimeQueryView class and associated logic to 0.16.0. So, for file slice deduction logic in 0.16.0 will be pretty much similar to 1.x reader. But the log file ordering for log reading purpose, we do not need to maintain parity with 1.x reader as of yet. (unless we make NBCC default with MDT).
Assuming 1.x reader and 1.x FSV should be able to read data written in older hudi versions, we also have a potential option here for avoid making nit-picky changes similar to the option called out earlier.
We could instantiate two different FSV depending on the table version. If table version is 7 (0.16.0), we could instantiate FSV_V0 may be and if table version is 8 (1.0.0), we could instantiate FSV_V1. Proposing this option just to have a super cautious approach, so that we don’t break/regress any of 0.16.0 read functionality in the interest of supporting 1.x table reads. 
We should strive to cover all scenarios and not let any bugs creep in, but trying to see if we can keep the changes isolated so that battle tested code (0.x FSV) is not touched or changed for the purpose of supporting 1.x table reads. If we run into any bugs with 1.x reads, we could ask users to not upgrade any of the writers to 1.x and stick with 0.16.0 unless we have say 1.0.1 or something. But it would be really bad if we break 0.16.0 table reads in some edge case.  Just calling out as one of the safe option to upgrade.
- Support ignoring partially failed log files from FSV. In 0.16.0, from FSV standpoint, all log files(including partially failed) are valid. We let the log record reader ignore the partially failed log files. But
    in 1.x, log files could be rolledback (deleted) by a concurrent rollback. So, the FSV should ensure it ignores the uncommitted log files. But we can only ignore the log files written in 1.x. Because in 0.x, we have log appends. And the log file naming does not contain 
delta commit times. So, based on table upgrade commit time, we have to only ignore uncommitted log files whose instant times are > table upgrade commit time. For older log files, we can let FSV expose it and let the log record reader take care of ignoring partially failed log files. or if we go with FSV0 and FSV1, FSV0 can still be same as 0.15.0 and FSV1 (if table is deduced to be 1.x) can be used 
to read 1.x tables and filtering out uncommitted log files will only be applicable for FSV1.

## Flink
Most of the timeline, FSV are shared b/w spark and flink. Only additional changes we did to Flink that is not in spark (yet) is the incremental read. We have introduced completion time based incremental query capabilities in 1.x Flink reads. 
So, we might have to port over the changes to 0.16.x reader. Depending on table version, we could go with either of the two implementations(0.x Incremental read, or 1.x Incremental reads). So, both implementations will reside in 0.16.0 depending on which table is being read. 
We do not want to make nit-picky changes and will keep two implementations separate.

#### Pending exploration:
1. We removed special suffixes to MDT operations in 1x. By skimming the code changes, we should be ok, i.e. no additional changes required. But we need to test the flow and flush out details if anything to be added to 0.16.x reader. 
2. Just during upgrade, if there are pending commits in timeline, if we trigger rollback, since rollback will follow 1.x flow, our rollback may not work (since we expect delta commit times in file names). But for log files written in 0.x,
we may not have delta commit times in 1.x So, we need to flush this out in finer detail. 

## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.