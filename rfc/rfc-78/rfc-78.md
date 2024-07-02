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
years. It introduces lot of differentiating features for Apache Hudi. We released beta releases which was meant for 
enthusiastic developers/users to give a try of advanced features. But as we are working towards 1.0 GA, we are proposing 
a bridge release (0.16.0) for smoother migration for existing hudi users. 

## Objectives 
Goal is to have a smooth migration experience for the users from 0.x to 1.0. We plan to have a 0.16.0 bridge release asking everyone to first migrate to 0.16.0 before they can upgrade to 1.x.

- 1.x reader should be able to read 0.16.x tables w/o any loss in functionality and no data inconsistencies.
- 0.16.x should have read capability for 1.x tables w/ some limitations. For features ported over from 0.x, no loss in functionality should be guaranteed. But for new features that was introduced in 1.x, we may not be able to support all of them. Will be calling out which new features may not work with 0.16.x reader. In this case, we explicitly request users to not turn on these features till readers are completely in 1.x.
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
- New metadata partitions could be added (optionally enabled)

### MDT changes
- New MDT partitions are available in 1.x. MDT schema upgraded.
- RLI schema is upgraded to hold row position

### Timeline:
- [storage changes] Completed write commits have completed times in the file name.
- [storage changes] Completed and inflight write commits are in avro format which were json in 0.x.
- We are switching the action type for clustering from “replace commit” to “cluster”.
- Similarly, for completed compaction, we are switching from “commit” to “compaction” in an effort to standardize actions for a given write operation.
- [storage changes] Timeline ➝ LST timeline. There is no archived timeline in 1.x
- [In-memory changes] HoodieInstant changes due to presence of completion time for completed HoodieInstants.

### Filegroup/FileSlice changes:
- Log files contain delta commit time instead of base instant time.
- Log appends are disabled in 1.x. In other words, each log block is already appended to a new log file.
- File Slice determination logic for log files changed (in 0.x, we have base instant time in log files and its straight forward. In 1.x, we find completion time for a log file and find the base instant time (parsed from base files) which has the highest value lesser than the completion time of the log file).
- Log file ordering within a file slice. (in 0.x, we use base instant time ➝l log file versions ➝ write token) to order diff log files. in 1.x, we will be using completion time to order).

### Log format changes:
- We have added new header types in 1.x. (IS_PARTIAL)

## Changes to be ported over 0.16.x to support reading 1.x tables
### What will be supported
- For features introduced in 0.x, and tables written in 1.x, 0.16.0 reader should be able to provide consistent reads w/o any breakage.
### What will not be supported
- A 0.16 writer cannot write to a table that has been upgraded-to/created using 1.x without downgrading to 0.16
- For new features introduced in 1.x, we might call out that 0.16.x reader may not support reads.
- if deletion vector is enabled, 0.16.0 reader should fallback to key based merges, giving up on the performance optimization. 
- if partial updates are enabled, 0.16.0 reader should fallback to key based merges, giving up on the performance optimization.
- 0.16.x reader may not support functional indexes built in 1.x. Read may not break, just that the optimization may not kick in.
- 0.16.x reader may not support secondary indexes built in 1.x. Read may not break, just that the optimization may not kick in.
- 0.16.x reader may not support Completion time based log file ordering while reading log files/blocks. 
  So, if you are looking to enable NBCC for your 1.x tables, would recommend to upgrade the readers to 1.x before tuning the feature on.  

#### Completion time based read in FileGroup/FileSlice initialization and log block reads
- If we were to make MDT NBCC default in 1.0, we have to support completion time based apis and implementations in 0.16. ??
- If not, we still have a chance to not support completion time based reads with 0.16.0.
- What's the impact if not for this support:
  - For users having single writer mode or OCC in 1.x, 0.16.0 reader w/o supporting completion time based read should still be ok. Both 1.x reader and 0.16.0 reader should have same behavior.
  - But only if someone has NBCC writes and have log files in different ordering written compared to the log file versions, 0.16.0 reader might result in data consistency issues. but we can call out 0.16.0 is a bridge release and recommend users to migrate all the readers to 1.x fully before starting to enable any new features for 1.x tables.
  - Example scenarios. say, we have lf1_10_25, lf2_15_20(format "logfile[index]_[starttime]_[completiontime]") for a file slice. In 1.x reader, we will order and read it as lf2 followed by lf1. w/o this support in 0.16.0, we might read lf1 followed by lf2. Just to re-iterate this might only impact users who have enabled NBCC and having multi writers writing log files in different ordering. Even if they were using OCC, one of the writers is expected to have failed (on the writer side) since data is overlapping for two writers in 1.x writer.
  
### Timeline
- Timeline read of 1.x need to be supported.
- Commit instants w/ completion time should be readable.
- We could ignore the completion time semantics since we don’t plan to support completion time based log file ordering in 0.16.0. But reads should not break.
- Commit metadata in avro instead of json should be readable.
   - More details on this under Implementation section.
- Clustering commits using “cluster” action should be readable in 0.16.0 reader.
- A completed compaction using “compaction” as action instead of “commit” should be readable with 0.16.0 reader.
- HoodieDefaultTimeline should be able to support both 0.x timeline and 1.x timeline.
   - More details on this under Implementation section.
- HoodieInstant parsing logic to parse completion time should be ported over
- Completion time based APIs might be ignored or unavailable with 0.x reader. If we were to make MDT NBCC default in 1.0, we have to support completion time based apis and implementations. For now, we are assuming we are not supporting completion time based read in 0.16.0.
- Support ignoring partially failed log files from FSV. In 0.16.0, from FSV standpoint, all log files(including partially failed) are valid. We let the log record reader ignore the partially failed log files. But 
in 1.x, log files could be rolledback (deleted) by a concurrent rollback. So, the FSV should ensure it ignores the uncommitted log files. 


### FileSystemView:
- FSV building/reading should account for commit instants in both 0.x and 1.x formats. Since completion time log file ordering is not supported in 0.16.0 (that’s our current assumption), we may not need to support exactly how a FSV in 1.x reader supports. But below items should be supported.
    - File slicing should be intact. For a single writer and OCC based writer in 1.x, the mapping of log files to base instant time or file slice should be same across 1.x reader and 0.16.0 reader.
    - Log file ordering will follow 0.16.0 logic. I.e. log version followed by write token based comparator. Even if there are log files which completed in different order using 1.x writer, since we don’t plan to support that feature in 0.16.0 reader, we can try to maintain parity with 0.16.0 reader or log file ordering.
    - We might have to revisit this if we plan to make NBCC default with MDT in 1.x
- If not for completion time based read support, what will break, check [here](#Completion time based read in FileGroup/FileSlice initialization and log block reads) 

### Table properties
- payload type inference from payload class need to be ported.

### MDT changes:
- MDT schema changes need to be ported.

### Log format changes:
- New log header type need to be ported.
- For unsupported features, meaningful errors should be thrown. For eg, if DV or partial update has been enabled and if those log files are read using 0.16.0 reader, we should fail and throw a meaningful error.

### Incremental reads and time travel query: 
- Incremental reads in 1.x is expected to have some changes and design is not fully out. So, until then we have to wait to design 0.16.x read support for 1.x tables for incremental queries. 
- Time travel query: I don't see any changes in 1.x wrt time travel query. So, as per master, we are still using commit time(i.e not completed time) to serve time travel query. Until we make any changes to that, we do not need to add any additional support to 0.16.x reader. But if we plan to make changes in 1.x, we might have to revisit this. 

## 0.16.0 ➝ 1.0 upgrade steps
This will be an automatic upgrade for users when they start using 1.x hudi library. Listing the changes we might need to do with the upgrade.
- Rewrite archival timeline to LSM based timeline.
- Do not touch active timeline, since there could be concurrent readers reading the table. So, 1.x reader should be capable of handling timeline w/ a mix of 0.x commit files and 1.x commit files.
- No changes to log reader.
- Check custom payload class in table properties and switch to payload type.
- Trigger compaction for latest file slices. We do not want a single file slice having a mix of log files from 0.x and log files from 1.x. So, we will trigger a full compaction 
of the table to ensure all latest file slices has just the base files. 
  - Lets dissect and see what it needs to support not requiring the full compaction. In general, we plan to add a table config to track the commit time (more on this later in this doc) when the upgrade was done. 
    So, using the upgrade commit time, we should be able to use different log file comparator to order log files within a given file slice. 

## 1.0 ➝ 0.16.0 downgrade steps
Users will have to use hudi-cli to do the downgrade. Here are the steps we might need to do during downgrade.
- Similar to our upgrade, lets trigger full compaction to latest file slice so that all latest file slices only has base files. We do not want to have any file slices w/ a mix of 1.x log files and 0.x log files.
- Rewrite LSM based timeline to archived timeline. we have to deduce writer properties and introduce boundaries based on that (i.e. until which commit to archive).
- We have two options wrt handling active timeline instants:
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
- Commit instants w/ completion time should be readable.
- We could ignore the completion time semantics since we don’t plan to support completion time based log file ordering in 0.16.0. But reading should not break.
- Commit metadata in both avro and json should be supported.
- Clustering commits using “cluster” action should be readable in 0.16.0 reader.
- A completed compaction using “compaction” as action instead of “commit” should be readable with 0.16.0 reader.
- HoodieDefaultTimeline should be able to support both 0.x timeline and 1.x timeline.
- HoodieInstant parsing logic to parse completion time should be ported over
- Completion time based APIs might be ignored or unavailable with 0.x reader. If we were to make MDT NBCC default in 1.0, we have to support completion time based apis and implementations. For now, we are assuming we are not supporting completion time based read in 0.16.0.
On a high level, we need to ensure commit metadata in either format (avro or json) need to be supported. And “cluster” and completed “compaction”s need to be readable in 0.16.0 reader.
- But the challenging part is, for every commit metadata, we might have to deserialize to avro and on exception try json. We could deduce the format using completion file name, but as per current code layering, deserialization methods does not know the file name( method takes byte[]).
- Similarly for clustering commits, unless we have some kind of watermark, we have to keep considering replace commits as well in the FSV building logic to ensure we do not miss any clustering commits.
- To be decided: We also need to use diff LogFileComparators depending on the file slice's base instant time. If the file slices's base instant time is < table upgrade commit time, we use older log file comparator to order log files. but if file slice's base instant time > table upgrade commit time, we have to use new log file comparator (completion time). Tricky part is if a file slice contains a mix of log files. 
 This fix definitely needs to go into 1.x, but whether we wanted to port this change to 0.16.x or not is yet to be discussed and decided. Lets zoom in a bit to see what will happen if a single file slice could contain a mix of log files using 1.x reader(this is a basic requirement to support 0.16.x tables in 1.x). 
Unless NBCC is enabled, we can leverage last mod time of commit meta files files instead of the completion time (since 0.16.0 commit meta files might be missing the completion time). By this approach, we are using completion time based ordering for a file slice w/ a mix of old and new log files. For 0.16.x tables, we will use older log file ordering, while completely new file slice will be using newer completion time based ordering.  

- a. So for above mentioned reasons, we plan to introduce a new table config named “table version upgrade commit time” which will be referring to the latest commit which was written in 0.16.0 just before upgrading to 1.x. Once we have this commit time as water mark, which marks the upgrade in the timeline, even a 1.x reader should only account for older compatible reads up until the water mark. For eg, to deser replace commits for the interest of clustering can be dropped after the water mark. Once we get past that, only “cluster”ing commits can represent clustering writes. We could use the same watermark for commit metadata parsing as well. So, that at some point we could safely assume entire timeline is only in avro (speaking from a 1.x FSV/reader standpoint) and not in json.
- b. We need to port any changes we have done in HoodieDefaultTimeline in 1.x to 0.16.0 as well. For eg, commit metadata parsing logic will have to support both avro and json. Using above watermark, we might need to add additional argument to deser methods. If not, we might try to deser using avro or inspect magic bytes and then fallback to json.
- c. Some of the methods were renamed in 1.x compared to 0.16.0.
findInstantsInRangeByStateTransitionTime -> findInstantsInRangeByCompletionTime
findInstantsModifiedAfterByStateTransitionTime -> findInstantsModifiedAfterByCompletionTime
getInstantsOrderedByStateTransitionTime -> getInstantsOrderedByCompletionTime
We need to add back these older methods to HoodieDefaultTimeline, so that we do not need to fix all the callers in 0.16.0. We could certainly introduce private methods and avoid code duplication.
- d. HoodieInstant changes from 1.x need to be ported over so that completion time is accounted for. If completion time is present, we should account for that, if not, we can fallback to last mod time of the completed commit meta file.
- e. We need to port code changes which accounts for uncommitted log files. In 0.16.0, from FSV standpoint, all log files(including partially failed) are valid. We let the log record reader ignore the partially failed log files. But
  in 1.x, log files could be rolledback (deleted) by a concurrent rollback. So, the FSV should ensure it ignores the uncommitted log files.
- f. Looks like we only have to make changes/appends to few methods in HoodieDefaultTimeline. But one option to potentially consider (if we see us making lot of changes to 0.16.0 HoodieDefaultTimeline in order to support reading 1.x tables), we could introduce Hoodie016xDefaultTimeline and Hoodie1xDefaultTimeline and use delegate pattern to delegate to either of the timelines. Using hoodie table version we could instantiate (internally to HoodieDefaultTimeline) to either of Hoodie016xDefaultTimeline or Hoodie1xDefaultTimeline. But for now, we don’t feel we might need to take this route. Just calling it out as an option depending on the changes we had to make.
- g. Since log file ordering logic will differ from 0.16.x and 1.x, and we have a table upgrade commit time, we could leverage that to use diff log file ordering logic based on whether a file slice's base instant time is less or greater than table upgrade commit time. 

### FileSystemView changes
Once all timeline changes are incorporated, we need to account for FSV changes. Major change as called out earlier is the Completion time based log files from 1.x writer and the log file naming referring to delta commit time instead of base commit time. So, w/o any changes to FSV/HoodieFileGroup/HoodieFileSlice code snippets, our file slice deduction logic might be wrong. Each log file could be tagged as its own file slice since each has a different base commit time (thats how 0.16.x HoodieLogFile would deduce it). So, we might have to port over CompletionTimeQueryView class and associated logic to 0.16.0. So, for file slice deduction logic in 0.16.0 will be pretty much similar to 1.x reader. But the log file ordering for log reading purpose, we do not need to maintain parity with 1.x reader as of yet. (unless we make NBCC default with MDT).
Assuming 1.x reader and 1.x FSV should be able to read data written in older hudi versions, we also have a potential option here for avoid making nit-picky changes similar to the option called out earlier.
We could instantiate two different FSV depending on the table version. If table version is 7 (0.16.0), we could instantiate FSV_V0 may be and if table version is 8 (1.0.0), we could instantiate FSV_V1. So that we don’t break/regress any of 0.16.0 read functionality in the interest of supporting 1.x table reads. We should strive to cover all scenarios and not let any bugs creep in, but trying to see if we can keep the changes isolated so that battle tested code (FSV) is not touched or changed for the purpose of supporting 1.x table reads. If we run into any bugs with 1.x reads, we could ask users to not upgrade any of the writers to 1.x and stick with 0.16.0 unless we have say 1.0.1 or something. But it would be really bad if we break 0.16.0 table read in some edge case.  Just calling out as one of the safe option to upgrade.

#### Pending exploration:
1. We removed special suffixes to MDT operations in 1x. we need to test the flow and flush out details if anything to be added to 0.16.x reader. 

## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.