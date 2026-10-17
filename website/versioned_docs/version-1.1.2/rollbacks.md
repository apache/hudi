---
title: Auto Rollbacks
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---

Your pipelines could fail due to numerous reasons like crashes, valid bugs in the code, unavailability of any external 
third-party system (like a lock provider), or user could kill the job midway to change some properties. A well-designed 
system should detect such partially failed commits, ensure dirty data is not exposed to the queries, and clean them up.
Hudi's rollback mechanism takes care of cleaning up such failed writes. 

Hudi’s timeline forms the core for reader and writer isolation. If a commit has not transitioned to complete as per the
hudi timeline, the readers will ignore the data from the respective write. And so partially failed writes are never read
by any readers (for all query types). But the curious question is, how is the partially written data eventually deleted? 
Does it require manual command to be executed from time to time or should it be automatically handled by the system? This
page presents insights on how "rollback" in Hudi can automatically clean up handling partially failed writes without 
manual input from users.

### Handling partially failed commits
Hudi has a lot of platformization built in so as to ease the operationalization of [lakehouse](https://hudi.apache.org/blog/2024/07/11/what-is-a-data-lakehouse/) tables. One such feature 
is the automatic cleanup of partially failed commits. Users don’t need to run any additional commands to clean up dirty 
data or the data produced by failed commits. If you continue to write to hudi tables, one of your future commits will 
take care of cleaning up older data that failed midway during a write/commit. We call this cleanup of a failed commit a 
"rollback". A rollback will revert everything about a commit, including deleting data and removal from the timeline. 
Additionally, the restore operation utilizes a series rollbacks to undo completed commits.

Let’s zoom in a bit and understand how such cleanups happen and the challenges involved in such cleaning when 
multi-writers are involved.

### Rolling back partially failed commits for a single writer
In case of single writer model, the rollback logic is fairly straightforward. Every action in Hudi's timeline goes 
through 3 states, namely requested, inflight and completed. Whenever a new commit starts, hudi checks the timeline 
for any actions/commits that is not yet committed and that refers to partially failed commit. So, immediately rollback 
is triggered and all dirty data is cleaned up followed by cleaning up the commit instants from the timeline.


![An example illustration of single writer rollbacks](/assets/images/blog/rollbacks/Rollback_1.png)
_Figure 1: single writer with eager rollbacks_


### Rolling back of partially failed commits w/ multi-writers
The challenging part is when multi-writers are invoked. Just because a commit is still non-completed as per the 
timeline, it does not mean current writer (new) can assume that it's a partially failed commit. Because, there could be 
a concurrent writer that’s currently making progress. Hudi has been designed to not have any centralized server 
running always and in such a case Hudi has an ingenious way to deduce such partially failed writes.

#### Heartbeats
We are leveraging heartbeats to our rescue here. Each commit will keep emitting heartbeats from the start of the 
write until its completion. During rollback deduction, Hudi checks for heartbeat timeouts for all ongoing or incomplete 
commits and detects partially failed commits on such timeouts. For any ongoing commits, the heartbeat should not 
have elapsed the timeout. For example, if a commit’s heartbeat is not updated for 10+ mins, we can safely assume the 
original writer has failed/crashed and is the incomplete commit is safe to clean up. So, the rollbacks in case of 
multi-writers are lazy and is not eager as we saw with single writer model. But it is still automatic and users don’t 
need to execute any explicit command to trigger such cleanup of failed writes. When such lazy rollback kicks in, both 
data files and timeline files for the failed writes are deleted.

Hudi employs a simple yet effective heartbeat mechanism to notify that a commit is still making progress. A heartbeat 
file is created for every commit under “.hoodie/.heartbeat/” (for eg, “.hoodie/.heartbeat/20230819183853177”). 
The writer will start a background thread which will keep updating this heartbeat file at a regular cadence to refresh
the last modification time of the file. So, checking for last modification time of the heartbeat file gives us 
information whether the writer that started the commit of interest is still making progress or not. On completion of 
the commit, the heartbeat file is deleted. Or if the write failed midway, the last modification time of the heartbeat 
file is no longer updated, so other writers can deduce the failed write after a period of time elapses.

![An example illustration of multi writer rollbacks](/assets/images/blog/rollbacks/rollback2_new.png)
_Figure 2: multi-writer with lazy cleaning of failed commits_

## Related Resources
<h3>Videos</h3>

* [How to Rollback to Previous Checkpoint during Disaster in Apache Hudi using Glue 4.0 Demo](https://www.youtube.com/watch?v=Vi25q4vzogs)