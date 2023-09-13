---
title: Partially Failed Commits
toc: true
---

## Partially failed commits

Your pipelines could fail due to numerous reasons like crashes, valid bugs in the code, unavailability of any external 
third party system (like lock provider), or user could kill mid-way to change some properties. A well designed system should 
detect such partially failed commits and ensure dirty data is not exposed to the read queries and also clean them up. 
We have already took a peek into Hudi’s timeline which forms the core for reader and writer isolation. If a commit has 
not transitioned to complete as per the hudi timeline, the readers will ignore the data from the respective write. 
And so partially failed writes are never read by any readers (for all query types). But the curious question is, how 
does the partially written data is eventually deleted? Does it require manual command to be executed from time to time 
or should it be automatically handled by the system?

### Handling partially failed commits
Hudi has a lot of platformization built in so as to ease the operationalization of lakehouse tables. Once such feature 
is the automatic clean up of partially failed commits. Users don’t need to run any additional commands to clean up dirty 
data or the data produced by failed commits. If you continue to write to hudi tables, one of your future commits will 
take care of cleaning up older data that failed mid-way during a write/commit. This keeps the storage in bounds w/o 
requiring any manual intervention from the users. 

Let’s zoom in a bit and understand how such clean ups happen and is there any challenges involved in such cleaning 
when multi-writers are involved.

#### Rolling back partially failed commits for a single writer 
Incase of single writer model, the rollback logic is fairly straightforward. Every action in Hudi's timeline, goes 
through 3 states, namely requested, inflight and completed. Whenever a new commit starts, hudi checks the timeline 
for any actions/commits that is not yet committed and that refers to partially failed commit. So, immediately rollback 
is triggered and all dirty data is cleaned up followed by cleaning up the commit instants from the timeline.


![An example illustration of single writer rollbacks](/assets/images/blog/rollbacks/single_write_rollback.png)


#### Rolling back of partially failed commits w/ multi-writers
The challenging part is when multi-writers are invoked. Just because, some commit is still non-completed as per the 
timeline, it does not mean current writer (new) can assume its a partially failed commit. Because, there could be a 
concurrent writer that’s currently making progress. And Hudi has been designed to not have any centralized server 
running always and so hudi has a  ingenious way to deduce such partially failed writes.

##### Heart beats to the rescue
We are leveraging heart beats to our rescue here. Each commit will keep emitting heart beats from the start of the 
write to its completion. During rollback deduction, hudi checks for heart beat timeouts for all on-going or incomplete 
commits and detects partially failed commits on such timeouts. For any ongoing commits, the heart beat should not 
have elapsed the timeout. For eg, if a commit’s heart beat is not updated for 10+ mins, we can safely assume the 
original writer has failed/crashed and is safe to clean it up. So, the rollbacks in case of multi-writers are lazy 
and is not eager as we saw with single writer model. But it is still automatic and users don’t need to execute any 
explicit command to trigger such cleanup of failed writes. When such lazy rollback kicks in, both data files and 
timeline files for the failed writes are deleted.

##### Heartbeats
Hudi employs a simple yet effective heartbeat mechanism to notify that a commit is still making progress. A heart beat 
file is created for every commit under “.hoodie/.heartbeat/” (for eg, “.hoodie/.heartbeat/20230819183853177”). 
The writer will start a background thread which will keep updating this heart beat file at regular cadence. So, checking 
for last mod time of the heart beat file will give us information whether the writer that started the commit of 
interest is still making progress or not. On completion of the commit, the heart beat file is deleted. 
Or if the write failed mid-way, the heart beat file is left as is in the heart beat folder and so other writers can 
deduce the failed write at sometime in future.

![An example illustration of multi writer rollbacks](/assets/images/blog/rollbacks/multi_writer_rollback.png)

## Conclusion
Hope this page gave you insights into how partially failed writes are handled by Hudi and how its automatically cleaned up
and users does not have to do any manual clean by themselves. 

