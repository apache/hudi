---
title: Timeline
toc: true
---

## Timeline
At its core, Hudi maintains a `timeline` of all actions performed on the table at different `instants` of time that helps provide instantaneous views of the table,
while also efficiently supporting retrieval of data in the order of arrival. A Hudi instant consists of the following components

* `Instant action` : Type of action performed on the table
* `Instant time` : Instant time is typically a timestamp (e.g: 20190117010349), which monotonically increases in the order of action's begin time.
* `state` : current state of the instant

Hudi guarantees that the actions performed on the timeline are atomic & timeline consistent based on the instant time.

Key actions performed include

* `COMMITS` - A commit denotes an **atomic write** of a batch of records into a table.
* `CLEANS` - Background activity that gets rid of older versions of files in the table, that are no longer needed.
* `DELTA_COMMIT` - A delta commit refers to an **atomic write** of a batch of records into a  MergeOnRead type table, where some/all of the data could be just written to delta logs.
* `COMPACTION` - Background activity to reconcile differential data structures within Hudi e.g: moving updates from row based log files to columnar formats. Internally, compaction manifests as a special commit on the timeline
* `ROLLBACK` - Indicates that a commit/delta commit was unsuccessful & rolled back, removing any partial files produced during such a write
* `SAVEPOINT` - Marks certain file groups as "saved", such that cleaner will not delete them. It helps restore the table to a point on the timeline, in case of disaster/data recovery scenarios.

Any given instant can be
in one of the following states

* `REQUESTED` - Denotes an action has been scheduled, but has not initiated
* `INFLIGHT` - Denotes that the action is currently being performed
* `COMPLETED` - Denotes completion of an action on the timeline

<figure>
    <img className="docimage" src={require("/assets/images/hudi_timeline.png").default} alt="hudi_timeline.png" />
</figure>

Example above shows upserts happenings between 10:00 and 10:20 on a Hudi table, roughly every 5 mins, leaving commit metadata on the Hudi timeline, along
with other background cleaning/compactions. One key observation to make is that the commit time indicates the `arrival time` of the data (10:20AM), while the actual data
organization reflects the actual time or `event time`, the data was intended for (hourly buckets from 07:00). These are two key concepts when reasoning about tradeoffs between latency and completeness of data.

When there is late arriving data (data intended for 9:00 arriving >1 hr late at 10:20), we can see the upsert producing new data into even older time buckets/folders.
With the help of the timeline, an incremental query attempting to get all new data that was committed successfully since 10:00 hours, is able to very efficiently consume
only the changed files without say scanning all the time buckets > 07:00.