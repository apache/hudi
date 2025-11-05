---
title: Record Mergers 
keywords: [hudi, merge, upsert, precombine]
toc: true
toc_min_heading_level: 2
toc_max_heading_level: 4
---

Hudi handles mutations to records and streaming data, as we briefly touched upon in [timeline ordering](timeline#ordering-of-actions) section. 
To provide users full-fledged support for stream processing, Hudi goes all the way making the storage engine and the underlying storage format 
understand how to merge changes to the same record key, that may arrive even in different order at different times. With the rise of mobile applications
 and IoT, these scenarios have become the normal than an exception. For e.g. a social networking application uploading user events several hours after they happened,
when the user connects to WiFi networks.

To achieve this, Hudi supports merge modes, which define how the base and log files are ordered in a file slice and further how different records with 
the same record key within that file slice are merged consistently to produce the same deterministic results for snapshot queries, writers and table services. Specifically, 
there are three merge modes supported as a table-level configuration, invoked in the following places. 

 * **(writing)** Combining multiple change records for the same record key while reading input data during writes. This is an optional optimization that 
    reduces the number of records written to log files to improve query and write performance subsequently.

 * **(writing)** Merging final change record (partial/full update/delete) against existing record in storage for CoW tables. 

 * **(compaction)** Compaction service merges all change records in log files against base files, respecting the merge mode.

 * **(query)** Merging change records in log files, after filtering/projections against base file for MoR table queries.

Note that the merge mode should not be altered once the table is created to avoid inconsistent behavior due to compaction producing 
different merge results when switching between the modes.

### COMMIT_TIME_ORDERING

Here, we expect the input records to arrive in strict order such that arrival order is same as their
delta commit order on the table. Merging simply picks the record belonging to the latest write as the merged result. In relational data mode speak, 
this provides overwrite semantics aligned with serializable writes on the timeline. 

<figure>
    <img className="docimage" src={require("/assets/images/commit-time-ordering-merge-mode.png").default} alt="upsert_path.png" />
</figure>

In the example above, the writer process consumes a database change log, expected to be in strict order of a logical sequence number (lsn)
that denotes the ordering of the writes in the upstream database.

### EVENT_TIME_ORDERING

This is the default merge mode. While commit time ordering provides a well-understood standard behavior, it's hardly sufficient. The commit time is unrelated to the actual 
ordering of data that a user may care about and strict ordering of input in complex distributed systems is difficult to achieve. 
With event time ordering, the merging picks the record with the highest value on a user specified _**ordering or precombine field**_ as the merged result. 

<figure>
    <img className="docimage" src={require("/assets/images/event-time-ordering-merge-mode.png").default} alt="upsert_path.png" />
</figure>

In the example above, two microservices product change records about orders at different times, that can arrive out-of-order. As color coded, 
this can lead to application-level inconsistent states in the table if simply merged in commit time order like a cancelled order being re-created or 
a paid order moved back to just created state expecting payment again. Event time ordering helps by ignoring older state changes that arrive late and
avoiding order status from "jumping back" in time. Combined with [non-blocking concurrency control](concurrency_control#non-blocking-concurrency-control-mode), 
this provides a very powerful way for processing such data streams efficiently and correctly.

### CUSTOM

In some cases, even more control and customization may be needed. Extending the same example above, the two microservices could be updating two different
set of columns "order_info" and "payment_info", along with order state. The merge logic is then expected to not only resolve the correct status, but merge 
order_info from the record in created state, into the record in cancelled state that already has payment_info fields populated with reasons payment failed.
Such reconciliation provide a simple denormalized data model for downstream consumption where queries (for e.g. fraud detection) can simply filter fields 
across order_info and payment_info without costly self-join on each access.

Hudi allows authoring of cross-language custom record mergers on top of a standard record merger API, that supports full and partial merges. The java APIs 
are sketched below at a high-level. It simply takes older/newer records in engine native formats and produces a merged record or returns empty to skip them entirely (e.g. soft deletes).
Record merger is configured using a `hoodie.write.record.merge.strategy.id` write config whose value is an uuid, that is taken by the writer to persist in the table config, and is expected to be returned by `getMergingStrategy()`
method below. Using this mechanism, Hudi can automatically deduce the record merger to use for the table across different language/engine runtimes.

```Java
interface HoodieRecordMerger {

    Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, 
                                             HoodieRecord newer, Schema newSchema, 
                                             TypedProperties props) {
        ...
    }

    Option<Pair<HoodieRecord, Schema>> partialMerge(HoodieRecord older, Schema oldSchema, 
                                                    HoodieRecord newer, Schema newSchema, 
                                                    Schema readerSchema, TypedProperties props) {
        ...
    }
     
    HoodieRecordType getRecordType() {...}
    
    String getMergingStrategy(); {...}
}
```

### Record Merge Configs

The record merge mode and optional record merge strategy ID and custom merge implementation classes can be specified using the below configs. 

| Config Name                            | Default                                                                | Description                                                                                                                                                                                                                         |
| ---------------------------------------| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.write.record.merge.mode  | EVENT_TIME_ORDERING | Determines the logic of merging different records with the same record key. Valid values: (1) `COMMIT_TIME_ORDERING`: use commit time to merge records, i.e., the record from later commit overwrites the earlier record with the same key. (2) `EVENT_TIME_ORDERING` (default): use event time as the ordering to merge records, i.e., the record with the larger event time overwrites the record with the smaller event time on the same key, regardless of commit time. The event time or preCombine field needs to be specified by the user. (3) `CUSTOM`: use custom merging logic specified by the user.<br />`Config Param: RECORD_MERGE_MODE`<br />`Since Version: 1.0.0` |
| hoodie.write.record.merge.strategy.id  | N/A (Optional) | ID of record merge strategy. When you specify this config, you also need to specify `hoodie.write.record.merge.custom.implementation.classes`. Hudi picks the `HoodieRecordMerger` implementation class from the list of classes in `hoodie.write.record.merge.custom.implementation.classes` that has the specified merge strategy ID.<br />`Config Param: RECORD_MERGE_STRATEGY_ID`<br />`Since Version: 0.13.0` |
| hoodie.write.record.merge.custom.implementation.classes  | N/A (Optional) | List of `HoodieRecordMerger` implementations constituting Hudi's merging strategy based on the engine used. Hudi picks the `HoodieRecordMerger` implementation class from this list based on the specified `hoodie.write.record.merge.strategy.id`.<br />`Config Param: RECORD_MERGE_IMPL_CLASSES`<br />`Since Version: 0.13.0` |


### Record Payloads

:::caution
Going forward, we recommend users to migrate and use the record merger APIs and not write new payload implementations.
:::
Record payload is an older abstraction/API for achieving similar record-level merge capabilities. While record payloads were very useful and popular, 
it had drawbacks like lower performance due to conversion of engine native record formats to Apache Avro for merging and lack of cross-language support.
As we shall see below, Hudi provides out-of-box support for different payloads for different use cases. Hudi implements fallback from
record merger APIs to payload APIs internally, to provide backwards compatibility for existing payload implementations.

#### OverwriteWithLatestAvroPayload
```scala
hoodie.datasource.write.payload.class=org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
```

This is the default record payload implementation. It picks the record with the greatest value (determined by calling
`.compareTo()` on the value of precombine key) to break ties and simply picks the latest record while merging. This gives
latest-write-wins style semantics.

#### DefaultHoodieRecordPayload
```scala
hoodie.datasource.write.payload.class=org.apache.hudi.common.model.DefaultHoodieRecordPayload
```
While `OverwriteWithLatestAvroPayload` precombines based on an ordering field and picks the latest record while merging,
`DefaultHoodieRecordPayload` honors the ordering field for both precombinig and merging. Let's understand the difference with an example:

Let's say the ordering field is `ts` and record key is `id` and schema is:

```
{
  [
    {"name":"id","type":"string"},
    {"name":"ts","type":"long"},
    {"name":"name","type":"string"},
    {"name":"price","type":"string"}
  ]
}
```

Current record in storage:

```
    id      ts      name    price
    1       2       name_2  price_2
```

Incoming record:

```
    id      ts      name    price
    1       1       name_1    price_1
```

Result data after merging using `OverwriteWithLatestAvroPayload` (latest-write-wins):

```
    id      ts      name    price
    1       1       name_1  price_1
```

Result data after merging using `DefaultHoodieRecordPayload` (always honors ordering field):

```
    id      ts      name    price
    1       2       name_2  price_2
```

#### EventTimeAvroPayload
```scala
hoodie.datasource.write.payload.class=org.apache.hudi.common.model.EventTimeAvroPayload
```
This is the default record payload for Flink based writing. Some use cases require merging records by event time and 
thus event time plays the role of an ordering field. This payload is particularly useful in the case of late-arriving data. 
For such use cases, users need to set the [payload event time field](configurations#RECORD_PAYLOAD) configuration.

#### OverwriteNonDefaultsWithLatestAvroPayload
```scala
hoodie.datasource.write.payload.class=org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload
```
This payload is quite similar to `OverwriteWithLatestAvroPayload` with slight difference while merging records. For
precombining, just like `OverwriteWithLatestAvroPayload`, it picks the latest record for a key, based on an ordering
field. While merging, it overwrites the existing record on storage only for the specified **fields that don't equal
default value** for that field.

#### PartialUpdateAvroPayload
```scala
hoodie.datasource.write.payload.class=org.apache.hudi.common.model.PartialUpdateAvroPayload
```
This payload supports partial update. Typically, once the merge step resolves which record to pick, then the record on
storage is fully replaced by the resolved record. But, in some cases, the requirement is to update only certain fields
and not replace the whole record. This is called partial update. `PartialUpdateAvroPayload` provides out-of-box support 
for such use cases. To illustrate the point, let us look at a simple example:

Let's say the ordering field is `ts` and record key is `id` and schema is:

```
{
  [
    {"name":"id","type":"string"},
    {"name":"ts","type":"long"},
    {"name":"name","type":"string"},
    {"name":"price","type":"string"}
  ]
}
```

Current record in storage:

```
    id      ts      name    price
    1       2       name_1  null
```

Incoming record:

```
    id      ts      name    price
    1       1       null    price_1
```

Result data after merging using `PartialUpdateAvroPayload`:

```
    id      ts      name    price
    1       2       name_1  price_1
```

#### Configs

Payload class can be specified using the below configs. For more advanced configs refer [here](https://hudi.apache.org/docs/configurations#RECORD_PAYLOAD)

**Spark based configs:**

| Config Name                            | Default                                                                | Description                                                                                                                                                                                                                         |
| ---------------------------------------| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.datasource.write.payload.class  | org.apache.hudi.common.model.OverwriteWithLatestAvroPayload (Optional) | Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective<br /><br />`Config Param: WRITE_PAYLOAD_CLASS_NAME` |

**Flink based configs:**

| Config Name                            | Default                                                                | Description                                                                                                                                                                                                                         |
| ---------------------------------------| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| payload.class                          | org.apache.hudi.common.model.EventTimeAvroPayload (Optional)           | Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for the option in-effective<br /><br /> `Config Param: PAYLOAD_CLASS_NAME`                    |


There are also quite a few other implementations. Developers may be interested in looking at the hierarchy of `HoodieRecordPayload` interface. For
example, [`MySqlDebeziumAvroPayload`](https://github.com/apache/hudi/blob/e76dd102bcaf8aec5a932e7277ccdbfd73ce1a32/hudi-common/src/main/java/org/apache/hudi/common/model/debezium/MySqlDebeziumAvroPayload.java) and [`PostgresDebeziumAvroPayload`](https://github.com/apache/hudi/blob/e76dd102bcaf8aec5a932e7277ccdbfd73ce1a32/hudi-common/src/main/java/org/apache/hudi/common/model/debezium/PostgresDebeziumAvroPayload.java) provides support for seamlessly applying changes 
captured via Debezium for MySQL and PostgresDB. [`AWSDmsAvroPayload`](https://github.com/apache/hudi/blob/e76dd102bcaf8aec5a932e7277ccdbfd73ce1a32/hudi-common/src/main/java/org/apache/hudi/common/model/AWSDmsAvroPayload.java) provides support for applying changes captured via Amazon Database Migration Service onto S3.
For full configurations, go [here](configurations#RECORD_PAYLOAD) and please check out [this FAQ](faq_writing_tables/#can-i-implement-my-own-logic-for-how-input-records-are-merged-with-record-on-storage) if you want to implement your own custom payloads.

