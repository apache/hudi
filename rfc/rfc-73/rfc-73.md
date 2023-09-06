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

# RFC-73: Multi-Table Transactions

## Proposers

- @codope

## Approvers

- @vinothchandar

## Status

JIRA: [HUDI-6709](https://issues.apache.org/jira/browse/HUDI-6709)

## Abstract

Modern data lake architectures often comprise numerous interconnected tables. Operations, such as data backfill,
upserts, deletes or complex transformations, may span across multiple tables. In these scenarios, it's crucial that
these operations are atomic - i.e., they either succeed across all tables or fail without partial writes. This ensures
data consistency across the entire dataset. Users can design data workflows with the assurance that operations spanning
multiple tables are treated as a single atomic unit.

## Background

Hudi has always emphasized transactional guarantees, ensuring data integrity and consistency for a specific table.
Central to Hudi's approach to transactions is its [timeline](https://hudi.apache.org/docs/timeline), which logs all
actions (like commits, deltacommits, rollbacks, etc) on the table. With timeline as the source of truth for all changes
on the table, Hudi employs tunable [concurrency control](https://hudi.apache.org/docs/concurrency_control) to allow for
concurrent reads and writes on the table
with [snapshot isolation](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+22+%3A+Snapshot+Isolation+using+Optimistic+Concurrency+Control+for+multi-writers).
This is achieved by leveraging the timeline to determine the latest consistent snapshot of the table. Additionally,
users can bring their own custom conflict resolution strategy by implementing the `ConflictResolutionStrategy`
interface.

However, the current implementation cannot be extended as-is to support atomic operations across multiple tables. First
of all, we need a notion of a "database" and its tables to be able to associate a transaction with multiple tables.
Secondly, Hudi's timeline would need to evolve to account for changes across multiple tables. This introduces
complexities in tracking and managing the order of operations across tables. With multiple tables involved, the points
of potential failures increase. A failure in one table can cascade and affect the entire transaction. In case of
failures, rolling back changes becomes more intricate in multi-table scenarios. Ensuring data consistency across tables
during rollbacks, especially when there are inter-table dependencies, introduces additional challenges. Finally, the
current concurrency control implementation is not designed to handle multi-table transactions. Hence, this RFC proposes
a new design to support multi-table transactions.

## Design

First of all, let us discuss the goals and non-goals which will help in understanding the design considerations better.

### Goals

1. **Atomicity Across Tables:** Ensure that a set of changes spanning multiple tables either fully completes or fully
   rolls back. No partial commits should occur.
2. **Consistency:** Ensure that, after every transaction, all involved tables are in a consistent state. Avoid dirty
   reads, non-repeatable reads, handle read-write and write-write conflicts.
3. **Isolation:** Multiple concurrent transactions should not interfere with each other. One transaction shouldn't see
   the intermediate states of another ongoing transaction.
4. **Durability:** Once a multi-table transaction is committed, the changes should be permanent and recoverable, even
   after system failures.
5. **Performance:** The multi-table transaction mechanism should introduce minimal overhead. Its performance impact on
   typical Hudi operations should be kept low. By corollary, locking (in case of OCC) cannot be coarser than file level.
6. **Monitoring:** Integration with tools such as hudi-cli to track the status and health of transactions. Separate
   transaction metrics. Also, provide comprehensive logs for debugging transaction failures or issues.
7. **Integration with Current Hudi Features:** The multi-table transaction should be seamlessly integrated with existing
   Hudi features and should not break existing APIs or workflows. In
   particular, `TransacationManager`, `LockManager`, `ConflictResolutionStrategy` APIs should work as they do today with
   single table.
8. **Configurability:** Allow users to configure the behavior of multi-table transactions, e.g., concurrency control
   mechanism, timeout durations, etc.
9. **Recovery**: Rollbacks, savepoint and restore should be supported.

### Non-goals

1. **Cross-Database Transactions:** Transactions spanning multiple databases might be too complex as an initial target.
2. **Granular Record Locking:** Instead of locking at the granularity of individual records, coarse-grained locking (
   like at the file level) might be more practical to start with. But, let's avoid table or partition level locking as
   mentioned in the goals.
3. **Distributed Transactions:** We are not going to consider transactions in the sense of distributed databases, and
   thus the need of a protocol such as 2PC or a quorum mechanism to proceed with the commits. Storage is assumed to be
   durable and resilient to distributed failures.
4. **Complex Conflict Resolution:** Initially, simple conflict resolution strategies (like aborting a transaction on
   conflict) can be implemented, leaving more sophisticated strategies for future iterations.
5. **Replication/CDC stream:** We are not going to build a replication or CDC stream out of the database level commit
   log, though that would be a good usage of the database timeline introduced in this RFC.

### Design

Our primary challenge is to support operations that span multiple tables within a single database, and maintain the ACID
properties across these tables.

1. Need for a catalog: We probably need a catalog API that tracks databases and its tables.
2. Need for a transaction coordinator: A centralized coordinator that will manage transaction logs, track ongoing
   transactions, handle timeouts, and manage transaction rollbacks.
3. Need for a transaction log: At the table level, the timeline incorporating state of an action with start and modified
   time serves the purpose of transaction log. At the database level, we need to track all multi-table transactions.
   Every start, update, or commit of a multi-table transaction gets recorded. Using a "database timeline" together with
   tables' timeline, we can guarantee that data files won't be visible until the all statements of a transaction are
   completed.
4. Locking/Conflict resolution mechanism: In OCC with external lock providers, lock the affected files during the
   multi-table transaction to prevent conflicting writes. Decide on conflict resolution strategies (e.g., last write
   wins, version vectors). Note that Hudi, by default, provides snapshot isolation between readers and writers using
   MVCC, so no lock is required for readers.

#### Concurrency Control and Conflict Resolution

Today we have an event log at the table level in the form of Hudi timeline. To support multi-table transactions, we are
going to need a unified view of all tables within the database to ensure atomicity and consistency. Hence, we propose a
database-level timeline as transaction log, which mainly contains the following:

* Transaction ID (instant time when the transaction started)
* End/Modified Timestamp
* State of transaction: REQUESTED, INFLIGHT, COMPLETED, ROLLED\_BACK
* Any other relevant metadata

Hudi naturally supports MVCC based snapshot isolation. We can leverage the Hudi table and database timeline to support
snapshot isolation with multi-table transactions with concurrent readers and writers.

Anomalies or Conflicts

With MVCC and snapshot isolation:

1. **Start of Transaction**: When a transaction begins, it's given a "snapshot" of the database as it appeared at the
   start time.
2. **Reads**: The transaction can read from its snapshot without seeing any intermediate changes made by other
   transactions.
3. **Writes**: The writes create a new version of the data. This doesn't affect other ongoing transactions as
   they continue to work with their respective snapshots.
4. **Commit**: During commit, the system checks for write-write conflicts, i.e., if another transaction has committed
   changes to the same data after the current transaction's start time. If there's a conflict, the current transaction
   may be aborted.

#### 1\. Dirty Reads:

**Definition**: A transaction reads data that's been written by a still in-progress transaction.

**Analysis with MVCC**: Dirty reads are inherently prevented in MVCC with snapshot isolation. Since every transaction is
reading from its snapshot, it won't see uncommitted changes made by other transactions.

#### 2\. Phantom Reads:

**Definition**: In the course of a transaction, new records get added or old records get removed, which fit the criteria
of a previous read in the transaction. See Appendix for an example.

**Analysis with MVCC**: Since transactions operate on snapshots, they won't see new records added after the snapshot was
taken. However, if a transaction's intent is to ensure that new records of a certain kind don't get added, additional
mechanisms, like predicate locks, might be needed. If we don't allow snapshot to be refreshed within the same
transaction, then phantom reads are not possible (including self-join).

#### 3\. Read-Write Conflict:

**Definition**: Given a scenario of a concurrent read happening while the transactions are ongoing: A transaction reads
multiple items, but by the time it finishes reading, another transaction has modified some of the earlier-read items,
resulting in an inconsistent view.

**Analysis with MVCC**: The data that a transaction reads remains consistent for the duration of the transaction. With
MVCC and snapshot isolation, the read operation will see a consistent snapshot of the database, depending on when the
read started. It won't see the uncommitted changes made by the ongoing transactions. So, the read is consistent, and
there's no anomaly related to the read operation in this setup.

#### 4\. Write-Write Conflict:

**Definition**: Two transactions read the same data, and based on the read, they both decide to modify the data, leading
to a situation where the final outcome might not be consistent with the intent of either transaction.

**Analysis with MVCC**: This is a **potential problem** even with snapshot isolation. Let's say two transactions, T1 and
T2, start at the same time and read the same file in one of the tables. Both decide to modify the file based on what
they've read. Since they are operating on snapshots, neither transaction sees the other's changes. When they try to
commit, they might both try to create a new version of the file, leading to a write-write conflict. The system, noticing
the conflict, would typically abort one of the transactions or apply some conflict resolution strategy.

#### Conflict Resolution

Conflict detection will happen on the basis of set of partitions and fileIDs mutated by the transaction. Let's say there
are two conflicting transaction T1 and T2. Both of them, when they start, fetch latest versions of all tables and
register themselves with start timestamp in the database timeline, which will be greater than all versions of tables
involved in T1 and T2. To handle conflict before committing, we have the following options:

1. **First committer/Younger transaction wins** - Essentially, no resolution required with end timestamp based ordering.
   If a transaction tries to modify data that has been modified by a younger transaction (with a later timestamp), the
   older transaction is rolled back to avoid the conflict. This ensures that transactions are serialized based on their
   timestamps.
    1. Pros: Transactions are serialized in a consistent order. Works well in environments with low contention.
    2. Cons: Have to maintain end timestamp for each action. Potentially high abort rates in high contention
       environments.

2. **OCC (with lock provider)** - With start and end timestamp, we have the option of "wait-die" and "
   wound-wait" ([CMU notes on 2PL section 3](https://15445.courses.cs.cmu.edu/fall2022/notes/16-twophaselocking.pdf))
   strategies. In the Wait-Die strategy, if an older transaction requests a lock held by a younger one, the older
   transaction is forced to wait or is aborted ("die") after some time. In the Wound-Wait scheme, the younger
   transaction is aborted ("wounded") to allow the older transaction to proceed.
    1. Pros: Provides a mechanism to prioritize older transactions.
    2. Cons: Complexity in implementation and potentially high waiting time if there are frequent short-running
       transactions.

3. [Compensating transaction](https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction) -
   Instead of aborting a transaction when a conflict is detected, another transaction is executed based on the latest
   snapshot to compensate for the effects of the conflicting transaction.
    1. Pros: Avoids the need to abort and retry. Could be useful for long-running transactions.
    2. Cons: Complexity in designing and ensuring the correctness of compensating transactions.

#### Multi-table Transaction Protocol

Let's break down the multi-table transaction protocol using the scenarios presented below (assuming we have a logical
notion of a database and each database will have its own timeline, called the database timeline, that will track
transactions across tables within that database).

**Scenario 1: Basic Transaction with Updates to Three Tables**

1. **Begin Transaction**
    * Writer `W1` initiates a transaction.
    * The `database timeline` logs the start of a new transaction with a unique transaction ID (`TxID1`).

2. **Updates to Tables**
    * `W1` writes updates to `t1`. The updates are staged and tracked under `TxID1` in `t1`'s timeline in a 'pending'
      state.
    * Similarly, `W1` writes updates to `t2` and `t3`. The updates are staged and tracked in respective table's
      timelines under `TxID1`.

3. **Commit Transaction**
    * `W1` signals the end of its transaction.
    * The `database timeline` marks the transaction `TxID1` as committed.
    * The timelines for `t1`, `t2`, and `t3` finalize the 'pending' updates under `TxID1`.

---

**Scenario 2: Transaction with a Conflict in One Table**

1. **Begin Transactions**
    * Writer `W1` initiates a transaction (`TxID1`).
    * Another writer `W2` starts a new transaction (`TxID2`).

2. **Conflicting Updates**
    * `W1` and `W2` both try to write to a shared file in `t1`.
    * The MVCC system detects the conflict when `W2` tries to commit its changes, as `W1` has already staged its
      updates.
    * `W2`'s changes to `t1` are aborted, and it receives a conflict notification.

---

**Scenario 3: Transaction with Concurrent Reads During Pending Updates**

1. **Begin Transaction**
    * Writer `W1` initiates a transaction (`TxID1`) and stages updates for `t1` and `t2`. `t3`'s update is still in
      progress.

2. **Concurrent Read by Reader `R1`**
    * `R1` wishes to read from `t1`, `t2`, and `t3`.
    * Since `W1`'s updates to `t1` and `t2` are only staged and the transaction is not yet committed, `R1` sees the
      data's previous state for these tables.
    * For `t3`, `R1` reads the current state, as no updates have been staged or committed yet by `W1`.

---

**Scenario 4: Transaction with Failed Update Requiring Rollback**

1. **Begin Transaction**
    * Writer `W1` initiates a transaction (`TxID1`) and successfully stages updates for `t1` and `t2`.

2. **Failed Update**
    * An update to `t3` fails due to an internal error, such as an I/O error.

3. **Rollback Transaction**
    * `W1` initiates a rollback for `TxID1`.
    * The `database timeline` marks the transaction `TxID1` as rolled back.
    * The staged updates in the timelines for `t1` and `t2` under `TxID1` are discarded.

---

Note: This protocol ensures snapshot isolation by ensuring that readers always see a consistent state of the data,
either before or after a transaction's updates but never a mix of the two.

## Implementation

### **SQL Writes**

```plain
BEGIN tx1
// anything done here is associated with tx1. 

// load table A; (load A's latest snapshot time into HoodieCatalog/driver memory)

// load table B; (load B's latest snapshot time into HoodieCatalog/driver memory)

// load table A again; (it will reuse the snapshot time already in HoodieCatalog/driver memory).

COMMIT / ROLLBACK
```

* Can we implement this within the `HoodieCatalog` in Spark
* How do we identify incoming requests/calls into Catalog with a given transaction.

1. First we need to write the grammar for BEGIN...END block and corresponding changes in the parser and lexer. We
   already did something similar with MERGE INTO.
2. We need to create a new abstraction, let's say `TransactionalHoodieCatalog` that implements the Spark `TableCatalog`
   interface while adding multi-table transaction support.
    1. State Management: We keep state about the current transaction and tables involved within a namespace. This will
       be persisted to file system in the database timeline.
    2. Transactional Methods: We need to add `beginTransaction`, `commitTransaction`, etc., methods. We must make
       the `createTable` and `loadTable` method transaction-aware. Similarly, other methods would also need
       transaction-awareness where necessary.
    3. Catalog Methods: Implement other methods related to namespace and tables from `TableCatalog` interface of Spark.
    4. TransactionalHoodieCatalog API
       ```scala
       abstract class TransactionalHoodieCatalog extends TableCatalog {
         val spark: SparkSession = SparkSession.active
         // Representing a Transaction
         case class Transaction(transactionId: String, tablesInvolved: mutable.Set[String])
         // State
         private var currentTransaction: Option[Transaction] = None
         // Databases and Tables - in-memory representation for this sketch
         private val databases: mutable.Map[String, CatalogDatabase] = mutable.Map()
         private val tables: mutable.Map[String, CatalogTable] = mutable.Map()
         // Transactional methods
         def beginTransaction(): String = {
           val newTransactionId = HoodieActiveTimeline.createNewInstantTime()
           currentTransaction = Some(Transaction(newTransactionId, mutable.Set()))
           newTransactionId
         }
         def addTableToTransaction(tableName: String): Unit = {
           currentTransaction match {
             case Some(tx) => tx.tablesInvolved += tableName
             case None => throw new IllegalStateException("No active transaction to add table to.")
           }
         }
         def commitTransaction(): Unit = {
           // Commit logic
           currentTransaction = None
         }
         def rollbackTransaction(): Unit = {
           // Rollback logic
           currentTransaction = None
         }
         // Implementation of Catalog methods
         // Database related operations
         override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = ???
         override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???
         override def alterDatabase(dbDefinition: CatalogDatabase): Unit = ???
         override def getDatabase(db: String): CatalogDatabase = {
           databases.getOrElse(db, throw new NoSuchDatabaseException(db))
         }
         override def databaseExists(db: String): Boolean = {
           databases.contains(db)
         }
         override def listDatabases(): Seq[String] = {
           databases.keys.toSeq
         }
         override def listDatabases(pattern: String): Seq[String] = {
           databases.keys.filter(_.matches(pattern)).toSeq
         }
         // ... More database methods
         // Table related operations
         override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
           // If there's an ongoing transaction, associate the table with it
           currentTransaction.foreach(tx => tx.tablesInvolved += tableDefinition.identifier.table)
           // Rest of the creation logic
           tables += (tableDefinition.identifier.table -> tableDefinition)
         }
         override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = ???
         override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = {
           tables.getOrElse(table, throw new NoSuchTableException(db, table))
         }
       }
       ```

Spark's `TableCatalog` API is a DataSource V2 API. So, this limits us to support multi-table txn just for datasource v2
while writing. For reading, we will fall back to v1, just like how we do in `HoodieCatalog` today.

1. Add a new sql command `BeginMultiTableTransactionCommand` which will be invoked when the parser determines the query
   to be a BEGIN...END block. This command will initiate the transaction using `beginTransaction` of the catalog.
2. Each DML statement part of BEGIN...END block then gets executed as usual (via `HoodieSparkSqlWriter` ),
   and `catalog.addTableToTransaction` will be called before making any changes.
3. At the end of the transaction scope (which will be determined by the parser hitting END), another
   command `EndMultiTableTransactionCommand` will call `catalog.commitTransaction()` to persist all changes
   or `rollbackTransaction()` to discard them.

#### DataSource(DS) v1 and v2

Hudi relations don't currently implement DSv2 read API
and [only support DSv2 write API](https://github.com/apache/hudi/blob/d7c16f56f4f9dfa3a160dac459ae11944f922ec8/hudi-spark-datasource/hudi-spark3.2plus-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieInternalV2Table.scala#L41).
According to the [LHBench paper](https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf),

> the DSv2 API is less mature and does not report some metrics useful in query planning, so this often results in less
> performant query plans over Iceberg. For example, in Q9, Spark optimizes a complex aggregation with a cross-join in
> Delta Lake and Hudi but not in Iceberg, leading to the largest relative performance difference in all of TPC-DS.

Also, for writing,
Hudi [falls back to DSv1](https://github.com/apache/hudi/blob/d7c16f56f4f9dfa3a160dac459ae11944f922ec8/hudi-spark-datasource/hudi-spark3.2plus-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieCatalog.scala#L127-L131) (
except for schema on read).

**Why fallback?**
While DataFrame shuffles are largely determined by Spark's logical and physical planning stages, DSv2 has interfaces (
like `SupportsRead`, `SupportsWrite`, and their sub-interfaces) that allow for better pushdown and optimization.
Properly leveraging these can reduce the need for shuffles, but the API itself doesn't directly control shuffling. If we
want to implement custom shuffling logic as part of a custom data source, DSv2 doesn't directly provide specific
interfaces for that. DSv2 allows data sources to report their physical distribution (like bucketing or partitioning) via
the `SupportsReportPartitioning` trait. When Spark is aware of the data's distribution and partitioning, it might avoid
unnecessary shuffles. However, if the data source does not correctly implement or report these characteristics, it could
lead to suboptimal execution plans. So, the onus is partially on the data source implementation to provide accurate
information.

We can follow the same approach i.e. in order to support multi-table transactions with DSv2, we can create v2Table in
the transactional catalog but fall back to `V1Write`.

### **DataFrame Writes**

```scala
// similar to catalog.beginTransaction
val transaction = HoodieTransactionManager.beginTransaction()

// similar to catalog.addTableToTransaction and execute the write. 
// Then pass the control to HoodieTransactionManager.
df1.write.format("hudi").withTransaction(transaction).options(opts1).save(basePath1)

// Same as above. If this failed, then rollback and pass the control to HoodieTransactionManager.
// HoodieTransactionManager will rollback all previous transactions associated with transaction id.
df2.write.format("hudi").withTransaction(transaction).options(opts2).save(basePath2)

// All success, go ahead and commit or finish rollback (ROLLED_BACK state on timeline is written in that case)
transaction.commit()
```

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?

The design is premised on certain format changes to the timeline. Users can use the feature on new tables created with
the upgraded version. Older datasets that were written without the notion of a database or a database timeline should
still be readable. However, users who want to leverage the new multi-table transaction capabilities with the existing
datasets would ideally need a smooth migration path. This could involve tools or scripts that help in restructuring data
or migrating the older timeline events into the new database timeline.

- If we are changing behavior how will we phase out the older behavior?

No need to phase out the older behavior. The new behavior will be activated explicitly by the use of new APIs or SQL
syntax.

- If we need special migration tools, describe them here.

TODO

## Test Plan

- Unit tests for the new APIs and SQL syntax.
- Unit tests for the new timeline format.
- Unit tests for conflict resolution.
- Integration tests for multi-table transactions with and without failures.

## Appendix

### Textbook phantom read example

  Suppose you have a table named `Employees` with columns `employee_id` and `manager_id`, where the `manager_id` for
  some employees refers to the `employee_id` of other employees in the same table. One might execute a self-join to
  retrieve a list of employees and their managers:

  ```plain
  SELECT e1.employee_id, e2.employee_id AS manager_id
  FROM Employees e1
  JOIN Employees e2 ON e1.manager_id = e2.employee_id;
  ```

  This query essentially matches each employee with their manager, using a self-join on the `Employees` table.

    1. **Transaction A** starts and runs the self-join query to get a list of employees and their respective managers.
    2. While **Transaction A** is still in progress, **Transaction B** starts and adds a new row to the `Employees`
       table, inserting a new employee with a manager whose `employee_id` is already in the table.
    3. **Transaction B** commits its changes.
    4. If **Transaction A** re-runs the same self-join query, it might see the newly added row, resulting in an
       additional result in the join output that wasn't there during the initial query. This is a phantom read.

With MVCC and snapshot isolation level, a transaction would continue to see the state of the database as it was when the
transaction started, even if it re-runs the self-join. This level will prevent the phantom read in this case. However,
it cannot be guaranteed with read committed.
