/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 namespace java org.apache.hudi.metaserver.thrift

 // table related
 struct Table {
   1: string tableName,
   2: string dbName,
   3: string owner,
   4: i32 createTime,
   5: string location,
   6: string tableType,
   7: list<FieldSchema> partitionKeys,
   8: map<string, string> parameters
 }

 struct FieldSchema {
   1: string name,
   2: string type,
   3: string comments
 }

// timeline related
// align with actions defined in HoodieTimeline
enum TAction {
    COMMIT = 1,
    DELTACOMMIT = 2,
    CLEAN = 3,
    ROLLBACK = 4,
    SAVEPOINT = 5,
    REPLACECOMMIT = 6,
    COMPACTION = 7,
    RESTORE = 8
}

// align with states defined in HoodieInstant
enum TState {
   REQUESTED = 1,
   INFLIGHT = 2,
   COMPLETED = 3,
   INVALID = 4
}

struct THoodieInstant {
   1: string timestamp,
   2: TAction action,
   3: TState state
}

struct HoodieInstantChangeResult {
  1: bool success,
  2: optional THoodieInstant instant,
  4: optional string msg
}

exception MetaStoreException {
  1: string message
}

exception MetaException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception AlreadyExistException {
  1: string message
}

service ThriftHoodieMetaServer {
  // table related
  void create_database(1:string db)
  void create_table(1:Table table)
  Table get_table(1:string db, 2:string tb)

  // timeline related
  list<THoodieInstant> list_instants(1:string db, 2:string tb, 3:i32 num)
  binary get_instant_meta(1:string db, 2:string tb, 3:THoodieInstant instant)
  string create_new_instant_time(1:string db, 2:string tb)
  HoodieInstantChangeResult create_new_instant_with_time(1:string db, 2:string tb, 3:THoodieInstant instant, 4:optional binary content)
  HoodieInstantChangeResult transition_instant_state(1:string db, 2:string tb, 3: THoodieInstant fromInstant, 4: THoodieInstant toInstant, 5:optional binary metadata)
  HoodieInstantChangeResult delete_instant(1:string db, 2:string tb, 3:THoodieInstant instant)

  // snapshot related
  binary list_files_in_partition(1:string db, 2:string tb, 3:string partition, 4:optional string timestamp)

  // partition related
  list<string> list_all_partitions(1:string db, 2:string tb)
}
