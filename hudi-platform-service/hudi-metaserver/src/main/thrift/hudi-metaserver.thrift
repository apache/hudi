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
   2: string databaseName,
   3: string owner,
   4: i64 createTime,
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
   NIL = 4
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

exception MetaserverStorageException {
  1: string message
}

exception MetaserverException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception AlreadyExistException {
  1: string message
}

service ThriftHoodieMetaserver {
  // table related
  void createDatabase(1:string db) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2, 3:AlreadyExistException o3)
  void createTable(1:Table table) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2, 3:AlreadyExistException o3, 4:MetaserverException o4)
  Table getTable(1:string db, 2:string tb) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2)

  // timeline related
  list<THoodieInstant> listInstants(1:string db, 2:string tb, 3:i32 num) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2)
  binary getInstantMetadata(1:string db, 2:string tb, 3:THoodieInstant instant) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2)
  string createNewInstantTime(1:string db, 2:string tb) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2)
  HoodieInstantChangeResult createNewInstantWithTime(1:string db, 2:string tb, 3:THoodieInstant instant, 4:optional binary content) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2)
  HoodieInstantChangeResult transitionInstantState(1:string db, 2:string tb, 3: THoodieInstant fromInstant, 4: THoodieInstant toInstant, 5:optional binary metadata) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2, 3:MetaserverException o3)
  HoodieInstantChangeResult deleteInstant(1:string db, 2:string tb, 3:THoodieInstant instant) throws (1:MetaserverStorageException o1, 2:NoSuchObjectException o2)
}
