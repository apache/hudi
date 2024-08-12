/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metaserver.store;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.metaserver.thrift.MetaserverStorageException;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;
import org.apache.hudi.metaserver.thrift.Table;

import java.util.List;

/**
 * Metadata store for meta server, stores all entities like database, table, instant and so on.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface MetaserverStorage extends AutoCloseable {

  void initStorage() throws MetaserverStorageException;

  // table related
  boolean createDatabase(String db) throws MetaserverStorageException;

  Long getDatabaseId(String db) throws MetaserverStorageException;

  @VisibleForTesting
  boolean deleteDatabase(Long dbId) throws MetaserverStorageException;

  boolean createTable(Long dbId, Table table) throws MetaserverStorageException;

  Table getTable(String db, String tb) throws MetaserverStorageException;

  Long getTableId(String db, String tb) throws MetaserverStorageException;

  @VisibleForTesting
  boolean deleteTable(Long tableId) throws MetaserverStorageException;

  // timeline related
  String createNewTimestamp(long tableId) throws MetaserverStorageException;

  @VisibleForTesting
  boolean deleteTableTimestamp(Long tableId) throws MetaserverStorageException;

  boolean createInstant(long tableId, THoodieInstant instant) throws MetaserverStorageException;

  boolean updateInstant(long tableId, THoodieInstant fromInstant, THoodieInstant toInstant) throws MetaserverStorageException;

  boolean deleteInstant(long tableId, THoodieInstant instant) throws MetaserverStorageException;

  List<THoodieInstant> scanInstants(long tableId, List<TState> states, int limit) throws MetaserverStorageException;

  List<THoodieInstant> scanInstants(long tableId, TState state, int limit) throws MetaserverStorageException;

  boolean instantExists(long tableId, THoodieInstant instant) throws MetaserverStorageException;

  void saveInstantMetadata(long tableId, THoodieInstant instant, byte[] metadata) throws MetaserverStorageException;

  boolean deleteInstantMetadata(long tableId, THoodieInstant instant) throws MetaserverStorageException;

  boolean deleteInstantAllMeta(long tableId, String timestamp) throws MetaserverStorageException;

  Option<byte[]> getInstantMetadata(long tableId, THoodieInstant instant) throws MetaserverStorageException;

  void close();

}
