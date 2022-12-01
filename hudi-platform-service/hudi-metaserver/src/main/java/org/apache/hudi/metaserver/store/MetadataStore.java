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

import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;

import java.util.List;

/**
 * Metadata store for meta server, stores all entities like database, table, instant and so on.
 */
public interface MetadataStore {
  void initStore() throws MetaStoreException;

  // table related
  boolean createDatabase(String db) throws MetaStoreException;

  Long getDatabaseId(String db) throws MetaStoreException;

  boolean createTable(Long dbId, Table table) throws MetaStoreException;

  Table getTable(String db, String tb) throws MetaStoreException, NoSuchObjectException;

  Long getTableId(String db, String tb) throws MetaStoreException;

  // partition related
  List<String> getAllPartitions(long tableId) throws MetaStoreException;

  // timeline related
  String createNewTimestamp(long tableId) throws MetaStoreException;

  boolean createInstant(long tableId, THoodieInstant instant) throws MetaStoreException;

  boolean updateInstant(long tableId, THoodieInstant fromInstant, THoodieInstant toInstant) throws MetaStoreException;

  boolean deleteInstant(long tableId, THoodieInstant instant) throws MetaStoreException;

  List<THoodieInstant> scanInstants(long tableId, List<TState> states, int limit) throws MetaStoreException;

  List<THoodieInstant> scanInstants(long tableId, TState state, int limit) throws MetaStoreException;

  boolean instantExists(long tableId, THoodieInstant instant) throws MetaStoreException;

  void saveInstantMeta(long tableId, THoodieInstant instant, byte[] metadata) throws MetaStoreException;

  boolean deleteInstantMeta(long tableId, THoodieInstant instant) throws MetaStoreException;

  boolean deleteInstantAllMeta(long tableId, String timestamp) throws MetaStoreException;

  byte[] getInstantMeta(long tableId, THoodieInstant instant) throws MetaStoreException;

  void close();

}
