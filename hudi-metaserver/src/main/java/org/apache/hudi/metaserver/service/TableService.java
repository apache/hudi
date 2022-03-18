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

package org.apache.hudi.metaserver.service;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.metaserver.store.MetadataStore;
import org.apache.hudi.metaserver.thrift.AlreadyExistException;
import org.apache.hudi.metaserver.thrift.MetaException;
import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.Table;

import java.io.Serializable;

/**
 * Handle all database / table related requests.
 */
public class TableService implements Serializable {
  private MetadataStore store;

  public TableService(MetadataStore metadataStore) {
    this.store = metadataStore;
  }

  public void createDatabase(String db) throws AlreadyExistException, MetaStoreException, MetaException {
    // todo: define the database entry in the thrift
    if (databaseExists(db)) {
      throw new AlreadyExistException("Database " + db + " already exists");
    }
    if (!store.createDatabase(db)) {
      throw new MetaException("Fail to create the database: " + db);
    }
  }

  public Table getTable(String db, String tb) throws MetaStoreException, NoSuchObjectException {
    Table table = store.getTable(db, tb);
    if (table == null) {
      throw new NoSuchObjectException(db + "." + tb + " does not exist");
    }
    // todo: add params
    table.setTableType(HoodieTableType.COPY_ON_WRITE.toString());
    return table;
  }

  public void createTable(Table table) throws MetaStoreException, NoSuchObjectException, AlreadyExistException, MetaException {
    Long dbId = store.getDatabaseId(table.getDbName());
    if (dbId == null) {
      createDatabase(table.getDbName());
      dbId = store.getDatabaseId(table.getDbName());
    }
    if (tableExists(table.getDbName(), table.getTableName())) {
      throw new AlreadyExistException(table.getDbName() + "." + table.getTableName() + " already exists");
    }
    if (!store.createTable(dbId, table)) {
      throw new MetaException("Fail to create the table: " + table);
    }
    // todo: add params
  }

  private boolean databaseExists(String db) throws MetaStoreException {
    return store.getDatabaseId(db) != null;
  }

  private boolean tableExists(String db, String tb) throws MetaStoreException {
    return store.getTableId(db, tb) != null;
  }
}
