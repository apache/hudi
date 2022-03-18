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

import org.apache.hudi.metaserver.HoodieMetaServer;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.TAction;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;
import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests on metadata store base on relation database of hoodie meta server.
 */
public class TestRelationDBBasedStore {

  private MetadataStore store;
  private final String db = "test_db";
  private final String tb = "test_tb";

  @Test
  public void testAPIs() throws TException {
    HoodieMetaServer.getEmbeddedMetaServer();
    store = HoodieMetaServer.getMetadataStore();
    testTableRelatedAPIs();
    testTimelineRelatedAPIs();
  }

  private void testTableRelatedAPIs() throws MetaStoreException, NoSuchObjectException {
    Assertions.assertTrue(store.createDatabase(db));
    Long dbId = store.getDatabaseId(db);
    Assertions.assertNotNull(dbId);

    Table table = new Table();
    table.setDbName(db);
    table.setTableName(tb);
    table.setOwner("owner");
    table.setLocation("test_db.db/test_tb");
    // check table related API
    Assertions.assertTrue(store.createTable(dbId, table));
    Long tableId = store.getTableId(db, tb);
    Assertions.assertNotNull(tableId);
    Assertions.assertEquals(store.getTable(db, tb).toString(), table.toString());
  }

  private void testTimelineRelatedAPIs() throws MetaStoreException {
    Long tableId = store.getTableId(db, tb);
    String ts = store.createNewTimestamp(tableId);
    Assertions.assertTrue(Long.valueOf(store.createNewTimestamp(tableId)) > Long.valueOf(ts));
    THoodieInstant requested = new THoodieInstant(ts, TAction.COMMIT, TState.REQUESTED);
    Assertions.assertTrue(store.createInstant(tableId, requested));
    Assertions.assertTrue(store.instantExists(tableId, requested));
    Assertions.assertThrows(MetaStoreException.class,
        () -> store.createInstant(tableId, new THoodieInstant(ts, TAction.REPLACECOMMIT, TState.REQUESTED)));
    // update instant and check it
    THoodieInstant inflight = new THoodieInstant(ts, TAction.COMMIT, TState.INFLIGHT);
    Assertions.assertTrue(store.updateInstant(tableId, requested, inflight));
    List<THoodieInstant> instants = store.scanInstants(tableId, Arrays.asList(TState.REQUESTED, TState.INFLIGHT), -1);
    Assertions.assertEquals(1, instants.size());
    Assertions.assertEquals(inflight, instants.get(0));
    // delete
    Assertions.assertTrue(store.deleteInstant(tableId, inflight));
    Assertions.assertTrue(store.scanInstants(tableId, Arrays.asList(TState.REQUESTED, TState.INFLIGHT), -1).isEmpty());

    // instant meta CRUD
    byte[] requestedMeta = "requested".getBytes(StandardCharsets.UTF_8);
    byte[] inflightMeta = "inflight".getBytes(StandardCharsets.UTF_8);
    store.saveInstantMeta(tableId, requested, requestedMeta);
    store.saveInstantMeta(tableId, inflight, inflightMeta);
    Assertions.assertTrue(store.deleteInstantMeta(tableId, requested));
    Assertions.assertNull(store.getInstantMeta(tableId, requested));
    Assertions.assertEquals("inflight", new String(store.getInstantMeta(tableId, inflight)));
    // delete all metadata of a timestamp
    store.saveInstantMeta(tableId, requested, requestedMeta);
    Assertions.assertEquals("requested", new String(store.getInstantMeta(tableId, requested)));
    Assertions.assertTrue(store.deleteInstantAllMeta(tableId, ts));
    Assertions.assertNull(store.getInstantMeta(tableId, requested));
    Assertions.assertNull(store.getInstantMeta(tableId, inflight));
  }

}
