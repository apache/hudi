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

import org.apache.hudi.metaserver.store.bean.InstantBean;
import org.apache.hudi.metaserver.store.bean.TableBean;
import org.apache.hudi.metaserver.store.jdbc.WrapperDao;
import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata store based on relation database.
 */
public class RelationDBBasedStore implements MetadataStore, Serializable {

  private final WrapperDao tableDao = new WrapperDao.TableDao();
  private final WrapperDao partitionDao = new WrapperDao.PartitionDao();
  private final WrapperDao timelineDao = new WrapperDao.TimelineDao();
  private final WrapperDao fileDao = new WrapperDao.FileDao();

  @Override
  public void initStore() throws MetaStoreException {
    WrapperDao dao = new WrapperDao("DDLMapper");
    dao.updateBySql("createDBs", null);
    dao.updateBySql("createTables", null);
    dao.updateBySql("createTableParams", null);
    dao.updateBySql("createPartitions", null);
    dao.updateBySql("createTableTimestamp", null);
    dao.updateBySql("createInstant", null);
    dao.updateBySql("createInstantMeta", null);
    dao.updateBySql("createFiles", null);
  }

  @Override
  public boolean createDatabase(String db) throws MetaStoreException {
    Map<String, Object> params = new HashMap<>();
    params.put("databaseName", db);
    return tableDao.insertBySql("insertDB", params) == 1;
  }

  @Override
  public Long getDatabaseId(String db) throws MetaStoreException {
    List<Long> ids = tableDao.queryForListBySql("selectDBId", db);
    assertIfNotSingle(ids, "db " + db);
    return ids.isEmpty() ? null : ids.get(0);
  }

  @Override
  public boolean createTable(Long dbId, Table table) throws MetaStoreException {
    Map<String, Object> params = new HashMap<>();
    params.put("dbId", dbId);
    TableBean tableBean = new TableBean(table);
    params.put("tableBean", tableBean);
    return tableDao.insertBySql("insertTable", params) == 1;
  }

  @Override
  public Table getTable(String db, String tb) throws MetaStoreException, NoSuchObjectException {
    Map<String, Object> params = new HashMap<>();
    params.put("databaseName", db);
    params.put("tableName", tb);
    List<TableBean> table = tableDao.queryForListBySql("selectTable", params);
    assertIfNotSingle(table, "table " + db + "." + tb);
    return table.isEmpty() ? null : table.get(0).toTable();
  }

  @Override
  public Long getTableId(String db, String tb) throws MetaStoreException {
    Map<String, Object> params = new HashMap<>();
    params.put("databaseName", db);
    params.put("tableName", tb);
    List<Long> ids = tableDao.queryForListBySql("selectTableId", params);
    assertIfNotSingle(ids, "table " + db + "." + tb);
    return ids.isEmpty() ? null : ids.get(0);
  }

  @Override
  public List<String> getAllPartitions(long tableId) throws MetaStoreException {
    List<String> partitionNames = partitionDao.queryForListBySql("selectAllPartitions", tableId);
    return partitionNames;
  }

  @Override
  public String createNewTimestamp(long tableId) throws MetaStoreException {
    // todo: support SSS
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String oldTimestamp;
    String newTimestamp;
    boolean success;
    try {
      do {
        oldTimestamp = getLatestTimestamp(tableId);
        newTimestamp = oldTimestamp == null
            ? sdf.format(new Date())
            : sdf.format(Math.max(new Date().getTime(), sdf.parse(oldTimestamp).getTime() + 1000));
        Map<String, Object> params = new HashMap<>();
        params.put("tableId", tableId);
        params.put("oldTimestamp", oldTimestamp);
        params.put("newTimestamp", newTimestamp);
        if (oldTimestamp == null) {
          success = timelineDao.insertBySql("insertTimestamp", params) == 1;
        } else {
          success = timelineDao.updateBySql("updateTimestamp", params) == 1;
        }
      } while (!success);
    } catch (ParseException e) {
      throw new MetaStoreException("Fail to parse the timestamp, " + e.getMessage());
    }
    return newTimestamp;
  }

  private String getLatestTimestamp(long tableId) throws MetaStoreException {
    List<String> timestamps = timelineDao.queryForListBySql("selectTimestampByTableId", tableId);
    assertIfNotSingle(timestamps, "timestamp");
    return timestamps.isEmpty() ? null : timestamps.get(0);
  }

  @Override
  public boolean createInstant(long tableId, THoodieInstant instant) throws MetaStoreException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    Map<String, Object> params = new HashMap<>();
    params.put("instant", instantBean);
    // todo: support heartbeat
    params.put("duration", 120);
    params.put("startTs", (int) (System.currentTimeMillis() / 1000L));
    return timelineDao.insertBySql("insertInstant", params) == 1;
  }

  @Override
  public boolean updateInstant(long tableId, THoodieInstant fromInstant, THoodieInstant toInstant) throws MetaStoreException {
    InstantBean oldInstant = new InstantBean(tableId, fromInstant);
    InstantBean newInstant = new InstantBean(tableId, toInstant);
    Map<String, Object> params = new HashMap<>();
    params.put("oldInstant", oldInstant);
    params.put("newInstant", newInstant);
    return timelineDao.updateBySql("updateInstant", params) == 1;
  }

  @Override
  public boolean deleteInstant(long tableId, THoodieInstant instant) throws MetaStoreException {
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    params.put("ts", instant.getTimestamp());
    return timelineDao.deleteBySql("deleteInstant", params) == 1;
  }

  @Override
  public List<THoodieInstant> scanInstants(long tableId, List<TState> states, int limit) throws MetaStoreException {
    if (states == null || states.isEmpty()) {
      throw new MetaStoreException("State has to be specified when scan instants");
    }
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    params.put("states", states.stream().mapToInt(TState::getValue).boxed().collect(Collectors.toList()));
    params.put("limit", limit);
    List<InstantBean> instantBeans = timelineDao.queryForListBySql("selectInstantsByStates", params);
    return instantBeans.stream().map(InstantBean::toTHoodieInstant).collect(Collectors.toList());
  }

  @Override
  public List<THoodieInstant> scanInstants(long tableId, TState state, int limit) throws MetaStoreException {
    return scanInstants(tableId, Arrays.asList(state), limit);
  }

  @Override
  public boolean instantExists(long tableId, THoodieInstant instant) throws MetaStoreException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    List<Long> ids = timelineDao.queryForListBySql("selectInstantId", instantBean);
    assertIfNotSingle(ids, instantBean.toString());
    return !ids.isEmpty();
  }

  // todo: check correctness
  @Override
  public void saveInstantMeta(long tableId, THoodieInstant instant, byte[] metadata) throws MetaStoreException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    Map<String, Object> params = new HashMap<>();
    params.put("instant", instantBean);
    params.put("metadata", metadata);
    // todo: array bytes to longblob
    timelineDao.insertBySql("insertInstantMeta", params);
  }

  @Override
  public boolean deleteInstantMeta(long tableId, THoodieInstant instant) throws MetaStoreException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    return timelineDao.deleteBySql("deleteInstantMeta", instantBean) == 1;
  }

  @Override
  public boolean deleteInstantAllMeta(long tableId, String timestamp) throws MetaStoreException {
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    params.put("ts", timestamp);
    return timelineDao.deleteBySql("deleteInstantAllMeta", params) >= 1;
  }

  @Override
  public byte[] getInstantMeta(long tableId, THoodieInstant instant) throws MetaStoreException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    Map<String, Object> result = timelineDao.queryForObjectBySql("selectInstantMeta", instantBean);
    return result == null ? null : (byte[]) result.get("data");
  }

  @Override
  public void close() {

  }

  public static void assertIfNotSingle(List<?> list, String errMsg) throws MetaStoreException {
    if (list != null && list.size() > 1) {
      throw new MetaStoreException("Found multiple records of " + errMsg + " , expected one");
    }
  }
}
