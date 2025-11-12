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

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metaserver.store.bean.InstantBean;
import org.apache.hudi.metaserver.store.bean.TableBean;
import org.apache.hudi.metaserver.store.jdbc.WrapperDao;
import org.apache.hudi.metaserver.thrift.MetaserverStorageException;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;
import org.apache.hudi.metaserver.thrift.Table;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Metadata store based on relation database.
 */
public class RelationalDBBasedStorage implements MetaserverStorage, Serializable {

  private final WrapperDao tableDao = new WrapperDao.TableDao();
  private final WrapperDao timelineDao = new WrapperDao.TimelineDao();

  @Override
  public void initStorage() throws MetaserverStorageException {
    WrapperDao dao = new WrapperDao("DDLMapper");
    dao.updateBySql("createDBs", null);
    dao.updateBySql("createTables", null);
    dao.updateBySql("createTableParams", null);
    dao.updateBySql("createPartitions", null);
    dao.updateBySql("createTableTimestamp", null);
    dao.updateBySql("createInstant", null);
    dao.updateBySql("createInstantMetadata", null);
    dao.updateBySql("createFiles", null);
  }

  @Override
  public boolean createDatabase(String db) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("databaseName", db);
    return tableDao.insertBySql("insertDB", params) == 1;
  }

  @Override
  public Long getDatabaseId(String db) throws MetaserverStorageException {
    List<Long> ids = tableDao.queryForListBySql("selectDBId", db);
    validate(ids, "db " + db);
    return ids.isEmpty() ? null : ids.get(0);
  }

  @Override
  public boolean deleteDatabase(Long dbId) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("dbId", dbId);
    return tableDao.deleteBySql("deleteDB", params) == 1;
  }

  @Override
  public boolean createTable(Long dbId, Table table) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("dbId", dbId);
    TableBean tableBean = new TableBean(table);
    params.put("tableBean", tableBean);
    return tableDao.insertBySql("insertTable", params) == 1;
  }

  @Override
  public Table getTable(String db, String tb) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("databaseName", db);
    params.put("tableName", tb);
    List<TableBean> table = tableDao.queryForListBySql("selectTable", params);
    validate(table, "table " + db + "." + tb);
    return table.isEmpty() ? null : table.get(0).toTable();
  }

  @Override
  public Long getTableId(String db, String tb) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("databaseName", db);
    params.put("tableName", tb);
    List<Long> ids = tableDao.queryForListBySql("selectTableId", params);
    validate(ids, "table " + db + "." + tb);
    return ids.isEmpty() ? null : ids.get(0);
  }

  @Override
  public boolean deleteTable(Long tableId) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    return tableDao.deleteBySql("deleteTable", params) == 1;
  }

  @Override
  public String createNewTimestamp(long tableId) throws MetaserverStorageException {
    String oldTimestamp;
    String newTimestamp;
    boolean success;
    do {
      oldTimestamp = getLatestTimestamp(tableId);
      do {
        newTimestamp = HoodieInstantTimeGenerator.formatDate(new Date(System.currentTimeMillis()));
      } while (oldTimestamp != null && compareTimestamps(newTimestamp,
          LESSER_THAN_OR_EQUALS, oldTimestamp));
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
    return newTimestamp;
  }

  private String getLatestTimestamp(long tableId) throws MetaserverStorageException {
    List<String> timestamps = timelineDao.queryForListBySql("selectTimestampByTableId", tableId);
    validate(timestamps, "timestamp");
    return timestamps.isEmpty() ? null : timestamps.get(0);
  }

  @Override
  public boolean deleteTableTimestamp(Long tableId) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    return timelineDao.deleteBySql("deleteTimestamp", params) == 1;
  }

  @Override
  public boolean createInstant(long tableId, THoodieInstant instant) throws MetaserverStorageException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    Map<String, Object> params = new HashMap<>();
    params.put("instant", instantBean);
    // todo: support heartbeat
    params.put("duration", 120);
    params.put("startTs", (int) (System.currentTimeMillis() / 1000L));
    return timelineDao.insertBySql("insertInstant", params) == 1;
  }

  @Override
  public boolean updateInstant(long tableId, THoodieInstant fromInstant, THoodieInstant toInstant) throws MetaserverStorageException {
    InstantBean oldInstant = new InstantBean(tableId, fromInstant);
    InstantBean newInstant = new InstantBean(tableId, toInstant);
    Map<String, Object> params = new HashMap<>();
    params.put("oldInstant", oldInstant);
    params.put("newInstant", newInstant);
    return timelineDao.updateBySql("updateInstant", params) == 1;
  }

  @Override
  public boolean deleteInstant(long tableId, THoodieInstant instant) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    params.put("ts", instant.getTimestamp());
    return timelineDao.deleteBySql("deleteInstant", params) == 1;
  }

  @Override
  public List<THoodieInstant> scanInstants(long tableId, List<TState> states, int limit) throws MetaserverStorageException {
    if (isNullOrEmpty(states)) {
      throw new MetaserverStorageException("State has to be specified when scan instants");
    }
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    params.put("states", states.stream().mapToInt(TState::getValue).boxed().collect(Collectors.toList()));
    params.put("limit", limit);
    List<InstantBean> instantBeans = timelineDao.queryForListBySql("selectInstantsByStates", params);
    return instantBeans.stream().map(InstantBean::toTHoodieInstant).collect(Collectors.toList());
  }

  @Override
  public List<THoodieInstant> scanInstants(long tableId, TState state, int limit) throws MetaserverStorageException {
    return scanInstants(tableId, Collections.singletonList(state), limit);
  }

  @Override
  public boolean instantExists(long tableId, THoodieInstant instant) throws MetaserverStorageException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    List<Long> ids = timelineDao.queryForListBySql("selectInstantId", instantBean);
    validate(ids, instantBean.toString());
    return !ids.isEmpty();
  }

  // todo: check correctness
  @Override
  public void saveInstantMetadata(long tableId, THoodieInstant instant, byte[] metadata) throws MetaserverStorageException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    Map<String, Object> params = new HashMap<>();
    params.put("instant", instantBean);
    params.put("metadata", metadata);
    // todo: array bytes to longblob
    timelineDao.insertBySql("insertInstantMetadata", params);
  }

  @Override
  public boolean deleteInstantMetadata(long tableId, THoodieInstant instant) throws MetaserverStorageException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    return timelineDao.deleteBySql("deleteInstantMetadata", instantBean) == 1;
  }

  @Override
  public boolean deleteInstantAllMeta(long tableId, String timestamp) throws MetaserverStorageException {
    Map<String, Object> params = new HashMap<>();
    params.put("tableId", tableId);
    params.put("ts", timestamp);
    return timelineDao.deleteBySql("deleteInstantAllMetadata", params) >= 1;
  }

  @Override
  public Option<byte[]> getInstantMetadata(long tableId, THoodieInstant instant) throws MetaserverStorageException {
    InstantBean instantBean = new InstantBean(tableId, instant);
    Map<String, Object> result = timelineDao.queryForObjectBySql("selectInstantMetadata", instantBean);
    return result == null ? Option.empty() : Option.of((byte[]) result.get("data"));
  }

  @Override
  public void close() {

  }

  public static void validate(List<?> entities, String entityName) throws MetaserverStorageException {
    try {
      checkState(isNullOrEmpty(entities) || entities.size() == 1, "Found multiple records of " + entityName + " , expected one");
    } catch (IllegalStateException e) {
      throw new MetaserverStorageException(e.getMessage());
    }
  }
}
