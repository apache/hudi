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

import org.apache.hudi.metaserver.thrift.HoodieInstantChangeResult;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaServer;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A proxy for meta server, accepts all thrift calls and routes them to the corresponding service.
 */
public class HoodieMetaServerService implements ThriftHoodieMetaServer.Iface, Serializable {
  private TableService tableService;
  private PartitionService partitionService;
  private TimelineService timelineService;

  public HoodieMetaServerService(TableService tableService, PartitionService partitionService,
                                 TimelineService timelineService) {
    this.tableService = tableService;
    this.partitionService = partitionService;
    this.timelineService = timelineService;
  }

  @Override
  public void create_database(String db) throws TException {
    tableService.createDatabase(db);
  }

  @Override
  public void create_table(Table table) throws TException {
    tableService.createTable(table);
  }

  @Override
  public Table get_table(String db, String tb) throws TException {
    return tableService.getTable(db, tb);
  }

  @Override
  public List<THoodieInstant> list_instants(String db, String tb, int num) throws TException {
    return timelineService.listInstants(db, tb, num);
  }

  @Override
  public ByteBuffer get_instant_meta(String db, String tb, THoodieInstant instant) throws TException {
    return timelineService.getInstantMeta(db, tb, instant);
  }

  @Override
  public String create_new_instant_time(String db, String tb) throws TException {
    return timelineService.createNewInstantTime(db, tb);
  }

  @Override
  public HoodieInstantChangeResult create_new_instant_with_time(String db, String tb, THoodieInstant instant, ByteBuffer content) throws TException {
    return timelineService.createNewInstantWithTime(db, tb, instant, content);
  }

  @Override
  public HoodieInstantChangeResult transition_instant_state(String db, String tb, THoodieInstant fromInstant, THoodieInstant toInstant, ByteBuffer metadata) throws TException {
    return timelineService.transitionInstantState(db, tb, fromInstant, toInstant, metadata);
  }

  @Override
  public HoodieInstantChangeResult delete_instant(String db, String tb, THoodieInstant instant) throws TException {
    return timelineService.deleteInstant(db, tb, instant);
  }

  @Override
  public ByteBuffer list_files_in_partition(String db, String tb, String partition, String timestamp) throws TException {
    throw new TException("not supported");
  }

  @Override
  public List<String> list_all_partitions(String db, String tb) throws TException {
    return partitionService.listAllPartitions(db, tb);
  }
}
