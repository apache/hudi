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
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaserver;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A proxy for meta server, accepts all thrift calls and routes them to the corresponding service.
 */
public class HoodieMetaserverService implements ThriftHoodieMetaserver.Iface, Serializable {
  private TableService tableService;
  private TimelineService timelineService;

  public HoodieMetaserverService(TableService tableService, TimelineService timelineService) {
    this.tableService = tableService;
    this.timelineService = timelineService;
  }

  @Override
  public void createDatabase(String db) throws TException {
    tableService.createDatabase(db);
  }

  @Override
  public void createTable(Table table) throws TException {
    tableService.createTable(table);
  }

  @Override
  public Table getTable(String db, String tb) throws TException {
    return tableService.getTable(db, tb);
  }

  @Override
  public List<THoodieInstant> listInstants(String db, String tb, int num) throws TException {
    return timelineService.listInstants(db, tb, num);
  }

  @Override
  public ByteBuffer getInstantMeta(String db, String tb, THoodieInstant instant) throws TException {
    return timelineService.getInstantMeta(db, tb, instant);
  }

  @Override
  public String createNewInstantTime(String db, String tb) throws TException {
    return timelineService.createNewInstantTime(db, tb);
  }

  @Override
  public HoodieInstantChangeResult createNewInstantWithTime(String db, String tb, THoodieInstant instant, ByteBuffer content) throws TException {
    return timelineService.createNewInstantWithTime(db, tb, instant, content);
  }

  @Override
  public HoodieInstantChangeResult transitionInstantState(String db, String tb, THoodieInstant fromInstant, THoodieInstant toInstant, ByteBuffer metadata) throws TException {
    return timelineService.transitionInstantState(db, tb, fromInstant, toInstant, metadata);
  }

  @Override
  public HoodieInstantChangeResult deleteInstant(String db, String tb, THoodieInstant instant) throws TException {
    return timelineService.deleteInstant(db, tb, instant);
  }
}
