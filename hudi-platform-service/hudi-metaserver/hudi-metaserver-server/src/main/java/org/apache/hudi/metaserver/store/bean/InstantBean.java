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

package org.apache.hudi.metaserver.store.bean;

import org.apache.hudi.metaserver.thrift.TAction;
import org.apache.hudi.metaserver.thrift.THoodieInstant;
import org.apache.hudi.metaserver.thrift.TState;

/**
 * Instant entity for store.
 */
public class InstantBean {

  private Long tableId;
  private String ts;
  private Integer action;
  private Integer state;

  public InstantBean(Long tableId, THoodieInstant instant) {
    this.tableId = tableId;
    this.ts = instant.getTimestamp();
    this.action = instant.getAction().getValue();
    this.state = instant.getState().getValue();
  }

  public InstantBean(String ts, Byte action, Byte state) {
    this.ts = ts;
    this.action = action & 0xFF;
    this.state = state & 0xFF;
  }

  public THoodieInstant toTHoodieInstant() {
    THoodieInstant instant = new THoodieInstant();
    instant.setTimestamp(ts);
    instant.setAction(TAction.findByValue(action));
    instant.setState(TState.findByValue(state));
    return instant;
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public String getTs() {
    return ts;
  }

  public void setTs(String ts) {
    this.ts = ts;
  }

  public Integer getAction() {
    return action;
  }

  public void setAction(Integer action) {
    this.action = action;
  }

  public Integer getState() {
    return state;
  }

  public void setState(Integer state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return "InstantBean{"
        + "tableId=" + tableId
        + ", ts='" + ts + '\''
        + ", action=" + action
        + ", state=" + state
        + '}';
  }
}
