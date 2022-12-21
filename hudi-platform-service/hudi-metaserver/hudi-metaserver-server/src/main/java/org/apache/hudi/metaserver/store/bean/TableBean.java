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

import org.apache.hudi.metaserver.thrift.Table;

/**
 * Table entity for store.
 */
public class TableBean {
  private String databaseName;
  private Long tblId;
  private String tableName;
  private String createTime;
  private String owner;
  private String location;

  public TableBean(Table table) {
    this.tableName = table.tableName;
    this.owner = table.owner;
    this.location = table.location;
  }

  public TableBean(String databaseName, Long tblId, String tableName, String createTime, String owner, String location) {
    this.databaseName = databaseName;
    this.tblId = tblId;
    this.tableName = tableName;
    this.createTime = createTime;
    this.owner = owner;
    this.location = location;
  }

  public Table toTable() {
    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setOwner(owner);
    table.setLocation(location);
    return table;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public Long getTblId() {
    return tblId;
  }

  public void setTblId(Long tblId) {
    this.tblId = tblId;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Override
  public String toString() {
    return "TableBean{"
        + "databaseName=" + databaseName
        + ", tblId=" + tblId
        + ", tableName='" + tableName + '\''
        + ", createTime='" + createTime + '\''
        + ", owner='" + owner + '\''
        + ", location='" + location + '\''
        + '}';
  }
}
