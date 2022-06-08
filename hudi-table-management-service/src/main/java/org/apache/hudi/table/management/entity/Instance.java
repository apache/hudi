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

package org.apache.hudi.table.management.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Builder
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Instance {

  private long id;

  private String dbName;

  private String tableName;

  private String basePath;

  private Engine executionEngine;

  private String owner;

  private String queue;

  private String resource;

  private String parallelism;

  private String instant;

  private int action;

  private int status;

  private int runTimes;

  private String applicationId;

  private String doradoJobId;

  private Date scheduleTime;

  private Date createTime;

  private Date updateTime;

  private boolean isDeleted;

  public String getFullTableName() {
    return dbName + "." + tableName;
  }

  public String getIdentifier() {
    return dbName + "." + tableName + "." + instant + "." + status;
  }

  public String getInstanceRunStatus() {
    return dbName + "." + tableName + "." + instant + "." + status + "."
        + runTimes + "." + updateTime;
  }

  public String getRecordKey() {
    return dbName + "." + tableName + "." + instant;
  }
}
