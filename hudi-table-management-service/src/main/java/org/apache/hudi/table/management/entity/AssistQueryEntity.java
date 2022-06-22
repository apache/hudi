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

import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.util.DateTimeUtils;

import lombok.Getter;

import java.util.Date;

@Getter
public class AssistQueryEntity {

  private int maxRetry = ServiceConfig.getInstance()
      .getInt(ServiceConfig.ServiceConfVars.MaxRetryNum);

  private Date queryStartTime = DateTimeUtils.addDay(-3);

  private int status;

  public AssistQueryEntity() {

  }

  public AssistQueryEntity(int status, Date queryStartTime) {
    this.status = status;
    this.queryStartTime = queryStartTime;
  }

}
