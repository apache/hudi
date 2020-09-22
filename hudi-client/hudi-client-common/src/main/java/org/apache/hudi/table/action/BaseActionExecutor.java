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

package org.apache.hudi.table.action;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

public abstract class BaseActionExecutor<T extends HoodieRecordPayload, I, K, O, P, R> implements Serializable {

  protected final transient HoodieEngineContext context;
  protected final transient Configuration hadoopConf;

  protected final HoodieWriteConfig config;

  protected final HoodieTable<T, I, K, O, P> table;

  protected final String instantTime;

  public BaseActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O, P> table, String instantTime) {
    this.context = context;
    this.hadoopConf = context.getHadoopConf().get();
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
  }

  public abstract R execute();
}
