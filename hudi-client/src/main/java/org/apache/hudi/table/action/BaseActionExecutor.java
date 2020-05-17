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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class BaseActionExecutor<R> implements Serializable {

  protected final transient JavaSparkContext jsc;
  protected final transient Configuration hadoopConf;

  protected final HoodieWriteConfig config;

  protected final HoodieTable<?> table;

  protected final String instantTime;

  public BaseActionExecutor(JavaSparkContext jsc, HoodieWriteConfig config, HoodieTable<?> table, String instantTime) {
    this.jsc = jsc;
    this.hadoopConf = jsc.hadoopConfiguration();
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
  }

  public abstract R execute();
}
