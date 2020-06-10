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

package org.apache.hudi.common;

import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.common.config.SerializableConfiguration;

/**
 * Base class contains the context information needed by the engine at runtime. It will be extended by different
 * engine implementation if needed.
 */
public class HoodieEngineContext {
  /**
   * A wrapped hadoop configuration which can be serialized.
   */
  private SerializableConfiguration hadoopConf;

  private TaskContextSupplier taskContextSupplier;

  public HoodieEngineContext(SerializableConfiguration hadoopConf, TaskContextSupplier taskContextSupplier) {
    this.hadoopConf = hadoopConf;
    this.taskContextSupplier = taskContextSupplier;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public TaskContextSupplier getTaskContextSupplier() {
    return taskContextSupplier;
  }
}
