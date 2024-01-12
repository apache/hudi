/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.multitable;

import java.util.ArrayList;
import java.util.List;

/**
 * TableServicePipeline is a container holding all table services task to execute for a specific hoodie table.
 */
public class TableServicePipeline {

  private final List<TableServiceTask> tableServiceTasks;

  public TableServicePipeline() {
    this.tableServiceTasks = new ArrayList<>();
  }

  /**
   * Add a table service task to the end of table service pipe. The task will be executed in FIFO manner.
   *
   * @param task table service task to run in pipeline.
   */
  public void add(TableServiceTask task) {
    tableServiceTasks.add(task);
  }

  /**
   * Run all table services task sequentially.
   */
  public void execute() {
    tableServiceTasks.forEach(TableServiceTask::run);
  }
}
