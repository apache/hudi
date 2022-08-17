/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bootstrap.aggregate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Data volume of ready tasks.
 */
public class IndexAlignmentTaskDetails implements Serializable {
  private static final long serialVersionUID = 1L;
  private final int paralleTasks;
  private Map<Integer, Long> subTaskCount = new HashMap<>();

  public IndexAlignmentTaskDetails(int paralleTasks) {
    this.paralleTasks = paralleTasks;
  }

  public void addSubTaskCount(Integer taskId, Long count) {
    this.subTaskCount.put(taskId, count);
  }

  public int getParalleTasks() {
    return paralleTasks;
  }

  public Map<Integer, Long> getSubTaskCount() {
    return subTaskCount;
  }

  public long getCount() {
    return subTaskCount.values().stream().reduce(Long::sum).orElse(0L);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("IndexAlignmentTaskDetails {");
    sb.append("paralleTasks=");
    sb.append(paralleTasks);
    sb.append(", subTaskCount=");
    sb.append(subTaskCount);
    sb.append("}");
    return sb.toString();
  }
}
