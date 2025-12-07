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

package org.apache.hudi.client;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Result of a write operation.
 */
@Getter
@Setter
public class HoodieWriteResult implements Serializable {

  private JavaRDD<WriteStatus> writeStatuses;
  private Map<String, List<String>> partitionToReplaceFileIds;

  public HoodieWriteResult(JavaRDD<WriteStatus> writeStatuses) {
    this(writeStatuses, Collections.emptyMap());
  }

  public HoodieWriteResult(JavaRDD<WriteStatus> writeStatuses, Map<String, List<String>> partitionToReplaceFileIds) {
    this.writeStatuses = writeStatuses;
    this.partitionToReplaceFileIds = partitionToReplaceFileIds;
  }

  @Override
  public String toString() {
    return "HoodieWriteResult{"
        + "writeStatuses=" + writeStatuses
        + ", partitionToReplaceFileIds=" + partitionToReplaceFileIds
        + '}';
  }
}
