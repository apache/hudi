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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Class to hold virtual key info when meta fields are disabled.
 */
public class HoodieVirtualKeyInfo implements Serializable {

  private final String recordKeyField;
  private final Option<String> partitionPathField;
  private final int recordKeyFieldIndex;
  private final Option<Integer> partitionPathFieldIndex;

  public HoodieVirtualKeyInfo(String recordKeyField, Option<String> partitionPathField, int recordKeyFieldIndex, Option<Integer> partitionPathFieldIndex) {
    this.recordKeyField = recordKeyField;
    this.partitionPathField = partitionPathField;
    this.recordKeyFieldIndex = recordKeyFieldIndex;
    this.partitionPathFieldIndex = partitionPathFieldIndex;
  }

  public String getRecordKeyField() {
    return recordKeyField;
  }

  public Option<String> getPartitionPathField() {
    return partitionPathField;
  }

  public int getRecordKeyFieldIndex() {
    return recordKeyFieldIndex;
  }

  public Option<Integer> getPartitionPathFieldIndex() {
    return partitionPathFieldIndex;
  }

  @Override
  public String toString() {
    return "HoodieVirtualKeyInfo{"
        + "recordKeyField='" + recordKeyField + '\''
        + ", partitionPathField='" + (partitionPathField.isPresent() ? partitionPathField.get() : "null") + '\''
        + ", recordKeyFieldIndex=" + recordKeyFieldIndex
        + ", partitionPathFieldIndex=" + (partitionPathFieldIndex.isPresent() ? partitionPathFieldIndex.get() : "-1")
        + '}';
  }
}