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

package org.apache.hudi.bootstrap;

import org.apache.hudi.client.utils.ClientUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BootstrapKeyGenerator implements Serializable {

  private final HoodieWriteConfig writeConfig;
  private final List<String> keyColumns;
  private final List<String> topLevelKeyColumns;

  public BootstrapKeyGenerator(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
    this.keyColumns = Arrays.asList(writeConfig.getBootstrapRecordKeyColumns().split(","));
    // For nested columns, pick top level column name
    this.topLevelKeyColumns = keyColumns.stream().map(k -> {
      int idx = k.indexOf('.');
      return idx > 0 ? k.substring(0, idx) : k;
    }).collect(Collectors.toList());
  }

  /**
   * Returns record key from generic record. The generic record
   */
  public String getRecordKey(GenericRecord record) {
    return keyColumns.stream().map(key -> ClientUtils.getNestedFieldValAsString(record, key))
        .collect(Collectors.joining("_"));
  }

  public List<String> getKeyColumns() {
    return keyColumns;
  }

  public List<String> getTopLevelKeyColumns() {
    return topLevelKeyColumns;
  }
}
