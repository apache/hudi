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

package org.apache.hudi.delta.sync;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Metadata implements Serializable {
  String id;
  Format format;
  String schemaString;
  String[] partitionColumns;
  Map<String, String> configuration = new HashMap<>();
  long createdTime;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Format getFormat() {
    return format;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public String getSchemaString() {
    return schemaString;
  }

  public void setSchemaString(String schemaString) {
    this.schemaString = schemaString;
  }

  public String[] getPartitionColumns() {
    return partitionColumns;
  }

  public void setPartitionColumns(String[] partitionColumns) {
    this.partitionColumns = partitionColumns;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }
}
