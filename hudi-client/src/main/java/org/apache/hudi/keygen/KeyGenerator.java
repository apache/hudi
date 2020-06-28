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

package org.apache.hudi.keygen;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Abstract class to extend for plugging in extraction of {@link HoodieKey} from an Avro record.
 */
public abstract class KeyGenerator implements Serializable {

  protected static final Logger LOG = LogManager.getLogger(KeyGenerator.class);

  protected transient TypedProperties config;

  private List<String> recordKeyFields;
  private List<String> partitionPathFields;
  private List<Integer> rowKeyFieldsPos;
  private List<Integer> rowPartitionPathFieldsPos;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public abstract HoodieKey getKey(GenericRecord record);

  public boolean isRowKeyExtractionSupported() {
    return false;
  }

  public void initializeRowKeyGenerator(StructType structType) {
    this.rowKeyFieldsPos = getRecordKeyFields().stream()
        .map(f -> (Integer)(structType.getFieldIndex(f).get()))
        .collect(Collectors.toList());
    System.out.println("Partition Path Fields : " + getPartitionPathFields());
    this.rowPartitionPathFieldsPos = getPartitionPathFields().stream()
        .filter(f -> !f.isEmpty())
        .map(f -> (Integer)(structType.getFieldIndex(f).get()))
        .collect(Collectors.toList());
  }

  public String getRecordKeyFromRow(Row row) {
    throw new IllegalArgumentException("Not Implemented");
  }

  public String getPartitionPathFromRow(Row row) {
    throw new IllegalArgumentException("Not Implemented");
  }

  protected List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  protected List<String> getPartitionPathFields() {
    return partitionPathFields;
  }

  protected void setRecordKeyFields(List<String> recordKeyFields) {
    this.recordKeyFields = recordKeyFields;
  }

  protected void setPartitionPathFields(List<String> partitionPathFields) {
    this.partitionPathFields = partitionPathFields;
  }

  protected List<Integer> getRowKeyFieldsPos() {
    return rowKeyFieldsPos;
  }

  protected void setRowKeyFieldsPos(List<Integer> rowKeyFieldsPos) {
    this.rowKeyFieldsPos = rowKeyFieldsPos;
  }

  protected List<Integer> getRowPartitionPathFieldsPos() {
    return rowPartitionPathFieldsPos;
  }

  protected void setRowPartitionPathFieldsPos(List<Integer> rowPartitionPathFieldsPos) {
    this.rowPartitionPathFieldsPos = rowPartitionPathFieldsPos;
  }
}
