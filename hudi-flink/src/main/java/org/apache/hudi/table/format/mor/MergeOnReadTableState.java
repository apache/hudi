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

package org.apache.hudi.table.format.mor;

import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.List;

/**
 * Statistics for merge on read table source.
 */
public class MergeOnReadTableState implements Serializable {

  private static final long serialVersionUID = 1L;

  private final RowType rowType;
  private final RowType requiredRowType;
  private final String avroSchema;
  private final String requiredAvroSchema;
  private final List<MergeOnReadInputSplit> inputSplits;

  public MergeOnReadTableState(
      RowType rowType,
      RowType requiredRowType,
      String avroSchema,
      String requiredAvroSchema,
      List<MergeOnReadInputSplit> inputSplits) {
    this.rowType = rowType;
    this.requiredRowType = requiredRowType;
    this.avroSchema = avroSchema;
    this.requiredAvroSchema = requiredAvroSchema;
    this.inputSplits = inputSplits;
  }

  public RowType getRowType() {
    return rowType;
  }

  public RowType getRequiredRowType() {
    return requiredRowType;
  }

  public String getAvroSchema() {
    return avroSchema;
  }

  public String getRequiredAvroSchema() {
    return requiredAvroSchema;
  }

  public List<MergeOnReadInputSplit> getInputSplits() {
    return inputSplits;
  }

  public int[] getRequiredPositions() {
    final List<String> fieldNames = rowType.getFieldNames();
    return requiredRowType.getFieldNames().stream()
        .map(fieldNames::indexOf)
        .mapToInt(i -> i)
        .toArray();
  }
}
