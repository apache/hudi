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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base class for all the built-in key generators. Contains methods structured for
 * code reuse amongst them.
 */
public abstract class BuiltinKeyGenerator extends KeyGenerator {

  protected List<String> recordKeyFields;
  protected List<String> partitionPathFields;
  protected final boolean encodePartitionPath;
  protected final boolean hiveStylePartitioning;

  protected Map<String, List<Integer>> recordKeyPositions = new HashMap<>();
  protected Map<String, List<Integer>> partitionPathPositions = new HashMap<>();
  protected StructType structType;

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
    this.encodePartitionPath = config.getBoolean(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL()));
    this.hiveStylePartitioning = config.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
  }

  /**
   * Generate a record Key out of provided generic record.
   */
  public abstract String getRecordKey(GenericRecord record);

  /**
   * Generate a partition path out of provided generic record.
   */
  public abstract String getPartitionPath(GenericRecord record);

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public final HoodieKey getKey(GenericRecord record) {
    if (getRecordKeyFields() == null || getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }
    return new HoodieKey(getRecordKey(record), getPartitionPath(record));
  }

  @Override
  public final List<String> getRecordKeyFieldNames() {
    // For nested columns, pick top level column name
    return getRecordKeyFields().stream().map(k -> {
      int idx = k.indexOf('.');
      return idx > 0 ? k.substring(0, idx) : k;
    }).collect(Collectors.toList());
  }

  void buildFieldPositionMapIfNeeded(StructType structType) {
    if (this.structType == null) {
      // parse simple fields
      getRecordKeyFields().stream()
          .filter(f -> !(f.contains(".")))
          .forEach(f -> {
            if (structType.getFieldIndex(f).isDefined()) {
              recordKeyPositions.put(f, Collections.singletonList((Integer) (structType.getFieldIndex(f).get())));
            } else {
              throw new HoodieKeyException("recordKey value not found for field: \"" + f + "\"");
            }
          });
      // parse nested fields
      getRecordKeyFields().stream()
          .filter(f -> f.contains("."))
          .forEach(f -> recordKeyPositions.put(f, RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, true)));
      // parse simple fields
      if (getPartitionPathFields() != null) {
        getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> !(f.contains(".")))
            .forEach(f -> {
              if (structType.getFieldIndex(f).isDefined()) {
                partitionPathPositions.put(f,
                    Collections.singletonList((Integer) (structType.getFieldIndex(f).get())));
              } else {
                partitionPathPositions.put(f, Collections.singletonList(-1));
              }
            });
        // parse nested fields
        getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> f.contains("."))
            .forEach(f -> partitionPathPositions.put(f,
                RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, false)));
      }
      this.structType = structType;
    }
  }

  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }
}
