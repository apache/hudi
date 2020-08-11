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

import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Function1;

/**
 * Base class for all the built-in key generators. Contains methods structured for
 * code reuse amongst them.
 */
public abstract class BuiltinKeyGenerator extends KeyGenerator {

  protected List<String> recordKeyFields;
  protected List<String> partitionPathFields;

  private Map<String, List<Integer>> recordKeyPositions = new HashMap<>();
  private Map<String, List<Integer>> partitionPathPositions = new HashMap<>();

  private transient Function1<Object, Object> converterFn = null;
  protected StructType structType;
  private String structName;
  private String recordNamespace;

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
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

  @Override
  public void initializeRowKeyGenerator(StructType structType, String structName, String recordNamespace) {
    // parse simple feilds
    getRecordKeyFields().stream()
        .filter(f -> !(f.contains(".")))
        .forEach(f -> recordKeyPositions.put(f, Collections.singletonList((Integer) (structType.getFieldIndex(f).get()))));
    // parse nested fields
    getRecordKeyFields().stream()
        .filter(f -> f.contains("."))
        .forEach(f -> recordKeyPositions.put(f, RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, true)));
    // parse simple fields
    if (getPartitionPathFields() != null) {
      getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> !(f.contains(".")))
          .forEach(f -> partitionPathPositions.put(f,
              Collections.singletonList((Integer) (structType.getFieldIndex(f).get()))));
      // parse nested fields
      getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> f.contains("."))
          .forEach(f -> partitionPathPositions.put(f,
              RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, false)));
    }
    this.structName = structName;
    this.structType = structType;
    this.recordNamespace = recordNamespace;
  }

  /**
   * Fetch record key from {@link Row}.
   * @param row instance of {@link Row} from which record key is requested.
   * @return the record key of interest from {@link Row}.
   */
  @Override
  public String getRecordKey(Row row) {
    if (null != converterFn) {
      converterFn = AvroConversionHelper.createConverterToAvro(structType, structName, recordNamespace);
    }
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getRecordKey();
  }

  /**
   * Fetch partition path from {@link Row}.
   * @param row instance of {@link Row} from which partition path is requested
   * @return the partition path of interest from {@link Row}.
   */
  @Override
  public String getPartitionPath(Row row) {
    if (null != converterFn) {
      converterFn = AvroConversionHelper.createConverterToAvro(structType, structName, recordNamespace);
    }
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getPartitionPath();
  }

  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }

  protected Map<String, List<Integer>> getRecordKeyPositions() {
    return recordKeyPositions;
  }

  protected Map<String, List<Integer>> getPartitionPathPositions() {
    return partitionPathPositions;
  }
}
