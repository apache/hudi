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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.utils.SparkRowSerDe;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for the built-in key generators. Contains methods structured for
 * code reuse amongst them.
 */
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  private static final String STRUCT_NAME = "hoodieRowTopLevelField";
  private static final String NAMESPACE = "hoodieRow";
  private transient Function1<Row, GenericRecord> converterFn = null;
  private SparkRowSerDe sparkRowSerDe;
  protected StructType structType;

  protected Map<String, List<Integer>> recordKeyPositions = new HashMap<>();
  protected Map<String, List<Integer>> partitionPathPositions = new HashMap<>();
  protected Map<String, List<DataType>> partitionPathDataTypes = null;

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  /**
   * Fetch record key from {@link Row}.
   * @param row instance of {@link Row} from which record key is requested.
   * @return the record key of interest from {@link Row}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getRecordKey(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionUtils.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    return getKey(converterFn.apply(row)).getRecordKey();
  }

  /**
   * Fetch partition path from {@link Row}.
   * @param row instance of {@link Row} from which partition path is requested
   * @return the partition path of interest from {@link Row}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getPartitionPath(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionUtils.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    return getKey(converterFn.apply(row)).getPartitionPath();
  }

  /**
   * Fetch partition path from {@link InternalRow}.
   *
   * @param internalRow {@link InternalRow} instance from which partition path needs to be fetched from.
   * @param structType  schema of the internalRow.
   * @return the partition path.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getPartitionPath(InternalRow internalRow, StructType structType) {
    try {
      initDeserializer(structType);
      Row row = sparkRowSerDe.deserializeRow(internalRow);
      return getPartitionPath(row);
    } catch (Exception e) {
      throw new HoodieIOException("Conversion of InternalRow to Row failed with exception " + e);
    }
  }

  private void initDeserializer(StructType structType) {
    if (sparkRowSerDe == null) {
      sparkRowSerDe = HoodieSparkUtils.getDeserializer(structType);
    }
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

  protected String getPartitionPathInternal(InternalRow row, StructType structType) {
    buildFieldDataTypesMapIfNeeded(structType);
    validatePartitionFieldsForInternalRow();
    return RowKeyGeneratorHelper.getPartitionPathFromInternalRow(row, getPartitionPathFields(),
        hiveStylePartitioning, partitionPathPositions, partitionPathDataTypes);
  }

  protected void validatePartitionFieldsForInternalRow() {
    partitionPathPositions.entrySet().forEach(entry -> {
      if (entry.getValue().size() > 1) {
        throw new IllegalArgumentException("Nested column for partitioning is not supported with disabling meta columns");
      }
    });
  }

  void buildFieldDataTypesMapIfNeeded(StructType structType) {
    buildFieldPositionMapIfNeeded(structType);
    if (this.partitionPathDataTypes == null) {
      this.partitionPathDataTypes = new HashMap<>();
      if (getPartitionPathFields() != null) {
        // populating simple fields are good enough
        getPartitionPathFields().stream().filter(f -> !f.isEmpty()).filter(f -> !(f.contains(".")))
            .forEach(f -> {
              if (structType.getFieldIndex(f).isDefined()) {
                partitionPathDataTypes.put(f,
                    Collections.singletonList((structType.fields()[structType.fieldIndex(f)].dataType())));
              } else {
                partitionPathDataTypes.put(f, Collections.singletonList(null));
              }
            });
      }
    }
  }
}

