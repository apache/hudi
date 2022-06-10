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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Function1;

/**
 * Base class for the built-in key generators. Contains methods structured for
 * code reuse amongst them.
 */
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  private static final String STRUCT_NAME = "hoodieRowTopLevelField";
  private static final String NAMESPACE = "hoodieRow";
  private Function1<Row, GenericRecord> rowConverterFn = null;
  private Function1<InternalRow, GenericRecord> internalConverterFn = null;
  private final AtomicBoolean validatePartitionFields = new AtomicBoolean(false);
  protected StructType structType;

  protected Map<String, Pair<List<Integer>, DataType>> recordKeySchemaInfo = new HashMap<>();
  protected Map<String, Pair<List<Integer>, DataType>> partitionPathSchemaInfo = new HashMap<>();

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  /**
   * Fetch record key from {@link Row}.
   *
   * @param row instance of {@link Row} from which record key is requested.
   * @return the record key of interest from {@link Row}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getRecordKey(Row row) {
    if (null == rowConverterFn) {
      rowConverterFn = AvroConversionUtils.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    return getKey(rowConverterFn.apply(row)).getRecordKey();
  }

  /**
   * Fetch record key from {@link InternalRow}.
   *
   * @param internalRow instance of {@link InternalRow} from which record key is requested.
   * @param structType schema of {@link InternalRow}
   * @return the record key of interest from {@link InternalRow}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getRecordKey(InternalRow internalRow, StructType structType) {
    if (null == internalConverterFn) {
      Schema schema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, STRUCT_NAME, NAMESPACE);
      internalConverterFn = AvroConversionUtils.createInternalRowToAvroConverter(structType, schema, true);
    }
    return getKey(internalConverterFn.apply(internalRow)).getRecordKey();
  }

  /**
   * Fetch partition path from {@link Row}.
   *
   * @param row instance of {@link Row} from which partition path is requested
   * @return the partition path of interest from {@link Row}.
   */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getPartitionPath(Row row) {
    if (null == rowConverterFn) {
      rowConverterFn = AvroConversionUtils.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    return getKey(rowConverterFn.apply(row)).getPartitionPath();
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
      buildFieldSchemaInfoIfNeeded(structType);
      return RowKeyGeneratorHelper.getPartitionPathFromInternalRow(internalRow, getPartitionPathFields(),
          hiveStylePartitioning, partitionPathSchemaInfo);
    } catch (Exception e) {
      throw new HoodieIOException("Conversion of InternalRow to Row failed with exception " + e);
    }
  }

  void buildFieldSchemaInfoIfNeeded(StructType structType) {
    if (this.structType == null) {
      getRecordKeyFields()
          .stream().filter(f -> !f.isEmpty())
          .forEach(f -> recordKeySchemaInfo.put(f, RowKeyGeneratorHelper.getFieldSchemaInfo(structType, f, true)));
      if (getPartitionPathFields() != null) {
        getPartitionPathFields().stream().filter(f -> !f.isEmpty())
            .forEach(f -> partitionPathSchemaInfo.put(f, RowKeyGeneratorHelper.getFieldSchemaInfo(structType, f, false)));
      }
      this.structType = structType;
    }
  }

  protected String getPartitionPathInternal(InternalRow row, StructType structType) {
    buildFieldSchemaInfoIfNeeded(structType);
    validatePartitionFieldsForInternalRow();
    return RowKeyGeneratorHelper.getPartitionPathFromInternalRow(row, getPartitionPathFields(),
        hiveStylePartitioning, partitionPathSchemaInfo);
  }

  protected void validatePartitionFieldsForInternalRow() {
    if (!validatePartitionFields.getAndSet(true)) {
      partitionPathSchemaInfo.values().forEach(entry -> {
        if (entry.getKey().size() > 1) {
          throw new IllegalArgumentException("Nested column for partitioning is not supported with disabling meta columns");
        }
      });
    }
  }
}

