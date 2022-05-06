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
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.utils.SparkRowSerDe;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for all built-in key generators.
 *
 * NOTE: By default it implements all the methods of {@link SparkKeyGeneratorInterface}, which
 *       by default however fallback to Avro implementation. For maximum performance (to avoid
 *       conversion from Spark's internal data-types to Avro) you should override these methods
 *       in your implementation.
 *
 * TODO rename to AvroFallbackBaseKeyGenerator
 */
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  private static final String STRUCT_NAME = "hoodieRowTopLevelField";
  private static final String NAMESPACE = "hoodieRow";

  private transient Function1<Row, GenericRecord> converterFn = null;
  private final AtomicBoolean validatePartitionFields = new AtomicBoolean(false);
  protected transient StructType structType;

  protected Map<String, Pair<List<Integer>, DataType>> recordKeySchemaInfo = new HashMap<>();
  protected Map<String, Pair<List<Integer>, DataType>> partitionPathSchemaInfo = new HashMap<>();

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  @Override
  public String getRecordKey(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionUtils.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    return getKey(converterFn.apply(row)).getRecordKey();
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    try {
      // TODO fix
      buildFieldSchemaInfoIfNeeded(schema);
      return UTF8String.fromString(RowKeyGeneratorHelper.getRecordKeyFromInternalRow(internalRow, getRecordKeyFields(), recordKeySchemaInfo, false));
    } catch (Exception e) {
      throw new HoodieException("Conversion of InternalRow to Row failed with exception", e);
    }
  }

  @Override
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
  public UTF8String getPartitionPath(InternalRow internalRow, StructType structType) {
    try {
      buildFieldSchemaInfoIfNeeded(structType);
      return UTF8String.fromString(RowKeyGeneratorHelper.getPartitionPathFromInternalRow(internalRow, getPartitionPathFields(), hiveStylePartitioning, partitionPathSchemaInfo));
    } catch (Exception e) {
      throw new HoodieException("Conversion of InternalRow to Row failed with exception", e);
    }
  }

  void buildFieldSchemaInfoIfNeeded(StructType structType) {
    if (this.structType == null) {
      this.structType = structType;
      getRecordKeyFields()
          .stream().filter(f -> !f.isEmpty())
          .forEach(f -> recordKeySchemaInfo.put(f, RowKeyGeneratorHelper.getFieldSchemaInfo(structType, f, true)));
      if (getPartitionPathFields() != null) {
        getPartitionPathFields().stream().filter(f -> !f.isEmpty())
            .forEach(f -> partitionPathSchemaInfo.put(f, RowKeyGeneratorHelper.getFieldSchemaInfo(structType, f, false)));
      }
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

