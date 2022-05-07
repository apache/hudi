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
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils$;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
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
@ThreadSafe
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  protected transient volatile SparkRowConverter rowConverter;
  protected transient volatile SparkRowAccessor rowAccessor;

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  @Override
  public String getRecordKey(Row row) {
    tryInitRowConverter(row.schema());
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    return getRecordKey(rowConverter.convertToAvro(row));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    tryInitRowConverter(schema);
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    return UTF8String.fromString(getRecordKey(rowConverter.convertToAvro(internalRow)));
  }

  @Override
  public String getPartitionPath(Row row) {
    tryInitRowConverter(row.schema());
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    return getPartitionPath(rowConverter.convertToAvro(row));
  }

  @Override
  public UTF8String getPartitionPath(InternalRow internalRow, StructType schema) {
    tryInitRowConverter(schema);
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    GenericRecord avroRecord = rowConverter.convertToAvro(internalRow);
    return UTF8String.fromString(getPartitionPath(avroRecord));
  }

  private void tryInitRowConverter(StructType structType) {
    if (rowConverter == null) {
      synchronized (this) {
        if (rowConverter == null) {
          rowConverter = new SparkRowConverter(structType);
        }
      }
    }
  }

  void tryInitRowAccessor(StructType schema) {
    if (this.rowAccessor == null) {
      synchronized (this) {
        if (this.rowAccessor == null) {
          this.rowAccessor = new SparkRowAccessor(schema);
        }
      }
    }
  }

  protected static class SparkRowConverter {
    private static final String STRUCT_NAME = "hoodieRowTopLevelField";
    private static final String NAMESPACE = "hoodieRow";

    private final Function1<Row, GenericRecord> avroConverter;
    private final SparkRowSerDe rowSerDe;

    SparkRowConverter(StructType schema) {
      this.rowSerDe = HoodieSparkUtils.getDeserializer(schema);
      this.avroConverter = AvroConversionUtils.createConverterToAvro(schema, STRUCT_NAME, NAMESPACE);
    }

    GenericRecord convertToAvro(Row row) {
      return avroConverter.apply(row);
    }

    GenericRecord convertToAvro(InternalRow row) {
      return avroConverter.apply(rowSerDe.deserializeRow(row));
    }
  }

  protected class SparkRowAccessor {
    private final HoodieUnsafeRowUtils.NestedFieldPath[] recordKeyFieldsPaths;
    private final HoodieUnsafeRowUtils.NestedFieldPath[] partitionPathFieldsPaths;

    SparkRowAccessor(StructType schema) {
      this.recordKeyFieldsPaths = unsafeCast(
          getRecordKeyFields().stream()
              .map(recordKeyField -> HoodieUnsafeRowUtils$.MODULE$.composeNestedFieldPath(schema, recordKeyField))
              .toArray());
      this.partitionPathFieldsPaths = unsafeCast(
          getPartitionPathFields().stream()
              .map(recordKeyField -> HoodieUnsafeRowUtils$.MODULE$.composeNestedFieldPath(schema, recordKeyField))
              .toArray());
    }

    public Object[] getRecordKeys(Row row) {
      return getNestedFieldValues(row, recordKeyFieldsPaths);
    }

    public Object[] getRecordPartitionPathValues(Row row) {
      return getNestedFieldValues(row, partitionPathFieldsPaths);
    }

    public Object[] getRecordKeys(InternalRow row) {
      return getNestedFieldValues(row, recordKeyFieldsPaths);
    }

    public Object[] getRecordPartitionPathValues(InternalRow row) {
      return getNestedFieldValues(row, partitionPathFieldsPaths);
    }

    private Object[] getNestedFieldValues(Row row, HoodieUnsafeRowUtils.NestedFieldPath[] nestedFieldsPaths) {
      Object[] nestedFieldValues = new Object[nestedFieldsPaths.length];
      for (int i = 0; i < nestedFieldsPaths.length; ++i) {
        nestedFieldValues[i] = HoodieUnsafeRowUtils$.MODULE$.getNestedRowValue(row, nestedFieldsPaths[i]);
      }
      return nestedFieldValues;
    }

    private Object[] getNestedFieldValues(InternalRow row, HoodieUnsafeRowUtils.NestedFieldPath[] nestedFieldsPaths) {
      Object[] nestedFieldValues = new Object[nestedFieldsPaths.length];
      for (int i = 0; i < nestedFieldsPaths.length; ++i) {
        nestedFieldValues[i] = HoodieUnsafeRowUtils$.MODULE$.getNestedInternalRowValue(row, nestedFieldsPaths[i]);
      }
      return nestedFieldValues;
    }
  }
}

