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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.UTF8StringBuilder;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;

import javax.annotation.concurrent.ThreadSafe;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR;
import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_RECORD_KEY_PARTS_SEPARATOR;
import static org.apache.hudi.keygen.KeyGenUtils.HUDI_DEFAULT_PARTITION_PATH;

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

  protected static final UTF8String HUDI_DEFAULT_PARTITION_PATH_UTF8 = UTF8String.fromString(HUDI_DEFAULT_PARTITION_PATH);

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

  protected void tryInitRowAccessor(StructType schema) {
    if (this.rowAccessor == null) {
      synchronized (this) {
        if (this.rowAccessor == null) {
          this.rowAccessor = new SparkRowAccessor(schema);
        }
      }
    }
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final String combinePartitionPath(Object ...partitionPathParts) {
    checkState(partitionPathParts.length == recordKeyFields.size());
    // Avoid creating [[StringBuilder]] in case there's just one partition-path part,
    // and Hive-style of partitioning is not required
    if (!hiveStylePartitioning && partitionPathParts.length == 1) {
      return partitionPathParts[0] != null
          ? partitionPathParts[0].toString()
          : HUDI_DEFAULT_PARTITION_PATH;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < partitionPathParts.length; ++i) {
      String partitionPathPartStr = partitionPathParts[i] != null
          ? partitionPathParts[i].toString()
          : HUDI_DEFAULT_PARTITION_PATH;

      // TODO support url-encoding

      if (hiveStylePartitioning) {
        sb.append(recordKeyFields.get(i))
            .append("=")
            .append(partitionPathPartStr);
      } else {
        sb.append(partitionPathPartStr);
      }

      if (i < partitionPathParts.length - 1) {
        sb.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }

    return sb.toString();
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final UTF8String combinePartitionPathUnsafe(Object ...partitionPathParts) {
    checkState(partitionPathParts.length == recordKeyFields.size());
    // Avoid creating [[StringBuilder]] in case there's just one partition-path part,
    // and Hive-style of partitioning is not required
    if (!hiveStylePartitioning && partitionPathParts.length == 1) {
      return partitionPathParts[0] != null
          ? toUTF8String(partitionPathParts[0])
          : HUDI_DEFAULT_PARTITION_PATH_UTF8;
    }

    UTF8StringBuilder sb = new UTF8StringBuilder();
    for (int i = 0; i < partitionPathParts.length; ++i) {
      UTF8String partitionPathPartStr = partitionPathParts[i] != null
          ? toUTF8String(partitionPathParts[i])
          : HUDI_DEFAULT_PARTITION_PATH_UTF8;

      if (hiveStylePartitioning) {
        sb.append(recordKeyFields.get(i));
        sb.append("=");
        sb.append(partitionPathPartStr);
      } else {
        sb.append(partitionPathPartStr);
      }

      if (i < partitionPathParts.length - 1) {
        sb.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }

    return sb.build();
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final String combineRecordKey(Object ...recordKeyParts) {
    if (recordKeyParts.length == 1) {
      return requireNonNullNonEmptyKey(recordKeyParts[0].toString());
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      sb.append(requireNonNullNonEmptyKey(recordKeyParts[i].toString()));

      if (i < recordKeyParts.length - 1) {
        sb.append(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.toString();
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final UTF8String combineRecordKeyUnsafe(Object ...recordKeyParts) {
    if (recordKeyParts.length == 1) {
      return requireNonNullNonEmptyKey(toUTF8String(recordKeyParts[0]));
    }

    UTF8StringBuilder sb = new UTF8StringBuilder();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      sb.append(requireNonNullNonEmptyKey(toUTF8String(recordKeyParts[i])));

      if (i < recordKeyParts.length - 1) {
        sb.append(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.build();
  }

  protected static UTF8String toUTF8String(Object o) {
    if (o instanceof UTF8String) {
      return (UTF8String) o;
    } else {
      // NOTE: If object is a [[String]], [[toString]] would be a no-op
      return UTF8String.fromString(o.toString());
    }
  }

  protected static String requireNonNullNonEmptyKey(String key) {
    if (key != null && key.length() > 0) {
      return key;
    } else {
      throw new HoodieKeyException("Record key has to be non-empty string!");
    }
  }

  protected static UTF8String requireNonNullNonEmptyKey(UTF8String key) {
    if (key != null && key.numChars() > 0) {
      return key;
    } else {
      throw new HoodieKeyException("Record key has to be non-empty string!");
    }
  }

  protected static <T> T handleNullRecordKey() {
    throw new HoodieKeyException("Record key has to be non-null!");
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

    public Object[] getRecordKeyParts(Row row) {
      return getNestedFieldValues(row, recordKeyFieldsPaths);
    }

    public Object[] getRecordPartitionPathValues(Row row) {
      return getNestedFieldValues(row, partitionPathFieldsPaths);
    }

    public Object[] getRecordKeyParts(InternalRow row) {
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

