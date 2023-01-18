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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.utils.SparkRowSerDe;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.factory.SparkRecordKeyGeneratorFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import scala.Function1;

import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_COMPOSITE_KEY_FILED_VALUE;
import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_RECORD_KEY_PARTS_SEPARATOR;

/**
 * Base class for all built-in key generators.
 * <p>
 * NOTE: By default it implements all the methods of {@link SparkKeyGeneratorInterface}, which
 * by default however fallback to Avro implementation. For maximum performance (to avoid
 * conversion from Spark's internal data-types to Avro) you should override these methods
 * in your implementation.
 * <p>
 * TODO rename to AvroFallbackBaseKeyGenerator
 */
@ThreadSafe
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  private static final Logger LOG = LogManager.getLogger(BuiltinKeyGenerator.class);

  private static final String COMPOSITE_KEY_FIELD_VALUE_INFIX = ":";

  protected static final String FIELDS_SEP = ",";

  protected transient volatile SparkRowConverter rowConverter;
  protected transient volatile SparkRowAccessor rowAccessor;

  protected transient volatile StringPartitionPathFormatter stringPartitionPathFormatter;
  protected transient volatile UTF8StringPartitionPathFormatter utf8StringPartitionPathFormatter;

  protected transient volatile SparkRecordKeyGenerator sparkRecordKeyGenerator;

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
          this.rowAccessor = new SparkRowAccessor(schema, getRecordKeyFieldNames(), getPartitionPathFields());
          this.sparkRecordKeyGenerator = SparkRecordKeyGeneratorFactory.getSparkRecordKeyGenerator(config,
              rowAccessor, recordKeyFields);
        }
      }
    }
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final String combinePartitionPath(Object... partitionPathParts) {
    return getStringPartitionPathFormatter().combine(partitionPathFields, partitionPathParts);
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final UTF8String combinePartitionPathUnsafe(Object... partitionPathParts) {
    return getUTF8StringPartitionPathFormatter().combine(partitionPathFields, partitionPathParts);
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final String combineRecordKey(List<String> fieldNames, List<Object> recordKeyParts) {
    return combineRecordKeyInternal(
        StringPartitionPathFormatter.JavaStringBuilder::new,
        SparkRowUtils::toString,
        SparkRowUtils::handleNullRecordKey,
        fieldNames,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final UTF8String combineRecordKeyUnsafe(List<String> fieldNames, List<Object> recordKeyParts) {
    return combineRecordKeyInternal(
        UTF8StringPartitionPathFormatter.UTF8StringBuilder::new,
        SparkRowUtils::toUTF8String,
        SparkRowUtils::handleNullRecordKey,
        fieldNames,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final String combineCompositeRecordKey(Object... recordKeyParts) {
    return combineCompositeRecordKeyInternal(
        StringPartitionPathFormatter.JavaStringBuilder::new,
        SparkRowUtils::toString,
        SparkRowUtils::handleNullOrEmptyCompositeKeyPart,
        SparkRowUtils::isNullOrEmptyCompositeKeyPart,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   * optimizations, like inlining)
   */
  protected final UTF8String combineCompositeRecordKeyUnsafe(Object... recordKeyParts) {
    return combineCompositeRecordKeyInternal(
        UTF8StringPartitionPathFormatter.UTF8StringBuilder::new,
        SparkRowUtils::toUTF8String,
        SparkRowUtils::handleNullOrEmptyCompositeKeyPartUTF8,
        SparkRowUtils::isNullOrEmptyCompositeKeyPartUTF8,
        recordKeyParts
    );
  }

  private <S> S combineRecordKeyInternal(
      Supplier<PartitionPathFormatterBase.StringBuilder<S>> builderFactory,
      Function<Object, S> converter,
      Function<S, S> emptyKeyPartHandler,
      List<String> fieldNames,
      List<Object> recordKeyParts
  ) {
    if (recordKeyParts.size() == 1) {
      return emptyKeyPartHandler.apply(converter.apply(recordKeyParts.get(0)));
    }

    PartitionPathFormatterBase.StringBuilder<S> sb = builderFactory.get();
    for (int i = 0; i < recordKeyParts.size(); ++i) {
      sb.appendJava(fieldNames.get(i)).appendJava(DEFAULT_COMPOSITE_KEY_FILED_VALUE);
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      sb.append(emptyKeyPartHandler.apply(converter.apply(recordKeyParts.get(i))));

      if (i < recordKeyParts.size() - 1) {
        sb.appendJava(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.build();
  }

  private <S> S combineCompositeRecordKeyInternal(
      Supplier<PartitionPathFormatterBase.StringBuilder<S>> builderFactory,
      Function<Object, S> converter,
      Function<S, S> emptyKeyPartHandler,
      Predicate<S> isNullOrEmptyKeyPartPredicate,
      Object... recordKeyParts
  ) {
    boolean hasNonNullNonEmptyPart = false;

    PartitionPathFormatterBase.StringBuilder<S> sb = builderFactory.get();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      S convertedKeyPart = emptyKeyPartHandler.apply(converter.apply(recordKeyParts[i]));

      sb.appendJava(recordKeyFields.get(i));
      sb.appendJava(COMPOSITE_KEY_FIELD_VALUE_INFIX);
      sb.append(convertedKeyPart);
      // This check is to validate that overall composite-key has at least one non-null, non-empty
      // segment
      hasNonNullNonEmptyPart |= !isNullOrEmptyKeyPartPredicate.test(convertedKeyPart);

      if (i < recordKeyParts.length - 1) {
        sb.appendJava(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    if (hasNonNullNonEmptyPart) {
      return sb.build();
    } else {
      throw new HoodieKeyException(String.format("All of the values for (%s) were either null or empty", recordKeyFields));
    }
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

  private StringPartitionPathFormatter getStringPartitionPathFormatter() {
    if (stringPartitionPathFormatter == null) {
      synchronized (this) {
        if (stringPartitionPathFormatter == null) {
          this.stringPartitionPathFormatter = new StringPartitionPathFormatter(
              StringPartitionPathFormatter.JavaStringBuilder::new, hiveStylePartitioning, encodePartitionPath);
        }
      }
    }

    return stringPartitionPathFormatter;
  }

  private UTF8StringPartitionPathFormatter getUTF8StringPartitionPathFormatter() {
    if (utf8StringPartitionPathFormatter == null) {
      synchronized (this) {
        if (utf8StringPartitionPathFormatter == null) {
          this.utf8StringPartitionPathFormatter = new UTF8StringPartitionPathFormatter(
              UTF8StringPartitionPathFormatter.UTF8StringBuilder::new, hiveStylePartitioning, encodePartitionPath);
        }
      }
    }

    return utf8StringPartitionPathFormatter;
  }

  protected static class SparkRowConverter {
    private static final String STRUCT_NAME = "hoodieRowTopLevelField";
    private static final String NAMESPACE = "hoodieRow";

    private final Function1<Row, GenericRecord> avroConverter;
    private final SparkRowSerDe rowSerDe;

    SparkRowConverter(StructType schema) {
      this.rowSerDe = HoodieSparkUtils.getCatalystRowSerDe(schema);
      this.avroConverter = AvroConversionUtils.createConverterToAvro(schema, STRUCT_NAME, NAMESPACE);
    }

    GenericRecord convertToAvro(Row row) {
      return avroConverter.apply(row);
    }

    GenericRecord convertToAvro(InternalRow row) {
      return avroConverter.apply(rowSerDe.deserializeRow(row));
    }
  }
}

