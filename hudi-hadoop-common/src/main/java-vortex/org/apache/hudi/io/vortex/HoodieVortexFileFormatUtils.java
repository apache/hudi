/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.vortex;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VortexUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.metadata.stats.ValueMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import dev.vortex.api.VortexColumnStatistics;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * {@link VortexUtils} that reads the Vortex footer with vortex-jni: the metadata segments, the row
 * count and the column statistics.
 *
 * <p>{@code HoodieHadoopIOFactory} loads this class by name, because only a JDK 17 build
 * compiles it.
 */
public class HoodieVortexFileFormatUtils extends VortexUtils {

  @Override
  public Map<String, String> readFooter(HoodieStorage storage, boolean required, StoragePath filePath,
                                        String... footerNames) {
    Map<String, byte[]> metadata;
    try (VortexInputFile file = open(storage, filePath)) {
      metadata = file.readMetadata();
    } catch (HoodieException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieException("Failed to read the footer of Vortex file " + filePath, e);
    }

    Map<String, String> values = new HashMap<>();
    for (String footerName : footerNames) {
      byte[] value = metadata.get(footerName);
      if (value != null) {
        values.put(footerName, StringUtils.fromUTF8Bytes(value));
      } else if (required) {
        throw new MetadataNotFoundException(
            "Could not find key " + footerName + " in the footer of Vortex file " + filePath);
      }
    }
    return values;
  }

  @Override
  public long getRowCount(HoodieStorage storage, StoragePath filePath) {
    try (VortexInputFile file = open(storage, filePath)) {
      return file.exactRowCount().orElseThrow(
          () -> new HoodieException("Vortex row count unavailable for file: " + filePath));
    }
  }

  /**
   * Returns the statistics of the requested top-level columns, from the footer of the file.
   *
   * <p>A column has no statistics in these cases:
   * <ul>
   *   <li>The column is nested, for example {@code a.b}. Vortex keeps file statistics for
   *   top-level columns only.</li>
   *   <li>Hudi has no value type for the Arrow type of the column.</li>
   *   <li>The footer has no null count, or the column has values but no bounds.</li>
   *   <li>The column is a floating-point column that has NaN values, or that has no NaN count.
   *   The bounds do not include NaN, and Spark orders NaN after all other values.</li>
   * </ul>
   * The bounds can be wider than the values. A wider bound is safe for data skipping.
   */
  @Override
  public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage,
                                                                                  StoragePath filePath,
                                                                                  List<String> columnList,
                                                                                  HoodieIndexVersion indexVersion) {
    Set<String> requested = columnList == null ? null : new HashSet<>(columnList);
    List<HoodieColumnRangeMetadata<Comparable>> result = new ArrayList<>();

    try (VortexInputFile file = open(storage, filePath);
         BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
             getClass().getSimpleName() + "-" + filePath.getName(),
             Long.parseLong(HoodieStorageConfig.VORTEX_READ_METADATA_ALLOCATOR_SIZE_BYTES.defaultValue()))) {
      List<Field> fields = file.arrowSchema(allocator).getFields();

      for (VortexColumnStatistics statistics : file.readStatistics()) {
        Field field = fields.get(statistics.columnIndex());
        if (requested != null && !requested.contains(field.getName())) {
          continue;
        }

        HoodieSchema valueSchema = valueSchema(field.getType());
        if (valueSchema == null) {
          continue;
        }

        HoodieColumnRangeMetadata<Comparable> columnStats =
            toColumnRange(filePath.getName(), field, valueSchema, statistics, indexVersion);
        if (columnStats != null) {
          result.add(columnStats);
        }
      }
    } catch (HoodieException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieException("Failed to read column statistics of Vortex file " + filePath, e);
    }
    return result;
  }

  private static HoodieColumnRangeMetadata<Comparable> toColumnRange(String fileName,
                                                                     Field field,
                                                                     HoodieSchema valueSchema,
                                                                     VortexColumnStatistics statistics,
                                                                     HoodieIndexVersion indexVersion) {
    OptionalLong nullCount = statistics.nullValueCount();
    if (!nullCount.isPresent()) {
      return null;
    }

    long valueCount = statistics.valueCount();
    boolean allNull = nullCount.getAsLong() >= valueCount;
    Object lower = statistics.lowerBound().orElse(null);
    Object upper = statistics.upperBound().orElse(null);
    if (!allNull && (lower == null || upper == null)) {
      return null;
    }

    if (isFloatingPoint(valueSchema)) {
      OptionalLong nanCount = statistics.nanValueCount();
      if (!nanCount.isPresent() || nanCount.getAsLong() > 0) {
        return null;
      }
    }

    ValueMetadata valueMetadata = ValueMetadata.getValueMetadata(valueSchema, indexVersion);
    Comparable min = allNull ? null : toJavaValue(lower, valueSchema, valueMetadata);
    Comparable max = allNull ? null : toJavaValue(upper, valueSchema, valueMetadata);

    // The footer gives only the compressed size of the column.
    long size = statistics.compressedSize();
    return HoodieColumnRangeMetadata.<Comparable>create(
        fileName, field.getName(), min, max, nullCount.getAsLong(), valueCount, size, size, valueMetadata);
  }

  /**
   * Converts a bound from Vortex to the value that the column stats index keeps. Index version
   * V1 keeps the values that the Parquet footer gives. Later versions standardize the value for
   * the type of the column.
   */
  private static Comparable toJavaValue(Object value, HoodieSchema valueSchema, ValueMetadata valueMetadata) {
    if (!valueMetadata.isV1()) {
      return valueMetadata.standardizeJavaTypeAndPromote(value);
    }

    switch (valueSchema.getType()) {
      case DATE:
        return java.sql.Date.valueOf(LocalDate.ofEpochDay(((Number) value).longValue()));
      case BYTES:
        return ByteBuffer.wrap((byte[]) value);
      default:
        return value instanceof BigInteger ? ((BigInteger) value).longValueExact() : (Comparable) value;
    }
  }

  private static boolean isFloatingPoint(HoodieSchema valueSchema) {
    return valueSchema.getType() == HoodieSchemaType.FLOAT || valueSchema.getType() == HoodieSchemaType.DOUBLE;
  }

  /**
   * Returns the schema of the values of an Arrow type, or null when the column stats index has no
   * type for it.
   */
  private static HoodieSchema valueSchema(ArrowType type) {
    switch (type.getTypeID()) {
      case Bool:
        return HoodieSchema.create(HoodieSchemaType.BOOLEAN);
      case Int: {
        ArrowType.Int intType = (ArrowType.Int) type;
        if (!intType.getIsSigned()) {
          return null;
        }
        return intType.getBitWidth() == 64
            ? HoodieSchema.create(HoodieSchemaType.LONG)
            : HoodieSchema.create(HoodieSchemaType.INT);
      }
      case FloatingPoint: {
        FloatingPointPrecision precision = ((ArrowType.FloatingPoint) type).getPrecision();
        if (precision == FloatingPointPrecision.SINGLE) {
          return HoodieSchema.create(HoodieSchemaType.FLOAT);
        }
        return precision == FloatingPointPrecision.DOUBLE ? HoodieSchema.create(HoodieSchemaType.DOUBLE) : null;
      }
      case Utf8:
      case LargeUtf8:
      case Utf8View:
        return HoodieSchema.create(HoodieSchemaType.STRING);
      case Binary:
      case LargeBinary:
      case BinaryView:
        return HoodieSchema.create(HoodieSchemaType.BYTES);
      case Decimal: {
        ArrowType.Decimal decimal = (ArrowType.Decimal) type;
        return HoodieSchema.createDecimal(decimal.getPrecision(), decimal.getScale());
      }
      case Date:
        return ((ArrowType.Date) type).getUnit() == DateUnit.DAY ? HoodieSchema.createDate() : null;
      case Timestamp: {
        ArrowType.Timestamp timestamp = (ArrowType.Timestamp) type;
        boolean utcAdjusted = timestamp.getTimezone() != null;
        if (timestamp.getUnit() == TimeUnit.MICROSECOND) {
          return utcAdjusted ? HoodieSchema.createTimestampMicros() : HoodieSchema.createLocalTimestampMicros();
        }
        if (timestamp.getUnit() == TimeUnit.MILLISECOND) {
          return utcAdjusted ? HoodieSchema.createTimestampMillis() : HoodieSchema.createLocalTimestampMillis();
        }
        return null;
      }
      default:
        return null;
    }
  }

  private static VortexInputFile open(HoodieStorage storage, StoragePath filePath) {
    return VortexInputFile.open(storage.getConf().unwrapAs(Configuration.class), filePath.toString());
  }
}
