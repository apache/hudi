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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class of Hudi's custom {@link ParquetWriter} implementations
 *
 * @param <R> target type of the object being written into Parquet files (for ex,
 *            {@code IndexedRecord}, {@code InternalRow})
 */
@Slf4j
public abstract class HoodieBaseParquetWriter<R> implements Closeable {

  private final AtomicLong writtenRecordCount = new AtomicLong(0);
  private final long maxFileSize;
  private final boolean sizeCheckEveryRecord;
  private long recordCountForNextSizeCheck;
  private final ParquetWriter parquetWriter;
  public static final String BLOOM_FILTER_EXPECTED_NDV = "parquet.bloom.filter.expected.ndv";
  public static final String BLOOM_FILTER_ENABLED = "parquet.bloom.filter.enabled";
  /**
   * Internal key for Parquet's per-column statistics switch ({@code ParquetOutputFormat.STATISTICS_ENABLED}), set as
   * {@code hoodie.parquet.internal.column.statistics.enabled#<dotted column path>=false} by a writer that needs it (for
   * example through a parquet config injector); Parquet's own generic key is not read, so an ordinary writer is built as upstream builds it. Parquet keeps a full copy of the min and max of every
   * finished row group's column chunk until the file closes; for a column of large binary values (inline blobs) that copy is
   * the writer's largest live term, so such a column turns statistics off. Applied only by Parquet 1.15 or later, which has the
   * builder method; on an older Parquet a key asking for {@code false} is refused, so a caller never believes statistics are off when they
   * are not.
   */
  public static final String COLUMN_STATISTICS_ENABLED = "hoodie.parquet.internal.column.statistics.enabled";
  /**
   * Internal key for Parquet's dictionary switch ({@code ParquetOutputFormat.ENABLE_DICTIONARY}), honoured here per column as
   * {@code hoodie.parquet.internal.column.dictionary.enabled#<dotted column path>=false}: that column is written PLAIN (writer
   * version 1) while the others keep the file's dictionary setting. Parquet's own generic key is not read. Large binary values (inline blobs) never repeat, so a dictionary only adds a
   * fallback for them.
   */
  public static final String COLUMN_DICTIONARY_ENABLED = "hoodie.parquet.internal.column.dictionary.enabled";
  /**
   * Internal key for the minimum row count between Parquet's page size checks
   * ({@code ParquetOutputFormat.MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK}); set by a writer that needs a finer cadence (for example
   * through a parquet config injector). Parquet's own generic key is not read.
   */
  public static final String INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK = "hoodie.parquet.internal.page.size.row.check.min";
  /** Internal key for the maximum row count between Parquet's page size checks; see {@link #INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK}. */
  public static final String INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK = "hoodie.parquet.internal.page.size.row.check.max";
  private static final Option<Method> COLUMN_STATISTICS_SWITCH = columnStatisticsSwitch();
  /**
   * {@code ParquetWriter.Builder#withMinRowCountForPageSizeCheck(int)} and {@code #withMaxRowCountForPageSizeCheck(int)}, looked
   * up once; empty before Parquet 1.12.3 (the Spark 3.3 build), where an explicit page-check cadence key is skipped and Parquet
   * keeps its default cadence (warned once per setter).
   */
  private static final Option<Method> MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK_SETTER = builderIntSetter("withMinRowCountForPageSizeCheck");
  private static final Option<Method> MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK_SETTER = builderIntSetter("withMaxRowCountForPageSizeCheck");
  private static final AtomicBoolean MIN_ROW_COUNT_SKIP_WARNED = new AtomicBoolean(false);
  private static final AtomicBoolean MAX_ROW_COUNT_SKIP_WARNED = new AtomicBoolean(false);
  /**
   * Internal opt-in: {@link #canWrite()} checks the file size on every record instead of first after 100 records, so a file of
   * large rows (inline blobs) rolls near its size target; set by a writer that needs it (for example through a parquet config injector).
   */
  public static final String INTERNAL_SIZE_CHECK_EVERY_RECORD = "hoodie.parquet.internal.size.check.every.record";

  public HoodieBaseParquetWriter(StoragePath file,
                                 HoodieParquetConfig<? extends WriteSupport<R>> parquetConfig) throws IOException {
    Configuration hadoopConf = parquetConfig.getStorageConf().unwrapAs(Configuration.class);
    this.sizeCheckEveryRecord = hadoopConf.getBoolean(INTERNAL_SIZE_CHECK_EVERY_RECORD, false);
    ParquetWriter.Builder parquetWriterbuilder = new ParquetWriter.Builder(
        HoodieWrapperFileSystem.convertToHoodiePath(file, hadoopConf)) {
      @Override
      protected ParquetWriter.Builder self() {
        return this;
      }

      @Override
      protected WriteSupport getWriteSupport(Configuration conf) {
        return parquetConfig.getWriteSupport();
      }
    };

    parquetWriterbuilder.withWriteMode(ParquetFileWriter.Mode.CREATE);
    parquetWriterbuilder.withCompressionCodec(parquetConfig.getCompressionCodecName());
    parquetWriterbuilder.withRowGroupSize(parquetConfig.getBlockSize());
    parquetWriterbuilder.withPageSize(parquetConfig.getPageSize());
    parquetWriterbuilder.withDictionaryPageSize(parquetConfig.getPageSize());
    parquetWriterbuilder.withDictionaryEncoding(parquetConfig.isDictionaryEnabled());
    parquetWriterbuilder.withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);
    parquetWriterbuilder.withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION);
    parquetWriterbuilder.withConf(HadoopFSUtils.registerFileSystem(file, hadoopConf));
    // withConf does not populate the builder's encoding properties. Explicit overrides also control
    // row-group memory checks; leave ordinary writers on Parquet's default cadence.
    applyPageSizeCheckRowCount(parquetWriterbuilder, hadoopConf, INTERNAL_MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK,
        MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK_SETTER, MIN_ROW_COUNT_SKIP_WARNED, ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK);
    applyPageSizeCheckRowCount(parquetWriterbuilder, hadoopConf, INTERNAL_MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK,
        MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK_SETTER, MAX_ROW_COUNT_SKIP_WARNED, ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK);
    handleParquetBloomFilters(parquetWriterbuilder, hadoopConf);
    handleColumnSettings(parquetWriterbuilder, hadoopConf);

    parquetWriter = parquetWriterbuilder.build();
    // We cannot accurately measure the snappy compressed output file size. We are choosing a
    // conservative 10%
    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
    // stream and the actual file size reported by HDFS
    this.maxFileSize = parquetConfig.getMaxFileSize()
        + Math.round(parquetConfig.getMaxFileSize() * parquetConfig.getCompressionRatio());
    this.recordCountForNextSizeCheck = ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK;
  }

  /**
   * Once we get parquet version >= 1.12 among all engines we can cleanup the reflexion hack.
   *
   * @param parquetWriterbuilder
   * @param hadoopConf
   */
  protected void handleParquetBloomFilters(ParquetWriter.Builder parquetWriterbuilder, Configuration hadoopConf) {
    // inspired from https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetOutputFormat.java#L458-L464
    hadoopConf.forEach(conf -> {
      String key = conf.getKey();
      if (key.startsWith(BLOOM_FILTER_ENABLED)) {
        String column = key.substring(BLOOM_FILTER_ENABLED.length() + 1);
        try {
          Method method = parquetWriterbuilder.getClass().getMethod("withBloomFilterEnabled", String.class, boolean.class);
          method.invoke(parquetWriterbuilder, column, Boolean.valueOf(conf.getValue()).booleanValue());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          // skip
        }
      }
      if (key.startsWith(BLOOM_FILTER_EXPECTED_NDV)) {
        String column = key.substring(BLOOM_FILTER_EXPECTED_NDV.length() + 1);
        try {
          Method method = parquetWriterbuilder.getClass().getMethod("withBloomFilterNDV", String.class, long.class);
          method.invoke(parquetWriterbuilder, column, Long.valueOf(conf.getValue()).longValue());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          // skip
        }
      }
    });
  }

  /**
   * Whether this Parquet can turn statistics off for one column ({@code ParquetWriter.Builder#withStatisticsEnabled(String,
   * boolean)}, Parquet 1.15 or later).
   */
  public static boolean supportsColumnStatisticsSwitch() {
    return COLUMN_STATISTICS_SWITCH.isPresent();
  }

  /** {@code ParquetWriter.Builder#withStatisticsEnabled(String, boolean)}, looked up once; empty before Parquet 1.15. */
  private static Option<Method> columnStatisticsSwitch() {
    try {
      return Option.of(ParquetWriter.Builder.class.getMethod("withStatisticsEnabled", String.class, boolean.class));
    } catch (NoSuchMethodException e) {
      return Option.empty();
    }
  }

  /** A {@code ParquetWriter.Builder} method taking one {@code int}, looked up once; empty when this Parquet has none. */
  private static Option<Method> builderIntSetter(String name) {
    try {
      return Option.of(ParquetWriter.Builder.class.getMethod(name, int.class));
    } catch (NoSuchMethodException e) {
      return Option.empty();
    }
  }

  /**
   * Applies {@code key} of {@code hadoopConf} to the builder when it is set and this Parquet has the setter; skips it otherwise,
   * with a warning logged once per JVM for that setter.
   */
  private static void applyPageSizeCheckRowCount(ParquetWriter.Builder parquetWriterbuilder, Configuration hadoopConf, String key,
                                                 Option<Method> setter, AtomicBoolean skipWarned, int defaultValue) {
    if (hadoopConf.get(key) == null) {
      return;
    }
    if (!setter.isPresent()) {
      if (skipWarned.compareAndSet(false, true)) {
        log.warn("{} is set but the Parquet version on the classpath has no ParquetWriter.Builder setter for it (Parquet 1.12.3 or later); "
            + "the page-check cadence is not applied", key);
      }
      return;
    }
    try {
      setter.get().invoke(parquetWriterbuilder, hadoopConf.getInt(key, defaultValue));
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException("Cannot apply " + key + "=" + hadoopConf.get(key), e);
    }
  }

  /**
   * Applies every {@link #COLUMN_STATISTICS_ENABLED}{@code #<column>} and {@link #COLUMN_DICTIONARY_ENABLED}{@code #<column>}
   * key of {@code hadoopConf} to the builder; {@code withConf} does not populate the builder's encoding properties.
   *
   * @throws HoodieNotSupportedException when a statistics key asks for {@code false} and this Parquet has no per-column
   *     statistics switch
   */
  private static void handleColumnSettings(ParquetWriter.Builder parquetWriterbuilder, Configuration hadoopConf) {
    String statisticsPrefix = COLUMN_STATISTICS_ENABLED + "#";
    String dictionaryPrefix = COLUMN_DICTIONARY_ENABLED + "#";
    for (Map.Entry<String, String> conf : hadoopConf) {
      if (conf.getKey().startsWith(dictionaryPrefix)) {
        parquetWriterbuilder.withDictionaryEncoding(conf.getKey().substring(dictionaryPrefix.length()),
            Boolean.parseBoolean(conf.getValue()));
      } else if (conf.getKey().startsWith(statisticsPrefix)) {
        applyColumnStatistics(parquetWriterbuilder, conf, conf.getKey().substring(statisticsPrefix.length()));
      }
    }
  }

  /** Applies one {@link #COLUMN_STATISTICS_ENABLED}{@code #<column>} key; see {@link #handleColumnSettings}. */
  private static void applyColumnStatistics(ParquetWriter.Builder parquetWriterbuilder, Map.Entry<String, String> conf, String column) {
    boolean enabled = Boolean.parseBoolean(conf.getValue());
    if (!COLUMN_STATISTICS_SWITCH.isPresent()) {
      if (enabled) {
        // what an older Parquet does anyway
        return;
      }
      throw new HoodieNotSupportedException(conf.getKey() + "=false needs Parquet 1.15 or later, which has per-column "
          + "statistics; this Parquet has none");
    }
    try {
      COLUMN_STATISTICS_SWITCH.get().invoke(parquetWriterbuilder, column, enabled);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException("Cannot apply " + conf.getKey() + "=" + conf.getValue(), e);
    }
  }

  public boolean canWrite() {
    long writtenCount = getWrittenRecordCount();
    // An empty handle always admits one whole row, even when the caller's file target is smaller.
    // The opt-in path checks file bytes on each call; ordinary writers keep their cadence.
    if (writtenCount > 0 && (sizeCheckEveryRecord || writtenCount >= recordCountForNextSizeCheck)) {
      long dataSize = getDataSize();
      // In some very extreme cases, like all records are same value, then it's possible
      // the dataSize is much lower than the writtenRecordCount(high compression ratio),
      // causing avgRecordSize to 0, we'll force the avgRecordSize to 1 for such cases.
      long avgRecordSize = Math.max(dataSize / writtenCount, 1);
      // Follow the parquet block size check logic here, return false
      // if it is within ~2 records of the limit
      if (dataSize > (maxFileSize - avgRecordSize * 2)) {
        return false;
      }
      recordCountForNextSizeCheck = writtenCount + Math.min(
          // Do check it in the halfway
          Math.max(ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK, (maxFileSize / avgRecordSize - writtenCount) / 2),
          ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK);
    }
    return true;
  }

  public long getDataSize() {
    return this.parquetWriter.getDataSize();
  }

  public void write(R object) throws IOException {
    this.parquetWriter.write(object);
    writtenRecordCount.incrementAndGet();
  }

  private static boolean isRequiredFieldNullError(String errorMessage) {
    return errorMessage.contains("null") && errorMessage.contains("required");
  }

  protected long getWrittenRecordCount() {
    return writtenRecordCount.get();
  }

  @VisibleForTesting
  protected long getRecordCountForNextSizeCheck() {
    return recordCountForNextSizeCheck;
  }

  @Override
  public void close() throws IOException {
    this.parquetWriter.close();
  }

  public Object getFileFormatMetadata() {
    return this.parquetWriter.getFooter();
  }
}
