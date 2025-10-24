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

import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class of Hudi's custom {@link ParquetWriter} implementations
 *
 * @param <R> target type of the object being written into Parquet files (for ex,
 *            {@code IndexedRecord}, {@code InternalRow})
 */
public abstract class HoodieBaseParquetWriter<R> implements Closeable {

  private final AtomicLong writtenRecordCount = new AtomicLong(0);
  private final long maxFileSize;
  private long recordCountForNextSizeCheck;
  private final ParquetWriter parquetWriter;
  public static final String BLOOM_FILTER_EXPECTED_NDV = "parquet.bloom.filter.expected.ndv";
  public static final String BLOOM_FILTER_ENABLED = "parquet.bloom.filter.enabled";

  public HoodieBaseParquetWriter(StoragePath file,
                                 HoodieParquetConfig<? extends WriteSupport<R>> parquetConfig) throws IOException {
    Configuration hadoopConf = parquetConfig.getStorageConf().unwrapAs(Configuration.class);
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
    parquetWriterbuilder.withDictionaryEncoding(parquetConfig.dictionaryEnabled());
    parquetWriterbuilder.withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);
    parquetWriterbuilder.withWriterVersion(ParquetWriter.DEFAULT_WRITER_VERSION);
    parquetWriterbuilder.withConf(HadoopFSUtils.registerFileSystem(file, hadoopConf));
    handleParquetBloomFilters(parquetWriterbuilder, hadoopConf);

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

  public boolean canWrite() {
    long writtenCount = getWrittenRecordCount();
    if (writtenCount >= recordCountForNextSizeCheck) {
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
}
