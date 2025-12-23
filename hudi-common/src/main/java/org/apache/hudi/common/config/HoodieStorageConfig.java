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

package org.apache.hudi.common.config;

import org.apache.hudi.common.bloom.BloomFilterTypeCode;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Storage related config.
 */
@Immutable
@ConfigClassProperty(name = "Storage Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control aspects around writing, sizing, reading base and log files.")
public class HoodieStorageConfig extends HoodieConfig {

  public static final ConfigProperty<String> PARQUET_MAX_FILE_SIZE = ConfigProperty
      .key("hoodie.parquet.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDocumentation("Target size in bytes for parquet files produced by Hudi write phases. "
          + "For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.");

  public static final ConfigProperty<String> PARQUET_BLOCK_SIZE = ConfigProperty
      .key("hoodie.parquet.block.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .markAdvanced()
      .withDocumentation("Parquet RowGroup size in bytes. It's recommended to make this large enough that scan costs can be"
          + " amortized by packing enough column values into a single row group.");

  public static final ConfigProperty<String> PARQUET_PAGE_SIZE = ConfigProperty
      .key("hoodie.parquet.page.size")
      .defaultValue(String.valueOf(1024 * 1024))
      .markAdvanced()
      .withDocumentation("Parquet page size in bytes. Page is the unit of read within a parquet file. "
          + "Within a block, pages are compressed separately.");

  public static final ConfigProperty<String> ORC_FILE_MAX_SIZE = ConfigProperty
      .key("hoodie.orc.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .markAdvanced()
      .withDocumentation("Target file size in bytes for ORC base files.");

  public static final ConfigProperty<String> ORC_STRIPE_SIZE = ConfigProperty
      .key("hoodie.orc.stripe.size")
      .defaultValue(String.valueOf(64 * 1024 * 1024))
      .markAdvanced()
      .withDocumentation("Size of the memory buffer in bytes for writing");

  public static final ConfigProperty<String> ORC_BLOCK_SIZE = ConfigProperty
      .key("hoodie.orc.block.size")
      .defaultValue(ORC_FILE_MAX_SIZE.defaultValue())
      .markAdvanced()
      .withDocumentation("ORC block size, recommended to be aligned with the target file size.");

  public static final ConfigProperty<String> HFILE_MAX_FILE_SIZE = ConfigProperty
      .key("hoodie.hfile.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .markAdvanced()
      .withDocumentation("Target file size in bytes for HFile base files.");

  public static final ConfigProperty<String> HFILE_BLOCK_SIZE = ConfigProperty
      .key("hoodie.hfile.block.size")
      .defaultValue(String.valueOf(1024 * 1024))
      .markAdvanced()
      .withDocumentation("Lower values increase the size in bytes of metadata tracked within HFile, but can offer potentially "
          + "faster lookup times.");

  public static final ConfigProperty<String> LOGFILE_DATA_BLOCK_FORMAT = ConfigProperty
      .key("hoodie.logfile.data.block.format")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Format of the data block within delta logs. Following formats are currently supported \"avro\", \"hfile\", \"parquet\"");

  public static final ConfigProperty<String> LOGFILE_MAX_SIZE = ConfigProperty
      .key("hoodie.logfile.max.size")
      .defaultValue(String.valueOf(1024 * 1024 * 1024)) // 1 GB
      .markAdvanced()
      .withDocumentation("LogFile max size in bytes. This is the maximum size allowed for a log file "
          + "before it is rolled over to the next version.");

  public static final ConfigProperty<String> LOGFILE_DATA_BLOCK_MAX_SIZE = ConfigProperty
      .key("hoodie.logfile.data.block.max.size")
      .defaultValue(String.valueOf(256 * 1024 * 1024))
      .markAdvanced()
      .withDocumentation("LogFile Data block max size in bytes. This is the maximum size allowed for a single data block "
          + "to be appended to a log file. This helps to make sure the data appended to the log file is broken up "
          + "into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.");

  public static final ConfigProperty<String> PARQUET_COMPRESSION_RATIO_FRACTION = ConfigProperty
      .key("hoodie.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.1))
      .markAdvanced()
      .withDocumentation("Expected compression of parquet data used by Hudi, when it tries to size new parquet files. "
          + "Increase this value, if bulk_insert is producing smaller than expected sized files");

  // Default compression codec for parquet
  public static final ConfigProperty<String> PARQUET_COMPRESSION_CODEC_NAME = ConfigProperty
      .key("hoodie.parquet.compression.codec")
      .defaultValue("gzip")
      .withDocumentation("Compression Codec for parquet files");

  public static final ConfigProperty<Boolean> PARQUET_DICTIONARY_ENABLED = ConfigProperty
      .key("hoodie.parquet.dictionary.enabled")
      .defaultValue(true)
      .markAdvanced()
      .withDocumentation("Whether to use dictionary encoding");

  public static final ConfigProperty<String> PARQUET_WRITE_LEGACY_FORMAT_ENABLED = ConfigProperty
      .key("hoodie.parquet.writelegacyformat.enabled")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Sets spark.sql.parquet.writeLegacyFormat. If true, data will be written in a way of Spark 1.4 and earlier. "
          + "For example, decimal values will be written in Parquet's fixed-length byte array format which other systems such as Apache Hive and Apache Impala use. "
          + "If false, the newer format in Parquet will be used. For example, decimals will be written in int-based format.");

  public static final ConfigProperty<String> PARQUET_OUTPUT_TIMESTAMP_TYPE = ConfigProperty
      .key("hoodie.parquet.outputtimestamptype")
      .defaultValue("TIMESTAMP_MICROS")
      .markAdvanced()
      .withDocumentation("Sets spark.sql.parquet.outputTimestampType. Parquet timestamp type to use when Spark writes data to Parquet files.");

  // SPARK-38094 Spark 3.3 checks if this field is enabled. Hudi has to provide this or there would be NPE thrown
  // Would ONLY be effective with Spark 3.3+
  // default value is true which is in accordance with Spark 3.3
  public static final ConfigProperty<String> PARQUET_FIELD_ID_WRITE_ENABLED = ConfigProperty
      .key("hoodie.parquet.field_id.write.enabled")
      .defaultValue("true")
      .markAdvanced()
      .sinceVersion("0.12.0")
      .withDocumentation("Would only be effective with Spark 3.3+. Sets spark.sql.parquet.fieldId.write.enabled. "
          + "If enabled, Spark will write out parquet native field ids that are stored inside StructField's metadata as parquet.field.id to parquet files.");

  public static final ConfigProperty<Boolean> PARQUET_WITH_BLOOM_FILTER_ENABLED = ConfigProperty
      .key("hoodie.parquet.bloom.filter.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Control whether to write bloom filter or not. Default true. "
          + "We can set to false in non bloom index cases for CPU resource saving.");

  public static final ConfigProperty<Boolean> WRITE_UTC_TIMEZONE = ConfigProperty
      .key("hoodie.parquet.write.utc-timezone.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Use UTC timezone or local timezone to the conversion between epoch"
              + " time and LocalDateTime. Default value is utc timezone for forward compatibility.");

  public static final ConfigProperty<String> HFILE_COMPRESSION_ALGORITHM_NAME = ConfigProperty
      .key("hoodie.hfile.compression.algorithm")
      .defaultValue("GZ")
      .markAdvanced()
      .withDocumentation("Compression codec to use for hfile base files.");

  public static final ConfigProperty<String> ORC_COMPRESSION_CODEC_NAME = ConfigProperty
      .key("hoodie.orc.compression.codec")
      .defaultValue("ZLIB")
      .markAdvanced()
      .withDocumentation("Compression codec to use for ORC base files.");

  // Default compression ratio for log file to parquet, general 3x
  public static final ConfigProperty<String> LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION = ConfigProperty
      .key("hoodie.logfile.to.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.35))
      .markAdvanced()
      .withDocumentation("Expected additional compression as records move from log files to parquet. Used for merge_on_read "
          + "table to send inserts into log files & control the size of compacted parquet file."
          + "When encoding log blocks in parquet format, increase this value for a more accurate estimation");

  // Configs that control the bloom filter that is written to the file footer
  public static final ConfigProperty<String> BLOOM_FILTER_TYPE = ConfigProperty
      .key("hoodie.bloom.index.filter.type")
      .defaultValue(BloomFilterTypeCode.DYNAMIC_V0.name())
      .withValidValues(BloomFilterTypeCode.SIMPLE.name(), BloomFilterTypeCode.DYNAMIC_V0.name())
      .markAdvanced()
      .withDocumentation(BloomFilterTypeCode.class);

  public static final ConfigProperty<String> BLOOM_FILTER_NUM_ENTRIES_VALUE = ConfigProperty
      .key("hoodie.index.bloom.num_entries")
      .defaultValue("60000")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM. "
          + "This is the number of entries to be stored in the bloom filter. "
          + "The rationale for the default: Assume the maxParquetFileSize is 128MB and averageRecordSize is 1kb and "
          + "hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. "
          + "Warning: Setting this very low, will generate a lot of false positives and index lookup "
          + "will have to scan a lot more files than it has to and setting this to a very high number will "
          + "increase the size every base file linearly (roughly 4KB for every 50000 entries). "
          + "This config is also used with DYNAMIC bloom filter which determines the initial size for the bloom.");

  public static final ConfigProperty<String> BLOOM_FILTER_FPP_VALUE = ConfigProperty
      .key("hoodie.index.bloom.fpp")
      .defaultValue("0.000000001")
      .markAdvanced()
      .withDocumentation("Only applies if index type is BLOOM. "
          + "Error rate allowed given the number of entries. This is used to calculate how many bits should be "
          + "assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), "
          + "we like to tradeoff disk space for lower false positives. "
          + "If the number of entries added to bloom filter exceeds the configured value (hoodie.index.bloom.num_entries), "
          + "then this fpp may not be honored.");

  public static final ConfigProperty<String> BLOOM_FILTER_DYNAMIC_MAX_ENTRIES = ConfigProperty
      .key("hoodie.bloom.index.filter.dynamic.max.entries")
      .defaultValue("100000")
      .markAdvanced()
      .withDocumentation("The threshold for the maximum number of keys to record in a dynamic Bloom filter row. "
          + "Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.");

  public static final ConfigProperty<String> HOODIE_AVRO_WRITE_SUPPORT_CLASS = ConfigProperty
      .key("hoodie.avro.write.support.class")
      .defaultValue("org.apache.hudi.avro.HoodieAvroWriteSupport")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Provided write support class should extend HoodieAvroWriteSupport class "
          + "and it is loaded at runtime. This is only required when trying to "
          + "override the existing write context.");

  public static final ConfigProperty<String> HOODIE_PARQUET_SPARK_ROW_WRITE_SUPPORT_CLASS = ConfigProperty
      .key("hoodie.parquet.spark.row.write.support.class")
      .defaultValue("org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport")
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Provided write support class should extend HoodieRowParquetWriteSupport class "
          + "and it is loaded at runtime. This is only required when trying to "
          + "override the existing write context when `hoodie.datasource.write.row.writer.enable=true`.");

  public static final ConfigProperty<String> HOODIE_PARQUET_FLINK_ROW_DATA_WRITE_SUPPORT_CLASS = ConfigProperty
      .key("hoodie.parquet.flink.rowdata.write.support.class")
      .defaultValue("org.apache.hudi.io.storage.row.HoodieRowDataParquetWriteSupport")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Provided write support class should extend HoodieRowDataParquetWriteSupport class "
          + "and it is loaded at runtime. This is only required when trying to override the existing write support.");

  public static final ConfigProperty<String> HOODIE_STORAGE_CLASS = ConfigProperty
      .key("hoodie.storage.class")
      .defaultValue("org.apache.hudi.storage.hadoop.HoodieHadoopStorage")
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("The fully-qualified class name of the `HoodieStorage` implementation class to instantiate. "
          + "The provided class should implement `org.apache.hudi.storage.HoodieStorage`");

  public static final ConfigProperty<String> HOODIE_IO_FACTORY_CLASS = ConfigProperty
      .key("hoodie.io.factory.class")
      .defaultValue("org.apache.hudi.io.storage.hadoop.HoodieHadoopIOFactory")
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("The fully-qualified class name of the factory class to return readers and writers of files used "
          + "by Hudi. The provided class should implement `org.apache.hudi.io.storage.HoodieIOFactory`.");

  /**
   * @deprecated Use {@link #PARQUET_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_FILE_MAX_BYTES = PARQUET_MAX_FILE_SIZE.key();
  /**
   * @deprecated Use {@link #PARQUET_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_FILE_MAX_BYTES = PARQUET_MAX_FILE_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_BLOCK_SIZE_BYTES = PARQUET_BLOCK_SIZE.key();
  /**
   * @deprecated Use {@link #PARQUET_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_BLOCK_SIZE_BYTES = PARQUET_BLOCK_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_PAGE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_PAGE_SIZE_BYTES = PARQUET_PAGE_SIZE.key();
  /**
   * @deprecated Use {@link #PARQUET_PAGE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_PAGE_SIZE_BYTES = PARQUET_PAGE_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String HFILE_FILE_MAX_BYTES = HFILE_MAX_FILE_SIZE.key();
  /**
   * @deprecated Use {@link #HFILE_MAX_FILE_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HFILE_FILE_MAX_BYTES = HFILE_MAX_FILE_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String HFILE_BLOCK_SIZE_BYTES = HFILE_BLOCK_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_BLOCK_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HFILE_BLOCK_SIZE_BYTES = HFILE_BLOCK_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #LOGFILE_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String LOGFILE_SIZE_MAX_BYTES = LOGFILE_MAX_SIZE.key();
  /**
   * @deprecated Use {@link #LOGFILE_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_LOGFILE_SIZE_MAX_BYTES = LOGFILE_MAX_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #LOGFILE_DATA_BLOCK_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = LOGFILE_DATA_BLOCK_MAX_SIZE.key();
  /**
   * @deprecated Use {@link #LOGFILE_DATA_BLOCK_MAX_SIZE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = LOGFILE_DATA_BLOCK_MAX_SIZE.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_COMPRESSION_RATIO = PARQUET_COMPRESSION_RATIO_FRACTION.key();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_STREAM_COMPRESSION_RATIO = PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_CODEC_NAME} and its methods instead
   */
  @Deprecated
  public static final String PARQUET_COMPRESSION_CODEC = PARQUET_COMPRESSION_CODEC_NAME.key();
  /**
   * @deprecated Use {@link #HFILE_COMPRESSION_ALGORITHM_NAME} and its methods instead
   */
  @Deprecated
  public static final String HFILE_COMPRESSION_ALGORITHM = HFILE_COMPRESSION_ALGORITHM_NAME.key();
  /**
   * @deprecated Use {@link #PARQUET_COMPRESSION_CODEC_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_PARQUET_COMPRESSION_CODEC = PARQUET_COMPRESSION_CODEC_NAME.defaultValue();
  /**
   * @deprecated Use {@link #HFILE_COMPRESSION_ALGORITHM_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_HFILE_COMPRESSION_ALGORITHM = HFILE_COMPRESSION_ALGORITHM_NAME.defaultValue();
  /**
   * @deprecated Use {@link #LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String LOGFILE_TO_PARQUET_COMPRESSION_RATIO = LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.key();
  /**
   * @deprecated Use {@link #LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_LOGFILE_TO_PARQUET_COMPRESSION_RATIO = LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue();

  private HoodieStorageConfig() {
    super();
  }

  public String getBloomFilterType() {
    return getString(BLOOM_FILTER_TYPE);
  }

  public static HoodieStorageConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieStorageConfig storageConfig = new HoodieStorageConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.storageConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.storageConfig.getProps().putAll(props);
      return this;
    }

    public Builder parquetMaxFileSize(long maxFileSize) {
      storageConfig.setValue(PARQUET_MAX_FILE_SIZE, String.valueOf(maxFileSize));
      return this;
    }

    public Builder parquetBlockSize(int blockSize) {
      storageConfig.setValue(PARQUET_BLOCK_SIZE, String.valueOf(blockSize));
      return this;
    }

    public Builder parquetPageSize(int pageSize) {
      storageConfig.setValue(PARQUET_PAGE_SIZE, String.valueOf(pageSize));
      return this;
    }

    public Builder hfileMaxFileSize(long maxFileSize) {
      storageConfig.setValue(HFILE_MAX_FILE_SIZE, String.valueOf(maxFileSize));
      return this;
    }

    public Builder hfileBlockSize(int blockSize) {
      storageConfig.setValue(HFILE_BLOCK_SIZE, String.valueOf(blockSize));
      return this;
    }

    public Builder logFileDataBlockFormat(String format) {
      storageConfig.setValue(LOGFILE_DATA_BLOCK_FORMAT, format);
      return this;
    }

    public Builder logFileDataBlockMaxSize(long dataBlockSize) {
      storageConfig.setValue(LOGFILE_DATA_BLOCK_MAX_SIZE, String.valueOf(dataBlockSize));
      return this;
    }

    public Builder logFileMaxSize(long logFileSize) {
      storageConfig.setValue(LOGFILE_MAX_SIZE, String.valueOf(logFileSize));
      return this;
    }

    public Builder parquetCompressionRatio(double parquetCompressionRatio) {
      storageConfig.setValue(PARQUET_COMPRESSION_RATIO_FRACTION, String.valueOf(parquetCompressionRatio));
      return this;
    }

    public Builder parquetCompressionCodec(String parquetCompressionCodec) {
      storageConfig.setValue(PARQUET_COMPRESSION_CODEC_NAME, parquetCompressionCodec);
      return this;
    }

    public Builder parquetWriteLegacyFormat(String parquetWriteLegacyFormat) {
      storageConfig.setValue(PARQUET_WRITE_LEGACY_FORMAT_ENABLED, parquetWriteLegacyFormat);
      return this;
    }

    public Builder parquetOutputTimestampType(String parquetOutputTimestampType) {
      storageConfig.setValue(PARQUET_OUTPUT_TIMESTAMP_TYPE, parquetOutputTimestampType);
      return this;
    }

    public Builder parquetFieldIdWrite(String parquetFieldIdWrite) {
      storageConfig.setValue(PARQUET_FIELD_ID_WRITE_ENABLED, parquetFieldIdWrite);
      return this;
    }

    public Builder parquetBloomFilterEnable(boolean parquetBloomFilterEnable) {
      storageConfig.setValue(PARQUET_WITH_BLOOM_FILTER_ENABLED, String.valueOf(parquetBloomFilterEnable));
      return this;
    }

    public Builder hfileCompressionAlgorithm(String hfileCompressionAlgorithm) {
      storageConfig.setValue(HFILE_COMPRESSION_ALGORITHM_NAME, hfileCompressionAlgorithm);
      return this;
    }

    public Builder logFileToParquetCompressionRatio(double logFileToParquetCompressionRatio) {
      storageConfig.setValue(LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION, String.valueOf(logFileToParquetCompressionRatio));
      return this;
    }

    public Builder orcMaxFileSize(long maxFileSize) {
      storageConfig.setValue(ORC_FILE_MAX_SIZE, String.valueOf(maxFileSize));
      return this;
    }

    public Builder orcStripeSize(int orcStripeSize) {
      storageConfig.setValue(ORC_STRIPE_SIZE, String.valueOf(orcStripeSize));
      return this;
    }

    public Builder orcBlockSize(int orcBlockSize) {
      storageConfig.setValue(ORC_BLOCK_SIZE, String.valueOf(orcBlockSize));
      return this;
    }

    public Builder orcCompressionCodec(String orcCompressionCodec) {
      storageConfig.setValue(ORC_COMPRESSION_CODEC_NAME, orcCompressionCodec);
      return this;
    }

    public Builder withAvroWriteSupport(String avroWriteSupportClassName) {
      storageConfig.setValue(HOODIE_AVRO_WRITE_SUPPORT_CLASS, avroWriteSupportClassName);
      return this;
    }

    public Builder withRowDataWriteSupport(String rowDataWriteSupportClassName) {
      storageConfig.setValue(HOODIE_PARQUET_FLINK_ROW_DATA_WRITE_SUPPORT_CLASS, rowDataWriteSupportClassName);
      return this;
    }

    public Builder withWriteUtcTimezone(boolean writeUtcTimezone) {
      storageConfig.setValue(WRITE_UTC_TIMEZONE, String.valueOf(writeUtcTimezone));
      return this;
    }

    /**
     * Sets the bloom filter type for the configuration.
     *
     * @param bloomFilterType The bloom filter type (SIMPLE or DYNAMIC_V0)
     * @return this builder instance for method chaining
     */
    public Builder withBloomFilterType(String bloomFilterType) {
      storageConfig.setValue(BLOOM_FILTER_TYPE, bloomFilterType);
      return this;
    }

    /**
     * Sets the number of entries to be stored in the bloom filter.
     *
     * @param numEntries The number of entries for the bloom filter
     * @return this builder instance for method chaining
     */
    public Builder withBloomFilterNumEntries(int numEntries) {
      storageConfig.setValue(BLOOM_FILTER_NUM_ENTRIES_VALUE, String.valueOf(numEntries));
      return this;
    }

    /**
     * Sets the false positive probability (FPP) for the bloom filter.
     *
     * @param fpp The false positive probability as a double
     * @return this builder instance for method chaining
     */
    public Builder withBloomFilterFpp(double fpp) {
      storageConfig.setValue(BLOOM_FILTER_FPP_VALUE, String.valueOf(fpp));
      return this;
    }

    /**
     * Sets the maximum number of entries for dynamic bloom filter.
     *
     * @param maxEntries The maximum number of entries for dynamic bloom filter
     * @return this builder instance for method chaining
     */
    public Builder withBloomFilterDynamicMaxEntries(int maxEntries) {
      storageConfig.setValue(BLOOM_FILTER_DYNAMIC_MAX_ENTRIES, String.valueOf(maxEntries));
      return this;
    }

    public HoodieStorageConfig build() {
      storageConfig.setDefaults(HoodieStorageConfig.class.getName());
      return storageConfig;
    }
  }
}
