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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDuplicateKeyException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.hfile.HFileContext;
import org.apache.hudi.io.hfile.HFileWriter;
import org.apache.hudi.io.hfile.HFileWriterImpl;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * HoodieHFileWriter writes IndexedRecords into an HFile. The record's key is used as the key and the
 * AVRO encoded record bytes are saved as the value.
 * <p>
 * Limitations (compared to columnar formats like Parquet or ORC):
 * 1. Records should be added in order of keys
 * 2. There are no column stats
 */
public class HoodieAvroHFileWriter
    implements HoodieAvroFileWriter {
  private static final AtomicLong RECORD_INDEX_COUNT = new AtomicLong(1);
  private static final Logger LOG = LoggerFactory.getLogger(HoodieAvroHFileWriter.class);
  private final Path file;
  private final HoodieHFileConfig hfileConfig;
  private final boolean isWrapperFileSystem;
  private final Option<HoodieWrapperFileSystem> wrapperFs;
  private final long maxFileSize;
  private final String instantTime;
  private final TaskContextSupplier taskContextSupplier;
  private final boolean populateMetaFields;
  private final Option<Schema.Field> keyFieldSchema;
  private HFileWriter nativeWriter;
  private String minRecordKey;
  private String maxRecordKey;
  private String prevRecordKey;

  // This is private in CacheConfig so have been copied here.
  private static final String DROP_BEHIND_CACHE_COMPACTION_KEY = "hbase.hfile.drop.behind.compaction";

  public HoodieAvroHFileWriter(String instantTime, StoragePath file, HoodieHFileConfig hfileConfig, Schema schema,
                               TaskContextSupplier taskContextSupplier, boolean populateMetaFields) throws IOException {

    Configuration conf = HadoopFSUtils.registerFileSystem(file, hfileConfig.getHadoopConf());
    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, conf);
    FileSystem fs = this.file.getFileSystem(conf);
    this.isWrapperFileSystem = fs instanceof HoodieWrapperFileSystem;
    this.wrapperFs = this.isWrapperFileSystem ? Option.of((HoodieWrapperFileSystem) fs) : Option.empty();
    this.hfileConfig = hfileConfig;
    this.keyFieldSchema = Option.ofNullable(schema.getField(hfileConfig.getKeyFieldName()));

    // TODO - compute this compression ratio dynamically by looking at the bytes written to the
    // stream and the actual file size reported by HDFS
    // this.maxFileSize = hfileConfig.getMaxFileSize()
    //    + Math.round(hfileConfig.getMaxFileSize() * hfileConfig.getCompressionRatio());
    this.maxFileSize = hfileConfig.getMaxFileSize();
    this.instantTime = instantTime;
    this.taskContextSupplier = taskContextSupplier;
    this.populateMetaFields = populateMetaFields;

    HFileContext context = new HFileContext.Builder()
        .withBlockSize(hfileConfig.getBlockSize())
        .withCompressionCodec(hfileConfig.getCompressionCodec())
        .build();

    conf.set(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY,
        String.valueOf(hfileConfig.shouldPrefetchBlocksOnOpen()));
    conf.set(HColumnDescriptor.CACHE_DATA_IN_L1, String.valueOf(hfileConfig.shouldCacheDataInL1()));
    conf.set(DROP_BEHIND_CACHE_COMPACTION_KEY,
        String.valueOf(hfileConfig.shouldDropBehindCacheCompaction()));

    FsPermission fsPermission = FsPermission.getFileDefault();
    FSDataOutputStream outputStream = create(fs, this.file, fsPermission, false);
    this.nativeWriter = new HFileWriterImpl(context, outputStream);
    this.nativeWriter.appendFileInfo(
        HoodieAvroHFileReaderImplBase.SCHEMA_KEY,
        getUTF8Bytes(schema.toString()));
    this.prevRecordKey = "";
  }

  @Override
  public void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException {
    if (populateMetaFields) {
      prepRecordWithMetadata(key, avroRecord, instantTime,
          taskContextSupplier.getPartitionIdSupplier().get(), RECORD_INDEX_COUNT.getAndIncrement(), file.getName());
      writeAvro(key.getRecordKey(), avroRecord);
    } else {
      writeAvro(key.getRecordKey(), avroRecord);
    }
  }

  @Override
  public boolean canWrite() {
    return !isWrapperFileSystem || wrapperFs.get().getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String recordKey, IndexedRecord record) throws IOException {
    if (prevRecordKey.equals(recordKey)) {
      throw new HoodieDuplicateKeyException("Duplicate recordKey " + recordKey + " found while writing to HFile."
          + "Record payload: " + record);
    }
    byte[] value = null;
    boolean isRecordSerialized = false;
    if (keyFieldSchema.isPresent()) {
      GenericRecord keyExcludedRecord = (GenericRecord) record;
      int keyFieldPos = this.keyFieldSchema.get().pos();
      boolean isKeyAvailable = (record.get(keyFieldPos) != null && !(record.get(keyFieldPos).toString().isEmpty()));
      if (isKeyAvailable) {
        Object originalKey = keyExcludedRecord.get(keyFieldPos);
        keyExcludedRecord.put(keyFieldPos, EMPTY_STRING);
        value = HoodieAvroUtils.avroToBytes(keyExcludedRecord);
        keyExcludedRecord.put(keyFieldPos, originalKey);
        isRecordSerialized = true;
      }
    }
    if (!isRecordSerialized) {
      value = HoodieAvroUtils.avroToBytes((GenericRecord) record);
    }
    nativeWriter.append(getUTF8Bytes(recordKey), value);

    if (hfileConfig.useBloomFilter()) {
      hfileConfig.getBloomFilter().add(recordKey);
      if (minRecordKey == null) {
        minRecordKey = recordKey;
      }
      maxRecordKey = recordKey;
    }
    prevRecordKey = recordKey;
  }

  @Override
  public void close() throws IOException {
    if (hfileConfig.useBloomFilter()) {
      final BloomFilter bloomFilter = hfileConfig.getBloomFilter();
      if (minRecordKey == null) {
        minRecordKey = "";
      }
      if (maxRecordKey == null) {
        maxRecordKey = "";
      }
      nativeWriter.appendFileInfo(
          HoodieAvroHFileReaderImplBase.KEY_MIN_RECORD, getUTF8Bytes(minRecordKey));
      nativeWriter.appendFileInfo(
          HoodieAvroHFileReaderImplBase.KEY_MAX_RECORD, getUTF8Bytes(maxRecordKey));
      nativeWriter.appendFileInfo(
          HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_TYPE_CODE,
          getUTF8Bytes(bloomFilter.getBloomFilterTypeCode().toString()));
      nativeWriter.appendMetaInfo(
          HoodieAvroHFileReaderImplBase.KEY_BLOOM_FILTER_META_BLOCK,
          getUTF8Bytes(bloomFilter.serializeToString()));
    }

    nativeWriter.close();
    nativeWriter = null;
  }

  /**
   * Create the specified file on the filesystem. By default, this will:
   * <ol>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or 4096 if not set)</li>
   * <li>use the default replication</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   * @param fs        {@link FileSystem} on which to write the file
   * @param path      {@link Path} to the file to write
   * @param perm      intial permissions
   * @param overwrite Whether or not the created file should be overwritten.
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(FileSystem fs, Path path, FsPermission perm,
                                          boolean overwrite) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Creating file={} with permission={}, overwrite={}", path, perm, overwrite);
    }
    return fs.create(path, perm, overwrite, getDefaultBufferSize(fs),
        getDefaultReplication(fs, path), getDefaultBlockSize(fs, path), null);
  }

  /**
   * Returns the default buffer size to use during writes. The size of the buffer should probably be
   * a multiple of hardware page size (4096 on Intel x86), and it determines how much data is
   * buffered during read and write operations.
   * @param fs filesystem object
   * @return default buffer size to use during writes
   */
  public static int getDefaultBufferSize(final FileSystem fs) {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  /**
   * Get the default replication.
   * @param fs filesystem object
   * @param path path of file
   * @return default replication for the path's filesystem
   */
  public static short getDefaultReplication(final FileSystem fs, final Path path) {
    return fs.getDefaultReplication(path);
  }

  /**
   * Return the number of bytes that large input files should be optimally be split into to minimize
   * i/o time.
   * @param fs filesystem object
   * @return the default block size for the path's filesystem
   */
  public static long getDefaultBlockSize(final FileSystem fs, final Path path) {
    return fs.getDefaultBlockSize(path);
  }
}
