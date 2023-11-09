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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

/**
 * An abstract reader context class for {@code HoodieFileGroupReader} to use, containing APIs for
 * engine-specific implementation on reading data files, getting field values from a record,
 * transforming a record, etc.
 * <p>
 * For each query engine, this class should be extended and plugged into {@code HoodieFileGroupReader}
 * to realize the file group reading.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow} in Spark
 *            and {@code RowData} in Flink.
 */
public abstract class HoodieReaderContext<T> {
  // These internal key names are only used in memory for record metadata and merging,
  // and should not be persisted to storage.
  public static final String INTERNAL_META_RECORD_KEY = "_0";
  public static final String INTERNAL_META_PARTITION_PATH = "_1";
  public static final String INTERNAL_META_ORDERING_FIELD = "_2";
  public static final String INTERNAL_META_OPERATION = "_3";
  public static final String INTERNAL_META_INSTANT_TIME = "_4";
  public static final String INTERNAL_META_SCHEMA = "_5";

  /**
   * Gets the file system based on the file path and configuration.
   *
   * @param path File path to get the file system.
   * @param conf Hadoop {@link Configuration} instance.
   * @return The {@link FileSystem} instance to use.
   */
  public abstract FileSystem getFs(String path, Configuration conf);

  /**
   * Gets the record iterator based on the type of engine-specific record representation from the
   * file.
   *
   * @param filePath       {@link Path} instance of a file.
   * @param start          Starting byte to start reading.
   * @param length         Bytes to read.
   * @param dataSchema     Schema of records in the file in {@link Schema}.
   * @param requiredSchema Schema containing required fields to read in {@link Schema} for projection.
   * @param conf           {@link Configuration} for reading records.
   * @return {@link ClosableIterator<T>} that can return all records through iteration.
   */
  public abstract ClosableIterator<T> getFileRecordIterator(
      Path filePath, long start, long length, Schema dataSchema, Schema requiredSchema, Configuration conf);

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an engine-specific record.
   *
   * @param avroRecord The Avro record.
   * @return An engine-specific record in Type {@link T}.
   */
  public abstract T convertAvroRecord(IndexedRecord avroRecord);

  /**
   * @param mergerStrategy Merger strategy UUID.
   * @return {@link HoodieRecordMerger} to use.
   */
  public abstract HoodieRecordMerger getRecordMerger(String mergerStrategy);

  /**
   * Gets the field value.
   *
   * @param record    The record in engine-specific type.
   * @param schema    The Avro schema of the record.
   * @param fieldName The field name.
   * @return The field value.
   */
  public abstract Object getValue(T record, Schema schema, String fieldName);

  /**
   * Gets the record key in String.
   *
   * @param record The record in engine-specific type.
   * @param schema The Avro schema of the record.
   * @return The record key in String.
   */
  public abstract String getRecordKey(T record, Schema schema);

  /**
   * Gets the ordering value in particular type.
   *
   * @param recordOption An option of record.
   * @param metadataMap  A map containing the record metadata.
   * @param schema       The Avro schema of the record.
   * @param props        Properties.
   * @return The ordering value.
   */
  public abstract Comparable getOrderingValue(Option<T> recordOption,
                                              Map<String, Object> metadataMap,
                                              Schema schema,
                                              TypedProperties props);

  /**
   * Constructs a new {@link HoodieRecord} based on the record of engine-specific type and metadata for merging.
   *
   * @param recordOption An option of the record in engine-specific type if exists.
   * @param metadataMap  The record metadata.
   * @return A new instance of {@link HoodieRecord}.
   */
  public abstract HoodieRecord<T> constructHoodieRecord(Option<T> recordOption,
                                                        Map<String, Object> metadataMap);

  /**
   * Seals the engine-specific record to make sure the data referenced in memory do not change.
   *
   * @param record The record.
   * @return The record containing the same data that do not change in memory over time.
   */
  public abstract T seal(T record);

  /**
   * Generates metadata map based on the information.
   *
   * @param recordKey     Record key in String.
   * @param partitionPath Partition path in String.
   * @param orderingVal   Ordering value in String.
   * @return A mapping containing the metadata.
   */
  public Map<String, Object> generateMetadataForRecord(
      String recordKey, String partitionPath, Comparable orderingVal) {
    Map<String, Object> meta = new HashMap<>();
    meta.put(INTERNAL_META_RECORD_KEY, recordKey);
    meta.put(INTERNAL_META_PARTITION_PATH, partitionPath);
    meta.put(INTERNAL_META_ORDERING_FIELD, orderingVal);
    return meta;
  }

  /**
   * Generates metadata of the record. Only fetches record key that is necessary for merging.
   *
   * @param record The record.
   * @param schema The Avro schema of the record.
   * @return A mapping containing the metadata.
   */
  public Map<String, Object> generateMetadataForRecord(T record, Schema schema) {
    Map<String, Object> meta = new HashMap<>();
    meta.put(INTERNAL_META_RECORD_KEY, getRecordKey(record, schema));
    meta.put(INTERNAL_META_SCHEMA, schema);
    return meta;
  }

  /**
   * Updates the schema and reset the ordering value in existing metadata mapping of a record.
   *
   * @param meta   Metadata in a mapping.
   * @param schema New schema to set.
   * @return The input metadata mapping.
   */
  public Map<String, Object> updateSchemaAndResetOrderingValInMetadata(Map<String, Object> meta,
                                                                       Schema schema) {
    meta.remove(INTERNAL_META_ORDERING_FIELD);
    meta.put(INTERNAL_META_SCHEMA, schema);
    return meta;
  }
}
