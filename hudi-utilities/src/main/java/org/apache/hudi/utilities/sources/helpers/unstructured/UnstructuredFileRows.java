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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.collection.LazyIterableIterator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The row shape unstructured file ingestion produces, and the conversion from a list of files to
 * that shape. Shared so that discovery by directory listing and discovery by cloud notification
 * emit identical tables: the blob placement and parse handling exist once, not once per source.
 */
public final class UnstructuredFileRows {

  private static final Metadata BLOB_METADATA = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.BLOB.name())
      .build();

  private static final StructType BLOB_REFERENCE_TYPE = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH, DataTypes.StringType, true),
      DataTypes.createStructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_OFFSET, DataTypes.LongType, true),
      DataTypes.createStructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_LENGTH, DataTypes.LongType, true),
      DataTypes.createStructField(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED, DataTypes.BooleanType, true)});

  private static final StructType BLOB_TYPE = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(HoodieSchema.Blob.TYPE, DataTypes.StringType, false),
      DataTypes.createStructField(HoodieSchema.Blob.INLINE_DATA_FIELD, DataTypes.BinaryType, true),
      DataTypes.createStructField(HoodieSchema.Blob.EXTERNAL_REFERENCE, BLOB_REFERENCE_TYPE, true)});

  private static final StructType CHUNK_TYPE = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("chunk_id", DataTypes.IntegerType, false),
      DataTypes.createStructField("text", DataTypes.StringType, false),
      DataTypes.createStructField("char_start", DataTypes.IntegerType, false)});

  public static final StructType SOURCE_SCHEMA = new StructType(new StructField[] {
      DataTypes.createStructField("path", DataTypes.StringType, false),
      DataTypes.createStructField("file_name", DataTypes.StringType, false),
      DataTypes.createStructField("extension", DataTypes.StringType, false),
      DataTypes.createStructField("size", DataTypes.LongType, false),
      DataTypes.createStructField("modification_time", DataTypes.LongType, false),
      new StructField("content", BLOB_TYPE, false, BLOB_METADATA),
      DataTypes.createStructField("extracted_text", DataTypes.StringType, true),
      DataTypes.createStructField("doc_metadata",
          DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true),
      DataTypes.createStructField("chunks", DataTypes.createArrayType(CHUNK_TYPE, false), true),
      DataTypes.createStructField("parse_status", DataTypes.StringType, false),
      DataTypes.createStructField("parse_error", DataTypes.StringType, true)});

  private UnstructuredFileRows() {
  }

  /**
   * Parses a comma separated extension list into a lowercase set, empty when nothing is configured.
   */
  public static Set<String> parseExtensions(String csv) {
    return csv == null || csv.trim().isEmpty()
        ? Collections.emptySet()
        : Arrays.stream(csv.toLowerCase(Locale.ROOT).split(","))
            .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Whether a file is selected: an explicit allowlist decides alone, otherwise everything except
   * the denylist.
   */
  public static boolean isEligible(String fileName, Set<String> allowed, Set<String> ignored) {
    String extension = UnstructuredFileRecordBuilder.extensionOf(fileName);
    return allowed.isEmpty() ? !ignored.contains(extension) : allowed.contains(extension);
  }

  /**
   * One file to one row, over {@code parallelism} Spark partitions. Rows are produced lazily so
   * the inline bytes and extracted text of a partition are never all resident at once, which keeps
   * task memory independent of the size of the batch.
   */
  public static Dataset<Row> toDataset(SparkSession spark, JavaSparkContext jsc, List<CloudObjectMetadata> objects,
                                       UnstructuredFileRecordBuilder builder, int parallelism) {
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(jsc.hadoopConfiguration());
    int partitions = Math.max(1, Math.min(objects.size(), parallelism));
    JavaRDD<Row> rows = jsc.parallelize(objects, partitions).mapPartitions(objectIterator ->
        new LazyIterableIterator<CloudObjectMetadata, Row>(objectIterator) {
          private FileSystem fs;

          @Override
          protected Row computeNext() {
            CloudObjectMetadata object = inputItr.next();
            try {
              if (fs == null) {
                fs = HadoopFSUtils.getFs(new Path(object.getPath()), storageConf);
              }
              return builder.buildRow(fs, object);
            } catch (IOException e) {
              throw new UncheckedIOException("Failed to build record for " + object.getPath(), e);
            }
          }
        });
    return spark.createDataFrame(rows, SOURCE_SCHEMA);
  }
}
