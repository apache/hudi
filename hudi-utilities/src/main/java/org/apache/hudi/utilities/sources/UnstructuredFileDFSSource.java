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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.LazyIterableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.unstructured.UnstructuredFilePathSelector;
import org.apache.hudi.utilities.sources.helpers.unstructured.UnstructuredFileRecordBuilder;

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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.FILE_EXTENSIONS;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.FILE_EXTENSIONS_IGNORE;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.LISTING_PARALLELISM;

/**
 * DFS source that ingests unstructured files (documents, images, videos) as rows carrying a
 * BLOB-typed column plus extracted text, metadata and chunks.
 *
 * <p>File discovery and checkpointing reuse {@link DFSPathSelector} (modification-time based,
 * incremental). Per file, blob placement is decided by size: files at or below
 * {@code hoodie.streamer.source.unstructured.blob.inline.max.bytes} are stored INLINE (bytes in
 * the table), larger files are stored OUT_OF_LINE as a reference to the original file in place —
 * their bytes never enter Spark rows, keeping memory and shuffle volume bounded regardless of
 * file sizes. Text extraction runs embedded in the executors through a pluggable
 * {@code DocumentParser} (Apache Tika by default); parse failures are recorded per row and never
 * fail the ingestion job.
 *
 * <p>Keying the table on {@code path} with ordering on {@code modification_time} makes
 * re-ingested files upsert in place, so downstream text (and any embedding columns added by
 * transformers) stay current with the source directory.
 */
public class UnstructuredFileDFSSource extends RowSource {

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

  private final UnstructuredFilePathSelector pathSelector;
  private final UnstructuredFileRecordBuilder recordBuilder;
  private final Set<String> allowedExtensions;
  private final Set<String> ignoredExtensions;
  private final int listingParallelism;

  public UnstructuredFileDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = new UnstructuredFilePathSelector(props, sparkContext.hadoopConfiguration());
    this.recordBuilder = new UnstructuredFileRecordBuilder(props);
    this.allowedExtensions = parseExtensions(getStringWithAltKeys(props, FILE_EXTENSIONS, true));
    this.ignoredExtensions = parseExtensions(getStringWithAltKeys(props, FILE_EXTENSIONS_IGNORE, true));
    int configuredParallelism = getIntWithAltKeys(props, LISTING_PARALLELISM);
    this.listingParallelism = configuredParallelism > 0
        ? configuredParallelism : sparkContext.defaultParallelism();
  }

  private static Set<String> parseExtensions(String csv) {
    return csv == null || csv.trim().isEmpty()
        ? new HashSet<>()
        : Arrays.stream(csv.toLowerCase(Locale.ROOT).split(","))
            .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
  }

  private boolean isEligible(String fileName) {
    String extension = UnstructuredFileRecordBuilder.extensionOf(fileName);
    // an explicit allowlist decides alone; otherwise everything except the denylist
    return allowedExtensions.isEmpty()
        ? !ignoredExtensions.contains(extension) : allowedExtensions.contains(extension);
  }

  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    UnstructuredFilePathSelector.Batch batch = pathSelector.selectNextBatch(lastCheckpoint, sourceLimit);
    List<UnstructuredFilePathSelector.FileEntry> eligible = batch.files.stream()
        .filter(entry -> isEligible(new Path(entry.path).getName()))
        .collect(Collectors.toList());
    return eligible.isEmpty()
        ? Pair.of(Option.empty(), batch.checkpoint)
        : Pair.of(Option.of(fromFiles(eligible)), batch.checkpoint);
  }

  private Dataset<Row> fromFiles(List<UnstructuredFilePathSelector.FileEntry> entries) {
    int parallelism = Math.max(1, Math.min(entries.size(), listingParallelism));
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(sparkContext.hadoopConfiguration());
    UnstructuredFileRecordBuilder builder = this.recordBuilder;
    // one file -> one row, lazily: inline bytes and extracted text of a partition must
    // never be resident all at once, or partition memory scales with total corpus size
    JavaRDD<Row> rows = sparkContext.parallelize(entries, parallelism).mapPartitions(entryIterator ->
        new LazyIterableIterator<UnstructuredFilePathSelector.FileEntry, Row>(entryIterator) {
          private FileSystem fs;

          @Override
          protected Row computeNext() {
            UnstructuredFilePathSelector.FileEntry entry = inputItr.next();
            try {
              if (fs == null) {
                fs = HadoopFSUtils.getFs(new Path(entry.path), storageConf);
              }
              return builder.buildRow(fs, entry);
            } catch (IOException e) {
              throw new UncheckedIOException("Failed to build record for " + entry.path, e);
            }
          }
        });
    return sparkSession.createDataFrame(rows, SOURCE_SCHEMA);
  }
}
