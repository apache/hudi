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
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;
import org.apache.hudi.utilities.sources.helpers.unstructured.UnstructuredFileRecordBuilder;
import org.apache.hudi.utilities.sources.helpers.unstructured.UnstructuredFileRows;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
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

  /** Retained for callers that referenced the schema on this class. */
  public static final StructType SOURCE_SCHEMA = UnstructuredFileRows.SOURCE_SCHEMA;

  private final DFSPathSelector pathSelector;
  private final UnstructuredFileRecordBuilder recordBuilder;
  private final Set<String> allowedExtensions;
  private final Set<String> ignoredExtensions;
  private final int listingParallelism;

  public UnstructuredFileDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = DFSPathSelector.createSourceSelector(props, sparkContext.hadoopConfiguration());
    this.recordBuilder = new UnstructuredFileRecordBuilder(props);
    this.allowedExtensions = UnstructuredFileRows.parseExtensions(getStringWithAltKeys(props, FILE_EXTENSIONS, true));
    this.ignoredExtensions = UnstructuredFileRows.parseExtensions(getStringWithAltKeys(props, FILE_EXTENSIONS_IGNORE, true));
    int configuredParallelism = getIntWithAltKeys(props, LISTING_PARALLELISM);
    this.listingParallelism = configuredParallelism > 0
        ? configuredParallelism : sparkContext.defaultParallelism();
  }

  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    Pair<Option<String>, Checkpoint> selected =
        pathSelector.getNextFilePathsAndMaxModificationTime(sparkContext, lastCheckpoint, sourceLimit);
    return selected.getLeft()
        .map(pathStr -> Pair.of(Option.of(fromFiles(pathStr)), selected.getRight()))
        .orElseGet(() -> Pair.of(Option.empty(), selected.getRight()));
  }

  private Dataset<Row> fromFiles(String pathStr) {
    List<CloudObjectMetadata> objects = Arrays.stream(pathStr.split(","))
        .filter(p -> UnstructuredFileRows.isEligible(new Path(p).getName(), allowedExtensions, ignoredExtensions))
        // listing gives no usable size here, so the row builder interrogates each file as before
        .map(p -> new CloudObjectMetadata(p, 0L))
        .collect(Collectors.toList());
    return UnstructuredFileRows.toDataset(sparkSession, sparkContext, objects, recordBuilder, listingParallelism);
  }
}
