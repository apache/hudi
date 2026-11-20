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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.blob.BlobRowBuilder;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMaterializer;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.ColumnarFileMaterializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.split;

/**
 * Materializes selected raw objects as inline bytes or unmanaged references to the objects where they are, one row per object path. Stock cloud
 * selection, existence checks and checkpoints stay in the fetcher; the fetcher's deduplication keeps one entry per (path, size),
 * so this materializer collapses the selection to one entry per path ({@link #onePerPath}) before building rows. Each row
 * reads its object's status when it is built, so the batch holds one source version per path and downstream jobs do not
 * deduplicate.
 */
@Slf4j
public class UnstructuredObjectMaterializer implements CloudObjectMaterializer {

  private static final long serialVersionUID = 1L;
  /** Inflation of inline bytes for the row and meta-field overhead, as {@link ColumnarFileMaterializer} applies. */
  private static final double ROW_OVERHEAD = 1.1;
  /** Orders one path's entries latest first by ordering token; an entry without one sorts last. */
  private static final Comparator<CloudObjectMetadata> LATEST_FIRST =
      Comparator.comparing(CloudObjectMetadata::getOrderingToken, Comparator.nullsFirst(Comparator.<String>naturalOrder()))
          .reversed();

  private final TypedProperties props;
  private final transient JavaSparkContext jsc;
  private final List<String> extensions;
  private final long inlineMaxBytes;

  /** Creates the byte materializer. */
  public UnstructuredObjectMaterializer(TypedProperties props, JavaSparkContext jsc) {
    this.props = props;
    this.jsc = jsc;
    this.extensions = UnstructuredSourcePolicy.fileExtensions(props);
    this.inlineMaxBytes = BlobTableConfig.inlineMaxBytes(props);
  }

  @Override
  public String objectKeyPredicate(String objectKey, TypedProperties props) {
    if (extensions.isEmpty()) {
      return " and false";
    }
    // The dot keeps "pdf" from matching a key that merely ends in those letters; LIKE is case-sensitive.
    return " and (" + extensions.stream().map(extension -> objectKey + " like '%." + extension + "'")
        .collect(Collectors.joining(" or ")) + ")";
  }

  /**
   * {@code max(ceil(inlineBytes x 1.1 / bytesPerPartition), min(outOfLineCount, defaultParallelism), minPartitions, 1)}.
   * Only inline bytes become row data, so they are sized like a columnar scan (about one Parquet file of rows per task); an
   * out-of-line object puts only a reference into its row and costs its status call, and each gets its own task up to the
   * task slots. Counted over one entry per path ({@link #onePerPath}), the entries {@link #materialize} builds rows from.
   *
   * @throws IllegalArgumentException if {@code bytesPerPartition} is not positive: zero would ask for Integer.MAX_VALUE
   *     partitions and a negative target sizes nothing
   */
  @Override
  public int partitionCount(List<CloudObjectMetadata> objects, long bytesPerPartition, int minPartitions) {
    ValidationUtils.checkArgument(bytesPerPartition > 0, () -> "Bytes per source partition must be positive, got "
        + bytesPerPartition + "; it is " + CloudSourceConfig.SOURCE_MAX_BYTES_PER_PARTITION.key() + ", or "
        + HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key() + " when that key is unset");
    long inlineBytes = 0;
    int outOfLineCount = 0;
    for (CloudObjectMetadata object : onePerPath(objects)) {
      if (object.getSize() <= inlineMaxBytes) {
        inlineBytes += object.getSize();
      } else {
        outOfLineCount++;
      }
    }
    int byInlineBytes = (int) Math.ceil(inlineBytes * ROW_OVERHEAD / bytesPerPartition);
    int byCopies = Math.min(outOfLineCount, jsc.defaultParallelism());
    return Math.max(Math.max(byInlineBytes, byCopies), Math.max(minPartitions, 1));
  }

  @Override
  public Option<Dataset<Row>> materialize(SparkSession spark, List<CloudObjectMetadata> objects,
                                          Option<SchemaProvider> schemaProvider, int numPartitions) {
    List<CloudObjectMetadata> bySize = onePerPath(objects);
    if (bySize.isEmpty()) {
      return Option.empty();
    }
    if (bySize.size() < objects.size()) {
      log.info("Collapsed {} selected entries to {} object paths, one entry per path", objects.size(), bySize.size());
    }
    bySize.sort(Comparator.comparingLong(CloudObjectMetadata::getSize).reversed());
    BlobRowBuilder rowBuilder = new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));
    // one list per partition: parallelize slices its n elements into the n partitions one each
    JavaRDD<Row> rows = jsc.parallelize(deal(bySize, numPartitions), numPartitions)
        .flatMap(List::iterator)
        .mapPartitions(rowBuilder::build);
    log.info("Materializing {} blob objects over {} partitions", bySize.size(), numPartitions);
    Dataset<Row> dataset = spark.createDataFrame(rows, BlobTableSchema.schema());
    // The same names the source schema contract and the embedding schema declare: trimmed, blank segments dropped.
    for (String partitionKey : PathPartitionFields.configured(props)) {
      // Reserve fixed metadata names ignoring case, including under Spark's default case-insensitive resolver.
      if (Arrays.stream(BlobTableSchema.schema().fieldNames()).noneMatch(name -> name.equalsIgnoreCase(partitionKey))) {
        dataset = dataset.withColumn(partitionKey, split(
            split(dataset.col(BlobTableSchema.SOURCE_FILE_PATH), partitionKey + "=").getItem(1),
            StoragePath.SEPARATOR).getItem(0));
      }
    }
    return Option.of(dataset);
  }

  /**
   * One entry per object path (bucket and key), in a new mutable list. The fetcher keeps one entry per (path, size), so an
   * object written twice with two sizes within one batch reaches here twice. Of a path's entries the one kept has the greatest
   * ordering token (S3's per-object sequencer): the latest write where the events carry one, else the first entry seen. The
   * row built from it reads the object's current status either way.
   */
  @VisibleForTesting
  static List<CloudObjectMetadata> onePerPath(List<CloudObjectMetadata> objects) {
    Map<String, CloudObjectMetadata> byPath = new LinkedHashMap<>();
    for (CloudObjectMetadata object : objects) {
      byPath.merge(object.getPath(), object, (kept, next) -> LATEST_FIRST.compare(next, kept) < 0 ? next : kept);
    }
    return new ArrayList<>(byPath.values());
  }

  /**
   * Deals {@code objects} round-robin into {@code numPartitions} lists: object i goes to list i mod n. Over a list sorted by
   * size descending, the n largest objects go to n different partitions, which contiguous slicing would not do.
   */
  @VisibleForTesting
  static List<List<CloudObjectMetadata>> deal(List<CloudObjectMetadata> objects, int numPartitions) {
    List<List<CloudObjectMetadata>> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(new ArrayList<>());
    }
    for (int i = 0; i < objects.size(); i++) {
      partitions.get(i % numPartitions).add(objects.get(i));
    }
    return partitions;
  }

}
