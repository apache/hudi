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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.unstructured.UnstructuredFileRecordBuilder;
import org.apache.hudi.utilities.sources.helpers.unstructured.UnstructuredFileRows;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.FILE_EXTENSIONS;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.FILE_EXTENSIONS_IGNORE;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.LISTING_PARALLELISM;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.PARSE_MAX_BYTES;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.WORK_BYTES_PER_PARTITION;

/**
 * Reads cloud objects as unstructured files: each object becomes one row carrying a BLOB column
 * plus extracted text, metadata and chunks, the same shape the directory-listing source produces.
 *
 * <p>Differs from a columnar read in all three of its responsibilities. Objects are selected by
 * document extension rather than by data-file format. Partitions are sized by the bytes that will
 * actually be parsed, because an object above {@code parse.max.bytes} is referenced without being
 * read and so costs almost nothing. And rows are built directly rather than through a Spark
 * datasource.
 */
public class UnstructuredFileMaterializer implements CloudObjectMaterializer {

  private static final long serialVersionUID = 1L;

  private final Set<String> allowedExtensions;
  private final Set<String> ignoredExtensions;
  private final long parseMaxBytes;
  private final long workBytesPerPartition;
  private final int configuredParallelism;
  private final int defaultParallelism;
  private final UnstructuredFileRecordBuilder recordBuilder;

  public UnstructuredFileMaterializer(TypedProperties props, JavaSparkContext jsc) {
    this.allowedExtensions = UnstructuredFileRows.parseExtensions(getStringWithAltKeys(props, FILE_EXTENSIONS, true));
    this.ignoredExtensions = UnstructuredFileRows.parseExtensions(getStringWithAltKeys(props, FILE_EXTENSIONS_IGNORE, true));
    this.parseMaxBytes = getLongWithAltKeys(props, PARSE_MAX_BYTES);
    this.workBytesPerPartition = getLongWithAltKeys(props, WORK_BYTES_PER_PARTITION);
    this.configuredParallelism = getIntWithAltKeys(props, LISTING_PARALLELISM);
    this.defaultParallelism = jsc.defaultParallelism();
    this.recordBuilder = new UnstructuredFileRecordBuilder(props);
  }

  @Override
  public String objectKeyPredicate(String objectKey, TypedProperties props) {
    if (!allowedExtensions.isEmpty()) {
      return CloudObjectsSelectorCommon.extensionPredicate(objectKey, String.join(",", allowedExtensions));
    }
    if (ignoredExtensions.isEmpty()) {
      return "";
    }
    // no allowlist, so select everything except the denied extensions
    List<String> denials = new ArrayList<>();
    for (String extension : ignoredExtensions) {
      denials.add(String.format("%s not like '%%%s'", objectKey, extension));
    }
    return " and " + String.join(" and ", denials);
  }

  @Override
  public int partitionCount(List<CloudObjectMetadata> objects, long bytesPerPartition, int minPartitions) {
    long parseableBytes = 0;
    for (CloudObjectMetadata object : objects) {
      if (object.getSize() <= parseMaxBytes) {
        parseableBytes += object.getSize();
      }
    }
    int byWork = (int) Math.max(1, Math.ceil((double) parseableBytes / workBytesPerPartition));
    // a batch of nothing but unparseable objects still has per-object work, so keep the cluster busy
    int floor = configuredParallelism > 0 ? configuredParallelism : defaultParallelism;
    int partitions = Math.max(byWork, Math.min(floor, Math.max(objects.size(), 1)));
    return Math.max(partitions, minPartitions);
  }

  @Override
  public Option<Dataset<Row>> materialize(SparkSession spark,
                                          List<CloudObjectMetadata> objects,
                                          Option<SchemaProvider> schemaProvider,
                                          int numPartitions) {
    List<CloudObjectMetadata> selected = new ArrayList<>();
    for (CloudObjectMetadata object : objects) {
      if (UnstructuredFileRows.isEligible(fileNameOf(object.getPath()), allowedExtensions, ignoredExtensions)) {
        selected.add(object);
      }
    }
    if (selected.isEmpty()) {
      return Option.empty();
    }
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    return Option.of(UnstructuredFileRows.toDataset(spark, jsc, selected, recordBuilder, numPartitions));
  }

  private static String fileNameOf(String path) {
    int slash = path.lastIndexOf('/');
    return slash < 0 ? path : path.substring(slash + 1);
  }
}
