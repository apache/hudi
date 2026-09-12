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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.sources.helpers.UnstructuredFileMaterializer;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Ingests unstructured files whose arrival is announced by GCS object notifications, rather than
 * discovered by listing a prefix. Discovery, batching and checkpointing are inherited unchanged
 * from {@link GcsEventsHoodieIncrSource}; only the step that turns the selected objects into rows
 * differs, and that is supplied by {@link UnstructuredFileMaterializer}.
 *
 * <p>The GCS twin of {@link UnstructuredFileS3EventsHoodieIncrSource}. Everything that differs
 * between the two stores already sits behind {@link CloudObjectsSelectorCommon.Type}, so the two
 * classes differ only in which base they extend.
 *
 * <p>Prefer this over {@code UnstructuredFileDFSSource} for a corpus that keeps growing: listing
 * costs a full walk of the prefix on every sync whether or not anything arrived, while the events
 * cost only what is new.
 */
public class UnstructuredFileGcsEventsHoodieIncrSource extends GcsEventsHoodieIncrSource {

  public UnstructuredFileGcsEventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext jsc,
      SparkSession spark,
      SchemaProvider schemaProvider,
      HoodieIngestionMetrics metrics) {
    this(props, jsc, spark, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public UnstructuredFileGcsEventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext jsc,
      SparkSession spark,
      HoodieIngestionMetrics metrics,
      StreamContext streamContext) {
    super(props, jsc, spark,
        new CloudDataFetcher(props, jsc, spark, metrics,
            new CloudObjectsSelectorCommon(props),
            new UnstructuredFileMaterializer(props, jsc)),
        new QueryRunner(spark, props),
        streamContext);
  }
}
