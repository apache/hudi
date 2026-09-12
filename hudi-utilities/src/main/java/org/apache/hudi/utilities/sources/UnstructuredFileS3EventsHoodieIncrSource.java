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
 * Ingests unstructured files whose arrival is announced by S3 event notifications, rather than
 * discovered by listing a prefix. Discovery, batching and checkpointing are inherited unchanged
 * from {@link S3EventsHoodieIncrSource}; only the step that turns the selected objects into rows
 * differs, and that is supplied by {@link UnstructuredFileMaterializer}.
 *
 * <p>Prefer this over {@code UnstructuredFileDFSSource} for a corpus that keeps growing: listing
 * costs a full walk of the prefix on every sync whether or not anything arrived, while the events
 * cost only what is new.
 */
public class UnstructuredFileS3EventsHoodieIncrSource extends S3EventsHoodieIncrSource {

  public UnstructuredFileS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider,
      HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public UnstructuredFileS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      HoodieIngestionMetrics metrics,
      StreamContext streamContext) {
    super(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkContext, sparkSession, metrics,
            new CloudObjectsSelectorCommon(props),
            new UnstructuredFileMaterializer(props, sparkContext)),
        streamContext);
  }
}
