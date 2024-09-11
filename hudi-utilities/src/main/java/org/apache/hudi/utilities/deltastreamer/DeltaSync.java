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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.StreamSync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Function;

/**
 * DeltaSync is renamed to {@link StreamSync}.
 * Please use {@link StreamSync} instead.
 */
@Deprecated
public class DeltaSync extends StreamSync {
  public DeltaSync(HoodieStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                   TypedProperties props, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                   Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
    super(cfg, sparkSession, schemaProvider, props, jssc, fs, conf, onInitializingHoodieWriteClient);
  }

  public DeltaSync(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                   TypedProperties props, HoodieSparkEngineContext hoodieSparkContext, FileSystem fs, Configuration conf,
                   Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient) throws IOException {
    super(cfg, sparkSession, props, hoodieSparkContext,
        fs, conf, onInitializingHoodieWriteClient, new DefaultStreamContext(schemaProvider, Option.empty()));
  }
}
