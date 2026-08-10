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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * HoodieDeltaStreamer is renamed to {@link HoodieStreamer}.
 * Please use {@link HoodieStreamer} instead.
 */
@Deprecated
public class HoodieDeltaStreamer extends HoodieStreamer {
  public HoodieDeltaStreamer(Config cfg,
                             JavaSparkContext jssc) throws IOException {
    super(cfg, jssc);
  }

  public HoodieDeltaStreamer(Config cfg,
                             JavaSparkContext jssc,
                             Option<TypedProperties> props) throws IOException {
    super(cfg, jssc, props);
  }

  public HoodieDeltaStreamer(Config cfg,
                             JavaSparkContext jssc,
                             FileSystem fs,
                             Configuration conf) throws IOException {
    super(cfg, jssc, fs, conf);
  }

  public HoodieDeltaStreamer(Config cfg,
                             JavaSparkContext jssc,
                             FileSystem fs,
                             Configuration conf,
                             Option<TypedProperties> propsOverride) throws IOException {
    super(cfg, jssc, fs, conf, propsOverride);
  }

  @Deprecated
  public static class Config extends HoodieStreamer.Config {
  }
}
