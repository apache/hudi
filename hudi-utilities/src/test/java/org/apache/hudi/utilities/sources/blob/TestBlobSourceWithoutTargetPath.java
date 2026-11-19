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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** The single-table streamer does not put its target path into the properties it builds. */
public class TestBlobSourceWithoutTargetPath {

  /** The single-table streamer does not put its target path into the properties. */
  @Test
  public void streamerDoesNotSetTheTargetBasePathProperty() {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = "file:///data/blob_table";
    cfg.tableType = "COPY_ON_WRITE";
    TypedProperties combined = HoodieStreamer.combineProperties(cfg, Option.empty(), new Configuration());
    assertFalse(combined.containsKey("hoodie.streamer.ingestion.targetBasePath"), combined.toString());
  }
}
