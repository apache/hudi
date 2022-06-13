/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sync.common;

import org.apache.hudi.common.config.TypedProperties;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.Properties;

/**
 * Base class to sync metadata with metastores to make
 * Hudi table queryable through external systems.
 */
public abstract class HoodieSyncTool implements AutoCloseable {

  public final HoodieSyncConfig config;

  public HoodieSyncTool(HoodieSyncConfig config) {
    this.config = config;
  }

  @Deprecated
  public HoodieSyncTool(TypedProperties props, Configuration conf, FileSystem fs) {
    this(new HoodieSyncConfig(props, conf));
  }

  @Deprecated
  public HoodieSyncTool(Properties props, FileSystem fileSystem) {
    this(new HoodieSyncConfig(props, fileSystem.getConf()));
  }

  public abstract void syncHoodieTable();

}
