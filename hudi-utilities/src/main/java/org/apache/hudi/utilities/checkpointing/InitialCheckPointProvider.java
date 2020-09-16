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

package org.apache.hudi.utilities.checkpointing;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;

/**
 * Provide the initial checkpoint for delta streamer.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class InitialCheckPointProvider {
  protected transient Path path;
  protected transient FileSystem fs;
  protected transient TypedProperties props;

  static class Config {
    private static String CHECKPOINT_PROVIDER_PATH_PROP = "hoodie.deltastreamer.checkpoint.provider.path";
  }

  /**
   * Construct InitialCheckPointProvider.
   * @param props All properties passed to Delta Streamer
   */
  public InitialCheckPointProvider(TypedProperties props) {
    this.props = props;
    this.path = new Path(props.getString(Config.CHECKPOINT_PROVIDER_PATH_PROP));
  }

  /**
   * Initialize the class with the current filesystem.
   *
   * @param config Hadoop configuration
   */
  public void init(Configuration config) throws HoodieException {
    try {
      this.fs = FileSystem.get(config);
    } catch (IOException e) {
      throw new HoodieException("CheckpointProvider initialization failed");
    }
  }

  /**
   * Get checkpoint string recognizable for delta streamer.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract String getCheckpoint() throws HoodieException;
}
