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

package org.apache.hudi.utilities.sources.selector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.HoodieDeltaStreamerConfig;

@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class AbstractDFSPathSelector{

  protected final TypedProperties props;
  protected final String inputPath;

  public AbstractDFSPathSelector(TypedProperties props, Configuration hadoopConf) {
    this.inputPath = props.getString(HoodieDeltaStreamerConfig.ROOT_INPUT_PATH_PROP);
    this.props = props;
  }

  public abstract Pair<Option<String>, String>  getNextFilePathsAndMaxModificationTime(Option<String> lastCheckpointStr, long sourceLimit);
}
