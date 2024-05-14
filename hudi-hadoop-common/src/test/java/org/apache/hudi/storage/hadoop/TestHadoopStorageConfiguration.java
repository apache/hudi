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

package org.apache.hudi.storage.hadoop;

import org.apache.hudi.io.storage.BaseTestStorageConfiguration;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Tests {@link HadoopStorageConfiguration}.
 */
public class TestHadoopStorageConfiguration extends BaseTestStorageConfiguration<Configuration> {
  @Override
  protected StorageConfiguration<Configuration> getStorageConfiguration(Configuration conf) {
    return new HadoopStorageConfiguration(conf);
  }

  @Override
  protected Configuration getConf(Map<String, String> mapping) {
    Configuration conf = new Configuration();
    mapping.forEach(conf::set);
    return conf;
  }
}
