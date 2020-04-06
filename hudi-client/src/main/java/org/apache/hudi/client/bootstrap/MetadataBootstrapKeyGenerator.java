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

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.client.bootstrap.translator.MetadataBootstrapPartitionPathTranslator;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.KeyGenerator;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.List;

public class MetadataBootstrapKeyGenerator implements Serializable {

  private final HoodieWriteConfig writeConfig;
  private final KeyGenerator keyGenerator;
  private final MetadataBootstrapPartitionPathTranslator translator;

  public MetadataBootstrapKeyGenerator(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
    TypedProperties properties = new TypedProperties();
    properties.putAll(writeConfig.getProps());
    this.keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(writeConfig.getBootstrapKeyGeneratorClass(),
        properties);
    this.translator = (MetadataBootstrapPartitionPathTranslator) ReflectionUtils.loadClass(
        writeConfig.getBootstrapPartitionPathTranslatorClass(), properties);
  }

  /**
   * Returns record key from generic record.
   */
  public String getRecordKey(GenericRecord record) {
    return keyGenerator.getRecordKey(record);
  }

  /**
   * Get Top Level record key columns needed for projection.
   * @return List of top level columns.
   */
  public List<String> getTopLevelRecordKeyFields() {
    return keyGenerator.getTopLevelRecordKeyFields();
  }

  /**
   * Allow users to change partitioning style while doing metadata bootstrap.
   * @param originalPath Original Partition Path
   * @return
   */
  public String getTranslatedPath(String originalPath) {
    return translator.getBootstrapTranslatedPath(originalPath);
  }
}