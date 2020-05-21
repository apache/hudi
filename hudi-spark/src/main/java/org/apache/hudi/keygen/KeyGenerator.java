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

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.List;

/**
 * Abstract class to extend for plugging in extraction of {@link HoodieKey} from an Avro record.
 */
public abstract class KeyGenerator implements Serializable {

  protected List<String> recordKeyFields;

  protected List<String> partitionPathFields;

  protected transient TypedProperties config;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public abstract HoodieKey getKey(GenericRecord record);

  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }
}
