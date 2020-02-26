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
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for all the built-in key generators. Contains methods structured for
 * code reuse amongst them.
 */
public abstract class BuiltinKeyGenerator extends KeyGenerator {

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  /**
   * Generate a record Key out of provided generic record.
   */
  public abstract String getRecordKey(GenericRecord record);

  /**
   * Generate a partition path out of provided generic record.
   */
  public abstract String getPartitionPath(GenericRecord record);

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public final HoodieKey getKey(GenericRecord record) {
    if (getRecordKeyFields() == null || getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }
    return new HoodieKey(getRecordKey(record), getPartitionPath(record));
  }

  /**
   * Return fields that constitute record key. Used by Metadata bootstrap.
   * Have a base implementation inorder to prevent forcing custom KeyGenerator implementation
   * to implement this method
   * @return list of record key fields
   */
  public List<String> getRecordKeyFields() {
    throw new IllegalStateException("This method is expected to be overridden by subclasses");
  }

  /**
   * Return fields that constiture partition path. Used by Metadata bootstrap.
   * Have a base implementation inorder to prevent forcing custom KeyGenerator implementation
   * to implement this method
   * @return list of partition path fields
   */
  public List<String> getPartitionPathFields() {
    throw new IllegalStateException("This method is expected to be overridden by subclasses");
  }

  @Override
  public final List<String> getRecordKeyFieldNames() {
    // For nested columns, pick top level column name
    return getRecordKeyFields().stream().map(k -> {
      int idx = k.indexOf('.');
      return idx > 0 ? k.substring(0, idx) : k;
    }).collect(Collectors.toList());
  }
}
