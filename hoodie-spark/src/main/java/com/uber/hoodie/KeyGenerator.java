/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.util.TypedProperties;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;

/**
 * Abstract class to extend for plugging in extraction of
 * {@link com.uber.hoodie.common.model.HoodieKey}
 * from an Avro record
 */
public abstract class KeyGenerator implements Serializable {

  protected transient TypedProperties config;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public abstract HoodieKey getKey(GenericRecord record);
}
